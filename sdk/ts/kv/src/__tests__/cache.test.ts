import { afterEach, describe, expect, it } from "vitest";
import { createCache } from "../cache.js";
import type { KvClient } from "../client.js";
import { httpClient, respClient, uniqueKey } from "./harness.js";

function sleep(ms: number) {
  return new Promise<void>((r) => setTimeout(r, ms));
}

function spyClient(client: KvClient) {
  let count = 0;
  const proxy = new Proxy(client, {
    get(target, prop, receiver) {
      const val = Reflect.get(target, prop, receiver);
      if (prop === "batch") {
        return (...args: unknown[]) => {
          count++;
          return (val as (...a: unknown[]) => unknown).apply(target, args);
        };
      }
      return val;
    },
  });
  return {
    client: proxy as KvClient,
    calls: () => count,
    reset: () => {
      count = 0;
    },
  };
}

// ── Unit ──────────────────────────────────────────────────────────────────────

describe("cache — unit", () => {
  it("TypeScript enforces key at compile time (runtime: key option present)", () => {
    const kv = httpClient();
    const myCache = createCache(kv);
    // key is required by the type system — this verifies the JS shape at runtime
    expect(() =>
      myCache(async function fetchVal() {
        return 1;
      }, { key: "k", ttl: 60 })
    ).not.toThrow();
  });
});

// ── HTTP backend ──────────────────────────────────────────────────────────────

describe("cache — HTTP backend", () => {
  it("miss calls fetcher; hit returns cached value without re-fetching", async () => {
    const kv = httpClient();
    const myCache = createCache(kv);
    let fetchCount = 0;
    const key = uniqueKey();
    async function fetchValue() {
      fetchCount++;
      return { v: fetchCount };
    }
    const getItem = myCache(fetchValue, { key, ttl: 60 });

    const first = await getItem();
    expect(first).toEqual({ v: 1 });
    expect(fetchCount).toBe(1);

    const second = await getItem();
    expect(second).toEqual({ v: 1 }); // cached — same value
    expect(fetchCount).toBe(1); // fetcher not called again
  });

  it("explicit static key", async () => {
    const kv = httpClient();
    const myCache = createCache(kv);
    const key = uniqueKey("static");
    async function fetchConfig() {
      return { version: 1 };
    }
    const getConfig = myCache(fetchConfig, { key, ttl: 60 });

    await getConfig();
    const { data: entry } = await kv.get(key);
    expect(entry).not.toBeNull();
    expect(entry!.json()).toEqual({ version: 1 });
  });

  it("explicit dynamic key function overrides default derivation", async () => {
    const kv = httpClient();
    const myCache = createCache(kv);
    const prefix = uniqueKey("dyn");
    async function fetchUser(id: string) {
      return { id };
    }
    const getUser = myCache(fetchUser, {
      key: (id: string) => `${prefix}:${id}`,
      ttl: 60,
    });

    await getUser("42");
    const { data: entry } = await kv.get(`${prefix}:42`);
    expect(entry).not.toBeNull();
    expect(entry!.json()).toEqual({ id: "42" });
  });

  it("stores entry with ttl+swr as total ttl", async () => {
    const kv = httpClient();
    const myCache = createCache(kv);
    const key = uniqueKey("ttl");
    async function fetchItem() {
      return "item";
    }
    const getItem = myCache(fetchItem, { key, ttl: 10, swr: 5 });

    await getItem();
    const { data: entry } = await kv.get(key);
    expect(entry).not.toBeNull();
    expect(entry!.ttl).toBeGreaterThan(12);
    expect(entry!.ttl).toBeLessThanOrEqual(15);
  });

  it("stores entry with only ttl when no swr", async () => {
    const kv = httpClient();
    const myCache = createCache(kv);
    const key = uniqueKey("ttlonly");
    async function fetchItem() {
      return "item";
    }
    const getItem = myCache(fetchItem, { key, ttl: 30 });

    await getItem();
    const { data: entry } = await kv.get(key);
    expect(entry!.ttl).toBeGreaterThan(27);
    expect(entry!.ttl).toBeLessThanOrEqual(30);
  });

  it("coalesces concurrent reads from the same handle into one batch() call", async () => {
    const base = httpClient();
    const spy = spyClient(base);
    const myCache = createCache(spy.client);

    const k1 = uniqueKey("c");
    const k2 = uniqueKey("c");
    const k3 = uniqueKey("c");
    async function fetchVal(k: string) {
      return k;
    }
    const getVal = myCache(fetchVal, { key: (k: string) => k, ttl: 60 });

    await Promise.all([getVal(k1), getVal(k2), getVal(k3)]);
    spy.reset();

    // All hits — 3 concurrent calls → exactly 1 batch() round-trip
    await Promise.all([getVal(k1), getVal(k2), getVal(k3)]);
    expect(spy.calls()).toBe(1);
  });

  it("coalesces reads across different cache handles on the same client", async () => {
    const base = httpClient();
    const spy = spyClient(base);
    const myCache = createCache(spy.client);

    const userKey = uniqueKey("user");
    const postKey = uniqueKey("post");
    async function fetchUser() {
      return "user";
    }
    async function fetchPost() {
      return "post";
    }
    const getUser = myCache(fetchUser, { key: userKey, ttl: 60 });
    const getPost = myCache(fetchPost, { key: postKey, ttl: 60 });

    // Populate both handles
    await Promise.all([getUser(), getPost()]);
    spy.reset();

    // Two different handles, same client — reads must share one batch() call
    await Promise.all([getUser(), getPost()]);
    expect(spy.calls()).toBe(1);
  });

  it("coalesces concurrent writes (misses) into one batch() call", async () => {
    const base = httpClient();
    const spy = spyClient(base);
    const myCache = createCache(spy.client);

    const k1 = uniqueKey("w");
    const k2 = uniqueKey("w");
    const k3 = uniqueKey("w");
    async function fetchVal(k: string) {
      return k;
    }
    const getVal = myCache(fetchVal, { key: (k: string) => k, ttl: 60 });

    spy.reset();
    // 3 concurrent misses → 1 batch() for reads + 1 batch() for writes = 2 total
    await Promise.all([getVal(k1), getVal(k2), getVal(k3)]);
    expect(spy.calls()).toBe(2);
  });

  it("stampede protection — fetcher called once for concurrent same-key misses", async () => {
    const kv = httpClient();
    const myCache = createCache(kv);
    let fetchCount = 0;
    const key = uniqueKey("stamp");
    async function fetchThing() {
      fetchCount++;
      await sleep(20);
      return "result";
    }
    const getThing = myCache(fetchThing, { key, ttl: 60 });

    const [a, b, c] = await Promise.all([getThing(), getThing(), getThing()]);
    expect(fetchCount).toBe(1);
    expect(a).toBe("result");
    expect(b).toBe("result");
    expect(c).toBe("result");
  });

  it("SWR — returns stale immediately; refresh verified by reading KV directly", async () => {
    const kv = httpClient();
    const myCache = createCache(kv);
    const key = uniqueKey("swr");
    let fetchCount = 0;

    async function fetchItem() {
      fetchCount++;
      return { version: fetchCount };
    }
    // storageTtl = 1 + 30 = 31s. After 1.5s remaining ≈ 29.5s ≤ 30s → stale.
    const getItem = myCache(fetchItem, { key, ttl: 1, swr: 30 });

    const first = await getItem();
    expect(first).toEqual({ version: 1 });
    expect(fetchCount).toBe(1);

    await sleep(1500); // push into stale window

    // Stale value returned immediately — doesn't wait for refresh
    const stale = await getItem();
    expect(stale).toEqual({ version: 1 });

    // Background refresh is fire-and-forget from the caller's perspective.
    // Verify it actually completed by reading the new value from KV directly.
    await sleep(500);
    const { data: entry } = await kv.get(key);
    expect(entry?.json()).toEqual({ version: 2 });
    expect(fetchCount).toBe(2);

    // Next cache call returns the refreshed value without another fetch
    const fresh = await getItem();
    expect(fresh).toEqual({ version: 2 });
    expect(fetchCount).toBe(2);
  });

  it("SWR — no background fetch when entry is within ttl window", async () => {
    const kv = httpClient();
    const myCache = createCache(kv);
    let fetchCount = 0;
    const key = uniqueKey("swrfresh");

    async function fetchItem() {
      fetchCount++;
      return { version: fetchCount };
    }
    // storageTtl = 10 + 3 = 13s. Immediately remaining ≈ 13s > 3s → fresh.
    const getItem = myCache(fetchItem, { key, ttl: 10, swr: 3 });

    await getItem();
    expect(fetchCount).toBe(1);

    await getItem();
    await sleep(100); // give any erroneous background refresh time to complete
    expect(fetchCount).toBe(1); // fetcher must not have been called again
    void kv;
  });

  it(".delete() removes the key; next call is a miss", async () => {
    const kv = httpClient();
    const myCache = createCache(kv);
    let fetchCount = 0;
    const key = uniqueKey("del");
    async function fetchItem() {
      fetchCount++;
      return { v: fetchCount };
    }
    const getItem = myCache(fetchItem, { key, ttl: 60 });

    await getItem();
    expect(fetchCount).toBe(1);

    await getItem.delete();
    // Key is gone from KV
    const { data: gone } = await kv.get(key);
    expect(gone).toBeNull();

    // Next call is a miss — fetcher invoked again
    await getItem();
    expect(fetchCount).toBe(2);
  });

  it(".refresh() force-fetches and updates the cache; next call returns new value", async () => {
    const kv = httpClient();
    const myCache = createCache(kv);
    let fetchCount = 0;
    const key = uniqueKey("ref");
    async function fetchItem() {
      fetchCount++;
      return { v: fetchCount };
    }
    const getItem = myCache(fetchItem, { key, ttl: 60 });

    const first = await getItem();
    expect(first).toEqual({ v: 1 });

    const refreshed = await getItem.refresh();
    expect(refreshed).toEqual({ v: 2 });
    expect(fetchCount).toBe(2);

    // Cache now holds the refreshed value — no additional fetch
    const hit = await getItem();
    expect(hit).toEqual({ v: 2 });
    expect(fetchCount).toBe(2);
  });

  it(".refresh() stampede protection — concurrent calls share one fetch", async () => {
    const kv = httpClient();
    const myCache = createCache(kv);
    let fetchCount = 0;
    const key = uniqueKey("refstamp");
    async function fetchItem() {
      fetchCount++;
      await sleep(20);
      return fetchCount;
    }
    const getItem = myCache(fetchItem, { key, ttl: 60 });
    await getItem(); // populate

    const [a, b] = await Promise.all([getItem.refresh(), getItem.refresh()]);
    expect(fetchCount).toBe(2); // one for initial, one for both refreshes
    expect(a).toBe(b);
  });

  it("onRefreshError — called when background SWR refresh fails", async () => {
    const kv = httpClient();
    const myCache = createCache(kv);
    const key = uniqueKey("rferr");
    let fetchCount = 0;
    let refreshError: unknown;

    async function fetchItem() {
      fetchCount++;
      if (fetchCount > 1) throw new Error("upstream down");
      return { v: 1 };
    }
    // ttl: 1, swr: 30 — after 1.5s the entry is stale
    const getItem = myCache(fetchItem, {
      key,
      ttl: 1,
      swr: 30,
      onRefreshError: (err) => {
        refreshError = err;
      },
    });

    await getItem(); // populate
    await sleep(1500); // push into stale window

    // Stale value returned; background refresh fires and fails
    const stale = await getItem();
    expect(stale).toEqual({ v: 1 });

    await sleep(200); // let background refresh complete
    expect(refreshError).toBeDefined();
    expect((refreshError as Error).message).toBe("upstream down");
  });

  it("error propagation — fetcher throws, caller rejects", async () => {
    const kv = httpClient();
    const myCache = createCache(kv);
    const key = uniqueKey("err");
    async function failingFetch() {
      throw new Error("fetch failed");
    }
    const getItem = myCache(failingFetch, { key, ttl: 60 });
    await expect(getItem()).rejects.toThrow("fetch failed");
    void kv;
  });

  it("createCache routes through the provided client, not the default", async () => {
    const custom = httpClient();
    const spy = spyClient(custom);
    const myCache = createCache(spy.client);
    const key = uniqueKey("cc");
    async function fetchVal() {
      return "val";
    }
    const getVal = myCache(fetchVal, { key, ttl: 60 });

    // Miss + write = 2 batch() calls through the custom client
    await getVal();
    expect(spy.calls()).toBe(2);

    // Hit = 1 more batch() call
    spy.reset();
    await getVal();
    expect(spy.calls()).toBe(1);
  });
});

// ── RESP backend ──────────────────────────────────────────────────────────────

describe("cache — RESP backend", () => {
  const clients: KvClient[] = [];
  afterEach(async () => {
    await Promise.all(clients.map((c) => c.close()));
    clients.length = 0;
  });

  function client() {
    const c = respClient();
    clients.push(c);
    return c;
  }

  it("miss calls fetcher; hit returns cached value without re-fetching", async () => {
    const kv = client();
    const myCache = createCache(kv);
    let fetchCount = 0;
    const key = uniqueKey("rk");
    async function fetchItem() {
      fetchCount++;
      return { id: fetchCount };
    }
    const getItem = myCache(fetchItem, { key, ttl: 60 });

    const first = await getItem();
    expect(first).toEqual({ id: 1 });
    expect(fetchCount).toBe(1);

    const second = await getItem();
    expect(second).toEqual({ id: 1 });
    expect(fetchCount).toBe(1);
  });

  it("coalesces concurrent reads into one batch() call", async () => {
    const base = client();
    const spy = spyClient(base);
    const myCache = createCache(spy.client);

    const k1 = uniqueKey("rc");
    const k2 = uniqueKey("rc");
    async function fetchVal(k: string) {
      return k;
    }
    const getVal = myCache(fetchVal, { key: (k: string) => k, ttl: 60 });

    await Promise.all([getVal(k1), getVal(k2)]);
    spy.reset();

    await Promise.all([getVal(k1), getVal(k2)]);
    expect(spy.calls()).toBe(1);
  });

  it("coalesces reads across different cache handles on the same client", async () => {
    const base = client();
    const spy = spyClient(base);
    const myCache = createCache(spy.client);

    const k1 = uniqueKey("rch1");
    const k2 = uniqueKey("rch2");
    async function fetchA() {
      return "a";
    }
    async function fetchB() {
      return "b";
    }
    const getA = myCache(fetchA, { key: k1, ttl: 60 });
    const getB = myCache(fetchB, { key: k2, ttl: 60 });

    await Promise.all([getA(), getB()]);
    spy.reset();

    await Promise.all([getA(), getB()]);
    expect(spy.calls()).toBe(1);
  });

  it("stampede protection — fetcher called once for concurrent same-key misses", async () => {
    const kv = client();
    const myCache = createCache(kv);
    let fetchCount = 0;
    const key = uniqueKey("rstamp");
    async function fetchThing() {
      fetchCount++;
      await sleep(20);
      return "ok";
    }
    const getThing = myCache(fetchThing, { key, ttl: 60 });

    const [a, b] = await Promise.all([getThing(), getThing()]);
    expect(fetchCount).toBe(1);
    expect(a).toBe("ok");
    expect(b).toBe("ok");
  });

  it(".delete() invalidates the key", async () => {
    const kv = client();
    const myCache = createCache(kv);
    let fetchCount = 0;
    const key = uniqueKey("rdel");
    async function fetchItem() {
      fetchCount++;
      return fetchCount;
    }
    const getItem = myCache(fetchItem, { key, ttl: 60 });

    await getItem();
    await getItem.delete();
    await getItem();
    expect(fetchCount).toBe(2);
  });
});

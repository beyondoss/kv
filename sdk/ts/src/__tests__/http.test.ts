import { describe, expect, it } from "vitest";
import type { KvClient } from "../client.js";
import { KvError } from "../errors.js";
import type { WatchEvent } from "../kv-types.js";
import type { WatchOptions } from "../kv-types.js";
import {
  dec,
  enc,
  getHttpUrl,
  httpClient,
  uniqueKey,
  uniqueNs,
} from "./harness.js";

// Each test gets its own namespace via httpClient() so tests never share state.

describe("HTTP backend — get / set / delete", () => {
  it("get returns null for a missing key", async () => {
    const kv = httpClient();
    const { data: entry } = await kv.get(uniqueKey());
    expect(entry).toBeNull();
  });

  it("get returns the stored value", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "hello");
    const { data: entry } = await kv.get(key);
    expect(entry).not.toBeNull();
    expect(dec(entry!.value)).toBe("hello");
  });

  it("get round-trips binary data", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    const bytes = new Uint8Array([0, 1, 127, 128, 255]);
    await kv.set(key, bytes);
    const { data: entry } = await kv.get(key);
    expect(entry?.value).toEqual(bytes);
  });

  it("get returns ttl when set", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "v", { ttl: 60 });
    const { data: entry } = await kv.get(key);
    expect(entry?.ttl).toBeGreaterThan(0);
    expect(entry?.ttl).toBeLessThanOrEqual(60);
  });

  it("get returns undefined ttl when not set", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "v");
    const { data: entry } = await kv.get(key);
    expect(entry?.ttl).toBeUndefined();
  });

  it("get returns metadata when set", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    const meta = { region: "us-east-1", version: 42 };
    await kv.set(key, "v", { metadata: meta });
    const { data: entry } = await kv.get(key);
    expect(entry?.metadata).toEqual(meta);
  });

  it("set overwrites an existing key", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "first");
    await kv.set(key, "second");
    expect(dec((await kv.get(key)).data!.value)).toBe("second");
  });

  it("delete removes a key", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "v");
    await kv.delete(key);
    const { data: entry } = await kv.get(key);
    expect(entry).toBeNull();
  });

  it("delete on a missing key does not throw", async () => {
    const kv = httpClient();
    const { error: delErr } = await kv.delete(uniqueKey());
    expect(delErr).toBeUndefined();
  });
});

describe("HTTP backend — ifAbsent / ifPresent", () => {
  it("ifAbsent succeeds on a missing key", async () => {
    const kv = httpClient();
    const { error } = await kv.set(uniqueKey(), "v", { ifAbsent: true });
    expect(error).toBeUndefined();
  });

  it("ifAbsent throws KvError(409) when the key already exists", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "original");
    const { error } = await kv.set(key, "new", { ifAbsent: true });
    expect(error).toSatisfy(
      (e: unknown) => e instanceof KvError && (e as KvError).status === 409,
    );
    expect(dec((await kv.get(key)).data!.value)).toBe("original");
  });

  it("ifPresent succeeds when the key exists", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "old");
    const { error } = await kv.set(key, "new", { ifPresent: true });
    expect(error).toBeUndefined();
    expect(dec((await kv.get(key)).data!.value)).toBe("new");
  });

  it("ifPresent throws KvError(409) when the key does not exist", async () => {
    const kv = httpClient();
    const { error } = await kv.set(uniqueKey(), "v", { ifPresent: true });
    expect(error).toSatisfy(
      (e: unknown) => e instanceof KvError && (e as KvError).status === 409,
    );
  });
});

describe("HTTP backend — list", () => {
  it("returns an empty result for an empty namespace", async () => {
    const kv = httpClient("db15"); // reserved: no other test writes to db15
    const { data: result } = await kv.list();
    expect(result!.keys).toHaveLength(0);
    expect(result!.nextCursor).toBeUndefined();
  });

  it("returns all keys inserted into the namespace", async () => {
    const kv = httpClient();
    const keys = [uniqueKey("a"), uniqueKey("b"), uniqueKey("c")];
    await kv.batchSet(keys.map((key) => ({ key, value: "v" })));

    const { data: result } = await kv.list();
    const names = result!.keys.map((k) => k.name);
    for (const key of keys) {
      expect(names).toContain(key);
    }
    expect(result!.nextCursor).toBeUndefined();
  });

  it("filters by prefix", async () => {
    const kv = httpClient();
    const prefix = `pfx:${crypto.randomUUID()}`;
    const matching = [`${prefix}:a`, `${prefix}:b`];
    const other = [uniqueKey("other")];
    await kv.batchSet(
      [...matching, ...other].map((key) => ({ key, value: "v" })),
    );

    const { data: result } = await kv.list({ prefix });
    const names = result!.keys.map((k) => k.name);
    expect(names.sort()).toEqual(matching.sort());
  });

  it("paginates correctly using cursor", async () => {
    const kv = httpClient();
    const prefix = `page:${crypto.randomUUID()}`;
    const total = 5;
    const allKeys = Array.from({ length: total }, (_, i) => `${prefix}:${i}`);
    await kv.batchSet(allKeys.map((key) => ({ key, value: "v" })));

    const seen: string[] = [];
    let cursor: string | undefined;

    do {
      const { data: page } = await kv.list({
        prefix,
        limit: 2,
        ...(cursor !== undefined ? { cursor } : {}),
      });
      seen.push(...page!.keys.map((k) => k.name));
      cursor = page!.nextCursor;
    } while (cursor !== undefined);

    expect(seen.sort()).toEqual(allKeys.sort());
  });
});

describe("HTTP backend — mget / mset", () => {
  it("mget returns null for missing keys and values for present keys", async () => {
    const kv = httpClient();
    const existing = uniqueKey();
    const missing = uniqueKey();
    await kv.set(existing, "hi");
    const { data: results } = await kv.batchGet([existing, missing]);
    expect(results).toHaveLength(2);
    expect(dec(results![0]!.value)).toBe("hi");
    expect(results![1]).toBeNull();
  });

  it("mget with empty array returns empty array", async () => {
    const kv = httpClient();
    const { data: results } = await kv.batchGet([]);
    expect(results).toEqual([]);
  });

  it("mset sets all entries atomically", async () => {
    const kv = httpClient();
    const entries = [
      { key: uniqueKey(), value: "one" },
      { key: uniqueKey(), value: "two" },
      { key: uniqueKey(), value: enc("three") },
    ];
    await kv.batchSet(entries);
    const { data: results } = await kv.batchGet(entries.map((e) => e.key));
    expect(dec(results![0]!.value)).toBe("one");
    expect(dec(results![1]!.value)).toBe("two");
    expect(dec(results![2]!.value)).toBe("three");
  });

  it("mset with empty array is a no-op", async () => {
    const kv = httpClient();
    const { error } = await kv.batchSet([]);
    expect(error).toBeUndefined();
  });

  it("mset respects per-entry ttl", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.batchSet([{ key, value: "v", opts: { ttl: 60 } }]);
    const { data: entry } = await kv.get(key);
    expect(entry?.ttl).toBeGreaterThan(0);
    expect(entry?.ttl).toBeLessThanOrEqual(60);
  });
});

describe("HTTP backend — namespace isolation", () => {
  it("keys in different namespaces do not overlap", async () => {
    const url = getHttpUrl();
    const { createKvClient } = await import("../client.js");
    const kvA = createKvClient({ url, namespace: "db13" });
    const kvB = createKvClient({ url, namespace: "db14" });

    const key = uniqueKey();
    await kvA.set(key, "in-a");

    expect((await kvB.get(key)).data).toBeNull();
    expect(dec((await kvA.get(key)).data!.value)).toBe("in-a");
  });
});

describe("HTTP backend — observability hooks", () => {
  it("fires onRequest and onResponse for each operation", async () => {
    const commands: string[] = [];
    const responses: string[] = [];
    const { createKvClient } = await import("../client.js");
    const kv = createKvClient({
      url: getHttpUrl(),
      namespace: uniqueNs(),
      onRequest: (e) => commands.push(e.command),
      onResponse: (e) => responses.push(e.command),
    });

    const key = uniqueKey();
    await kv.set(key, "v");
    await kv.get(key);
    await kv.delete(key);

    expect(commands).toEqual(["SET", "GET", "DEL"]);
    expect(responses).toEqual(["SET", "GET", "DEL"]);
  });

  it("onResponse includes a non-negative durationMs", async () => {
    const durations: number[] = [];
    const { createKvClient } = await import("../client.js");
    const kv = createKvClient({
      url: getHttpUrl(),
      namespace: uniqueNs(),
      onResponse: (e) => durations.push(e.durationMs),
    });

    await kv.set(uniqueKey(), "v");
    expect(durations[0]).toBeGreaterThanOrEqual(0);
  });

  it("onMetadataParseError is called when x-kv-metadata is invalid JSON", async () => {
    const errors: unknown[] = [];
    let capturedKey = "";
    const { createKvClient } = await import("../client.js");

    // Intercept fetch to inject a bad metadata header on GET responses.
    const realFetch = globalThis.fetch;
    const ns = uniqueNs();
    const key = uniqueKey();

    const mockFetch: typeof fetch = async (input, init) => {
      const res = await realFetch(input, init);
      if (res.status === 200) {
        const headers = new Headers(res.headers);
        headers.set("x-kv-metadata", "{bad json}");
        return new Response(res.body, { status: res.status, headers });
      }
      return res;
    };

    const kv = createKvClient({
      url: getHttpUrl(),
      namespace: ns,
      fetch: mockFetch,
      onMetadataParseError: (k, _raw, err) => {
        capturedKey = k;
        errors.push(err);
      },
    });

    await kv.set(key, "v");
    await kv.get(key);

    expect(errors).toHaveLength(1);
    expect(capturedKey).toBe(key);
  });
});

describe("HTTP backend — retry on 5xx", () => {
  it("retries on 5xx and succeeds on subsequent attempt", async () => {
    let attempt = 0;
    const { createKvClient } = await import("../client.js");
    const ns = uniqueNs();
    const key = uniqueKey();

    // First call returns 503; subsequent calls delegate to the real server.
    const realFetch = globalThis.fetch;
    const mockFetch: typeof fetch = async (input, init) => {
      if (attempt++ === 0) {
        return new Response("Service Unavailable", { status: 503 });
      }
      return realFetch(input, init);
    };

    const kv = createKvClient({
      url: getHttpUrl(),
      namespace: ns,
      fetch: mockFetch,
      retries: 2,
    });
    await kv.set(key, "v");
    expect(attempt).toBeGreaterThan(1);
  });
});

describe("HTTP backend — key encoding edge cases", () => {
  it.each([
    ["key with spaces", "key with spaces"],
    ["path/like/key", "path/like/key"],
    ["key?with=query", "key?with=query"],
    ["unicode 日本語", "unicode 日本語"],
    ["colon:key", "colon:key"],
  ])("round-trips key %s", async (_label, key) => {
    const kv = httpClient();
    await kv.set(key, "v");
    const { data: entry } = await kv.get(key);
    expect(entry).not.toBeNull();
    expect(dec(entry!.value)).toBe("v");
    expect((await kv.get(key + "-missing")).data).toBeNull();
  });
});

describe("HTTP backend — timeout", () => {
  it("aborts the request when the timeout elapses", async () => {
    const { createKvClient } = await import("../client.js");

    // Fetch that hangs forever until aborted.
    const hangingFetch: typeof fetch = (_input, init) =>
      new Promise((_resolve, reject) => {
        init?.signal?.addEventListener("abort", () =>
          reject(new DOMException("aborted", "AbortError")),
        );
      });

    const kv = createKvClient({
      url: getHttpUrl(),
      namespace: uniqueNs(),
      fetch: hangingFetch,
      timeout: 50,
      retries: 0,
    });

    const { error } = await kv.get(uniqueKey());
    expect(error).toBeDefined();
  });
});

describe("HTTP backend — close", () => {
  it("close() resolves without error", async () => {
    const kv = httpClient();
    await expect(kv.close()).resolves.toBeUndefined();
  });
});

// ── watch helpers ─────────────────────────────────────────────────────────────

/**
 * Subscribe to `kv.watch(key, opts)`, wait for the `"ready"` event, call
 * `act()`, then collect events until `predicate(events)` returns true or
 * the timeout fires (default 5 s).  Returns all collected events including
 * `"ready"`.
 */
async function watchCollect(
  kv: KvClient,
  key: string,
  opts: WatchOptions,
  predicate: (events: WatchEvent[]) => boolean,
  act: () => Promise<unknown>,
): Promise<WatchEvent[]> {
  const ac = new AbortController();
  const timeout = setTimeout(() => ac.abort(), 5_000);
  const events: WatchEvent[] = [];

  let readyResolve!: () => void;
  const readyPromise = new Promise<void>((r) => {
    readyResolve = r;
  });

  const collectTask = (async () => {
    for await (const ev of kv.watch(key, { ...opts, signal: ac.signal })) {
      events.push(ev);
      if (ev.type === "ready") readyResolve();
      if (predicate(events)) break;
    }
  })();

  await readyPromise;
  await act();
  await collectTask;
  clearTimeout(timeout);
  ac.abort();
  return events;
}

type MutationEvent = Extract<WatchEvent, { type: "set" | "del" }>;
type SetEvent = Extract<WatchEvent, { type: "set" }>;

function nonReady(events: WatchEvent[]): MutationEvent[] {
  return events.filter((e): e is MutationEvent => e.type !== "ready");
}

// ── watch tests ───────────────────────────────────────────────────────────────

describe("HTTP backend — watch (exact key)", () => {
  it("emits a ready event on subscription", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    const events = await watchCollect(
      kv,
      key,
      {},
      (evs) => evs.some((e) => e.type === "ready"),
      async () => {},
    );
    expect(events.find((e) => e.type === "ready")).toBeDefined();
  });

  it("delivers a set event when a key is written after subscription", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    const events = await watchCollect(
      kv,
      key,
      {},
      (evs) => nonReady(evs).length >= 1,
      async () => kv.set(key, "hello"),
    );
    const setEvent = events.find((e) => e.type === "set") as
      | SetEvent
      | undefined;
    expect(setEvent).toBeDefined();
    expect(dec(setEvent!.value)).toBe("hello");
    expect(setEvent!.key).toBe(key);
    expect(setEvent!.revision).toBeGreaterThan(0);
  });

  it("includes the current value immediately when the key already exists (since=0)", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "preexisting");

    const events = await watchCollect(
      kv,
      key,
      {},
      (evs) => evs.some((e) => e.type === "ready"),
      async () => {},
    );
    // The initial set event should arrive before ready
    const setEvent = events.find((e) => e.type === "set") as
      | SetEvent
      | undefined;
    expect(setEvent).toBeDefined();
    expect(dec(setEvent!.value)).toBe("preexisting");
  });

  it("delivers a del event when the key is deleted", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "will-be-deleted");

    const events = await watchCollect(
      kv,
      key,
      {},
      (evs) => nonReady(evs).some((e) => e.type === "del"),
      async () => kv.delete(key),
    );
    const delEvent = events.find(
      (e): e is Extract<WatchEvent, { type: "del" }> => e.type === "del",
    );
    expect(delEvent).toBeDefined();
    expect(delEvent!.key).toBe(key);
    expect(delEvent!.revision).toBeGreaterThan(0);
  });

  it("delivers multiple sequential mutations in order", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    const events = await watchCollect(
      kv,
      key,
      {},
      (evs) => nonReady(evs).length >= 3,
      async () => {
        await kv.set(key, "v1");
        await kv.set(key, "v2");
        await kv.delete(key);
      },
    );
    const mutations = nonReady(events);
    expect(mutations).toHaveLength(3);
    expect(mutations[0]!.type).toBe("set");
    expect(dec((mutations[0]! as SetEvent).value)).toBe("v1");
    expect(mutations[1]!.type).toBe("set");
    expect(dec((mutations[1]! as SetEvent).value)).toBe("v2");
    expect(mutations[2]!.type).toBe("del");
  });

  it("revisions are strictly increasing", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    const events = await watchCollect(
      kv,
      key,
      {},
      (evs) => nonReady(evs).length >= 2,
      async () => {
        await kv.set(key, "a");
        await kv.set(key, "b");
      },
    );
    const mutations = nonReady(events);
    expect(mutations[1]!.revision).toBeGreaterThan(mutations[0]!.revision);
  });

  it("cancellation via AbortSignal stops the stream cleanly", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    const ac = new AbortController();

    const events: WatchEvent[] = [];
    const watchTask = (async () => {
      for await (const ev of kv.watch(key, { signal: ac.signal })) {
        events.push(ev);
        if (ev.type === "ready") ac.abort();
      }
    })();

    await watchTask;
    expect(events.find((e) => e.type === "ready")).toBeDefined();
    // No mutations were emitted — only the ready sentinel
    expect(nonReady(events)).toHaveLength(0);
  });

  it("since=revision replays mutations missed between reconnects", async () => {
    const kv = httpClient();
    const key = uniqueKey();

    // First subscription: record the revision of a write
    const firstEvents = await watchCollect(
      kv,
      key,
      {},
      (evs) => nonReady(evs).length >= 1,
      async () => kv.set(key, "first"),
    );
    const firstRev = (nonReady(firstEvents)[0] as SetEvent).revision;

    // Write a second value while "disconnected" (no active watch)
    await kv.set(key, "second");

    // Reconnect with since=firstRev — must replay the "second" write
    const replayEvents = await watchCollect(
      kv,
      key,
      { since: firstRev },
      (evs) => evs.some((e) => e.type === "ready"),
      async () => {},
    );
    const replayed = replayEvents.filter(
      (e): e is SetEvent => e.type === "set",
    );
    expect(replayed.length).toBeGreaterThanOrEqual(1);
    const lastValue = dec(replayed[replayed.length - 1]!.value);
    expect(lastValue).toBe("second");
  });
});

describe("HTTP backend — watch (prefix)", () => {
  it("streams set events for all keys matching the prefix", async () => {
    const kv = httpClient();
    const prefix = `cfg:${crypto.randomUUID()}:`;
    const k1 = `${prefix}alpha`;
    const k2 = `${prefix}beta`;

    const events = await watchCollect(
      kv,
      prefix,
      { prefix: true },
      (evs) => nonReady(evs).length >= 2,
      async () => {
        await kv.set(k1, "v1");
        await kv.set(k2, "v2");
      },
    );
    const mutations = nonReady(events);
    expect(mutations).toHaveLength(2);
    expect(mutations.map((e) => e.key).sort()).toEqual([k1, k2].sort()); // MutationEvent always has key
  });

  it("does not emit events for keys outside the prefix", async () => {
    const kv = httpClient();
    const prefix = `watch-pfx:${crypto.randomUUID()}:`;
    const inside = `${prefix}inside`;
    const outside = uniqueKey("other");

    const events = await watchCollect(
      kv,
      prefix,
      { prefix: true },
      (evs) => nonReady(evs).length >= 1,
      async () => {
        await kv.set(outside, "should-not-appear");
        await kv.set(inside, "appears");
      },
    );
    const keys = nonReady(events).map((e) => e.key); // MutationEvent always has key
    expect(keys).not.toContain(outside);
    expect(keys).toContain(inside);
  });
});

describe("HTTP backend — incr", () => {
  it("incr on missing key starts at 1", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    expect((await kv.incr(key)).data).toBe(1);
  });

  it("incr increments an existing value", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "5");
    expect((await kv.incr(key)).data).toBe(6);
  });

  it("incr with positive delta adds delta", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "10");
    expect((await kv.incr(key, 5)).data).toBe(15);
  });

  it("incr with negative delta decrements", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "10");
    expect((await kv.incr(key, -3)).data).toBe(7);
  });

  it("incr on a non-integer value returns KvError", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, enc("hello"));
    expect((await kv.incr(key)).error).toBeInstanceOf(KvError);
  });
});

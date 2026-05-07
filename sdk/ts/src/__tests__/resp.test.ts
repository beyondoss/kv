import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createKvClient, type KvClient } from "../client.js";
import { KvError } from "../errors.js";
import { dec, enc, getRespUrl, uniqueKey } from "./harness.js";

// Each describe block uses a distinct db number to isolate state.
// Db 0 → general tests; db 1 → isolation tests; db 14 → empty-db list test.

let kv: KvClient;

beforeAll(() => {
  kv = createKvClient({ url: getRespUrl(), db: 0 });
});

afterAll(() => kv.close());

describe("RESP backend — get / set / delete", () => {
  it("get returns null for a missing key", async () => {
    expect((await kv.get(uniqueKey())).data).toBeNull();
  });

  it("get returns the stored value", async () => {
    const key = uniqueKey();
    await kv.set(key, "hello");
    const { data: entry } = await kv.get(key);
    expect(entry).not.toBeNull();
    expect(dec(entry!.value)).toBe("hello");
  });

  it("get round-trips binary data", async () => {
    const key = uniqueKey();
    const bytes = new Uint8Array([0, 1, 127, 128, 255]);
    await kv.set(key, bytes);
    const { data: entry } = await kv.get(key);
    expect(entry?.value).toEqual(bytes);
  });

  it("get returns ttl when set", async () => {
    const key = uniqueKey();
    await kv.set(key, "v", { ttl: 60 });
    const { data: entry } = await kv.get(key);
    expect(entry?.ttl).toBeGreaterThan(0);
    expect(entry?.ttl).toBeLessThanOrEqual(60);
  });

  it("get returns undefined ttl for a persistent key", async () => {
    const key = uniqueKey();
    await kv.set(key, "v");
    const { data: entry } = await kv.get(key);
    expect(entry?.ttl).toBeUndefined();
  });

  it("set overwrites an existing key", async () => {
    const key = uniqueKey();
    await kv.set(key, "first");
    await kv.set(key, "second");
    expect(dec((await kv.get(key)).data!.value)).toBe("second");
  });

  it("delete removes a key", async () => {
    const key = uniqueKey();
    await kv.set(key, "v");
    await kv.delete(key);
    expect((await kv.get(key)).data).toBeNull();
  });

  it("delete on a missing key does not throw", async () => {
    const { error: delErr } = await kv.delete(uniqueKey());
    expect(delErr).toBeUndefined();
  });
});

describe("RESP backend — ifAbsent / ifPresent", () => {
  it("ifAbsent succeeds on a missing key", async () => {
    const { error } = await kv.set(uniqueKey(), "v", { ifAbsent: true });
    expect(error).toBeUndefined();
  });

  it("ifAbsent throws KvError(409) when the key already exists", async () => {
    const key = uniqueKey();
    await kv.set(key, "original");
    const { error } = await kv.set(key, "new", { ifAbsent: true });
    expect(error).toSatisfy((e) => e instanceof KvError && e.status === 409);
    expect(dec((await kv.get(key)).data!.value)).toBe("original");
  });

  it("ifAbsent with ttl succeeds on a missing key", async () => {
    const key = uniqueKey();
    await kv.set(key, "v", { ifAbsent: true, ttl: 60 });
    expect((await kv.get(key)).data?.ttl).toBeGreaterThan(0);
  });

  it("ifPresent succeeds when the key exists", async () => {
    const key = uniqueKey();
    await kv.set(key, "old");
    await kv.set(key, "new", { ifPresent: true });
    expect(dec((await kv.get(key)).data!.value)).toBe("new");
  });

  it("ifPresent throws KvError(409) when the key does not exist", async () => {
    const { error } = await kv.set(uniqueKey(), "v", { ifPresent: true });
    expect(error).toSatisfy((e) => e instanceof KvError && e.status === 409);
  });

  it("ifPresent with ttl succeeds when the key exists", async () => {
    const key = uniqueKey();
    await kv.set(key, "old");
    await kv.set(key, "new", { ifPresent: true, ttl: 60 });
    const { data: entry } = await kv.get(key);
    expect(dec(entry!.value)).toBe("new");
    expect(entry!.ttl).toBeGreaterThan(0);
  });
});

describe("RESP backend — list / scan", () => {
  it("returns an empty result when no keys match prefix", async () => {
    const { data: result } = await kv.list({
      prefix: `empty:${crypto.randomUUID()}`,
    });
    expect(result!.keys).toHaveLength(0);
    expect(result!.nextCursor).toBeUndefined();
  });

  it("filters by prefix", async () => {
    const prefix = `pfx:${crypto.randomUUID()}`;
    const matching = [`${prefix}:a`, `${prefix}:b`];
    await kv.batchSet(matching.map((key) => ({ key, value: "v" })));

    const { data: result } = await kv.list({ prefix });
    const names = result!.keys.map((k) => k.name).sort();
    expect(names).toEqual(matching.sort());
  });

  it("paginates correctly using cursor", async () => {
    const prefix = `page:${crypto.randomUUID()}`;
    const total = 5;
    const allKeys = Array.from({ length: total }, (_, i) => `${prefix}:${i}`);
    await kv.batchSet(allKeys.map((key) => ({ key, value: "v" })));

    const seen: string[] = [];
    let cursor: string | undefined;

    do {
      const { data: result } = await kv.list({
        prefix,
        limit: 2,
        ...(cursor !== undefined ? { cursor } : {}),
      });
      seen.push(...result!.keys.map((k) => k.name));
      cursor = result!.nextCursor;
    } while (cursor !== undefined);

    expect(seen.sort()).toEqual(allKeys.sort());
  });
});

describe("RESP backend — mget / mset", () => {
  it("mget returns null for missing and values for present keys", async () => {
    const existing = uniqueKey();
    const missing = uniqueKey();
    await kv.set(existing, "hi");
    const { data: results } = await kv.batchGet([existing, missing]);
    expect(results).toHaveLength(2);
    expect(dec(results![0]!.value)).toBe("hi");
    expect(results![1]).toBeNull();
  });

  it("mget with empty array returns empty array", async () => {
    expect((await kv.batchGet([])).data).toEqual([]);
  });

  it("mget returns ttl for keys that have one", async () => {
    const withTtl = uniqueKey();
    const noTtl = uniqueKey();
    await kv.batchSet([
      { key: withTtl, value: "timed", opts: { ttl: 60 } },
      { key: noTtl, value: "forever" },
    ]);
    const { data: results } = await kv.batchGet([withTtl, noTtl]);
    const [a, b] = results!;
    expect(a?.ttl).toBeGreaterThan(0);
    expect(a?.ttl).toBeLessThanOrEqual(60);
    expect(b?.ttl).toBeUndefined();
  });

  it("mset sets all entries", async () => {
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
    const { error } = await kv.batchSet([]);
    expect(error).toBeUndefined();
  });

  it("mset respects per-entry ttl", async () => {
    const key = uniqueKey();
    await kv.batchSet([{ key, value: "v", opts: { ttl: 60 } }]);
    const { data: entry } = await kv.get(key);
    expect(entry?.ttl).toBeGreaterThan(0);
    expect(entry?.ttl).toBeLessThanOrEqual(60);
  });

  it("mset handles mixed ttl and non-ttl entries", async () => {
    const withTtl = uniqueKey();
    const noTtl = uniqueKey();
    await kv.batchSet([
      { key: withTtl, value: "timed", opts: { ttl: 60 } },
      { key: noTtl, value: "forever" },
    ]);
    expect((await kv.get(withTtl)).data?.ttl).toBeGreaterThan(0);
    expect((await kv.get(noTtl)).data?.ttl).toBeUndefined();
  });
});

describe("RESP backend — database isolation", () => {
  let kv0: KvClient;
  let kv1: KvClient;
  let kv14: KvClient;

  beforeAll(() => {
    kv0 = createKvClient({ url: getRespUrl(), db: 0 });
    kv1 = createKvClient({ url: getRespUrl(), db: 1 });
    kv14 = createKvClient({ url: getRespUrl(), db: 14 });
  });

  afterAll(() => Promise.all([kv0.close(), kv1.close(), kv14.close()]));

  it("keys in different db numbers do not overlap", async () => {
    const key = uniqueKey("isolation");
    await kv0.set(key, "in-db0");

    expect((await kv1.get(key)).data).toBeNull();
    expect(dec((await kv0.get(key)).data!.value)).toBe("in-db0");
  });

  it("list on an empty db returns no keys and no nextCursor", async () => {
    // Db 14 is used only in this test so it is guaranteed to be empty.
    const { data: result } = await kv14.list({
      prefix: `empty:${crypto.randomUUID()}`,
    });
    expect(result!.keys).toHaveLength(0);
    expect(result!.nextCursor).toBeUndefined();
  });
});

describe("RESP backend — observability hooks", () => {
  let tracked1: KvClient;
  let tracked2: KvClient;
  let tracked3: KvClient;

  const commands: string[] = [];
  const responses: string[] = [];
  const durations: number[] = [];
  const mgetCounts: number[] = [];

  beforeAll(() => {
    tracked1 = createKvClient({
      url: getRespUrl(),
      db: 0,
      onRequest: (e) => commands.push(e.command),
      onResponse: (e) => responses.push(e.command),
    });
    tracked2 = createKvClient({
      url: getRespUrl(),
      db: 0,
      onResponse: (e) => durations.push(e.durationMs),
    });
    tracked3 = createKvClient({
      url: getRespUrl(),
      db: 0,
      onRequest: (e) => {
        if (e.command === "MGET") mgetCounts.push(e.keyCount);
      },
    });
  });

  afterAll(() =>
    Promise.all([tracked1.close(), tracked2.close(), tracked3.close()]),
  );

  it("fires onRequest and onResponse for each operation", async () => {
    const key = uniqueKey();
    await tracked1.set(key, "v");
    await tracked1.get(key);
    await tracked1.delete(key);

    expect(commands).toEqual(["SET", "GET", "DEL"]);
    expect(responses).toEqual(["SET", "GET", "DEL"]);
  });

  it("onResponse includes a non-negative durationMs", async () => {
    await tracked2.set(uniqueKey(), "v");
    expect(durations[0]).toBeGreaterThanOrEqual(0);
  });

  it("MGET reports the correct keyCount", async () => {
    const keys = [uniqueKey(), uniqueKey(), uniqueKey()];
    await tracked3.batchGet(keys);
    expect(mgetCounts[0]).toBe(3);
  });
});

describe("RESP backend — incr", () => {
  it("incr on missing key starts at 1", async () => {
    const key = uniqueKey();
    expect((await kv.incr(key)).data).toBe(1);
  });

  it("incr increments an existing value", async () => {
    const key = uniqueKey();
    await kv.set(key, "5");
    expect((await kv.incr(key)).data).toBe(6);
  });

  it("incr with positive delta adds delta", async () => {
    const key = uniqueKey();
    await kv.set(key, "10");
    expect((await kv.incr(key, 5)).data).toBe(15);
  });

  it("incr with negative delta decrements", async () => {
    const key = uniqueKey();
    await kv.set(key, "10");
    expect((await kv.incr(key, -3)).data).toBe(7);
  });

  it("incr on a non-integer value returns error", async () => {
    const key = uniqueKey();
    await kv.set(key, "hello");
    expect((await kv.incr(key)).error).toBeDefined();
  });
});

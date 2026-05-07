import { describe, expect, it } from "vitest";
import { KvError } from "../errors.js";
import { dec, httpClient, respClient, uniqueKey } from "./harness.js";

// ── exists() ──────────────────────────────────────────────────────────────────

describe("HTTP backend — exists()", () => {
  it("returns false for a missing key", async () => {
    const kv = httpClient();
    const { data } = await kv.exists(uniqueKey());
    expect(data).toBe(false);
  });

  it("returns true for a present key", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "v");
    const { data } = await kv.exists(key);
    expect(data).toBe(true);
  });

  it("returns false after the key is deleted", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "v");
    await kv.delete(key);
    const { data } = await kv.exists(key);
    expect(data).toBe(false);
  });
});

describe("RESP backend — exists()", () => {
  it("returns false for a missing key", async () => {
    const kv = respClient();
    const { data } = await kv.exists(uniqueKey());
    expect(data).toBe(false);
    await kv.close();
  });

  it("returns true for a present key", async () => {
    const kv = respClient();
    const key = uniqueKey();
    await kv.set(key, "v");
    const { data } = await kv.exists(key);
    expect(data).toBe(true);
    await kv.close();
  });

  it("returns false after the key is deleted", async () => {
    const kv = respClient();
    const key = uniqueKey();
    await kv.set(key, "v");
    await kv.delete(key);
    const { data } = await kv.exists(key);
    expect(data).toBe(false);
    await kv.close();
  });
});

// ── getAndSet() ───────────────────────────────────────────────────────────────

describe("HTTP backend — getAndSet()", () => {
  it("returns null when the key did not exist and stores the new value", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    const { data: old } = await kv.getAndSet(key, "new");
    expect(old).toBeNull();
    expect(dec((await kv.get(key)).data!.value)).toBe("new");
  });

  it("returns the previous entry and stores the new value", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "old");
    const { data: old } = await kv.getAndSet(key, "new");
    expect(old).not.toBeNull();
    expect(dec(old!.value)).toBe("old");
    expect(dec((await kv.get(key)).data!.value)).toBe("new");
  });

  it("round-trips binary data", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    const bytes = new Uint8Array([0, 1, 127, 255]);
    await kv.set(key, bytes);
    const { data: old } = await kv.getAndSet(key, "new");
    expect(old?.value).toEqual(bytes);
  });
});

describe("RESP backend — getAndSet()", () => {
  it("returns null when the key did not exist and stores the new value", async () => {
    const kv = respClient();
    const key = uniqueKey();
    const { data: old } = await kv.getAndSet(key, "new");
    expect(old).toBeNull();
    expect(dec((await kv.get(key)).data!.value)).toBe("new");
    await kv.close();
  });

  it("returns the previous entry and stores the new value", async () => {
    const kv = respClient();
    const key = uniqueKey();
    await kv.set(key, "old");
    const { data: old } = await kv.getAndSet(key, "new");
    expect(old).not.toBeNull();
    expect(dec(old!.value)).toBe("old");
    expect(dec((await kv.get(key)).data!.value)).toBe("new");
    await kv.close();
  });

  it("round-trips binary data", async () => {
    const kv = respClient();
    const key = uniqueKey();
    const bytes = new Uint8Array([0, 1, 127, 255]);
    await kv.set(key, bytes);
    const { data: old } = await kv.getAndSet(key, "new");
    expect(old?.value).toEqual(bytes);
    await kv.close();
  });
});

// ── expire() ──────────────────────────────────────────────────────────────────

describe("HTTP backend — expire()", () => {
  it("sets a TTL on a key with ttl option", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "v");
    const { error } = await kv.expire(key, { ttl: 60 });
    expect(error).toBeUndefined();
    const { data: entry } = await kv.get(key);
    expect(entry?.ttl).toBeGreaterThan(0);
    expect(entry?.ttl).toBeLessThanOrEqual(60);
  });

  it("sets a TTL with ttl_ms option", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "v");
    await kv.expire(key, { ttlMs: 60_000 });
    const { data: entry } = await kv.get(key);
    expect(entry?.ttl).toBeGreaterThan(0);
    expect(entry?.ttl).toBeLessThanOrEqual(60);
  });

  it("sets a TTL with ttl_at option", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "v");
    const futureTs = Math.floor(Date.now() / 1000) + 60;
    await kv.expire(key, { ttlAt: futureTs });
    const { data: entry } = await kv.get(key);
    expect(entry?.ttl).toBeGreaterThan(0);
    expect(entry?.ttl).toBeLessThanOrEqual(60);
  });

  it("removes TTL with persist option", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "v", { ttl: 60 });
    await kv.expire(key, { persist: true });
    const { data: entry } = await kv.get(key);
    expect(entry?.ttl).toBeUndefined();
  });

  it("returns 404 error for a missing key", async () => {
    const kv = httpClient();
    const { error } = await kv.expire(uniqueKey(), { ttl: 60 });
    expect(error).toSatisfy(
      (e: unknown) => e instanceof KvError && (e as KvError).status === 404,
    );
  });

  it("returns null when returnValue is false (default)", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "v");
    const { data } = await kv.expire(key, { ttl: 60 });
    expect(data).toBeNull();
  });

  it("returns the current entry when returnValue is true (GETEX)", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "hello");
    const { data: entry } = await kv.expire(key, {
      ttl: 60,
      returnValue: true,
    });
    expect(entry).not.toBeNull();
    expect(dec(entry!.value)).toBe("hello");
    // TTL header reflects pre-GETEX state; key had no TTL so header is absent
  });
});

describe("RESP backend — expire()", () => {
  it("sets a TTL on a key with ttl option", async () => {
    const kv = respClient();
    const key = uniqueKey();
    await kv.set(key, "v");
    const { error } = await kv.expire(key, { ttl: 60 });
    expect(error).toBeUndefined();
    const { data: entry } = await kv.get(key);
    expect(entry?.ttl).toBeGreaterThan(0);
    expect(entry?.ttl).toBeLessThanOrEqual(60);
    await kv.close();
  });

  it("sets a TTL with ttl_ms option", async () => {
    const kv = respClient();
    const key = uniqueKey();
    await kv.set(key, "v");
    await kv.expire(key, { ttlMs: 60_000 });
    const { data: entry } = await kv.get(key);
    expect(entry?.ttl).toBeGreaterThan(0);
    expect(entry?.ttl).toBeLessThanOrEqual(60);
    await kv.close();
  });

  it("removes TTL with persist option", async () => {
    const kv = respClient();
    const key = uniqueKey();
    await kv.set(key, "v", { ttl: 60 });
    await kv.expire(key, { persist: true });
    const { data: entry } = await kv.get(key);
    expect(entry?.ttl).toBeUndefined();
    await kv.close();
  });

  it("returns 404 error for a missing key", async () => {
    const kv = respClient();
    const { error } = await kv.expire(uniqueKey(), { ttl: 60 });
    expect(error).toSatisfy(
      (e: unknown) => e instanceof KvError && (e as KvError).status === 404,
    );
    await kv.close();
  });

  it("returns null when returnValue is false (default)", async () => {
    const kv = respClient();
    const key = uniqueKey();
    await kv.set(key, "v");
    const { data } = await kv.expire(key, { ttl: 60 });
    expect(data).toBeNull();
    await kv.close();
  });

  it("returns the current entry when returnValue is true (GETEX)", async () => {
    const kv = respClient();
    const key = uniqueKey();
    await kv.set(key, "hello");
    const { data: entry } = await kv.expire(key, {
      ttl: 60,
      returnValue: true,
    });
    expect(entry).not.toBeNull();
    expect(dec(entry!.value)).toBe("hello");
    expect(entry?.ttl).toBeGreaterThan(0);
    await kv.close();
  });
});

// ── delete() with returnOld ───────────────────────────────────────────────────

describe("HTTP backend — delete() with returnOld", () => {
  it("returns null when the key does not exist", async () => {
    const kv = httpClient();
    const { data } = await kv.delete(uniqueKey(), { returnOld: true });
    expect(data).toBeNull();
  });

  it("returns the previous entry and removes the key", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "hello");
    const { data: old } = await kv.delete(key, { returnOld: true });
    expect(old).not.toBeNull();
    expect(dec(old!.value)).toBe("hello");
    expect((await kv.get(key)).data).toBeNull();
  });

  it("returned entry includes TTL", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "v", { ttl: 60 });
    const { data: old } = await kv.delete(key, { returnOld: true });
    expect(old?.ttl).toBeGreaterThan(0);
  });
});

describe("RESP backend — delete() with returnOld", () => {
  it("returns null when the key does not exist", async () => {
    const kv = respClient();
    const { data } = await kv.delete(uniqueKey(), { returnOld: true });
    expect(data).toBeNull();
    await kv.close();
  });

  it("returns the previous entry and removes the key", async () => {
    const kv = respClient();
    const key = uniqueKey();
    await kv.set(key, "hello");
    const { data: old } = await kv.delete(key, { returnOld: true });
    expect(old).not.toBeNull();
    expect(dec(old!.value)).toBe("hello");
    expect((await kv.get(key)).data).toBeNull();
    await kv.close();
  });
});

// ── count() ───────────────────────────────────────────────────────────────────

describe("HTTP backend — count()", () => {
  it("returns a non-negative integer", async () => {
    const kv = httpClient();
    const { data, error } = await kv.count();
    expect(error).toBeUndefined();
    expect(data).toBeGreaterThanOrEqual(0);
  });

  it("increases after a set", async () => {
    const kv = httpClient();
    const { data: before } = await kv.count();
    await kv.set(uniqueKey(), "a");
    await kv.set(uniqueKey(), "b");
    await kv.set(uniqueKey(), "c");
    const { data: after } = await kv.count();
    expect(after).toBeGreaterThanOrEqual(before! + 3);
  });
});

describe("RESP backend — count()", () => {
  it("returns a non-negative integer", async () => {
    const kv = respClient();
    const { data, error } = await kv.count();
    expect(error).toBeUndefined();
    expect(data).toBeGreaterThanOrEqual(0);
    await kv.close();
  });

  it("increases after a set", async () => {
    const kv = respClient();
    const { data: before } = await kv.count();
    await kv.set(uniqueKey(), "v");
    const { data: after } = await kv.count();
    expect(after).toBeGreaterThan(before!);
    await kv.close();
  });
});

// ── flush() ───────────────────────────────────────────────────────────────────

describe("HTTP backend — flush()", () => {
  it("removes all keys in the namespace", async () => {
    // Use a dedicated namespace (db6) to avoid stomping on concurrent tests.
    const kv = httpClient("db6");
    await kv.flush();
    await kv.set(uniqueKey(), "a");
    await kv.set(uniqueKey(), "b");
    const { error } = await kv.flush();
    expect(error).toBeUndefined();
    const { data } = await kv.count();
    expect(data).toBe(0);
  });

  it("is idempotent on an empty namespace", async () => {
    const kv = httpClient("db7");
    await kv.flush();
    const { error } = await kv.flush();
    expect(error).toBeUndefined();
  });
});

describe("RESP backend — flush()", () => {
  it("removes all keys in the db", async () => {
    // Use a dedicated db to avoid stomping on concurrent tests.
    const { createKvClient } = await import("../client.js");
    const { getRespUrl } = await import("./harness.js");
    const kv = createKvClient({ url: getRespUrl(), db: 5 });
    await kv.set(uniqueKey(), "a");
    await kv.set(uniqueKey(), "b");
    const { error } = await kv.flush();
    expect(error).toBeUndefined();
    const { data } = await kv.count();
    expect(data).toBe(0);
    await kv.close();
  });
});

// ── compact() ─────────────────────────────────────────────────────────────────

describe("HTTP backend — compact()", () => {
  it("returns without error", async () => {
    const kv = httpClient();
    const { error } = await kv.compact();
    expect(error).toBeUndefined();
  });
});

describe("RESP backend — compact()", () => {
  it("returns without error", async () => {
    const kv = respClient();
    const { error } = await kv.compact();
    expect(error).toBeUndefined();
    await kv.close();
  });
});

// ── mset with full options ────────────────────────────────────────────────────

describe("HTTP backend — mset with BatchSetOpts", () => {
  it("respects ttl_ms per entry", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.batchSet([{ key, value: "v", opts: { ttlMs: 60_000 } }]);
    const { data: entry } = await kv.get(key);
    expect(entry?.ttl).toBeGreaterThan(0);
    expect(entry?.ttl).toBeLessThanOrEqual(60);
  });

  it("respects metadata per entry", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    const meta = { src: "mset-test" };
    await kv.batchSet([{ key, value: "v", opts: { metadata: meta } }]);
    const { data: entry } = await kv.get(key);
    expect(entry?.metadata).toEqual(meta);
  });

  it("respects ifAbsent per entry", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "original");
    await kv.batchSet([{ key, value: "new", opts: { ifAbsent: true } }]);
    expect(dec((await kv.get(key)).data!.value)).toBe("original");
  });
});

describe("RESP backend — mset with BatchSetOpts", () => {
  it("respects ttl_ms per entry", async () => {
    const kv = respClient();
    const key = uniqueKey();
    await kv.batchSet([{ key, value: "v", opts: { ttlMs: 60_000 } }]);
    const { data: entry } = await kv.get(key);
    expect(entry?.ttl).toBeGreaterThan(0);
    expect(entry?.ttl).toBeLessThanOrEqual(60);
    await kv.close();
  });

  it("respects ifAbsent per entry", async () => {
    const kv = respClient();
    const key = uniqueKey();
    await kv.set(key, "original");
    await kv.batchSet([{ key, value: "new", opts: { ifAbsent: true } }]);
    expect(dec((await kv.get(key)).data!.value)).toBe("original");
    await kv.close();
  });

  it("mixes plain and complex entries in one call", async () => {
    const kv = respClient();
    const k1 = uniqueKey();
    const k2 = uniqueKey();
    await kv.batchSet([
      { key: k1, value: "plain" },
      { key: k2, value: "with-ttl", opts: { ttl: 60 } },
    ]);
    expect(dec((await kv.get(k1)).data!.value)).toBe("plain");
    const { data: e2 } = await kv.get(k2);
    expect(dec(e2!.value)).toBe("with-ttl");
    expect(e2?.ttl).toBeGreaterThan(0);
    await kv.close();
  });
});

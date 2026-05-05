import { describe, expect, it } from "vitest";
import { KvError } from "../errors.js";
import { dec, httpClient, respClient, uniqueKey } from "./harness.js";

// ── cas() ─────────────────────────────────────────────────────────────────────

describe("HTTP backend — cas()", () => {
  it("returns the new revision on success", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "v1");
    const { data: entry } = await kv.get(key);

    const { data: newRev, error: casErr } = await kv.cas(
      key,
      "v2",
      entry!.revision,
    );
    expect(casErr).toBeUndefined();
    expect(newRev).toBeGreaterThan(entry!.revision);
  });

  it("new revision can be used immediately in the next cas()", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "v0");
    const { data: e0 } = await kv.get(key);

    const { data: rev1, error: casErr1 } = await kv.cas(
      key,
      "v1",
      e0!.revision,
    );
    expect(casErr1).toBeUndefined();
    const { data: rev2, error: casErr2 } = await kv.cas(key, "v2", rev1!);
    expect(casErr2).toBeUndefined();
    expect(rev2).toBeGreaterThan(rev1!);
    expect(dec((await kv.get(key)).data!.value)).toBe("v2");
  });

  it("throws KvError(409) on revision mismatch", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "v1");
    const { data: entry } = await kv.get(key);
    await kv.set(key, "v2");

    const { error: casErr } = await kv.cas(key, "v3", entry!.revision);
    expect(casErr).toSatisfy(
      (e) => e instanceof KvError && e.status === 409,
    );
    expect(dec((await kv.get(key)).data!.value)).toBe("v2");
  });

  it("throws KvError(409) for a missing key", async () => {
    const kv = httpClient();
    const { error: casErr } = await kv.cas(uniqueKey(), "v", 12345);
    expect(casErr).toSatisfy(
      (e) => e instanceof KvError && e.status === 409,
    );
  });

  it("respects the ttl option on success", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "v1");
    const { data: entry } = await kv.get(key);

    await kv.cas(key, "v2", entry!.revision, { ttl: 60 });
    const { data: updated } = await kv.get(key);
    expect(updated?.ttl).toBeGreaterThan(0);
    expect(updated?.ttl).toBeLessThanOrEqual(60);
  });
});

describe("RESP backend — cas()", () => {
  it("returns the new revision on success", async () => {
    const kv = respClient();
    const key = uniqueKey();
    await kv.set(key, "v1");
    const { data: entry } = await kv.get(key);

    const { data: newRev, error: casErr } = await kv.cas(
      key,
      "v2",
      entry!.revision,
    );
    expect(casErr).toBeUndefined();
    expect(newRev).toBeGreaterThan(entry!.revision);
    await kv.close();
  });

  it("new revision can be used immediately in the next cas()", async () => {
    const kv = respClient();
    const key = uniqueKey();
    await kv.set(key, "v0");
    const { data: e0 } = await kv.get(key);

    const { data: rev1, error: casErr1 } = await kv.cas(
      key,
      "v1",
      e0!.revision,
    );
    expect(casErr1).toBeUndefined();
    const { data: rev2, error: casErr2 } = await kv.cas(key, "v2", rev1!);
    expect(casErr2).toBeUndefined();
    expect(rev2).toBeGreaterThan(rev1!);
    expect(dec((await kv.get(key)).data!.value)).toBe("v2");
    await kv.close();
  });

  it("throws KvError(409) on revision mismatch", async () => {
    const kv = respClient();
    const key = uniqueKey();
    await kv.set(key, "v1");
    const { data: entry } = await kv.get(key);
    await kv.set(key, "v2");

    const { error: casErr } = await kv.cas(key, "v3", entry!.revision);
    expect(casErr).toSatisfy(
      (e) => e instanceof KvError && e.status === 409,
    );
    expect(dec((await kv.get(key)).data!.value)).toBe("v2");
    await kv.close();
  });

  it("throws KvError(409) for a missing key", async () => {
    const kv = respClient();
    const { error: casErr } = await kv.cas(uniqueKey(), "v", 12345);
    expect(casErr).toSatisfy(
      (e) => e instanceof KvError && e.status === 409,
    );
    await kv.close();
  });
});

// ── decr() ────────────────────────────────────────────────────────────────────

describe("HTTP backend — decr()", () => {
  it("decr on missing key starts at -1", async () => {
    const kv = httpClient();
    expect((await kv.decr(uniqueKey())).data).toBe(-1);
  });

  it("decr decrements an existing value", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "10");
    expect((await kv.decr(key)).data).toBe(9);
  });

  it("decr with delta subtracts the delta", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "20");
    expect((await kv.decr(key, 7)).data).toBe(13);
  });

  it("decr is symmetric with incr", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "0");
    await kv.incr(key, 5);
    expect((await kv.decr(key, 3)).data).toBe(2);
  });
});

describe("RESP backend — decr()", () => {
  it("decr on missing key starts at -1", async () => {
    const kv = respClient();
    expect((await kv.decr(uniqueKey())).data).toBe(-1);
    await kv.close();
  });

  it("decr decrements an existing value", async () => {
    const kv = respClient();
    const key = uniqueKey();
    await kv.set(key, "10");
    expect((await kv.decr(key)).data).toBe(9);
    await kv.close();
  });

  it("decr with delta subtracts the delta", async () => {
    const kv = respClient();
    const key = uniqueKey();
    await kv.set(key, "20");
    expect((await kv.decr(key, 7)).data).toBe(13);
    await kv.close();
  });

  it("decr is symmetric with incr", async () => {
    const kv = respClient();
    const key = uniqueKey();
    await kv.set(key, "0");
    await kv.incr(key, 5);
    expect((await kv.decr(key, 3)).data).toBe(2);
    await kv.close();
  });
});

// ── keepTtl ───────────────────────────────────────────────────────────────────

describe("HTTP backend — keepTtl", () => {
  it("preserves the TTL when overwriting a key", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "v1", { ttl: 60 });
    const { data: before } = await kv.get(key);
    expect(before?.ttl).toBeGreaterThan(0);

    await kv.set(key, "v2", { keepTtl: true });
    const { data: after } = await kv.get(key);
    expect(dec(after!.value)).toBe("v2");
    expect(after?.ttl).toBeGreaterThan(0);
  });

  it("clears the TTL when keepTtl is not set", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "v1", { ttl: 60 });
    await kv.set(key, "v2");
    const { data: entry } = await kv.get(key);
    expect(entry?.ttl).toBeUndefined();
  });
});

// ── getAndDelete() ────────────────────────────────────────────────────────────

describe("HTTP backend — getAndDelete()", () => {
  it("returns null for a missing key and does not throw", async () => {
    const kv = httpClient();
    expect((await kv.getAndDelete(uniqueKey())).data).toBeNull();
  });

  it("returns the entry and removes the key", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "hello");
    const { data: entry } = await kv.getAndDelete(key);
    expect(entry).not.toBeNull();
    expect(dec(entry!.value)).toBe("hello");
    expect((await kv.get(key)).data).toBeNull();
  });

  it("returned entry includes TTL", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "v", { ttl: 60 });
    const { data: entry } = await kv.getAndDelete(key);
    expect(entry?.ttl).toBeGreaterThan(0);
    expect((await kv.get(key)).data).toBeNull();
  });

  it("returned entry includes revision", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "v1");
    const { data: before } = await kv.get(key);
    const { data: entry } = await kv.getAndDelete(key);
    expect(entry?.revision).toBe(before!.revision);
  });

  it("returned entry includes metadata", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    const meta = { source: "test" };
    await kv.set(key, "v", { metadata: meta });
    const { data: entry } = await kv.getAndDelete(key);
    expect(entry?.metadata).toEqual(meta);
  });

  it("is idempotent — second call returns null", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "v");
    await kv.getAndDelete(key);
    expect((await kv.getAndDelete(key)).data).toBeNull();
  });

  it("round-trips binary data", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    const bytes = new Uint8Array([0, 1, 127, 255]);
    await kv.set(key, bytes);
    const { data: entry } = await kv.getAndDelete(key);
    expect(entry?.value).toEqual(bytes);
  });
});

describe("RESP backend — getAndDelete()", () => {
  it("returns null for a missing key and does not throw", async () => {
    const kv = respClient();
    expect((await kv.getAndDelete(uniqueKey())).data).toBeNull();
    await kv.close();
  });

  it("returns the entry and removes the key", async () => {
    const kv = respClient();
    const key = uniqueKey();
    await kv.set(key, "hello");
    const { data: entry } = await kv.getAndDelete(key);
    expect(entry).not.toBeNull();
    expect(dec(entry!.value)).toBe("hello");
    expect((await kv.get(key)).data).toBeNull();
    await kv.close();
  });

  it("returned entry includes TTL", async () => {
    const kv = respClient();
    const key = uniqueKey();
    await kv.set(key, "v", { ttl: 60 });
    const { data: entry } = await kv.getAndDelete(key);
    expect(entry?.ttl).toBeGreaterThan(0);
    expect((await kv.get(key)).data).toBeNull();
    await kv.close();
  });

  it("returned entry includes revision", async () => {
    const kv = respClient();
    const key = uniqueKey();
    await kv.set(key, "v1");
    const { data: before } = await kv.get(key);
    const { data: entry } = await kv.getAndDelete(key);
    expect(entry?.revision).toBe(before!.revision);
    await kv.close();
  });

  it("is idempotent — second call returns null", async () => {
    const kv = respClient();
    const key = uniqueKey();
    await kv.set(key, "v");
    await kv.getAndDelete(key);
    expect((await kv.getAndDelete(key)).data).toBeNull();
    await kv.close();
  });

  it("round-trips binary data", async () => {
    const kv = respClient();
    const key = uniqueKey();
    const bytes = new Uint8Array([0, 1, 127, 255]);
    await kv.set(key, bytes);
    const { data: entry } = await kv.getAndDelete(key);
    expect(entry?.value).toEqual(bytes);
    await kv.close();
  });
});

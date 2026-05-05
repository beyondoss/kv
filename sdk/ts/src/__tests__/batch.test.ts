import { describe, expect, it } from "vitest";
import { KvError } from "../errors.js";
import { dec, httpClient, respClient, uniqueKey } from "./harness.js";

// ── HTTP batch ────────────────────────────────────────────────────────────────

describe("HTTP backend — batch", () => {
  it("executes mixed get/set/delete/incr in one call", async () => {
    const kv = httpClient();
    const k1 = uniqueKey("b");
    const k2 = uniqueKey("b");
    const k3 = uniqueKey("b");

    await kv.set(k1, "existing");
    await kv.set(k3, "10");

    const { data: results, error: batchErr } = await kv.batch(
      [
        { op: "get", key: k1 },
        { op: "set", key: k2, value: "new" },
        { op: "incr", key: k3, delta: 5 },
      ] as const,
    );
    expect(batchErr).toBeUndefined();

    expect(dec(results![0]!.value)).toBe("existing");
    expect(results![1]).toBeUndefined();
    expect(results![2]).toBe(15);
  });

  it("batch get returns null for missing key", async () => {
    const kv = httpClient();
    const { data: results, error: batchErr } = await kv.batch(
      [{ op: "get", key: uniqueKey() }] as const,
    );
    expect(batchErr).toBeUndefined();
    expect(results![0]).toBeNull();
  });

  it("batch set ifAbsent throws KvError(409) when key exists", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "original");

    const { error } = await kv.batch(
      [{ op: "set", key, value: "new", opts: { ifAbsent: true } }] as const,
    );
    expect(error).toSatisfy((e) => e instanceof KvError && e.status === 409);

    expect(dec((await kv.get(key)).data!.value)).toBe("original");
  });

  it("batch set ifPresent throws KvError(409) when key is absent", async () => {
    const kv = httpClient();
    const { error } = await kv.batch(
      [
        {
          op: "set",
          key: uniqueKey(),
          value: "v",
          opts: { ifPresent: true },
        },
      ] as const,
    );
    expect(error).toSatisfy((e) => e instanceof KvError && e.status === 409);
  });

  it("batch delete removes the key", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "v");

    await kv.batch([{ op: "delete", key }] as const);
    expect((await kv.get(key)).data).toBeNull();
  });

  it("empty batch returns empty array", async () => {
    const kv = httpClient();
    const { data: results } = await kv.batch([] as const);
    expect(results).toEqual([]);
  });
});

// ── HTTP batch: new ops & fields ──────────────────────────────────────────────

describe("HTTP backend — new batch features", () => {
  it("batch set with keepTtl preserves expiry", async () => {
    const kv = httpClient();
    const key = uniqueKey("keepttl");
    await kv.set(key, "v1", { ttl: 60 });
    const before = (await kv.get(key)).data!.ttl!;

    await kv.batch(
      [{ op: "set", key, value: "v2", opts: { keepTtl: true } }] as const,
    );

    const entry = (await kv.get(key)).data!;
    expect(dec(entry.value)).toBe("v2");
    expect(entry.ttl).toBeDefined();
    expect(entry.ttl!).toBeGreaterThan(0);
    expect(entry.ttl!).toBeLessThanOrEqual(before);
  });

  it("batch set with ttl_ms sets expiry", async () => {
    const kv = httpClient();
    const key = uniqueKey("ttlms");
    await kv.batch(
      [
        { op: "set", key, value: "v", opts: { ttl_ms: 30_000 } },
      ] as const,
    );
    const entry = (await kv.get(key)).data!;
    expect(entry.ttl).toBeDefined();
    expect(entry.ttl!).toBeGreaterThan(0);
    expect(entry.ttl!).toBeLessThanOrEqual(30);
  });

  it("batch get returns ttl_ms when key has expiry", async () => {
    const kv = httpClient();
    const key = uniqueKey("getttlms");
    await kv.set(key, "v", { ttl: 60 });

    const { data: results } = await kv.batch([{ op: "get", key }] as const);
    const entry = results![0]!;
    expect(entry.ttl_ms).toBeDefined();
    expect(entry.ttl_ms!).toBeGreaterThan(0);
    expect(entry.ttl_ms!).toBeLessThanOrEqual(60_000);
  });

  it("GET response includes ttl_ms header", async () => {
    const kv = httpClient();
    const key = uniqueKey("hdrttlms");
    await kv.set(key, "v", { ttl: 60 });
    const { data: entry } = await kv.get(key);
    expect(entry!.ttl_ms).toBeDefined();
    expect(entry!.ttl_ms!).toBeGreaterThan(0);
    expect(entry!.ttl_ms!).toBeLessThanOrEqual(60_000);
  });

  it("batch delete with returnOld returns previous entry", async () => {
    const kv = httpClient();
    const key = uniqueKey("getdel");
    await kv.set(key, "precious");

    const { data: results } = await kv.batch(
      [
        { op: "delete", key, opts: { returnOld: true } },
      ] as const,
    );

    const old = results![0] as any;
    expect(old).not.toBeNull();
    expect(dec(old.value)).toBe("precious");
    expect((await kv.get(key)).data).toBeNull();
  });

  it("batch delete with returnOld on missing key returns null", async () => {
    const kv = httpClient();
    const { data: results } = await kv.batch(
      [
        {
          op: "delete",
          key: uniqueKey("getdel-miss"),
          opts: { returnOld: true },
        },
      ] as const,
    );
    expect(results![0]).toBeNull();
  });

  it("batch exists returns true for live key", async () => {
    const kv = httpClient();
    const key = uniqueKey("ex");
    await kv.set(key, "v");

    const { data: results } = await kv.batch([{ op: "exists", key }] as const);
    expect(results![0]).toBe(true);
  });

  it("batch exists returns false for missing key", async () => {
    const kv = httpClient();
    const { data: results } = await kv.batch(
      [
        { op: "exists", key: uniqueKey("ex-miss") },
      ] as const,
    );
    expect(results![0]).toBe(false);
  });
});

// ── RESP batch ────────────────────────────────────────────────────────────────

describe("RESP backend — batch", () => {
  it("executes mixed get/set/delete/incr in one pipeline", async () => {
    const kv = respClient();
    const k1 = uniqueKey("b");
    const k2 = uniqueKey("b");
    const k3 = uniqueKey("b");

    await kv.set(k1, "existing");
    await kv.set(k3, "10");

    const { data: results, error: batchErr } = await kv.batch(
      [
        { op: "get", key: k1 },
        { op: "set", key: k2, value: "new" },
        { op: "incr", key: k3, delta: 5 },
      ] as const,
    );
    expect(batchErr).toBeUndefined();

    expect(dec(results![0]!.value)).toBe("existing");
    expect(results![1]).toBeUndefined();
    expect(results![2]).toBe(15);
    await kv.close();
  });

  it("batch get returns null for missing key", async () => {
    const kv = respClient();
    const { data: results, error: batchErr } = await kv.batch(
      [{ op: "get", key: uniqueKey() }] as const,
    );
    expect(batchErr).toBeUndefined();
    expect(results![0]).toBeNull();
    await kv.close();
  });

  // Regression: SET NX returning null was previously swallowed silently.
  it("batch set ifAbsent throws KvError(409) when key already exists", async () => {
    const kv = respClient();
    const key = uniqueKey();
    await kv.set(key, "original");

    const { error } = await kv.batch(
      [{ op: "set", key, value: "new", opts: { ifAbsent: true } }] as const,
    );
    expect(error).toSatisfy((e) => e instanceof KvError && e.status === 409);

    expect(dec((await kv.get(key)).data!.value)).toBe("original");
    await kv.close();
  });

  // Regression: SET XX returning null was previously swallowed silently.
  it("batch set ifPresent throws KvError(409) when key is absent", async () => {
    const kv = respClient();
    const { error } = await kv.batch(
      [
        {
          op: "set",
          key: uniqueKey(),
          value: "v",
          opts: { ifPresent: true },
        },
      ] as const,
    );
    expect(error).toSatisfy((e) => e instanceof KvError && e.status === 409);
    await kv.close();
  });

  it("batch set ifAbsent succeeds on a missing key", async () => {
    const kv = respClient();
    const key = uniqueKey();
    await kv.batch(
      [{ op: "set", key, value: "new", opts: { ifAbsent: true } }] as const,
    );
    expect(dec((await kv.get(key)).data!.value)).toBe("new");
    await kv.close();
  });

  it("batch set ifPresent succeeds when key exists", async () => {
    const kv = respClient();
    const key = uniqueKey();
    await kv.set(key, "old");
    await kv.batch(
      [{
        op: "set",
        key,
        value: "updated",
        opts: { ifPresent: true },
      }] as const,
    );
    expect(dec((await kv.get(key)).data!.value)).toBe("updated");
    await kv.close();
  });

  it("batch delete removes the key", async () => {
    const kv = respClient();
    const key = uniqueKey();
    await kv.set(key, "v");
    await kv.batch([{ op: "delete", key }] as const);
    expect((await kv.get(key)).data).toBeNull();
    await kv.close();
  });

  it("empty batch returns empty array", async () => {
    const kv = respClient();
    const { data: results } = await kv.batch([] as const);
    expect(results).toEqual([]);
    await kv.close();
  });
});

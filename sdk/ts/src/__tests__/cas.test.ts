import { describe, expect, it } from "vitest";
import { KvError } from "../errors.js";
import { dec, httpClient, respClient, uniqueKey } from "./harness.js";

describe("HTTP backend — CAS (compare-and-swap)", () => {
  it("get returns a non-zero revision after set", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "v1");
    const entry = await kv.get(key);
    expect(entry).not.toBeNull();
    expect(entry!.revision).toBeGreaterThan(0);
  });

  it("each write produces a larger revision", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "v1");
    const e1 = await kv.get(key);
    await kv.set(key, "v2");
    const e2 = await kv.get(key);
    expect(e2!.revision).toBeGreaterThan(e1!.revision);
  });

  it("ifMatch succeeds when revision matches", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "v1");
    const entry = await kv.get(key);

    await expect(
      kv.set(key, "v2", { ifMatch: entry!.revision }),
    ).resolves.toBeUndefined();

    const updated = await kv.get(key);
    expect(dec(updated!.value)).toBe("v2");
  });

  it("ifMatch throws KvError(409) when revision is stale", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "v1");
    const entry = await kv.get(key);

    // Advance the revision with another write.
    await kv.set(key, "v2");

    await expect(
      kv.set(key, "v3", { ifMatch: entry!.revision }),
    ).rejects.toSatisfy(
      (e) => e instanceof KvError && e.status === 409 && e.code === "conflict",
    );

    // Value must be unchanged after failed CAS.
    const current = await kv.get(key);
    expect(dec(current!.value)).toBe("v2");
  });

  it("ifMatch throws KvError(409) for a missing key", async () => {
    const kv = httpClient();
    const key = uniqueKey();

    await expect(
      kv.set(key, "v1", { ifMatch: 12345 }),
    ).rejects.toSatisfy(
      (e) => e instanceof KvError && e.status === 409,
    );

    expect(await kv.get(key)).toBeNull();
  });

  it("revision changes after successful CAS", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "v1");
    const e1 = await kv.get(key);

    await kv.set(key, "v2", { ifMatch: e1!.revision });
    const e2 = await kv.get(key);

    expect(e2!.revision).toBeGreaterThan(e1!.revision);
    expect(dec(e2!.value)).toBe("v2");
  });

  it("CAS chain: multiple sequential CAS ops all succeed", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "v0");

    for (let i = 1; i <= 5; i++) {
      const entry = await kv.get(key);
      await kv.set(key, `v${i}`, { ifMatch: entry!.revision });
    }

    const final = await kv.get(key);
    expect(dec(final!.value)).toBe("v5");
  });

  it("stale-then-retry: re-GET after failure allows next CAS to succeed", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "initial");
    const stale = await kv.get(key);

    // Advance the revision.
    await kv.set(key, "updated");

    // First attempt with stale revision fails.
    await expect(
      kv.set(key, "mine", { ifMatch: stale!.revision }),
    ).rejects.toSatisfy((e) => e instanceof KvError && e.status === 409);

    // Re-read and retry succeeds.
    const fresh = await kv.get(key);
    await expect(
      kv.set(key, "mine", { ifMatch: fresh!.revision }),
    ).resolves.toBeUndefined();

    expect(dec((await kv.get(key))!.value)).toBe("mine");
  });

  it("ifMatch with TTL sets expiry on success", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "v1");
    const entry = await kv.get(key);

    await kv.set(key, "v2", { ifMatch: entry!.revision, ttl: 60 });

    const updated = await kv.get(key);
    expect(dec(updated!.value)).toBe("v2");
    expect(updated!.ttl).toBeGreaterThan(0);
    expect(updated!.ttl).toBeLessThanOrEqual(60);
  });

  it("concurrent CAS: exactly one of N simultaneous writers wins", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "initial");
    const entry = await kv.get(key);
    const rev = entry!.revision;

    const N = 10;
    const results = await Promise.allSettled(
      Array.from(
        { length: N },
        (_, i) => kv.set(key, `writer-${i}`, { ifMatch: rev }),
      ),
    );

    const successes = results.filter((r) => r.status === "fulfilled");
    const conflicts = results.filter(
      (r) =>
        r.status === "rejected"
        && r.reason instanceof KvError
        && r.reason.status === 409,
    );

    expect(successes).toHaveLength(1);
    expect(conflicts).toHaveLength(N - 1);

    const final = await kv.get(key);
    expect(final!.revision).toBeGreaterThan(rev);
    expect(dec(final!.value)).toMatch(/^writer-\d+$/);
  });

  it("failed CAS does not change the revision", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "v1");
    const e1 = await kv.get(key);

    await kv.set(key, "v2");
    const e2 = await kv.get(key);

    await expect(
      kv.set(key, "v3", { ifMatch: e1!.revision }),
    ).rejects.toSatisfy((e) => e instanceof KvError && e.status === 409);

    const e3 = await kv.get(key);
    expect(e3!.revision).toBe(e2!.revision);
  });

  it("ifMatch fails after the key is deleted", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "v1");
    const entry = await kv.get(key);

    await kv.delete(key);

    await expect(
      kv.set(key, "v2", { ifMatch: entry!.revision }),
    ).rejects.toSatisfy((e) => e instanceof KvError && e.status === 409);

    expect(await kv.get(key)).toBeNull();
  });
});

describe("RESP backend — CAS (compare-and-swap)", () => {
  it("get returns a non-zero revision after set", async () => {
    const kv = respClient();
    const key = uniqueKey();
    await kv.set(key, "v1");
    const entry = await kv.get(key);
    expect(entry).not.toBeNull();
    expect(entry!.revision).toBeGreaterThan(0);
    await kv.close();
  });

  it("each write produces a larger revision", async () => {
    const kv = respClient();
    const key = uniqueKey();
    await kv.set(key, "v1");
    const e1 = await kv.get(key);
    await kv.set(key, "v2");
    const e2 = await kv.get(key);
    expect(e2!.revision).toBeGreaterThan(e1!.revision);
    await kv.close();
  });

  it("ifMatch succeeds when revision matches", async () => {
    const kv = respClient();
    const key = uniqueKey();
    await kv.set(key, "v1");
    const entry = await kv.get(key);

    await expect(
      kv.set(key, "v2", { ifMatch: entry!.revision }),
    ).resolves.toBeUndefined();

    const updated = await kv.get(key);
    expect(dec(updated!.value)).toBe("v2");
    await kv.close();
  });

  it("ifMatch throws KvError(409) when revision is stale", async () => {
    const kv = respClient();
    const key = uniqueKey();
    await kv.set(key, "v1");
    const entry = await kv.get(key);

    await kv.set(key, "v2");

    await expect(
      kv.set(key, "v3", { ifMatch: entry!.revision }),
    ).rejects.toSatisfy(
      (e) => e instanceof KvError && e.status === 409 && e.code === "conflict",
    );

    const current = await kv.get(key);
    expect(dec(current!.value)).toBe("v2");
    await kv.close();
  });

  it("ifMatch throws KvError(409) for a missing key", async () => {
    const kv = respClient();
    const key = uniqueKey();

    await expect(
      kv.set(key, "v1", { ifMatch: 12345 }),
    ).rejects.toSatisfy(
      (e) => e instanceof KvError && e.status === 409,
    );

    expect(await kv.get(key)).toBeNull();
    await kv.close();
  });

  it("revision changes after successful CAS", async () => {
    const kv = respClient();
    const key = uniqueKey();
    await kv.set(key, "v1");
    const e1 = await kv.get(key);

    await kv.set(key, "v2", { ifMatch: e1!.revision });
    const e2 = await kv.get(key);

    expect(e2!.revision).toBeGreaterThan(e1!.revision);
    expect(dec(e2!.value)).toBe("v2");
    await kv.close();
  });

  it("CAS chain: multiple sequential CAS ops all succeed", async () => {
    const kv = respClient();
    const key = uniqueKey();
    await kv.set(key, "v0");

    for (let i = 1; i <= 5; i++) {
      const entry = await kv.get(key);
      await kv.set(key, `v${i}`, { ifMatch: entry!.revision });
    }

    const final = await kv.get(key);
    expect(dec(final!.value)).toBe("v5");
    await kv.close();
  });

  it("stale-then-retry: re-GET after failure allows next CAS to succeed", async () => {
    const kv = respClient();
    const key = uniqueKey();
    await kv.set(key, "initial");
    const stale = await kv.get(key);

    await kv.set(key, "updated");

    await expect(
      kv.set(key, "mine", { ifMatch: stale!.revision }),
    ).rejects.toSatisfy((e) => e instanceof KvError && e.status === 409);

    const fresh = await kv.get(key);
    await expect(
      kv.set(key, "mine", { ifMatch: fresh!.revision }),
    ).resolves.toBeUndefined();

    expect(dec((await kv.get(key))!.value)).toBe("mine");
    await kv.close();
  });

  it("ifMatch with TTL sets expiry on success", async () => {
    const kv = respClient();
    const key = uniqueKey();
    await kv.set(key, "v1");
    const entry = await kv.get(key);

    await kv.set(key, "v2", { ifMatch: entry!.revision, ttl: 60 });

    const updated = await kv.get(key);
    expect(dec(updated!.value)).toBe("v2");
    expect(updated!.ttl).toBeGreaterThan(0);
    expect(updated!.ttl).toBeLessThanOrEqual(60);
    await kv.close();
  });

  it("concurrent CAS: exactly one of N simultaneous writers wins", async () => {
    const kv = respClient();
    const key = uniqueKey();
    await kv.set(key, "initial");
    const entry = await kv.get(key);
    const rev = entry!.revision;

    const N = 10;
    const results = await Promise.allSettled(
      Array.from(
        { length: N },
        (_, i) => kv.set(key, `writer-${i}`, { ifMatch: rev }),
      ),
    );

    const successes = results.filter((r) => r.status === "fulfilled");
    const conflicts = results.filter(
      (r) =>
        r.status === "rejected"
        && r.reason instanceof KvError
        && r.reason.status === 409,
    );

    expect(successes).toHaveLength(1);
    expect(conflicts).toHaveLength(N - 1);

    const final = await kv.get(key);
    expect(final!.revision).toBeGreaterThan(rev);
    expect(dec(final!.value)).toMatch(/^writer-\d+$/);
    await kv.close();
  });

  it("failed CAS does not change the revision", async () => {
    const kv = respClient();
    const key = uniqueKey();
    await kv.set(key, "v1");
    const e1 = await kv.get(key);

    await kv.set(key, "v2");
    const e2 = await kv.get(key);

    await expect(
      kv.set(key, "v3", { ifMatch: e1!.revision }),
    ).rejects.toSatisfy((e) => e instanceof KvError && e.status === 409);

    const e3 = await kv.get(key);
    expect(e3!.revision).toBe(e2!.revision);
    await kv.close();
  });

  it("ifMatch fails after the key is deleted", async () => {
    const kv = respClient();
    const key = uniqueKey();
    await kv.set(key, "v1");
    const entry = await kv.get(key);

    await kv.delete(key);

    await expect(
      kv.set(key, "v2", { ifMatch: entry!.revision }),
    ).rejects.toSatisfy((e) => e instanceof KvError && e.status === 409);

    expect(await kv.get(key)).toBeNull();
    await kv.close();
  });
});

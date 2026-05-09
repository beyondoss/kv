import { describe, expect, it } from "vitest";
import { httpClient, respClient, uniqueKey } from "./harness.js";

describe("HTTP backend — distributed lock", () => {
  it("tryLock returns a Lock handle and sets the key with TTL", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    const { data: lock, error } = await kv.tryLock(key, { ttl: 10 });
    expect(error).toBeUndefined();
    expect(lock).not.toBeNull();
    expect(typeof lock!.release).toBe("function");
    const { data: entry } = await kv.get(key);
    expect(entry).not.toBeNull();
    expect(entry!.ttl).toBeGreaterThan(0);
    await lock!.release();
  });

  it("tryLock returns null when key is already held", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    const { data: first } = await kv.tryLock(key, { ttl: 10 });
    expect(first).not.toBeNull();
    const { data: second, error } = await kv.tryLock(key, { ttl: 10 });
    expect(error).toBeUndefined();
    expect(second).toBeNull();
    await first!.release();
  });

  it("tryLock succeeds again after release", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    const { data: first } = await kv.tryLock(key, { ttl: 10 });
    await first!.release();
    const { data: second, error } = await kv.tryLock(key, { ttl: 10 });
    expect(error).toBeUndefined();
    expect(second).not.toBeNull();
    await second!.release();
  });

  it("lock() runs fn, returns its result, and releases the key", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    const { data, error } = await kv.lock(key, async () => "result");
    expect(error).toBeUndefined();
    expect(data).toBe("result");
    const { data: entry } = await kv.get(key);
    expect(entry).toBeNull();
  });

  it("lock() waits for a contended lock then acquires after release", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    const { data: held } = await kv.tryLock(key, { ttl: 10 });
    expect(held).not.toBeNull();

    let executed = false;
    const lockPromise = kv.lock(key, async () => {
      executed = true;
      return true;
    });

    // Release after a short delay so lock() can proceed.
    await new Promise<void>((r) => setTimeout(r, 150));
    await held!.release();

    const { data, error } = await lockPromise;
    expect(error).toBeUndefined();
    expect(data).toBe(true);
    expect(executed).toBe(true);
  });

  it("lock() returns a timeout error when lock is never released", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    const { data: held } = await kv.tryLock(key, { ttl: 10 });
    expect(held).not.toBeNull();

    let executed = false;
    const { data, error } = await kv.lock(
      key,
      async () => {
        executed = true;
      },
      { timeout: 300 },
    );
    expect(data).toBeUndefined();
    expect(error).toBeDefined();
    expect(error!.status).toBe(408);
    expect(error!.code).toBe("timeout");
    expect(executed).toBe(false);
    await held!.release();
  });

  it("lock release is idempotent", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    const { data: lock } = await kv.tryLock(key, { ttl: 10 });
    expect(lock).not.toBeNull();
    const r1 = await lock!.release();
    const r2 = await lock!.release();
    expect(r1.error).toBeUndefined();
    expect(r2.error).toBeUndefined();
  });

  it("lock TTL auto-expires and allows re-acquisition", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.tryLock(key, { ttl: 1 });
    // Wait for TTL to expire
    await new Promise<void>((r) => setTimeout(r, 1500));
    const { data: lock, error } = await kv.tryLock(key, { ttl: 10 });
    expect(error).toBeUndefined();
    expect(lock).not.toBeNull();
    await lock!.release();
  });
});

describe("RESP backend — distributed lock", () => {
  it("tryLock returns a Lock handle and sets the key with TTL", async () => {
    const kv = respClient();
    const key = uniqueKey();
    const { data: lock, error } = await kv.tryLock(key, { ttl: 10 });
    expect(error).toBeUndefined();
    expect(lock).not.toBeNull();
    expect(typeof lock!.release).toBe("function");
    const { data: entry } = await kv.get(key);
    expect(entry).not.toBeNull();
    expect(entry!.ttl).toBeGreaterThan(0);
    await lock!.release();
    await kv.close();
  });

  it("tryLock returns null when key is already held", async () => {
    const kv = respClient();
    const key = uniqueKey();
    const { data: first } = await kv.tryLock(key, { ttl: 10 });
    expect(first).not.toBeNull();
    const { data: second, error } = await kv.tryLock(key, { ttl: 10 });
    expect(error).toBeUndefined();
    expect(second).toBeNull();
    await first!.release();
    await kv.close();
  });

  it("tryLock succeeds again after release", async () => {
    const kv = respClient();
    const key = uniqueKey();
    const { data: first } = await kv.tryLock(key, { ttl: 10 });
    await first!.release();
    const { data: second, error } = await kv.tryLock(key, { ttl: 10 });
    expect(error).toBeUndefined();
    expect(second).not.toBeNull();
    await second!.release();
    await kv.close();
  });

  it("lock() runs fn, returns its result, and releases the key", async () => {
    const kv = respClient();
    const key = uniqueKey();
    const { data, error } = await kv.lock(key, async () => "result");
    expect(error).toBeUndefined();
    expect(data).toBe("result");
    const { data: entry } = await kv.get(key);
    expect(entry).toBeNull();
    await kv.close();
  });

  it("lock() waits for a contended lock then acquires after release", async () => {
    const kv = respClient();
    const key = uniqueKey();
    const { data: held } = await kv.tryLock(key, { ttl: 10 });
    expect(held).not.toBeNull();

    let executed = false;
    const lockPromise = kv.lock(key, async () => {
      executed = true;
      return true;
    });

    await new Promise<void>((r) => setTimeout(r, 150));
    await held!.release();

    const { data, error } = await lockPromise;
    expect(error).toBeUndefined();
    expect(data).toBe(true);
    expect(executed).toBe(true);
    await kv.close();
  });

  it("lock() returns a timeout error when lock is never released", async () => {
    const kv = respClient();
    const key = uniqueKey();
    const { data: held } = await kv.tryLock(key, { ttl: 10 });
    expect(held).not.toBeNull();

    let executed = false;
    const { data, error } = await kv.lock(
      key,
      async () => {
        executed = true;
      },
      { timeout: 300 },
    );
    expect(data).toBeUndefined();
    expect(error).toBeDefined();
    expect(error!.status).toBe(408);
    expect(error!.code).toBe("timeout");
    expect(executed).toBe(false);
    await held!.release();
    await kv.close();
  });

  it("lock release is idempotent", async () => {
    const kv = respClient();
    const key = uniqueKey();
    const { data: lock } = await kv.tryLock(key, { ttl: 10 });
    expect(lock).not.toBeNull();
    const r1 = await lock!.release();
    const r2 = await lock!.release();
    expect(r1.error).toBeUndefined();
    expect(r2.error).toBeUndefined();
    await kv.close();
  });

  it("lock TTL auto-expires and allows re-acquisition", async () => {
    const kv = respClient();
    const key = uniqueKey();
    await kv.tryLock(key, { ttl: 1 });
    await new Promise<void>((r) => setTimeout(r, 1500));
    const { data: lock, error } = await kv.tryLock(key, { ttl: 10 });
    expect(error).toBeUndefined();
    expect(lock).not.toBeNull();
    await lock!.release();
    await kv.close();
  });
});

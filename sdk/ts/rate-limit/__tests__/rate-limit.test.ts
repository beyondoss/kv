import { afterEach, describe, expect, it } from "vitest";
import { fixedWindow, slidingWindow, tokenBucket } from "../src/client.js";
import type { RateLimiter } from "../src/client.js";
import { RateLimitError } from "../src/errors.js";
import {
  httpRateLimiter,
  respRateLimiter,
  sleep,
  uniqueKey,
} from "./harness.js";

// Run each test suite against both backends.
const backends = [
  { name: "http", make: httpRateLimiter },
  { name: "resp", make: respRateLimiter },
] as const;

for (const { name, make } of backends) {
  describe(`fixed window [${name}]`, () => {
    let rl: RateLimiter;
    afterEach(() => rl.close());

    it("allows up to the limit", async () => {
      rl = make(fixedWindow({ limit: 3, window: 5_000 }));
      const key = uniqueKey();
      for (let i = 0; i < 3; i++) {
        const { data, error } = await rl.limit(key);
        expect(error).toBeUndefined();
        expect(data?.allowed).toBe(true);
      }
    });

    it("denies requests over the limit", async () => {
      rl = make(fixedWindow({ limit: 2, window: 5_000 }));
      const key = uniqueKey();
      await rl.limit(key);
      await rl.limit(key);
      const { data, error } = await rl.limit(key);
      expect(error).toBeUndefined();
      expect(data?.allowed).toBe(false);
      expect(data?.remaining).toBe(0);
      expect(data?.retryAfter).toBeGreaterThan(0);
    });

    it("decrements remaining correctly", async () => {
      rl = make(fixedWindow({ limit: 5, window: 5_000 }));
      const key = uniqueKey();
      for (let i = 5; i > 0; i--) {
        const { data } = await rl.limit(key);
        expect(data?.remaining).toBe(i - 1);
      }
    });

    it("reset timestamp is in the future", async () => {
      rl = make(fixedWindow({ limit: 5, window: 5_000 }));
      const before = Date.now();
      const { data } = await rl.limit(uniqueKey());
      expect(data?.reset).toBeGreaterThan(before);
    });

    it("resets after the window elapses", async () => {
      rl = make(fixedWindow({ limit: 1, window: 300 }));
      const key = uniqueKey();
      const first = await rl.limit(key);
      expect(first.data?.allowed).toBe(true);
      const denied = await rl.limit(key);
      expect(denied.data?.allowed).toBe(false);
      await sleep(350);
      const retry = await rl.limit(key);
      expect(retry.data?.allowed).toBe(true);
    });
  });

  describe(`sliding window [${name}]`, () => {
    let rl: RateLimiter;
    afterEach(() => rl.close());

    it("allows up to the limit", async () => {
      rl = make(slidingWindow({ limit: 3, window: 5_000 }));
      const key = uniqueKey();
      for (let i = 0; i < 3; i++) {
        const { data, error } = await rl.limit(key);
        expect(error).toBeUndefined();
        expect(data?.allowed).toBe(true);
      }
    });

    it("denies requests over the limit", async () => {
      rl = make(slidingWindow({ limit: 2, window: 5_000 }));
      const key = uniqueKey();
      await rl.limit(key);
      await rl.limit(key);
      const { data } = await rl.limit(key);
      expect(data?.allowed).toBe(false);
      expect(data?.remaining).toBe(0);
    });

    it("old counts decay as the window slides", async () => {
      rl = make(slidingWindow({ limit: 2, window: 300 }));
      const key = uniqueKey();
      // Fill the window.
      await rl.limit(key);
      await rl.limit(key);
      // Wait for over one full window so old bucket fully decays.
      await sleep(350);
      const { data } = await rl.limit(key);
      expect(data?.allowed).toBe(true);
    });
  });

  describe(`token bucket [${name}]`, () => {
    let rl: RateLimiter;
    afterEach(() => rl.close());

    it("allows up to capacity in a burst", async () => {
      rl = make(tokenBucket({ capacity: 3, refillRate: 0.1 }));
      const key = uniqueKey();
      for (let i = 0; i < 3; i++) {
        const { data, error } = await rl.limit(key);
        expect(error).toBeUndefined();
        expect(data?.allowed).toBe(true);
      }
    });

    it("denies when bucket is empty", async () => {
      rl = make(tokenBucket({ capacity: 2, refillRate: 0.1 }));
      const key = uniqueKey();
      await rl.limit(key);
      await rl.limit(key);
      const { data } = await rl.limit(key);
      expect(data?.allowed).toBe(false);
      expect(data?.remaining).toBe(0);
      expect(data?.retryAfter).toBeGreaterThan(0);
    });

    it("refills tokens over time", async () => {
      // 10 tokens/sec refill rate, capacity 1 — bucket empties instantly,
      // refills one token after ~100ms.
      rl = make(tokenBucket({ capacity: 1, refillRate: 10 }));
      const key = uniqueKey();
      const first = await rl.limit(key);
      expect(first.data?.allowed).toBe(true);
      const denied = await rl.limit(key);
      expect(denied.data?.allowed).toBe(false);
      await sleep(150);
      const refilled = await rl.limit(key);
      expect(refilled.data?.allowed).toBe(true);
    });
  });

  describe(`blockFor [${name}]`, () => {
    let rl: RateLimiter;
    afterEach(() => rl.close());

    it("resolves when the limit clears", async () => {
      rl = make(fixedWindow({ limit: 1, window: 200, delay: 50 }));
      const key = uniqueKey();
      await rl.limit(key); // consume the only slot
      const info = await rl.blockFor(key, 2_000);
      expect(info.allowed).toBe(true);
    });

    it("throws RateLimitError on timeout", async () => {
      rl = make(fixedWindow({ limit: 1, window: 10_000, delay: 50 }));
      const key = uniqueKey();
      await rl.limit(key); // consume the only slot
      await expect(rl.blockFor(key, 150)).rejects.toMatchObject({
        name: "RateLimitError",
        code: "timeout",
        key,
      });
    });

    it("throws RateLimitError instance on timeout", async () => {
      rl = make(slidingWindow({ limit: 1, window: 10_000, delay: 50 }));
      const key = uniqueKey();
      await rl.limit(key);
      const err = await rl.blockFor(key, 150).catch((e: unknown) => e);
      expect(err).toBeInstanceOf(RateLimitError);
    });
  });

  describe(`close [${name}]`, () => {
    it("can be called safely after use", async () => {
      const rl = make(fixedWindow({ limit: 5, window: 1_000 }));
      await rl.limit(uniqueKey());
      await expect(rl.close()).resolves.toBeUndefined();
    });
  });
}

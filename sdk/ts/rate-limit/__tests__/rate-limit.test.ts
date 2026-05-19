import Fastify from "fastify";
import { Hono } from "hono";
import { NextRequest } from "next/server";
import { afterEach, describe, expect, it } from "vitest";
import {
  createRateLimiter,
  fixedWindow,
  type RateLimiter,
  type RateLimitInfo,
  type RateLimitRequestEvent,
  type RateLimitResponseEvent,
  slidingWindow,
  tokenBucket,
} from "../src/client.js";
import { RateLimitError } from "../src/errors.js";
import { rateLimit as expressMiddleware } from "../src/middleware/express.js";
import { rateLimit as rateLimitPlugin } from "../src/middleware/fastify.js";
import { rateLimit as honoMiddleware } from "../src/middleware/hono.js";
import { extractIp } from "../src/middleware/ip.js";
import { withRateLimit } from "../src/middleware/next.js";
import {
  getHttpUrl,
  httpRateLimiter,
  respRateLimiter,
  sleep,
  uniqueKey,
} from "./harness.js";

// ── Middleware test helpers ───────────────────────────────────────────────────

const NOW = Date.now();

function mockLimiter(
  allowed: boolean,
  overrides: Partial<RateLimitInfo> = {},
): RateLimiter {
  const info: RateLimitInfo = allowed
    ? {
      allowed: true,
      remaining: 9,
      limit: 10,
      reset: NOW + 60_000,
      ...overrides,
    }
    : {
      allowed: false,
      remaining: 0,
      limit: 10,
      reset: NOW + 60_000,
      retryAfter: 1_000,
      ...overrides,
    };
  return {
    limit: async (_key) => ({ data: info, error: undefined }),
    blockFor: async () => {
      throw new Error("not used in middleware tests");
    },
    close: async () => {},
  };
}

function errorLimiter(): RateLimiter {
  return {
    limit: async (_key) => ({
      data: undefined,
      error: new RateLimitError("kv_error", "backend down", "k"),
    }),
    blockFor: async () => {
      throw new Error("not used");
    },
    close: async () => {},
  };
}

// ── Unit tests (no backend needed) ───────────────────────────────────────────

describe("input validation", () => {
  it("fixedWindow throws RangeError for limit < 1", () => {
    expect(() => fixedWindow({ limit: 0, window: 1_000 })).toThrow(RangeError);
    expect(() => fixedWindow({ limit: -1, window: 1_000 })).toThrow(RangeError);
  });

  it("fixedWindow throws RangeError for window <= 0", () => {
    expect(() => fixedWindow({ limit: 1, window: 0 })).toThrow(RangeError);
    expect(() => fixedWindow({ limit: 1, window: -100 })).toThrow(RangeError);
  });

  it("slidingWindow throws RangeError for limit < 1", () => {
    expect(() => slidingWindow({ limit: 0, window: 1_000 })).toThrow(
      RangeError,
    );
  });

  it("slidingWindow throws RangeError for window <= 0", () => {
    expect(() => slidingWindow({ limit: 1, window: 0 })).toThrow(RangeError);
  });

  it("tokenBucket throws RangeError for capacity < 1", () => {
    expect(() => tokenBucket({ capacity: 0, refillRate: 1 })).toThrow(
      RangeError,
    );
    expect(() => tokenBucket({ capacity: -5, refillRate: 1 })).toThrow(
      RangeError,
    );
  });

  it("tokenBucket throws RangeError for refillRate <= 0", () => {
    expect(() => tokenBucket({ capacity: 1, refillRate: 0 })).toThrow(
      RangeError,
    );
    expect(() => tokenBucket({ capacity: 1, refillRate: -1 })).toThrow(
      RangeError,
    );
  });
});

describe("RateLimitError", () => {
  it("has correct properties from constructor", () => {
    const err = new RateLimitError(
      "kv_error",
      "something went wrong",
      "user:123",
      500,
    );
    expect(err).toBeInstanceOf(Error);
    expect(err).toBeInstanceOf(RateLimitError);
    expect(err.name).toBe("RateLimitError");
    expect(err.code).toBe("kv_error");
    expect(err.key).toBe("user:123");
    expect(err.retryAfter).toBe(500);
    expect(err.message).toBe("something went wrong");
  });

  it("retryAfter is undefined when not provided", () => {
    const err = new RateLimitError("timeout", "timed out", "key:abc");
    expect(err.retryAfter).toBeUndefined();
    expect(err.code).toBe("timeout");
    expect(err.key).toBe("key:abc");
    expect(err.name).toBe("RateLimitError");
  });

  it("is an instance of Error for catch-clause compatibility", () => {
    const err = new RateLimitError("kv_error", "msg", "k");
    expect(err instanceof Error).toBe(true);
    expect(err instanceof RateLimitError).toBe(true);
  });
});

// ── HTTP-only integration tests ───────────────────────────────────────────────

describe("kv error handling", () => {
  it("limit returns { data: undefined, error: RateLimitError } on connection failure", async () => {
    const rl = createRateLimiter({
      url: "http://127.0.0.1:1",
      algorithm: fixedWindow({ limit: 5, window: 5_000 }),
      retries: 0,
    });
    const key = uniqueKey();
    const { data, error } = await rl.limit(key);
    expect(data).toBeUndefined();
    expect(error).toBeInstanceOf(RateLimitError);
    expect(error?.code).toBe("kv_error");
    expect(error?.key).toBe(key);
    await rl.close();
  });

  it("blockFor throws RateLimitError with code kv_error on connection failure", async () => {
    const rl = createRateLimiter({
      url: "http://127.0.0.1:1",
      algorithm: fixedWindow({ limit: 5, window: 5_000 }),
      retries: 0,
    });
    const err = await rl.blockFor(uniqueKey(), 500).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(RateLimitError);
    expect((err as RateLimitError).code).toBe("kv_error");
    await rl.close();
  });
});

describe("hooks", () => {
  it("onRequest fires with command='limit' for each limit call", async () => {
    const commands: string[] = [];
    const rl = createRateLimiter({
      url: getHttpUrl(),
      algorithm: fixedWindow({ limit: 5, window: 5_000 }),
      onRequest: ({ command }) => commands.push(command),
    });
    await rl.limit(uniqueKey());
    await rl.limit(uniqueKey());
    expect(commands).toEqual(["limit", "limit"]);
    await rl.close();
  });

  it("onResponse fires with correct allowed, durationMs >= 0, and command", async () => {
    const events: RateLimitResponseEvent[] = [];
    const rl = createRateLimiter({
      url: getHttpUrl(),
      algorithm: fixedWindow({ limit: 1, window: 5_000 }),
      onResponse: (e) => events.push(e),
    });
    const key = uniqueKey();
    await rl.limit(key); // allowed
    await rl.limit(key); // denied
    expect(events).toHaveLength(2);
    expect(events[0]?.command).toBe("limit");
    expect(events[0]?.allowed).toBe(true);
    expect(events[0]?.durationMs).toBeGreaterThanOrEqual(0);
    expect(events[1]?.command).toBe("limit");
    expect(events[1]?.allowed).toBe(false);
    expect(events[1]?.durationMs).toBeGreaterThanOrEqual(0);
    await rl.close();
  });

  it("onResponse fires with allowed=false when KV fails", async () => {
    const events: RateLimitResponseEvent[] = [];
    const rl = createRateLimiter({
      url: "http://127.0.0.1:1",
      algorithm: fixedWindow({ limit: 5, window: 5_000 }),
      retries: 0,
      onResponse: (e) => events.push(e),
    });
    await rl.limit(uniqueKey()); // will fail
    expect(events).toHaveLength(1);
    expect(events[0]?.allowed).toBe(false);
    await rl.close();
  });

  it("hooks fire on every poll inside blockFor", async () => {
    const requests: RateLimitRequestEvent[] = [];
    const responses: RateLimitResponseEvent[] = [];
    const rl = createRateLimiter({
      url: getHttpUrl(),
      algorithm: fixedWindow({ limit: 1, window: 200, delay: 50 }),
      onRequest: (e) => requests.push(e),
      onResponse: (e) => responses.push(e),
    });
    const key = uniqueKey();
    await rl.limit(key); // consume the only slot (fires 1 pair)
    await rl.blockFor(key, 2_000); // polls via limit() until window resets
    // blockFor polls via limit() internally, so total hook count must be > 1
    expect(requests.length).toBeGreaterThan(1);
    expect(responses.length).toBeGreaterThan(1);
    await rl.close();
  });
});

describe("keyPrefix isolation", () => {
  it("rate limiters with different keyPrefix values are independent", async () => {
    const key = uniqueKey();
    const rlA = createRateLimiter({
      url: getHttpUrl(),
      algorithm: fixedWindow({ limit: 1, window: 5_000 }),
      keyPrefix: `prefix-a-${crypto.randomUUID()}`,
    });
    const rlB = createRateLimiter({
      url: getHttpUrl(),
      algorithm: fixedWindow({ limit: 1, window: 5_000 }),
      keyPrefix: `prefix-b-${crypto.randomUUID()}`,
    });
    await rlA.limit(key);
    const { data: deniedA } = await rlA.limit(key);
    expect(deniedA?.allowed).toBe(false);
    // rlB with a different prefix should see a fresh counter.
    const { data: allowedB } = await rlB.limit(key);
    expect(allowedB?.allowed).toBe(true);
    await Promise.all([rlA.close(), rlB.close()]);
  });

  it("rate limiters sharing the same keyPrefix share counter state", async () => {
    const sharedPrefix = `shared-${crypto.randomUUID()}`;
    const key = uniqueKey();
    const rlA = createRateLimiter({
      url: getHttpUrl(),
      algorithm: fixedWindow({ limit: 1, window: 5_000 }),
      keyPrefix: sharedPrefix,
    });
    const rlB = createRateLimiter({
      url: getHttpUrl(),
      algorithm: fixedWindow({ limit: 1, window: 5_000 }),
      keyPrefix: sharedPrefix,
    });
    await rlA.limit(key); // consume via rlA
    const { data } = await rlB.limit(key); // rlB must see the same counter
    expect(data?.allowed).toBe(false);
    await Promise.all([rlA.close(), rlB.close()]);
  });
});

// ── Backend matrix tests ──────────────────────────────────────────────────────

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
      // Wait past 2*windowMs TTL so prevCount=0 regardless of elapsed position.
      await sleep(700);
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

    it("resolves when the limit clears (fixed window)", async () => {
      rl = make(fixedWindow({ limit: 1, window: 200, delay: 50 }));
      const key = uniqueKey();
      await rl.limit(key); // consume the only slot
      const info = await rl.blockFor(key, 2_000);
      expect(info.allowed).toBe(true);
    });

    it("resolves when the token bucket refills", async () => {
      // 10 tokens/sec — one token available after ~100ms.
      rl = make(tokenBucket({ capacity: 1, refillRate: 10, delay: 50 }));
      const key = uniqueKey();
      await rl.limit(key); // empty the bucket
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

    it("throws RateLimitError on timeout for token bucket", async () => {
      // 0.1 token/sec — needs 10 seconds to refill; will timeout first.
      rl = make(tokenBucket({ capacity: 1, refillRate: 0.1, delay: 50 }));
      const key = uniqueKey();
      await rl.limit(key); // empty the bucket
      await expect(rl.blockFor(key, 200)).rejects.toMatchObject({
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

    it("timeout error carries retryAfter=undefined and correct key", async () => {
      rl = make(fixedWindow({ limit: 1, window: 10_000, delay: 50 }));
      const key = uniqueKey();
      await rl.limit(key);
      const err = await rl.blockFor(key, 150).catch((e: unknown) => e);
      expect(err).toBeInstanceOf(RateLimitError);
      expect((err as RateLimitError).retryAfter).toBeUndefined();
      expect((err as RateLimitError).key).toBe(key);
      expect((err as RateLimitError).code).toBe("timeout");
    });
  });

  describe(`close [${name}]`, () => {
    it("can be called safely after use", async () => {
      const rl = make(fixedWindow({ limit: 5, window: 1_000 }));
      await rl.limit(uniqueKey());
      await expect(rl.close()).resolves.toBeUndefined();
    });
  });

  describe(`RateLimitInfo contract [${name}]`, () => {
    let rl: RateLimiter;
    afterEach(() => rl.close());

    it("fixed window: limit field equals configured limit", async () => {
      rl = make(fixedWindow({ limit: 7, window: 5_000 }));
      const { data } = await rl.limit(uniqueKey());
      expect(data?.limit).toBe(7);
    });

    it("sliding window: limit field equals configured limit", async () => {
      rl = make(slidingWindow({ limit: 4, window: 5_000 }));
      const { data } = await rl.limit(uniqueKey());
      expect(data?.limit).toBe(4);
    });

    it("token bucket: limit field equals capacity", async () => {
      rl = make(tokenBucket({ capacity: 6, refillRate: 1 }));
      const { data } = await rl.limit(uniqueKey());
      expect(data?.limit).toBe(6);
    });

    it("fixed window: denied response includes retryAfter > 0 and reset in future", async () => {
      rl = make(fixedWindow({ limit: 1, window: 5_000 }));
      const key = uniqueKey();
      const before = Date.now();
      await rl.limit(key);
      const { data } = await rl.limit(key);
      expect(data?.allowed).toBe(false);
      expect(data?.retryAfter).toBeGreaterThan(0);
      expect(data?.reset).toBeGreaterThan(before);
    });

    it("sliding window: denied response includes retryAfter > 0", async () => {
      rl = make(slidingWindow({ limit: 1, window: 5_000 }));
      const key = uniqueKey();
      await rl.limit(key);
      const { data } = await rl.limit(key);
      expect(data?.allowed).toBe(false);
      expect(data?.retryAfter).toBeGreaterThan(0);
    });

    it("token bucket: remaining decrements correctly across a burst", async () => {
      rl = make(tokenBucket({ capacity: 3, refillRate: 0.1 }));
      const key = uniqueKey();
      const r1 = await rl.limit(key);
      const r2 = await rl.limit(key);
      const r3 = await rl.limit(key);
      expect(r1.data?.remaining).toBe(2);
      expect(r2.data?.remaining).toBe(1);
      expect(r3.data?.remaining).toBe(0);
    });
  });

  describe(`retryAfter accuracy [${name}]`, () => {
    let rl: RateLimiter;
    afterEach(() => rl.close());

    // Two awaited limit() calls can straddle a window boundary on a slow
    // runner (limitFixedWindow buckets by Math.floor(now / window)), so the
    // "second request is denied" precondition is racy. Drive each limiter
    // until we observe a real denial, then verify the retryAfter sleep
    // actually unblocks. Bounded to a small max so a broken limiter still
    // fails the test loudly.
    async function untilDenied(rl: RateLimiter, key: string) {
      for (let i = 0; i < 6; i++) {
        const { data, error } = await rl.limit(key);
        if (error) throw error;
        if (!data!.allowed) return data!;
      }
      throw new Error("limiter never denied after 6 calls");
    }

    it("fixed window: sleeping retryAfter ms allows the next request", async () => {
      rl = make(fixedWindow({ limit: 1, window: 500 }));
      const key = uniqueKey();
      await rl.limit(key);
      const data = await untilDenied(rl, key);
      expect(data.retryAfter).toBeGreaterThan(0);
      await sleep(data.retryAfter! + 50);
      const { data: retry } = await rl.limit(key);
      expect(retry?.allowed).toBe(true);
    });

    it("token bucket: sleeping retryAfter ms allows the next request", async () => {
      // 5 tokens/sec → ~200ms per token; retryAfter should be ~200ms.
      rl = make(tokenBucket({ capacity: 1, refillRate: 5 }));
      const key = uniqueKey();
      await rl.limit(key); // empty the bucket
      const data = await untilDenied(rl, key);
      expect(data.retryAfter).toBeGreaterThan(0);
      await sleep(data.retryAfter! + 50);
      const { data: retry } = await rl.limit(key);
      expect(retry?.allowed).toBe(true);
    });

    it("sliding window: sleeping retryAfter ms allows the next request", async () => {
      rl = make(slidingWindow({ limit: 1, window: 500 }));
      const key = uniqueKey();
      await rl.limit(key);
      const data = await untilDenied(rl, key);
      expect(data.retryAfter).toBeGreaterThan(0);
      // Add 100ms buffer: at the bucket boundary elapsed≈0, so the weighted
      // estimate still equals prevCount; a few ms further it drops below limit.
      await sleep(data.retryAfter! + 100);
      const { data: retry } = await rl.limit(key);
      expect(retry?.allowed).toBe(true);
    });
  });

  describe(`concurrent token bucket [${name}]`, () => {
    it("allowed count never exceeds capacity under concurrent requests", async () => {
      const rl = make(tokenBucket({ capacity: 5, refillRate: 0.1 }));
      const key = uniqueKey();
      const results = await Promise.all(
        Array.from({ length: 10 }, () => rl.limit(key)),
      );
      const allowed = results.filter((r) => r.data?.allowed === true).length;
      expect(allowed).toBeLessThanOrEqual(5);
      await rl.close();
    });
  });
}

// ── extractIp ────────────────────────────────────────────────────────────────

describe("extractIp", () => {
  function headers(map: Record<string, string>) {
    return { get: (name: string) => map[name] ?? null };
  }

  it("prefers x-beyond-ip over everything", () => {
    expect(extractIp(headers({
      "x-beyond-ip": "1.2.3.4",
      "x-real-ip": "5.6.7.8",
      "x-forwarded-for": "9.10.11.12",
    }))).toBe("1.2.3.4");
  });

  it("falls back to x-real-ip", () => {
    expect(extractIp(headers({
      "x-real-ip": "5.6.7.8",
      "x-forwarded-for": "9.10.11.12",
    }))).toBe("5.6.7.8");
  });

  it("falls back to first x-forwarded-for token", () => {
    expect(extractIp(headers({ "x-forwarded-for": "9.10.11.12, 13.14.15.16" })))
      .toBe("9.10.11.12");
  });

  it("falls back to socketIp", () => {
    expect(extractIp(headers({}), "127.0.0.1")).toBe("127.0.0.1");
  });

  it("returns unknown when nothing is present", () => {
    expect(extractIp(headers({}))).toBe("unknown");
  });
});

// ── Hono middleware ───────────────────────────────────────────────────────────

describe("hono middleware", () => {
  it("allows request and sets rate limit headers", async () => {
    const app = new Hono();
    app.use(honoMiddleware(mockLimiter(true)));
    app.get("/", (c) => c.text("ok"));

    const res = await app.request("/");
    expect(res.status).toBe(200);
    expect(res.headers.get("x-ratelimit-limit")).toBe("10");
    expect(res.headers.get("x-ratelimit-remaining")).toBe("9");
    expect(res.headers.get("x-ratelimit-reset")).toBeTruthy();
  });

  it("denies request with 429 and Retry-After header", async () => {
    const app = new Hono();
    app.use(honoMiddleware(mockLimiter(false)));
    app.get("/", (c) => c.text("ok"));

    const res = await app.request("/");
    expect(res.status).toBe(429);
    expect(res.headers.get("retry-after")).toBe("1");
    const body = await res.json() as { error: string };
    expect(body.error).toBe("Too Many Requests");
  });

  it("calls custom key extractor", async () => {
    const seen: string[] = [];
    const app = new Hono();
    app.use(honoMiddleware(mockLimiter(true), {
      key: (c) => {
        seen.push(c.req.header("x-user-id") ?? "anon");
        return seen.at(-1)!;
      },
    }));
    app.get("/", (c) => c.text("ok"));

    await app.request("/", { headers: { "x-user-id": "u42" } });
    expect(seen).toEqual(["u42"]);
  });

  it("skips rate limiting when skip returns true", async () => {
    const app = new Hono();
    app.use(honoMiddleware(mockLimiter(false), { skip: () => true }));
    app.get("/", (c) => c.text("ok"));

    const res = await app.request("/");
    expect(res.status).toBe(200);
  });

  it("calls custom onDenied handler and still sets rate-limit headers", async () => {
    const app = new Hono();
    app.use(honoMiddleware(mockLimiter(false), {
      onDenied: (c, _info) => c.json({ code: "RATE_LIMITED" }, 429),
    }));
    app.get("/", (c) => c.text("ok"));

    const res = await app.request("/");
    expect(res.status).toBe(429);
    const body = await res.json() as { code: string };
    expect(body.code).toBe("RATE_LIMITED");
    expect(res.headers.get("x-ratelimit-limit")).toBe("10");
    expect(res.headers.get("retry-after")).toBe("1");
  });

  it("propagates KV errors", async () => {
    const app = new Hono();
    app.use(honoMiddleware(errorLimiter()));
    app.get("/", (c) => c.text("ok"));
    app.onError((err, c) => c.json({ error: (err as Error).message }, 500));

    const res = await app.request("/");
    expect(res.status).toBe(500);
  });
});

// ── Next.js middleware ────────────────────────────────────────────────────────

describe("next middleware", () => {
  function req(headers: Record<string, string> = {}) {
    return new NextRequest("http://localhost/api/test", { headers });
  }

  it("allows request and sets rate limit headers", async () => {
    const mw = withRateLimit(mockLimiter(true));
    const res = await mw(req(), {} as never);
    expect(res?.status).toBe(200);
    expect(res?.headers.get("x-ratelimit-limit")).toBe("10");
    expect(res?.headers.get("x-ratelimit-remaining")).toBe("9");
  });

  it("denies request with 429 and Retry-After", async () => {
    const mw = withRateLimit(mockLimiter(false));
    const res = await mw(req(), {} as never);
    expect(res?.status).toBe(429);
    expect(res?.headers.get("retry-after")).toBe("1");
  });

  it("calls custom key extractor", async () => {
    const seen: string[] = [];
    const mw = withRateLimit(mockLimiter(true), {
      key: (r) => {
        const k = r.headers.get("x-user-id") ?? "anon";
        seen.push(k);
        return k;
      },
    });
    await mw(req({ "x-user-id": "u99" }), {} as never);
    expect(seen).toEqual(["u99"]);
  });

  it("skips when skip returns true", async () => {
    const mw = withRateLimit(mockLimiter(false), { skip: () => true });
    const res = await mw(req(), {} as never);
    expect(res?.status).toBe(200);
  });

  it("uses x-beyond-ip for default key", async () => {
    let capturedKey = "";
    const limiter: RateLimiter = {
      ...mockLimiter(true),
      limit: async (k) => {
        capturedKey = k;
        return {
          data: { allowed: true, remaining: 9, limit: 10, reset: NOW + 60_000 },
          error: undefined,
        };
      },
    };
    const mw = withRateLimit(limiter);
    await mw(req({ "x-beyond-ip": "42.42.42.42" }), {} as never);
    expect(capturedKey).toBe("42.42.42.42");
  });

  it("custom onDenied still receives rate-limit headers in wrapped response", async () => {
    const mw = withRateLimit(mockLimiter(false), {
      onDenied: (_req, _info) =>
        new Response(JSON.stringify({ code: "RATE_LIMITED" }), { status: 429 }),
    });
    const res = await mw(req(), {} as never);
    expect(res?.status).toBe(429);
    expect(res?.headers.get("x-ratelimit-limit")).toBe("10");
    expect(res?.headers.get("retry-after")).toBe("1");
  });
});

// ── Fastify middleware ────────────────────────────────────────────────────────

describe("fastify plugin", () => {
  it("allows request and sets rate limit headers", async () => {
    const app = Fastify();
    await app.register(rateLimitPlugin, { limiter: mockLimiter(true) });
    app.get("/", async () => ({ ok: true }));

    const res = await app.inject({ method: "GET", url: "/" });
    expect(res.statusCode).toBe(200);
    expect(res.headers["x-ratelimit-limit"]).toBe("10");
    expect(res.headers["x-ratelimit-remaining"]).toBe("9");
  });

  it("denies request with 429 and Retry-After", async () => {
    const app = Fastify();
    await app.register(rateLimitPlugin, { limiter: mockLimiter(false) });
    app.get("/", async () => ({ ok: true }));

    const res = await app.inject({ method: "GET", url: "/" });
    expect(res.statusCode).toBe(429);
    expect(res.headers["retry-after"]).toBe("1");
    expect(res.json<{ error: string }>().error).toBe("Too Many Requests");
  });

  it("calls custom key extractor", async () => {
    const seen: string[] = [];
    const app = Fastify();
    await app.register(rateLimitPlugin, {
      limiter: mockLimiter(true),
      key: (req) => {
        const k = req.headers["x-user-id"] as string ?? "anon";
        seen.push(k);
        return k;
      },
    });
    app.get("/", async () => ({}));

    await app.inject({
      method: "GET",
      url: "/",
      headers: { "x-user-id": "u7" },
    });
    expect(seen).toEqual(["u7"]);
  });

  it("skips when skip returns true", async () => {
    const app = Fastify();
    await app.register(rateLimitPlugin, {
      limiter: mockLimiter(false),
      skip: () => true,
    });
    app.get("/", async () => ({ ok: true }));

    const res = await app.inject({ method: "GET", url: "/" });
    expect(res.statusCode).toBe(200);
  });

  it("calls custom onDenied handler", async () => {
    const app = Fastify();
    await app.register(rateLimitPlugin, {
      limiter: mockLimiter(false),
      onDenied: (_req, reply, _info) => {
        reply.code(429).send({ code: "RATE_LIMITED" });
      },
    });
    app.get("/", async () => ({}));

    const res = await app.inject({ method: "GET", url: "/" });
    expect(res.statusCode).toBe(429);
    expect(res.json<{ code: string }>().code).toBe("RATE_LIMITED");
  });
});

// ── Express middleware ────────────────────────────────────────────────────────

describe("express middleware", () => {
  type MockReq = {
    headers: Record<string, string>;
    ip: string;
  };
  type MockRes = {
    status: (code: number) => MockRes;
    json: (body: unknown) => void;
    setHeader: (name: string, value: string) => void;
    headersSent: boolean;
    _status: number;
    _body: unknown;
    _headers: Record<string, string>;
  };

  function makeReq(
    headers: Record<string, string> = {},
    ip = "127.0.0.1",
  ): MockReq {
    return { headers, ip };
  }

  function makeRes(): MockRes {
    const res: MockRes = {
      _status: 200,
      _body: null,
      _headers: {},
      headersSent: false,
      status(code) {
        res._status = code;
        return res;
      },
      json(body) {
        res._body = body;
        res.headersSent = true;
      },
      setHeader(name, value) {
        res._headers[name.toLowerCase()] = value;
      },
    };
    return res;
  }

  it("allows request and sets rate limit headers", async () => {
    const mw = expressMiddleware(mockLimiter(true));
    const req = makeReq() as never;
    const res = makeRes() as never;
    let nextCalled = false;
    await (mw as (req: never, res: never, next: () => void) => Promise<void>)(
      req,
      res,
      () => {
        nextCalled = true;
      },
    );

    expect(nextCalled).toBe(true);
    const typedRes = res as unknown as ReturnType<typeof makeRes>;
    expect(typedRes._headers["x-ratelimit-limit"]).toBe("10");
    expect(typedRes._headers["x-ratelimit-remaining"]).toBe("9");
  });

  it("denies request with 429 and Retry-After", async () => {
    const mw = expressMiddleware(mockLimiter(false));
    const req = makeReq() as never;
    const res = makeRes() as never;
    await (mw as (req: never, res: never, next: () => void) => Promise<void>)(
      req,
      res,
      () => {},
    );

    const typedRes = res as unknown as ReturnType<typeof makeRes>;
    expect(typedRes._status).toBe(429);
    expect(typedRes._headers["retry-after"]).toBe("1");
    expect((typedRes._body as { error: string }).error).toBe(
      "Too Many Requests",
    );
  });

  it("skips when skip returns true", async () => {
    const mw = expressMiddleware(mockLimiter(false), { skip: () => true });
    const req = makeReq() as never;
    const res = makeRes() as never;
    let nextCalled = false;
    await (mw as (req: never, res: never, next: () => void) => Promise<void>)(
      req,
      res,
      () => {
        nextCalled = true;
      },
    );
    expect(nextCalled).toBe(true);
  });

  it("uses x-beyond-ip for default key", async () => {
    let capturedKey = "";
    const limiter: RateLimiter = {
      ...mockLimiter(true),
      limit: async (k) => {
        capturedKey = k;
        return {
          data: { allowed: true, remaining: 9, limit: 10, reset: NOW + 60_000 },
          error: undefined,
        };
      },
    };
    const mw = expressMiddleware(limiter);
    const req = makeReq({ "x-beyond-ip": "55.55.55.55" }) as never;
    const res = makeRes() as never;
    await (mw as (req: never, res: never, next: () => void) => Promise<void>)(
      req,
      res,
      () => {},
    );
    expect(capturedKey).toBe("55.55.55.55");
  });

  it("passes errors to next(err)", async () => {
    const mw = expressMiddleware(errorLimiter());
    const req = makeReq() as never;
    const res = makeRes() as never;
    let caughtErr: unknown;
    await (mw as (
      req: never,
      res: never,
      next: (err?: unknown) => void,
    ) => Promise<void>)(req, res, (err) => {
      caughtErr = err;
    });
    expect(caughtErr).toBeInstanceOf(RateLimitError);
  });
});

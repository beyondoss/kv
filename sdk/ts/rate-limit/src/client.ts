import { createKvClient, type KvClient } from "@beyond.dev/kv";
import { limitFixedWindow } from "./algorithms/fixed-window.js";
import { limitSlidingWindow } from "./algorithms/sliding-window.js";
import { limitTokenBucket } from "./algorithms/token-bucket.js";
import { RateLimitError } from "./errors.js";

// ── Algorithm descriptors ─────────────────────────────────────────────────────

export type Algorithm =
  | { type: "fixedWindow"; limit: number; window: number; delay: number }
  | { type: "slidingWindow"; limit: number; window: number; delay: number }
  | {
    type: "tokenBucket";
    capacity: number;
    refillRate: number;
    delay: number;
  };

/** Fixed window: count requests in fixed-length time buckets. Simple, but
 *  allows 2× the limit at a window boundary. */
export function fixedWindow(opts: {
  limit: number;
  /** Window size in milliseconds. */
  window: number;
  /** Fallback poll interval (ms) for {@link RateLimiter.blockFor}. Default: 50. */
  delay?: number;
}): Algorithm {
  return {
    type: "fixedWindow",
    limit: opts.limit,
    window: opts.window,
    delay: opts.delay ?? 50,
  };
}

/** Sliding window: two-bucket weighted approximation. No burst-at-boundary
 *  problem; O(1) KV state. Recommended default. */
export function slidingWindow(opts: {
  limit: number;
  /** Window size in milliseconds. */
  window: number;
  /** Fallback poll interval (ms) for {@link RateLimiter.blockFor}. Default: 50. */
  delay?: number;
}): Algorithm {
  return {
    type: "slidingWindow",
    limit: opts.limit,
    window: opts.window,
    delay: opts.delay ?? 50,
  };
}

/** Token bucket: tokens refill at a steady rate. Allows bursts up to
 *  `capacity` while enforcing a `refillRate` req/sec sustained average. */
export function tokenBucket(opts: {
  /** Maximum burst size (tokens). */
  capacity: number;
  /** Tokens added per second (sustained request rate). */
  refillRate: number;
  /** Fallback poll interval (ms) for {@link RateLimiter.blockFor}. Default: 50. */
  delay?: number;
}): Algorithm {
  return {
    type: "tokenBucket",
    capacity: opts.capacity,
    refillRate: opts.refillRate,
    delay: opts.delay ?? 50,
  };
}

// ── Result & event types ──────────────────────────────────────────────────────

export interface RateLimitInfo {
  /** Whether this request is allowed. */
  allowed: boolean;
  /** Requests remaining in the current window (0 for token bucket when denied). */
  remaining: number;
  /** Configured limit (`capacity` for token bucket). */
  limit: number;
  /** Absolute ms timestamp when the window resets or a token becomes available. */
  reset: number;
  /** Milliseconds to wait before the next request may be allowed (when denied). */
  retryAfter?: number;
}

export type RateLimitResult = Promise<
  | { data: RateLimitInfo; error: undefined }
  | { data: undefined; error: RateLimitError }
>;

export interface RateLimitRequestEvent {
  command: string;
}

export interface RateLimitResponseEvent {
  command: string;
  durationMs: number;
  allowed: boolean;
}

// ── Client interface & options ────────────────────────────────────────────────

export interface RateLimiter {
  /** Check and record one request for `key`. Always resolves (never throws). */
  limit(key: string): RateLimitResult;
  /**
   * Block until the rate limit allows `key`, or `timeoutMs` elapses.
   * Uses `retryAfter` from the response to sleep between checks; falls back
   * to `algorithm.delay` when `retryAfter` is absent.
   *
   * @throws {RateLimitError} with `code: "timeout"` if the timeout elapses.
   */
  blockFor(key: string, timeoutMs: number): Promise<RateLimitInfo>;
  /** Release underlying KV connections. Call when the rate limiter is no longer needed. */
  close(): Promise<void>;
}

export interface RateLimiterOptions {
  /** KV backend URL — same format as `createKvClient` (`redis://` or `http://`). */
  url: string;
  algorithm: Algorithm;
  /** KV key namespace prefix. Default: `"rl"`. */
  keyPrefix?: string;
  /** Per-operation KV timeout in milliseconds. */
  timeout?: number;
  /** Max retry attempts on transient KV failures. Default: 2. */
  retries?: number;
  /** Called before each `limit` / `blockFor` invocation. */
  onRequest?: (event: RateLimitRequestEvent) => void;
  /** Called after each `limit` / `blockFor` invocation. */
  onResponse?: (event: RateLimitResponseEvent) => void;
}

// ── Factory ───────────────────────────────────────────────────────────────────

function toRateLimitError(err: unknown, key: string): RateLimitError {
  const message = err instanceof Error ? err.message : String(err);
  const rl = new RateLimitError("kv_error", message, key);
  if (err instanceof Error) {
    // Attach original as cause for stack traces.
    (rl as unknown as { cause: unknown }).cause = err;
  }
  return rl;
}

function sleep(ms: number): Promise<void> {
  return new Promise<void>((r) => setTimeout(r, ms));
}

/** Creates a distributed rate limiter backed by a beyond-kv instance. */
export function createRateLimiter(opts: RateLimiterOptions): RateLimiter {
  const kv: KvClient = createKvClient({
    url: opts.url,
    retries: opts.retries ?? 2,
    ...(opts.timeout !== undefined ? { timeout: opts.timeout } : {}),
  });
  const prefix = opts.keyPrefix ?? "rl";
  const algo = opts.algorithm;
  const { onRequest, onResponse } = opts;

  async function run(
    command: string,
    fn: () => Promise<RateLimitInfo>,
  ): Promise<RateLimitInfo> {
    onRequest?.({ command });
    const start = Date.now();
    try {
      const info = await fn();
      onResponse?.({
        command,
        durationMs: Date.now() - start,
        allowed: info.allowed,
      });
      return info;
    } catch (err) {
      onResponse?.({ command, durationMs: Date.now() - start, allowed: false });
      throw err;
    }
  }

  const limiter: RateLimiter = {
    async limit(key) {
      try {
        const info = await run("limit", () => {
          if (algo.type === "fixedWindow") {
            return limitFixedWindow(kv, key, { ...algo, prefix });
          }
          if (algo.type === "slidingWindow") {
            return limitSlidingWindow(kv, key, { ...algo, prefix });
          }
          return limitTokenBucket(kv, key, { ...algo, prefix });
        });
        return { data: info, error: undefined };
      } catch (err) {
        return { data: undefined, error: toRateLimitError(err, key) };
      }
    },

    async blockFor(key, timeoutMs) {
      const deadline = Date.now() + timeoutMs;
      while (Date.now() < deadline) {
        const { data, error } = await limiter.limit(key);
        if (error) throw error;
        if (data.allowed) return data;
        const wait = Math.min(
          data.retryAfter ?? algo.delay,
          deadline - Date.now(),
        );
        if (wait <= 0) break;
        await sleep(wait);
      }
      throw new RateLimitError(
        "timeout",
        `Rate limit for "${key}" not cleared within ${timeoutMs}ms`,
        key,
      );
    },

    close: () => kv.close(),
  };

  return limiter;
}

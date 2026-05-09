import { createRateLimiter, type RateLimiter } from "./client.js";

let _rateLimit: RateLimiter | undefined;

/**
 * Default rate limiter configured from environment variables.
 * Reads `BEYOND_KV_URL` (required), `BEYOND_RATE_LIMIT_ALGORITHM` (`"sliding"` default),
 * `BEYOND_RATE_LIMIT_LIMIT` (default `100`), and `BEYOND_RATE_LIMIT_WINDOW` (default `60000` ms).
 * Initialized lazily on first method call.
 */
export const rateLimit: RateLimiter = new Proxy({} as RateLimiter, {
  get(_, prop) {
    _rateLimit ??= createRateLimiter({});
    return (_rateLimit as unknown as Record<string | symbol, unknown>)[prop];
  },
});

export {
  type Algorithm,
  createRateLimiter,
  fixedWindow,
  type RateLimiter,
  type RateLimiterOptions,
  type RateLimitInfo,
  type RateLimitRequestEvent,
  type RateLimitResponseEvent,
  type RateLimitResult,
  slidingWindow,
  tokenBucket,
} from "./client.js";
export { RateLimitError } from "./errors.js";

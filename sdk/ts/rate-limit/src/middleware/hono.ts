import type { Context, MiddlewareHandler } from "hono";
import type { RateLimitInfo } from "../client.js";
import type { RateLimiter } from "../client.js";
import { extractIp } from "./ip.js";

export interface RateLimitMiddlewareOptions {
  /**
   * Derive the rate-limit key from the request.
   * Defaults to the client IP (`x-beyond-ip` → `x-real-ip` → `x-forwarded-for` → socket).
   */
  key?: (c: Context) => string | Promise<string>;
  /**
   * Called when the request is denied.
   * Return a `Response` to send. Defaults to `{ "error": "Too Many Requests" }` with status 429.
   */
  onDenied?: (c: Context, info: RateLimitInfo) => Response | Promise<Response>;
  /**
   * Return `true` to bypass rate limiting for this request entirely.
   */
  skip?: (c: Context) => boolean | Promise<boolean>;
}

function rateLimitHeaders(info: RateLimitInfo): Record<string, string> {
  return {
    "X-RateLimit-Limit": String(info.limit),
    "X-RateLimit-Remaining": String(info.remaining),
    "X-RateLimit-Reset": String(Math.ceil(info.reset / 1000)),
  };
}

export function rateLimitMiddleware(
  limiter: RateLimiter,
  opts: RateLimitMiddlewareOptions = {},
): MiddlewareHandler {
  const { key, onDenied, skip } = opts;

  return async (c, next) => {
    if (skip && (await skip(c))) return next();

    const k = key
      ? await key(c)
      : extractIp(c.req.raw.headers);

    const { data, error } = await limiter.limit(k);
    if (error) throw error;

    if (!data.allowed) {
      for (const [name, value] of Object.entries(rateLimitHeaders(data))) {
        c.header(name, value);
      }
      c.header("Retry-After", String(Math.ceil((data.retryAfter ?? 0) / 1000)));
      if (onDenied) return onDenied(c, data);
      return c.json({ error: "Too Many Requests" }, 429);
    }

    for (const [name, value] of Object.entries(rateLimitHeaders(data))) {
      c.header(name, value);
    }
    return next();
  };
}

export { extractIp } from "./ip.js";

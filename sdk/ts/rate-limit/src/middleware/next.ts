import type { NextMiddleware, NextRequest } from "next/server";
import { NextResponse } from "next/server";
import type { RateLimiter, RateLimitInfo } from "../client.js";
import { extractIp } from "./ip.js";

export interface RateLimitMiddlewareOptions {
  /**
   * Derive the rate-limit key from the request.
   * Defaults to the client IP (`x-beyond-ip` → `x-real-ip` → `x-forwarded-for` → socket).
   */
  key?: (req: NextRequest) => string | Promise<string>;
  /**
   * Called when the request is denied.
   * Return a `Response` to send. Defaults to `{ "error": "Too Many Requests" }` with status 429.
   */
  onDenied?: (
    req: NextRequest,
    info: RateLimitInfo,
  ) => Response | Promise<Response>;
  /**
   * Return `true` to bypass rate limiting for this request entirely.
   */
  skip?: (req: NextRequest) => boolean | Promise<boolean>;
}

function rateLimitHeaders(info: RateLimitInfo): Record<string, string> {
  return {
    "X-RateLimit-Limit": String(info.limit),
    "X-RateLimit-Remaining": String(info.remaining),
    "X-RateLimit-Reset": String(Math.ceil(info.reset / 1000)),
  };
}

/**
 * Wrap a Next.js edge middleware function with rate limiting.
 *
 * @example
 * ```ts
 * // middleware.ts
 * export default withRateLimit(limiter);
 * export const config = { matcher: ["/((?!_next|favicon.ico).*)"] };
 * ```
 *
 * NOTE: `url` in `createRateLimiter` must use `http://` — the Redis/RESP
 * client is not available in the Next.js edge runtime.
 */
export function withRateLimit(
  limiter: RateLimiter,
  opts: RateLimitMiddlewareOptions = {},
): NextMiddleware {
  const { key, onDenied, skip } = opts;

  return async (req) => {
    if (skip && (await skip(req))) return NextResponse.next();

    const k = key
      ? await key(req)
      : extractIp(req.headers);

    const { data, error } = await limiter.limit(k);
    if (error) throw error;

    if (!data.allowed) {
      const deniedHeaders = {
        ...rateLimitHeaders(data),
        "Retry-After": String(Math.ceil((data.retryAfter ?? 0) / 1000)),
      };
      if (onDenied) {
        const custom = await onDenied(req, data);
        // Copy custom response and inject rate-limit headers (don't override user-set headers)
        const wrapped = new Response(custom.body, {
          status: custom.status,
          statusText: custom.statusText,
          headers: new Headers(custom.headers),
        });
        for (const [name, value] of Object.entries(deniedHeaders)) {
          if (!wrapped.headers.has(name)) wrapped.headers.set(name, value);
        }
        return wrapped;
      }
      return NextResponse.json(
        { error: "Too Many Requests" },
        { status: 429, headers: deniedHeaders },
      );
    }

    const res = NextResponse.next();
    for (const [name, value] of Object.entries(rateLimitHeaders(data))) {
      res.headers.set(name, value);
    }
    return res;
  };
}

export { extractIp } from "./ip.js";

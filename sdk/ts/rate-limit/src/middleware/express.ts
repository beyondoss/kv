import type { NextFunction, Request, RequestHandler, Response } from "express";
import type { RateLimiter, RateLimitInfo } from "../client.js";
import { extractIp, nodeHeaders } from "./ip.js";

export interface RateLimitMiddlewareOptions {
  /**
   * Derive the rate-limit key from the request.
   * Defaults to the client IP (`x-beyond-ip` → `x-real-ip` → `x-forwarded-for` → `req.ip`).
   */
  key?: (req: Request) => string | Promise<string>;
  /**
   * Called when the request is denied.
   * Defaults to sending `{ "error": "Too Many Requests" }` with status 429.
   */
  onDenied?: (
    req: Request,
    res: Response,
    info: RateLimitInfo,
  ) => void | Promise<void>;
  /**
   * Return `true` to bypass rate limiting for this request entirely.
   */
  skip?: (req: Request) => boolean | Promise<boolean>;
}

function setRateLimitHeaders(res: Response, info: RateLimitInfo): void {
  res.setHeader("X-RateLimit-Limit", String(info.limit));
  res.setHeader("X-RateLimit-Remaining", String(info.remaining));
  res.setHeader("X-RateLimit-Reset", String(Math.ceil(info.reset / 1000)));
}

export function rateLimitMiddleware(
  limiter: RateLimiter,
  opts: RateLimitMiddlewareOptions = {},
): RequestHandler {
  const { key, onDenied, skip } = opts;

  return async (req: Request, res: Response, next: NextFunction) => {
    try {
      if (skip && (await skip(req))) return next();

      const k = key
        ? await key(req)
        : extractIp(nodeHeaders(req.headers), req.ip);

      const { data, error } = await limiter.limit(k);
      if (error) throw error;

      if (!data.allowed) {
        setRateLimitHeaders(res, data);
        res.setHeader(
          "Retry-After",
          String(Math.ceil((data.retryAfter ?? 0) / 1000)),
        );
        if (onDenied) {
          await onDenied(req, res, data);
        } else {
          res.status(429).json({ error: "Too Many Requests" });
        }
        return;
      }

      setRateLimitHeaders(res, data);
      next();
    } catch (err) {
      next(err);
    }
  };
}

export { extractIp } from "./ip.js";

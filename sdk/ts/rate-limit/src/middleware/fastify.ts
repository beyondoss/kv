import type {
  FastifyPluginCallback,
  FastifyReply,
  FastifyRequest,
} from "fastify";
import fp from "fastify-plugin";
import type { RateLimiter, RateLimitInfo } from "../client.js";
import { extractIp, nodeHeaders } from "./ip.js";

export interface RateLimitPluginOptions {
  limiter: RateLimiter;
  /**
   * Derive the rate-limit key from the request.
   * Defaults to the client IP (`x-beyond-ip` → `x-real-ip` → `x-forwarded-for` → `req.ip`).
   */
  key?: (req: FastifyRequest) => string | Promise<string>;
  /**
   * Called when the request is denied.
   * Defaults to sending `{ "error": "Too Many Requests" }` with status 429.
   */
  onDenied?: (
    req: FastifyRequest,
    reply: FastifyReply,
    info: RateLimitInfo,
  ) => void | Promise<void>;
  /**
   * Return `true` to bypass rate limiting for this request entirely.
   */
  skip?: (req: FastifyRequest) => boolean | Promise<boolean>;
}

function setRateLimitHeaders(reply: FastifyReply, info: RateLimitInfo): void {
  reply.header("X-RateLimit-Limit", String(info.limit));
  reply.header("X-RateLimit-Remaining", String(info.remaining));
  reply.header("X-RateLimit-Reset", String(Math.ceil(info.reset / 1000)));
}

const plugin: FastifyPluginCallback<RateLimitPluginOptions> = (
  fastify,
  opts,
  done,
) => {
  const { limiter, key, onDenied, skip } = opts;

  fastify.addHook("preHandler", async (req, reply) => {
    if (skip && (await skip(req))) return;

    const k = key
      ? await key(req)
      : extractIp(nodeHeaders(req.headers), req.ip);

    const { data, error } = await limiter.limit(k);
    if (error) throw error;

    if (!data.allowed) {
      setRateLimitHeaders(reply, data);
      reply.header(
        "Retry-After",
        String(Math.ceil((data.retryAfter ?? 0) / 1000)),
      );
      if (onDenied) {
        await onDenied(req, reply, data);
      } else {
        reply.code(429).send({ error: "Too Many Requests" });
      }
      return;
    }

    setRateLimitHeaders(reply, data);
  });

  done();
};

export const rateLimit = fp(plugin, {
  name: "@beyond.dev/rate-limit",
  fastify: ">=4",
});

export { extractIp } from "./ip.js";

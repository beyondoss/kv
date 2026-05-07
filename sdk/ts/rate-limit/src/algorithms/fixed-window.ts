import type { KvClient } from "@beyond.dev/kv";
import type { RateLimitInfo } from "../client.js";

export interface FixedWindowParams {
  limit: number;
  window: number;
  prefix: string;
}

export async function limitFixedWindow(
  kv: KvClient,
  key: string,
  { limit, window: windowMs, prefix }: FixedWindowParams,
): Promise<RateLimitInfo> {
  const now = Date.now();
  const bucket = Math.floor(now / windowMs);
  const kvKey = `${prefix}:fw:${key}:${bucket}`;
  const reset = (bucket + 1) * windowMs;

  const incrResult = await kv.incr(kvKey);
  if (incrResult.error) throw incrResult.error;
  const count = incrResult.data;

  if (count === 1) {
    // First request in this window — set TTL so the key auto-expires.
    const expireResult = await kv.expire(kvKey, { ttlMs: windowMs });
    if (expireResult.error) throw expireResult.error;
  }

  const remaining = Math.max(0, limit - count);
  const allowed = count <= limit;

  return {
    allowed,
    remaining,
    limit,
    reset,
    ...(allowed ? {} : { retryAfter: reset - now }),
  };
}

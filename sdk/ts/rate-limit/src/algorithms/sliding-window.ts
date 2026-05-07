import type { KvClient } from "@beyond.dev/kv";
import type { RateLimitInfo } from "../client.js";

export interface SlidingWindowParams {
  limit: number;
  window: number;
  prefix: string;
}

export async function limitSlidingWindow(
  kv: KvClient,
  key: string,
  { limit, window: windowMs, prefix }: SlidingWindowParams,
): Promise<RateLimitInfo> {
  const now = Date.now();
  const currentBucket = Math.floor(now / windowMs);
  const prevBucket = currentBucket - 1;
  const elapsed = (now % windowMs) / windowMs;

  const currentKey = `${prefix}:sw:${key}:${currentBucket}`;
  const prevKey = `${prefix}:sw:${key}:${prevBucket}`;
  const reset = (currentBucket + 1) * windowMs;

  // Read both buckets in one round-trip.
  const batchResult = await kv.batch(
    [
      { op: "get", key: currentKey },
      { op: "get", key: prevKey },
    ] as const,
  );
  if (batchResult.error) throw batchResult.error;

  const currentCount = batchResult.data[0] != null
    ? Number(batchResult.data[0].text())
    : 0;
  const prevCount = batchResult.data[1] != null
    ? Number(batchResult.data[1].text())
    : 0;

  // Weighted estimate of requests in the sliding window.
  const estimated = prevCount * (1 - elapsed) + currentCount;

  if (estimated >= limit) {
    // Time until the weighted estimate drops below limit.
    const retryAfter = prevCount > 0
      ? Math.ceil(((estimated - limit + 1) / prevCount) * windowMs)
      : reset - now;

    return {
      allowed: false,
      remaining: 0,
      limit,
      reset,
      retryAfter,
    };
  }

  // Under the limit — increment the current bucket.
  const incrResult = await kv.incr(currentKey);
  if (incrResult.error) throw incrResult.error;

  if (incrResult.data === 1) {
    // First write to this bucket — set TTL covering both current and next window.
    const expireResult = await kv.expire(currentKey, { ttlMs: windowMs * 2 });
    if (expireResult.error) throw expireResult.error;
  }

  return {
    allowed: true,
    remaining: Math.max(0, limit - Math.floor(estimated) - 1),
    limit,
    reset,
  };
}

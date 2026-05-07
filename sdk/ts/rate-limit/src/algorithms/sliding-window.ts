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

  // Increment first — atomically claim a slot. If over limit, we roll back.
  // This eliminates false-allow races: each concurrent request gets a unique
  // count value; decisions are based on that value, not a stale read.
  const incrResult = await kv.incr(currentKey);
  if (incrResult.error) throw incrResult.error;
  const newCurrentCount = incrResult.data;

  if (newCurrentCount === 1) {
    // First write to this bucket — set TTL covering current and next window.
    const expireResult = await kv.expire(currentKey, { ttlMs: windowMs * 2 });
    if (expireResult.error) throw expireResult.error;
  }

  // Read previous bucket to compute weighted estimate.
  const prevResult = await kv.get(prevKey);
  if (prevResult.error) throw prevResult.error;
  const prevCount = prevResult.data != null
    ? Number(prevResult.data.text())
    : 0;

  // Weighted estimate including this request.
  const estimated = prevCount * (1 - elapsed) + newCurrentCount;

  if (estimated > limit) {
    // Over limit — roll back our increment and deny.
    const decrResult = await kv.decr(currentKey);
    if (decrResult.error) throw decrResult.error;

    // prevCount > 0: time until the weighted estimate drops to exactly `limit`
    // in the current bucket (formula derived by solving for elapsed).
    // prevCount == 0: estimate = newCurrentCount. After rollback, storedCurrent
    // = newCurrentCount-1 becomes prevCount for bucket B+1, so estimated in B+1
    // stays above limit until B+1 itself expires. Point to the start of B+2
    // (where prevBucket=B+1 was fully rolled back and prevCount=0).
    const retryAfter = prevCount > 0
      ? Math.ceil(((estimated - limit) / prevCount) * windowMs)
      : Math.ceil(reset - now + windowMs);

    return {
      allowed: false,
      remaining: 0,
      limit,
      reset,
      retryAfter,
    };
  }

  return {
    allowed: true,
    remaining: Math.max(0, limit - Math.floor(estimated)),
    limit,
    reset,
  };
}

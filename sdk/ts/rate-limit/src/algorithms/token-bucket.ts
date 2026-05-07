import type { KvClient } from "@beyond.dev/kv";
import type { RateLimitInfo } from "../client.js";

export interface TokenBucketParams {
  capacity: number;
  refillRate: number;
  prefix: string;
}

interface BucketState {
  tokens: number;
  lastRefill: number;
}

const MAX_CAS_RETRIES = 10;

export async function limitTokenBucket(
  kv: KvClient,
  key: string,
  { capacity, refillRate, prefix }: TokenBucketParams,
): Promise<RateLimitInfo> {
  const kvKey = `${prefix}:tb:${key}`;

  for (let attempt = 0; attempt <= MAX_CAS_RETRIES; attempt++) {
    const now = Date.now();
    const getResult = await kv.get(kvKey);
    if (getResult.error) throw getResult.error;

    if (getResult.data == null) {
      // Key absent — first request ever. Initialize with full bucket minus one token.
      const state: BucketState = { tokens: capacity - 1, lastRefill: now };
      const setResult = await kv.set(kvKey, JSON.stringify(state), {
        ifAbsent: true,
      });
      if (setResult.error) {
        if (setResult.error.status === 409) continue; // another writer beat us
        throw setResult.error;
      }
      return {
        allowed: true,
        remaining: capacity - 1,
        limit: capacity,
        reset: now + Math.ceil(1000 / refillRate),
      };
    }

    const raw = getResult.data.json<BucketState>();
    const revision = getResult.data.revision;
    const elapsed = (now - raw.lastRefill) / 1000;
    const tokens = Math.min(capacity, raw.tokens + elapsed * refillRate);

    if (tokens < 1) {
      const retryAfter = Math.ceil((1 - tokens) / refillRate * 1000);
      return {
        allowed: false,
        remaining: 0,
        limit: capacity,
        reset: now + retryAfter,
        retryAfter,
      };
    }

    const newState: BucketState = { tokens: tokens - 1, lastRefill: now };
    const casResult = await kv.cas(kvKey, JSON.stringify(newState), revision);
    if (casResult.error) {
      if (casResult.error.status === 409) continue; // revision conflict — retry
      throw casResult.error;
    }

    return {
      allowed: true,
      remaining: Math.floor(tokens - 1),
      limit: capacity,
      reset: now + Math.ceil(1000 / refillRate),
    };
  }

  throw new Error(
    `token bucket CAS failed after ${MAX_CAS_RETRIES} retries for key "${key}"`,
  );
}

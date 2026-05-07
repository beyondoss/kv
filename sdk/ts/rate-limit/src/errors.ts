/**
 * Returned in the `error` field of {@link RateLimitResult}, or thrown by
 * {@link RateLimiter.blockFor} on timeout.
 *
 * @example
 * ```ts
 * const { error } = await rateLimit.limit("user:123");
 * if (error instanceof RateLimitError) {
 *   console.error(error.code, error.message);
 * }
 * ```
 */
export class RateLimitError extends Error {
  readonly code: "timeout" | "kv_error";
  readonly key: string;
  /** Milliseconds until the next request may be allowed (timeout case). */
  readonly retryAfter: number | undefined;

  constructor(
    code: "timeout" | "kv_error",
    message: string,
    key: string,
    retryAfter?: number,
  ) {
    super(message);
    this.name = "RateLimitError";
    this.code = code;
    this.key = key;
    this.retryAfter = retryAfter;
  }
}

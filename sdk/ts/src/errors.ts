/**
 * Thrown when the KV service returns a non-2xx response.
 *
 * @example
 * ```ts
 * try {
 *   await kv.getOrThrow("missing-key")
 * } catch (err) {
 *   if (err instanceof KvError) {
 *     console.error(err.code, err.message)
 *   }
 * }
 * ```
 */
export class KvError extends Error {
  readonly code: string;
  readonly status: number;

  constructor(code: string, message: string, status: number) {
    super(message);
    this.name = "KvError";
    this.code = code;
    this.status = status;
  }
}

/**
 * Thrown by `getOrThrow` when the key does not exist.
 *
 * @example
 * ```ts
 * try {
 *   const entry = await kv.getOrThrow("my-key")
 * } catch (err) {
 *   if (err instanceof KvNotFoundError) {
 *     return new Response("Not Found", { status: 404 })
 *   }
 * }
 * ```
 */
export class KvNotFoundError extends KvError {
  readonly key: string;

  constructor(key: string) {
    super("not_found", `key not found: ${key}`, 404);
    this.name = "KvNotFoundError";
    this.key = key;
  }
}

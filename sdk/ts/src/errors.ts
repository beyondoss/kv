/**
 * Returned in the `error` field when the KV service returns a non-2xx response.
 *
 * @example
 * ```ts
 * const { data, error } = await kv.get("my-key")
 * if (error instanceof KvError) {
 *   console.error(error.code, error.message)
 * }
 * ```
 */
export class KvError extends Error {
  readonly code: string;
  readonly status: number;
  readonly response: Response | undefined;
  readonly hint: string | undefined;

  constructor(
    code: string,
    message: string,
    status: number,
    response?: Response,
    hint?: string,
  ) {
    super(message);
    this.name = "KvError";
    this.code = code;
    this.status = status;
    this.response = response;
    this.hint = hint;
  }
}

/**
 * Returned in the `error` field when the requested key does not exist.
 *
 * @example
 * ```ts
 * const { data, error } = await kv.get("my-key")
 * if (error instanceof KvNotFoundError) {
 *   return new Response("Not Found", { status: 404 })
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

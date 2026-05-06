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

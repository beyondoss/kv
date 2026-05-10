/** Error thrown by the flags SDK. */
export class FlagError extends Error {
  override readonly name = "FlagError";
  /** Machine-readable code. */
  readonly code:
    | "no_context"
    | "missing_id"
    | "kv_error"
    | "invalid_state"
    | "watch_unavailable";

  constructor(
    code: FlagError["code"],
    message: string,
    options?: { cause?: unknown },
  ) {
    super(message, options as ErrorOptions);
    this.code = code;
  }
}

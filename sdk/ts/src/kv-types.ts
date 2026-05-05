const decoder = new TextDecoder();

export interface KvEntry {
  value: Uint8Array;
  /** Decode the value as a UTF-8 string. */
  text(): string;
  /** Parse the value as JSON. */
  json<T = unknown>(): T;
  /** Remaining TTL in seconds. Absent if the key has no expiry. */
  ttl?: number;
  /**
   * Arbitrary JSON metadata attached to the entry. Populated by the HTTP
   * backend only; always `undefined` when using the RESP backend.
   */
  metadata?: unknown;
  /**
   * Monotonically-increasing revision (server write timestamp in ms).
   * Use with `ifMatch` in `KvSetOptions` for compare-and-swap.
   */
  revision: number;
}

export function makeEntry(raw: {
  value: Uint8Array;
  ttl?: number;
  metadata?: unknown;
  revision: number;
}): KvEntry {
  return {
    ...raw,
    text() {
      return decoder.decode(this.value);
    },
    json<T = unknown>() {
      return JSON.parse(this.text()) as T;
    },
  };
}

export interface KvSetOptions {
  /** TTL in seconds. */
  ttl?: number;
  /**
   * Arbitrary JSON metadata to store alongside the value.
   * HTTP backend only — silently ignored by the RESP backend.
   */
  metadata?: unknown;
  /** Set only if the key does not already exist. Throws `KvError` (409) if it does. */
  ifAbsent?: boolean;
  /** Set only if the key already exists. Throws `KvError` (409) if it does not. */
  ifPresent?: boolean;
  /**
   * Compare-and-swap: only set if the current revision matches this value.
   * Throws `KvError` (409) on mismatch.
   * Obtain the current revision from `kv.get()`.
   */
  ifMatch?: number;
}

export interface KvMSetEntry {
  key: string;
  value: string | Uint8Array;
  opts?: Pick<KvSetOptions, "ttl">;
}

export interface KvListOptions {
  prefix?: string;
  /**
   * Opaque pagination cursor from a previous `list()` response. Pass the
   * value verbatim — do not construct or modify it.
   */
  cursor?: string;
  limit?: number;
}

export interface KvListResult {
  /** Keys returned for this page. Call `get()` to fetch values. */
  keys: KvListKey[];
  /**
   * Opaque cursor to pass to the next `list()` call. Absent when the scan
   * has reached the end of the keyspace.
   */
  nextCursor?: string;
}

export interface KvListKey {
  name: string;
}

/** A watch event emitted by `KvClient.watch()`. */
export type KvWatchEvent =
  | { type: "ready" }
  | {
    type: "set";
    key: string;
    value: Uint8Array;
    metadata?: unknown;
    ttl?: number;
    revision: number;
  }
  | { type: "del"; key: string; revision: number };

export interface KvWatchOptions {
  /** If true, treat `key` as a prefix and watch all matching keys. */
  prefix?: boolean;
  /** Resume from this revision (exclusive). 0 = deliver current state then live stream. */
  since?: number;
  /** Cancellation signal. */
  signal?: AbortSignal;
}

export interface KvDeleteOptions {
  /**
   * Compare-and-delete: only delete if the stored revision matches this value.
   * Throws `KvError` (409) on mismatch. HTTP backend only.
   */
  ifMatch?: number;
}

export type KvBatchOp =
  | { op: "get"; key: string }
  | { op: "set"; key: string; value: string | Uint8Array; opts?: KvSetOptions }
  | { op: "delete"; key: string; opts?: KvDeleteOptions }
  | { op: "incr"; key: string; delta?: number };

type KvBatchOpResult<T extends KvBatchOp> = T extends { op: "get" }
  ? KvEntry | null
  : T extends { op: "incr" } ? number
  : void;

export type KvBatchResults<T extends readonly KvBatchOp[]> = {
  [K in keyof T]: T[K] extends KvBatchOp ? KvBatchOpResult<T[K]> : never;
};

export interface KvEntry {
  value: Uint8Array;
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
   * HTTP backend only; `0` when using the RESP backend (use RESP `GET` + `SET … REV` directly).
   */
  revision: number;
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
  nx?: boolean;
  /** Set only if the key already exists. Throws `KvError` (409) if it does not. */
  xx?: boolean;
  /**
   * Compare-and-swap: only set if the current revision matches this value.
   * Throws `KvError` (409) on mismatch. HTTP backend only.
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
   * value verbatim — do not construct or modify it. `"0"` is the implicit
   * start cursor.
   */
  cursor?: string;
  limit?: number;
}

export interface KvListResult {
  /** Keys returned for this page. Call `get()` to fetch values. */
  keys: KvListKey[];
  /**
   * Opaque cursor for the next `list()` call. Absent when `complete` is `true`.
   */
  cursor?: string;
  /** `true` when the scan has reached the end of the keyspace. */
  complete: boolean;
}

export interface KvListKey {
  name: string;
}

export type KvWatchEventType = "set" | "del" | "ready";

export interface KvWatchEvent {
  type: KvWatchEventType;
  /** Key that changed. Absent on `"ready"` events. */
  key?: string;
  /** New value, base64-decoded. Present on `"set"` events. */
  value?: Uint8Array;
  metadata?: unknown;
  /** Remaining TTL in seconds. Present on `"set"` events when the key has a TTL. */
  ttl?: number;
  /** Revision (server timestamp ms) of the write. 0 on `"ready"` events. */
  revision: number;
}

export interface KvWatchOptions {
  /** If true, treat `key` as a prefix and watch all matching keys. */
  prefix?: boolean;
  /** Resume from this revision (exclusive). 0 = deliver current state then live stream. */
  since?: number;
  /** Cancellation signal. */
  signal?: AbortSignal;
}

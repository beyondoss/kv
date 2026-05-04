export interface KvEntry {
  value: Uint8Array;
  /** Remaining TTL in seconds. Absent if the key has no expiry. */
  ttl?: number;
  /**
   * Arbitrary JSON metadata attached to the entry. Populated by the HTTP
   * backend only; always `undefined` when using the RESP backend.
   */
  metadata?: unknown;
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

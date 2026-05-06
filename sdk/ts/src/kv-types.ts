import type { KvError } from "./errors.js";

const decoder = new TextDecoder();

export interface Entry {
  value: Uint8Array;
  /** Decode the value as a UTF-8 string. */
  text(): string;
  /** Parse the value as JSON. */
  json<T = unknown>(): T;
  /** Remaining TTL in seconds. Absent if the key has no expiry. */
  ttl?: number;
  /** Remaining TTL in milliseconds. Absent if the key has no expiry. */
  ttlMs?: number;
  /**
   * Arbitrary JSON metadata attached to the entry.
   * [HTTP only] — always `undefined` when using the RESP backend.
   */
  metadata?: unknown;
  /**
   * Monotonically-increasing revision (server write timestamp in ms).
   * Use with `ifMatch` in `SetOptions` for compare-and-swap.
   */
  revision: number;
}

export function makeEntry(raw: {
  value: Uint8Array;
  ttl?: number;
  ttlMs?: number;
  metadata?: unknown;
  revision: number;
}): Entry {
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

export interface SetOptions {
  /** TTL in seconds. */
  ttl?: number;
  /**
   * Arbitrary JSON metadata to store alongside the value.
   * [HTTP only] — silently ignored by the RESP backend.
   */
  metadata?: unknown;
  /** Set only if the key does not already exist. Returns error (409) if it does. */
  ifAbsent?: boolean;
  /** Set only if the key already exists. Returns error (409) if it does not. */
  ifPresent?: boolean;
  /**
   * Compare-and-swap: only set if the current revision matches this value.
   * Returns error (409) on mismatch.
   * Obtain the current revision from `kv.get()`.
   * Prefer `kv.cas()` over this when you need the new revision after a successful write.
   */
  ifMatch?: number;
  /**
   * Preserve the existing TTL when overwriting a key. Mutually exclusive with `ttl`.
   * [HTTP only] — silently ignored by the RESP backend.
   */
  keepTtl?: boolean;
}

export type BatchSetOpts = SetOptions & {
  /** TTL in milliseconds. Takes priority over `ttl` (seconds) when both are set. */
  ttlMs?: number;
};

/** Options for {@link KvClient.expire}. Exactly one TTL option must be supplied. */
export interface ExpiryOptions {
  /** New TTL in seconds from now. */
  ttl?: number;
  /** New TTL in milliseconds from now. */
  ttlMs?: number;
  /** Absolute expiry as a Unix timestamp in seconds. */
  ttlAt?: number;
  /** Absolute expiry as a Unix timestamp in milliseconds. */
  ttlAtMs?: number;
  /** Remove the TTL entirely. Mutually exclusive with all other options. */
  persist?: boolean;
  /**
   * Also fetch and return the current value in the same operation (GETEX semantics).
   * When `true`, the returned `Entry` contains the current value bytes.
   * When `false` (default), returns `null`.
   */
  returnValue?: boolean;
}

/** Options for {@link KvClient.getAndSet}. Mutually exclusive with conditional-write options. */
export interface GetAndSetOptions {
  /** TTL in seconds to set on the key after the swap. */
  ttl?: number;
  /**
   * Arbitrary JSON metadata to attach to the new value.
   * [HTTP only] — silently ignored by the RESP backend.
   */
  metadata?: unknown;
}

/** Options for {@link KvClient.cas}. */
export interface CasOptions {
  /** TTL in seconds. Sets a new expiry on successful write. */
  ttl?: number;
}

export interface MSetEntry {
  key: string;
  value: string | Uint8Array;
  /** Full set options per entry. TTL-only entries are batched with MSET on the RESP backend; all others use individual SET commands. */
  opts?: BatchSetOpts;
}

export interface ListOptions {
  prefix?: string;
  /**
   * Opaque pagination cursor from a previous `list()` response. Pass the
   * value verbatim — do not construct or modify it.
   */
  cursor?: string;
  limit?: number;
}

export interface ListResult {
  /** Keys returned for this page. Call `get()` to fetch values. */
  keys: ListKey[];
  /**
   * Opaque cursor to pass to the next `list()` call. Absent when the scan
   * has reached the end of the keyspace.
   */
  nextCursor?: string;
}

export interface ListKey {
  name: string;
}

/** A watch event emitted by `KvClient.watch()`. */
export type WatchEvent =
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

export interface WatchOptions {
  /** If true, treat `key` as a prefix and watch all matching keys. */
  prefix?: boolean;
  /** Resume from this revision (exclusive). 0 = deliver current state then live stream. */
  since?: number;
  /** Cancellation signal. */
  signal?: AbortSignal;
}

export interface DeleteOptions {
  /**
   * Compare-and-delete: only delete if the stored revision matches this value.
   * Returns error (409) on mismatch.
   * [HTTP only] — silently ignored by the RESP backend.
   */
  ifMatch?: number;
  /**
   * Return the previous entry before deleting. Absent entries return `null`.
   * Use the overload signature that returns `KvResult<Entry | null>`.
   */
  returnOld?: boolean;
}

export type BatchOp =
  | { op: "get"; key: string }
  | { op: "set"; key: string; value: string | Uint8Array; opts?: BatchSetOpts }
  | { op: "delete"; key: string; opts?: DeleteOptions }
  | { op: "incr"; key: string; delta?: number }
  | { op: "exists"; key: string };

type BatchOpResult<T extends BatchOp> = T extends { op: "get" } ? Entry | null
  : T extends { op: "incr" } ? number
  : T extends { op: "exists" } ? boolean
  : T extends { op: "delete"; opts: { returnOld: true } } ? Entry | null
  : void;

export type BatchResults<T extends readonly BatchOp[]> = {
  [K in keyof T]: T[K] extends BatchOp ? BatchOpResult<T[K]> : never;
};

/** Result type returned by all KvClient methods. Never throws — errors are in `error`. */
export type KvResult<T> =
  | { data: T; error: undefined }
  | { data: undefined; error: KvError };

/** Result type returned by HTTP KvClient methods. Includes the raw HTTP response. */
export type KvHttpResult<T> =
  | { data: T; error: undefined; response: Response }
  | { data: undefined; error: KvError; response: Response | undefined };

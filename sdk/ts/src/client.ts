import { createHttpKvClient } from "./http.js";
import { createRespKvClient } from "./resp.js";
import type {
  KvBatchOp,
  KvBatchResults,
  KvDeleteOptions,
  KvEntry,
  KvListOptions,
  KvListResult,
  KvMSetEntry,
  KvSetOptions,
  KvWatchEvent,
  KvWatchOptions,
} from "./types.js";

export interface KvCommandEvent {
  /** Logical command name: `"GET"`, `"SET"`, `"MGET"`, `"MSET"`, `"DEL"`, `"SCAN"`. */
  command: string;
  keyCount: number;
}

export interface KvResponseEvent {
  command: string;
  keyCount: number;
  durationMs: number;
}

/** The KvClient interface — satisfied by both the RESP and HTTP backends. */
export interface KvClient {
  get(key: string): Promise<KvEntry | null>;
  getOrThrow(key: string): Promise<KvEntry>;
  set(
    key: string,
    value: string | Uint8Array,
    opts?: KvSetOptions,
  ): Promise<void>;
  delete(key: string, opts?: KvDeleteOptions): Promise<void>;
  list(opts?: KvListOptions): Promise<KvListResult>;
  /** Fetch multiple keys in one round-trip. RESP: pipelined GET+TTL. HTTP: parallel requests. */
  mget(keys: string[]): Promise<(KvEntry | null)[]>;
  /** Set multiple entries in one round-trip. RESP: pipelined MSET/SET. HTTP: parallel requests. */
  mset(entries: KvMSetEntry[]): Promise<void>;
  /**
   * Atomically increment the integer stored at `key` by `delta` (default 1).
   * Missing keys are treated as 0. Returns the new value.
   * Throws if the stored value is not a valid integer or if the result would overflow.
   */
  incr(key: string, delta?: number): Promise<number>;
  /**
   * Execute multiple operations in one round-trip.
   * RESP backend: commands are pipelined. HTTP backend: requests run in parallel.
   * Results are returned in the same order as `ops`.
   */
  batch<T extends readonly KvBatchOp[]>(ops: T): Promise<KvBatchResults<T>>;
  /**
   * Subscribe to changes on a key or prefix.
   *
   * Yields `"ready"` once the initial state has been delivered, then streams
   * `"set"` / `"del"` events as mutations arrive. Pass `since` to resume a
   * previous stream from a known revision (catches up on any missed mutations).
   *
   * Supported on both RESP and HTTP backends.
   */
  watch(key: string, opts?: KvWatchOptions): AsyncGenerator<KvWatchEvent>;
  /** Release underlying connections. Call when the client is no longer needed. */
  close(): Promise<void>;
}

export interface KvClientOptions {
  /**
   * Server URL. Scheme determines the backend:
   * - `redis://` or `rediss://` → RESP (recommended)
   * - `http://` or `https://` → HTTP
   */
  url: string;

  // ── RESP options ────────────────────────────────────────────────────────────
  /**
   * Database number (0–15) mapping to a beyond-kv namespace.
   * 0 → `default`, 1 → `db1`, …, 15 → `db15`. Default: 0.
   * RESP backend only.
   */
  db?: number;

  // ── HTTP options ────────────────────────────────────────────────────────────
  /**
   * Namespace name. Default: `"default"`. HTTP backend only.
   */
  namespace?: string;
  /**
   * Custom `fetch` implementation for connection pooling or test mocking.
   * HTTP backend only.
   */
  fetch?: typeof globalThis.fetch;
  /**
   * Called when an `x-kv-metadata` response header cannot be parsed as JSON.
   * HTTP backend only.
   */
  onMetadataParseError?: (key: string, raw: string, err: unknown) => void;

  // ── Shared options ──────────────────────────────────────────────────────────
  /** Per-command timeout in milliseconds. */
  timeout?: number;
  /**
   * Max retry attempts on transient failures. Default: 2.
   * RESP: maps to `maxRetriesPerRequest`. HTTP: exponential backoff.
   */
  retries?: number;
  /** Called before each command. */
  onCommand?: (event: KvCommandEvent) => void;
  /** Called after each command response. */
  onResponse?: (event: KvResponseEvent) => void;
}

/** Creates a KV client. Backend is selected automatically from the URL scheme. */
export function createKvClient(opts: KvClientOptions): KvClient {
  const { protocol } = new URL(opts.url);
  if (protocol === "redis:" || protocol === "rediss:") {
    return createRespKvClient(opts);
  }
  return createHttpKvClient(opts);
}

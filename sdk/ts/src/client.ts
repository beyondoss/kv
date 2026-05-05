import createFetchClient, { type Client } from "openapi-fetch";
import { createHttpKvClient } from "./http.js";
import type {
  KvBatchOp,
  KvBatchResults,
  KvCasOptions,
  KvDeleteOptions,
  KvEntry,
  KvListOptions,
  KvListResult,
  KvMSetEntry,
  KvSetOptions,
  KvWatchEvent,
  KvWatchOptions,
} from "./kv-types.js";
import { createRespKvClient } from "./resp.js";
import type { components, paths } from "./types.js";

export type { components, paths };
export type { KvCasOptions } from "./kv-types.js";
export type { operations } from "./types.js";

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
   * Atomically decrement the integer stored at `key` by `delta` (default 1).
   * Missing keys are treated as 0. Returns the new value.
   * Throws if the stored value is not a valid integer or if the result would overflow.
   */
  decr(key: string, delta?: number): Promise<number>;
  /**
   * Compare-and-swap: atomically set `key` to `value` only if the stored revision
   * matches `revision`. Returns the new revision on success.
   * Throws `KvError` (409) if the revision does not match or the key is absent.
   *
   * Unlike `set(key, value, { ifMatch })`, `cas()` returns the new revision so you
   * can chain CAS operations without an extra `get()` round-trip.
   *
   * @example
   * ```ts
   * const entry = await kv.get("counter");
   * const newRev = await kv.cas("counter", "42", entry!.revision);
   * // newRev is the revision to use for the next CAS
   * ```
   */
  cas(
    key: string,
    value: string | Uint8Array,
    revision: number,
    opts?: KvCasOptions,
  ): Promise<number>;
  /**
   * Atomically fetch and delete `key` in a single operation.
   * Returns the entry that existed before deletion, or `null` if the key was absent.
   *
   * On the RESP backend this is a best-effort pipeline (REVISION + TTL + GETDEL)
   * rather than a single atomic command; for strict atomicity use the HTTP backend.
   */
  getAndDelete(key: string): Promise<KvEntry | null>;
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

interface KvBaseClientOptions {
  /**
   * Server URL. Scheme determines the backend:
   * - `redis://` or `rediss://` → RESP (recommended)
   * - `http://` or `https://` → HTTP
   */
  url: string;
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

/** Options for the HTTP backend (`http://` or `https://` URLs). */
export interface KvHttpClientOptions extends KvBaseClientOptions {
  /**
   * Namespace name, e.g. `"default"`, `"db1"` … `"db15"`. Default: `"default"`.
   * Maps to the `?ns=` wire param — `"default"` → 0, `"db1"` → 1, etc.
   */
  namespace?: string;
  /**
   * Custom `fetch` implementation for connection pooling or test mocking.
   */
  fetch?: typeof globalThis.fetch;
  /**
   * Called when an `x-kv-metadata` response header cannot be parsed as JSON.
   */
  onMetadataParseError?: (key: string, raw: string, err: unknown) => void;
}

/** Options for the RESP backend (`redis://` or `rediss://` URLs). */
export interface KvRespClientOptions extends KvBaseClientOptions {
  /**
   * Database number (0–15) mapping to a beyond-kv namespace.
   * 0 → `default`, 1 → `db1`, …, 15 → `db15`. Default: 0.
   */
  db?: number;
}

/** Union of HTTP and RESP options. Backend is selected from the URL scheme. */
export type KvClientOptions = KvHttpClientOptions | KvRespClientOptions;

/** Options for {@link createClient}. */
export interface KvRawClientOptions {
  /** Base URL of the KV HTTP server, e.g. `http://kv:4869`. Trailing slash is stripped. */
  baseUrl: string;
}

/**
 * Creates a fully-typed raw HTTP client for the beyond/kv REST API.
 *
 * Built on `openapi-fetch` — every path, method, query parameter, and response
 * type is inferred directly from the generated OpenAPI spec.
 */
export function createClient(opts: KvRawClientOptions): Client<paths> {
  return createFetchClient<paths>({
    baseUrl: opts.baseUrl.replace(/\/+$/, ""),
  });
}

/** Creates a KV client. Backend is selected automatically from the URL scheme. */
export function createKvClient(opts: KvClientOptions): KvClient {
  const { protocol } = new URL(opts.url);
  if (protocol === "redis:" || protocol === "rediss:") {
    return createRespKvClient(opts as KvRespClientOptions);
  }
  return createHttpKvClient(opts as KvHttpClientOptions);
}

import createFetchClient, { type Client } from "openapi-fetch";
import { env } from "std-env";
import { KvError } from "./errors.js";
import { createHttpKvClient } from "./http.js";
import type {
  BatchOp,
  BatchResults,
  BatchSetOpts,
  CasOptions,
  DeleteOptions,
  Entry,
  ExpiryOptions,
  GetAndSetOptions,
  KvHttpResult,
  KvResult,
  ListOptions,
  ListResult,
  Lock,
  LockOptions,
  MSetEntry,
  SetOptions,
  WatchEvent,
  WatchOptions,
} from "./kv-types.js";
import { createRespKvClient } from "./resp.js";
import type { components, paths } from "./types.js";

export type { components, paths };
export type {
  CasOptions,
  ExpiryOptions,
  GetAndSetOptions,
  KvHttpResult,
  KvResult,
  Lock,
  LockOptions,
} from "./kv-types.js";
export type { operations } from "./types.js";

/** Emitted before each KV command. Pass `onRequest` to {@link KvClientOptions} to subscribe. */
export interface KvRequestEvent {
  /** Logical command name: `"GET"`, `"SET"`, `"MGET"`, `"MSET"`, `"DEL"`, `"SCAN"`, `"BATCH"`. */
  command: string;
  /** Number of keys involved in the command. */
  keyCount: number;
}

/** Emitted after each KV command completes. Pass `onResponse` to {@link KvClientOptions} to subscribe. */
export interface KvResponseEvent {
  /** Logical command name: `"GET"`, `"SET"`, `"MGET"`, `"MSET"`, `"DEL"`, `"SCAN"`, `"BATCH"`. */
  command: string;
  /** Number of keys involved in the command. */
  keyCount: number;
  /** Wall-clock duration of the command in milliseconds. */
  durationMs: number;
}

/**
 * The KV client interface — satisfied by both the RESP and HTTP backends.
 *
 * Obtain a client via {@link createKvClient} (auto-selects backend from the URL scheme),
 * or use the default {@link kv} singleton configured from `BEYOND_KV_URL`.
 *
 * All methods return a `{ data, error }` result — they **never throw**. Check
 * `error` before using `data`.
 *
 * @example
 * ```ts
 * import { kv } from '@beyond.dev/kv'
 *
 * // set
 * const { error } = await kv.set('greeting', 'hello', { ttl: 60 })
 * if (error) throw error
 *
 * // get
 * const { data: entry } = await kv.get('greeting')
 * console.log(entry?.text()) // "hello"
 * ```
 */
export interface KvClient {
  /**
   * Fetch the entry stored at `key`, or `null` if the key does not exist.
   *
   * @example
   * ```ts
   * const { data: entry, error } = await kv.get('users:alice')
   * if (error) throw error
   * if (entry === null) return null
   * return entry.json<User>()
   * ```
   */
  get(key: string): Promise<KvResult<Entry | null>>;
  /**
   * Store `value` at `key`. Use `opts` to set expiry, conditional-write
   * semantics, or metadata.
   *
   * @example
   * ```ts
   * // Simple write
   * await kv.set('greeting', 'hello')
   *
   * // Write with TTL
   * await kv.set('session:abc', token, { ttl: 3600 })
   *
   * // Write only if key doesn't exist (creates atomically)
   * const { error } = await kv.set('lock:job', '1', { ifAbsent: true })
   * if (error?.status === 409) { /* already exists *\/ }
   * ```
   */
  set(
    key: string,
    value: string | Uint8Array,
    opts?: SetOptions,
  ): Promise<KvResult<void>>;
  /** Check whether `key` exists without fetching its value. */
  exists(key: string): Promise<KvResult<boolean>>;
  /** Atomically set `key` to `value` and return the entry that existed before the write, or `null` if the key was absent. */
  getAndSet(
    key: string,
    value: string | Uint8Array,
    opts?: GetAndSetOptions,
  ): Promise<KvResult<Entry | null>>;
  /**
   * Update the TTL of `key` without changing its value. Exactly one TTL option must be supplied.
   * Returns `null` when `returnValue` is false (default), or the current `Entry` when `returnValue` is true.
   * Returns a 404 error if the key does not exist.
   */
  expire(key: string, opts: ExpiryOptions): Promise<KvResult<Entry | null>>;
  /**
   * Delete `key` and return the entry that existed before deletion.
   * Returns `null` if the key was absent.
   */
  delete(
    key: string,
    opts: DeleteOptions & { returnOld: true },
  ): Promise<KvResult<Entry | null>>;
  /** Delete `key`. Idempotent — returns void whether or not the key existed. */
  delete(key: string, opts?: DeleteOptions): Promise<KvResult<void>>;
  /**
   * List keys in the namespace, optionally filtered by `prefix`.
   * Results are paginated — pass the returned `nextCursor` back as `cursor` to
   * fetch the next page. Iterate until `nextCursor` is absent.
   *
   * @example
   * ```ts
   * let cursor: string | undefined
   * do {
   *   const { data, error } = await kv.list({ prefix: 'users:', cursor })
   *   if (error) throw error
   *   for (const key of data.keys) console.log(key.name)
   *   cursor = data.nextCursor
   * } while (cursor)
   * ```
   */
  list(opts?: ListOptions): Promise<KvResult<ListResult>>;
  /** Return the total number of keys in the namespace. */
  count(): Promise<KvResult<number>>;
  /** Delete all keys in the namespace. Idempotent. */
  flush(): Promise<KvResult<void>>;
  /** Trigger a background log compaction (equivalent to BGREWRITEAOF). Returns immediately. */
  compact(): Promise<KvResult<void>>;
  /**
   * Atomically increment the integer stored at `key` by `delta` (default 1).
   * Missing keys are treated as 0. Returns the new value.
   */
  incr(key: string, delta?: number): Promise<KvResult<number>>;
  /**
   * Atomically decrement the integer stored at `key` by `delta` (default 1).
   * Missing keys are treated as 0. Returns the new value.
   */
  decr(key: string, delta?: number): Promise<KvResult<number>>;
  /**
   * Compare-and-swap: atomically set `key` to `value` only if the stored revision
   * matches `revision`. Returns the new revision on success.
   * Returns error (409) if the revision does not match or the key is absent.
   *
   * Unlike `set(key, value, { ifMatch })`, `cas()` returns the new revision so you
   * can chain CAS operations without an extra `get()` round-trip.
   *
   * @example
   * ```ts
   * const { data: entry } = await kv.get("counter");
   * const { data: newRev } = await kv.cas("counter", "42", entry!.revision);
   * ```
   */
  cas(
    key: string,
    value: string | Uint8Array,
    revision: number,
    opts?: CasOptions,
  ): Promise<KvResult<number>>;
  /**
   * Atomically fetch and delete `key` in a single operation.
   * Returns the entry that existed before deletion, or `null` if the key was absent.
   *
   * On the RESP backend this is a best-effort pipeline (REVISION + TTL + GETDEL)
   * rather than a single atomic command; for strict atomicity use the HTTP backend.
   */
  getAndDelete(key: string): Promise<KvResult<Entry | null>>;
  /**
   * Execute multiple operations in one round-trip.
   * RESP backend: commands are pipelined. HTTP backend: single batch request.
   * Results are returned in the same order as `ops`.
   */
  batch<T extends readonly BatchOp[]>(
    ops: T,
  ): Promise<KvResult<BatchResults<T>>>;
  /** Fetch multiple keys in one round-trip. RESP: pipelined GET+TTL. HTTP: batch request. */
  batchGet(keys: readonly string[]): Promise<KvResult<(Entry | null)[]>>;
  /** Set multiple entries in one round-trip. RESP: pipelined MSET/SET. HTTP: batch request. */
  batchSet(entries: MSetEntry[]): Promise<KvResult<void>>;
  /**
   * Subscribe to changes on a key or prefix.
   *
   * Yields `"ready"` once the initial state has been delivered, then streams
   * `"set"` / `"del"` events as mutations arrive. Pass `since` to resume a
   * previous stream from a known revision (catches up on any missed mutations).
   *
   * Supported on both RESP and HTTP backends.
   */
  watch(key: string, opts?: WatchOptions): AsyncGenerator<WatchEvent>;
  /**
   * Attempt to acquire a distributed lock on `key` in a single non-blocking
   * attempt. Returns a `Lock` handle if acquired, or `null` if already held.
   *
   * The lock is automatically released after `opts.ttl` seconds (default 30)
   * even if the holder crashes. Call `lock.release()` to release early.
   *
   * @see {@link lock} for the RAII callback form that waits to acquire.
   */
  tryLock(key: string, opts?: LockOptions): Promise<KvResult<Lock | null>>;
  /**
   * Acquire a distributed lock on `key`, execute `fn` while holding it, then
   * release automatically — even if `fn` throws.
   *
   * Waits efficiently via `watch()` (no busy-polling) until the lock becomes
   * available. Use `opts.timeout` (ms) to set a deadline.
   *
   * @example
   * ```ts
   * const { data, error } = await kv.lock("job:123", async () => {
   *   return doExclusiveWork();
   * });
   * ```
   */
  lock<T>(
    key: string,
    fn: () => Promise<T>,
    opts?: LockOptions,
  ): Promise<KvResult<T>>;
  /** Release underlying connections. Call when the client is no longer needed. */
  close(): Promise<void>;
}

/** HTTP KvClient — same as {@link KvClient} but every result includes the raw HTTP `response`. */
export interface KvHttpClient extends KvClient {
  get(key: string): Promise<KvHttpResult<Entry | null>>;
  set(
    key: string,
    value: string | Uint8Array,
    opts?: SetOptions,
  ): Promise<KvHttpResult<void>>;
  exists(key: string): Promise<KvHttpResult<boolean>>;
  getAndSet(
    key: string,
    value: string | Uint8Array,
    opts?: GetAndSetOptions,
  ): Promise<KvHttpResult<Entry | null>>;
  expire(key: string, opts: ExpiryOptions): Promise<KvHttpResult<Entry | null>>;
  delete(
    key: string,
    opts: DeleteOptions & { returnOld: true },
  ): Promise<KvHttpResult<Entry | null>>;
  delete(key: string, opts?: DeleteOptions): Promise<KvHttpResult<void>>;
  list(opts?: ListOptions): Promise<KvHttpResult<ListResult>>;
  count(): Promise<KvHttpResult<number>>;
  flush(): Promise<KvHttpResult<void>>;
  compact(): Promise<KvHttpResult<void>>;
  batchGet(keys: readonly string[]): Promise<KvHttpResult<(Entry | null)[]>>;
  batchSet(entries: MSetEntry[]): Promise<KvHttpResult<void>>;
  incr(key: string, delta?: number): Promise<KvHttpResult<number>>;
  decr(key: string, delta?: number): Promise<KvHttpResult<number>>;
  cas(
    key: string,
    value: string | Uint8Array,
    revision: number,
    opts?: CasOptions,
  ): Promise<KvHttpResult<number>>;
  getAndDelete(key: string): Promise<KvHttpResult<Entry | null>>;
  batch<T extends readonly BatchOp[]>(
    ops: T,
  ): Promise<KvHttpResult<BatchResults<T>>>;
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
  /** Called before each request. */
  onRequest?: (event: KvRequestEvent) => void;
  /** Called after each response. */
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

/**
 * Schema object with a `parse` method — works with Zod, ArkType, or any library
 * that exposes `parse(input: unknown): T`. For Valibot wrap with
 * `{ parse: (v) => v.parse(schema, v) }`.
 */
export interface KvSchema<T> {
  parse(input: unknown): T;
}

/** A record mapping glob patterns (supporting `*`) to value schemas. */
export type KvSchemaMap = Record<string, KvSchema<unknown>>;

// ── Type-level glob matching ──────────────────────────────────────────────────

/** True when string literal K matches glob pattern P (single `*` wildcard). */
type GlobMatch<K extends string, P extends string> = P extends
  `${infer Pre}*${infer Suf}` ? K extends `${Pre}${string}${Suf}` ? true : false
  : K extends P ? true
  : false;

/** The first pattern key in Map that K matches, or never. */
type MatchedPattern<K extends string, Map extends KvSchemaMap> = {
  [P in keyof Map & string]: GlobMatch<K, P> extends true ? P : never;
}[keyof Map & string];

/** Infer the parsed value type for key K from Map. Falls back to Entry when no pattern matches. */
export type KvSchemaType<K extends string, Map extends KvSchemaMap> =
  [MatchedPattern<K, Map>] extends [never] ? Entry
    : Map[MatchedPattern<K, Map> & keyof Map] extends KvSchema<infer T> ? T
    : Entry;

type SetValue<K extends string, Map extends KvSchemaMap> =
  [MatchedPattern<K, Map>] extends [never] ? string | Uint8Array
    : KvSchemaType<K, Map>;

/** Watch event with value typed by schema for key K. */
export type SchemaAwareWatchEvent<K extends string, Map extends KvSchemaMap> =
  | { type: "ready" }
  | {
    type: "set";
    key: string;
    value: KvSchemaType<K, Map>;
    metadata?: unknown;
    ttl?: number;
    revision: number;
  }
  | { type: "del"; key: string; revision: number };

/** Batch op result typed by schema — get ops return schema type, others unchanged. */
type SchemaAwareBatchOpResult<T extends BatchOp, Map extends KvSchemaMap> =
  T extends { op: "get"; key: infer K extends string }
    ? KvSchemaType<K, Map> | null
    : T extends { op: "incr" } ? number
    : T extends { op: "exists" } ? boolean
    : T extends { op: "delete"; opts: { returnOld: true } } ? Entry | null
    : void;

export type SchemaAwareBatchResults<
  T extends readonly BatchOp[],
  Map extends KvSchemaMap,
> = {
  [K in keyof T]: T[K] extends BatchOp ? SchemaAwareBatchOpResult<T[K], Map>
    : never;
};

/**
 * Typed KV client — same as {@link KvClient} but `get`, `set`, `batchGet`,
 * `batchSet`, and `batch` are typed per the schema map. Keys matching a glob
 * pattern return/accept the schema's type; unmatched keys fall back to
 * `Entry` / `string | Uint8Array`.
 */
export interface KvSchemaClient<Map extends KvSchemaMap> extends
  Omit<
    KvClient,
    | "get"
    | "set"
    | "getAndSet"
    | "getAndDelete"
    | "delete"
    | "cas"
    | "batchGet"
    | "batchSet"
    | "batch"
    | "watch"
  >
{
  get<K extends string>(key: K): Promise<KvResult<KvSchemaType<K, Map> | null>>;
  set<K extends string>(
    key: K,
    value: SetValue<K, Map>,
    opts?: SetOptions,
  ): Promise<KvResult<void>>;
  getAndSet<K extends string>(
    key: K,
    value: SetValue<K, Map>,
    opts?: GetAndSetOptions,
  ): Promise<KvResult<KvSchemaType<K, Map> | null>>;
  getAndDelete<K extends string>(
    key: K,
  ): Promise<KvResult<KvSchemaType<K, Map> | null>>;
  delete<K extends string>(
    key: K,
    opts: DeleteOptions & { returnOld: true },
  ): Promise<KvResult<KvSchemaType<K, Map> | null>>;
  delete<K extends string>(
    key: K,
    opts?: DeleteOptions,
  ): Promise<KvResult<void>>;
  cas<K extends string>(
    key: K,
    value: SetValue<K, Map>,
    revision: number,
    opts?: CasOptions,
  ): Promise<KvResult<number>>;
  batchGet<const Keys extends readonly string[]>(
    keys: Keys,
  ): Promise<
    KvResult<{ [K in keyof Keys]: KvSchemaType<Keys[K] & string, Map> | null }>
  >;
  batchSet<const Entries extends readonly { key: string }[]>(
    entries: {
      [I in keyof Entries]: {
        key: Entries[I]["key"] & string;
        value: SetValue<Entries[I]["key"] & string, Map>;
        opts?: BatchSetOpts;
      };
    },
  ): Promise<KvResult<void>>;
  batch<T extends readonly BatchOp[]>(
    ops: T,
  ): Promise<KvResult<SchemaAwareBatchResults<T, Map>>>;
  /** Watch a prefix — event values are typed by the matching schema for each emitted key. */
  watch<K extends string>(
    key: K,
    opts: WatchOptions & { prefix: true },
  ): AsyncGenerator<SchemaAwareWatchEvent<`${K}${string}`, Map>>;
  watch<K extends string>(
    key: K,
    opts?: WatchOptions,
  ): AsyncGenerator<SchemaAwareWatchEvent<K, Map>>;
}

/** Options for {@link createClient}. */
export interface KvRawClientOptions {
  /** Base URL of the KV HTTP server, e.g. `http://kv:4869`. Trailing slash is stripped. */
  url: string;
}

const DEFAULT_LOCK_TTL = 30;

// ── Per-client coalescing ─────────────────────────────────────────────────────
//
// get() and set() calls within the same microtask tick are collapsed into a
// single batch() round-trip. Identical gets for the same key share one result.

type PendingGet = {
  key: string;
  waiters: Array<(r: KvResult<Entry | null>) => void>;
};
type PendingSet = {
  key: string;
  value: string | Uint8Array;
  opts?: SetOptions;
  resolve(r: KvResult<void>): void;
};
type CoalesceState = {
  gets: Map<string, PendingGet>;
  sets: PendingSet[];
  scheduled: boolean;
};

function withCoalescing(inner: KvClient): KvClient {
  const state: CoalesceState = { gets: new Map(), sets: [], scheduled: false };

  function schedule(): void {
    if (state.scheduled) return;
    state.scheduled = true;
    Promise.resolve().then(flush);
  }

  async function flush(): Promise<void> {
    state.scheduled = false;
    const gets = [...state.gets.values()];
    const sets = [...state.sets];
    state.gets.clear();
    state.sets.length = 0;
    if (!gets.length && !sets.length) return;

    // Single-op shortcut: preserves command names and metadata-error hooks for solo calls.
    if (gets.length === 1 && sets.length === 0) {
      const [g] = gets;
      const result = await inner.get(g!.key);
      g!.waiters.forEach((r) => r(result));
      return;
    }
    if (gets.length === 0 && sets.length === 1) {
      const [s] = sets;
      const result = await inner.set(s!.key, s!.value, s!.opts);
      s!.resolve(result as KvResult<void>);
      return;
    }

    const ops: BatchOp[] = [
      ...gets.map((g) => ({ op: "get" as const, key: g.key })),
      ...sets.map((s) =>
        s.opts !== undefined
          ? { op: "set" as const, key: s.key, value: s.value, opts: s.opts }
          : { op: "set" as const, key: s.key, value: s.value }
      ),
    ];

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const result = await inner.batch(ops as any);

    if (result.error) {
      const err = result.error;
      for (const g of gets) {
        g.waiters.forEach((r) => r({ data: undefined, error: err }));
      }
      for (const s of sets) s.resolve({ data: undefined, error: err });
      return;
    }

    const data = result.data as Array<Entry | null | undefined>;
    for (let i = 0; i < gets.length; i++) {
      const entry = (data[i] ?? null) as Entry | null;
      gets[i]!.waiters.forEach((r) => r({ data: entry, error: undefined }));
    }
    for (const s of sets) s.resolve({ data: undefined, error: undefined });
  }

  return {
    ...inner,
    get(key: string): Promise<KvResult<Entry | null>> {
      return new Promise((resolve) => {
        let pending = state.gets.get(key);
        if (!pending) {
          pending = { key, waiters: [] };
          state.gets.set(key, pending);
        }
        pending.waiters.push(resolve);
        schedule();
      });
    },
    set(
      key: string,
      value: string | Uint8Array,
      opts?: SetOptions,
    ): Promise<KvResult<void>> {
      // Conditional writes need independent semantics — don't coalesce them.
      if (opts?.ifAbsent || opts?.ifPresent || opts?.ifMatch !== undefined) {
        return inner.set(key, value, opts);
      }
      return new Promise((resolve) => {
        const entry: PendingSet = { key, value, resolve };
        if (opts !== undefined) entry.opts = opts;
        state.sets.push(entry);
        schedule();
      });
    },
  };
}

/**
 * Builds `lock` and `tryLock` on top of standard KvClient primitives.
 * Shared by both backends — spread the result into the returned client object.
 */
export function createLockMethods(
  base: KvClient,
): Pick<KvClient, "lock" | "tryLock"> {
  async function tryLock(
    key: string,
    opts?: LockOptions,
  ): Promise<KvResult<Lock | null>> {
    const ttl = opts?.ttl ?? DEFAULT_LOCK_TTL;
    const token = crypto.randomUUID();
    const result = await base.set(key, token, { ifAbsent: true, ttl });
    if (result.error) {
      if (result.error.status === 409) return { data: null, error: undefined };
      return { data: undefined, error: result.error };
    }
    const lock: Lock = {
      async release(): Promise<KvResult<void>> {
        const getResult = await base.get(key);
        if (getResult.error) return { data: undefined, error: undefined };
        const entry = getResult.data;
        if (entry === null || entry.text() !== token) {
          return { data: undefined, error: undefined };
        }
        // ifMatch is atomic on HTTP; silently ignored on RESP —
        // the token check above is the primary ownership guard on both backends.
        const delResult = await base.delete(key, { ifMatch: entry.revision });
        if (delResult.error && delResult.error.status !== 409) {
          return { data: undefined, error: delResult.error };
        }
        return { data: undefined, error: undefined };
      },
    };
    return { data: lock, error: undefined };
  }

  async function lock<T>(
    key: string,
    fn: () => Promise<T>,
    opts?: LockOptions,
  ): Promise<KvResult<T>> {
    const ttl = opts?.ttl ?? DEFAULT_LOCK_TTL;
    const deadline = opts?.timeout != null
      ? Date.now() + opts.timeout
      : undefined;
    const signal = opts?.signal;
    const watchAc = new AbortController();

    function onExternalAbort() {
      watchAc.abort();
    }
    signal?.addEventListener("abort", onExternalAbort, { once: true });

    try {
      while (true) {
        if (signal?.aborted) {
          return {
            data: undefined,
            error: new KvError("aborted", "lock acquisition aborted", 499),
          };
        }
        if (deadline != null && Date.now() >= deadline) {
          return {
            data: undefined,
            error: new KvError(
              "timeout",
              `timed out waiting to acquire lock: ${key}`,
              408,
            ),
          };
        }

        const acquired = await tryLock(key, { ttl });
        if (acquired.error) return { data: undefined, error: acquired.error };

        if (acquired.data !== null) {
          const handle = acquired.data;
          let fnResult: T;
          try {
            fnResult = await fn();
          } catch (err) {
            await handle.release();
            return {
              data: undefined,
              error: new KvError(
                "fn_error",
                err instanceof Error ? err.message : String(err),
                500,
              ),
            };
          }
          await handle.release();
          return { data: fnResult, error: undefined };
        }

        // Lock is held — wait for deletion via watch (no busy-polling).
        // Node >= 18 lacks AbortSignal.any(), so compose signals manually.
        const iterAc = new AbortController();
        watchAc.signal.addEventListener("abort", () => iterAc.abort(), {
          once: true,
        });
        let iterTimeout: ReturnType<typeof setTimeout> | undefined;
        if (deadline != null) {
          const remaining = deadline - Date.now();
          if (remaining <= 0) continue;
          iterTimeout = setTimeout(() => iterAc.abort(), remaining);
        }

        try {
          for await (
            const event of base.watch(key, {
              signal: iterAc.signal,
            })
          ) {
            if (event.type === "del") break;
          }
        } catch {
          if (signal?.aborted) {
            return {
              data: undefined,
              error: new KvError("aborted", "lock acquisition aborted", 499),
            };
          }
          // Timeout or transient error — loop back to check deadline.
        } finally {
          if (iterTimeout != null) clearTimeout(iterTimeout);
        }
      }
    } finally {
      signal?.removeEventListener("abort", onExternalAbort);
      watchAc.abort();
    }
  }

  return { tryLock, lock };
}

/**
 * Creates a fully-typed raw HTTP client for the beyond/kv REST API.
 *
 * Built on `openapi-fetch` — every path, method, query parameter, and response
 * type is inferred directly from the generated OpenAPI spec.
 */
export function createClient(opts: KvRawClientOptions): Client<paths> {
  return createFetchClient<paths>({
    baseUrl: opts.url.replace(/\/+$/, ""),
  });
}

/** Creates a KV client. Backend is selected automatically from the URL scheme. */
export function createKvClient(
  opts?: KvClientOptions & { ttl?: number },
): KvClient;
/**
 * Creates a typed KV client. Keys matching a pattern in `schema` have their
 * values parsed (on `get`) and serialized (on `set`) automatically. `ttl` sets
 * a default TTL in seconds applied to every `set` unless overridden per-call.
 *
 * @example
 * ```ts
 * const kv = createKvClient({
 *   url: "redis://localhost:6379",
 *   schema: {
 *     "users:*": z.object({ username: z.string() }),
 *   },
 * });
 * await kv.set("users:foo", { username: "alice" });
 * const { data } = await kv.get("users:foo"); // { username: string } | null
 * ```
 */
export function createKvClient<Map extends KvSchemaMap>(
  opts: KvClientOptions & { schema: Map; ttl?: number },
): KvSchemaClient<Map>;
export function createKvClient<Map extends KvSchemaMap>(
  opts?: Partial<KvClientOptions> & { schema?: Map; ttl?: number },
): KvClient | KvSchemaClient<Map> {
  const url = opts?.url ?? env["BEYOND_KV_URL"];
  if (!url) {
    throw new KvError(
      "invalid_request",
      "BEYOND_KV_URL is required (pass `url` or set the BEYOND_KV_URL env var)",
      0,
    );
  }
  // Spread opts with url resolved to string so internal factories (http.ts, resp.ts) see it as required.
  const resolved = { ...opts, url } as KvClientOptions & {
    schema?: Map;
    ttl?: number;
  };
  const { protocol } = new URL(url);
  const respDb = (resolved as KvRespClientOptions).db
    ?? (env["BEYOND_KV_DB"] != null ? Number(env["BEYOND_KV_DB"]) : undefined);
  const httpNamespace = (resolved as KvHttpClientOptions).namespace
    ?? env["BEYOND_KV_NAMESPACE"];
  const base: KvClient = withCoalescing(
    protocol === "redis:" || protocol === "rediss:"
      ? createRespKvClient({
        ...(resolved as KvRespClientOptions),
        ...(respDb !== undefined && { db: respDb }),
      })
      : createHttpKvClient({
        ...(resolved as KvHttpClientOptions),
        ...(httpNamespace !== undefined && { namespace: httpNamespace }),
      }),
  );

  const { schema: schemaMap, ttl: defaultTtl } = opts ?? {};

  if (!schemaMap && defaultTtl == null) return base;

  // Pre-compile glob patterns to regexes once, most specific first.
  const compiled = schemaMap
    ? Object.entries(schemaMap)
      .sort((a, b) => specificity(b[0]) - specificity(a[0]))
      .map(([pattern, schema]) => ({ re: globToRegex(pattern), schema }))
    : [];

  function findSchema(key: string): KvSchema<unknown> | undefined {
    for (const { re, schema } of compiled) {
      if (re.test(key)) return schema;
    }
    return undefined;
  }

  function schemaError(err: unknown): KvResult<never> {
    return {
      data: undefined,
      error: new KvError(
        "schema_error",
        err instanceof Error ? err.message : String(err),
        422,
      ),
    };
  }

  function parseEntry(key: string, entry: Entry): KvResult<unknown> {
    const schema = findSchema(key);
    if (!schema) return { data: entry, error: undefined };
    try {
      return { data: schema.parse(entry.json()), error: undefined };
    } catch (err) {
      return schemaError(err);
    }
  }

  function serializeValue(key: string, value: unknown): string | Uint8Array {
    return findSchema(key)
      ? JSON.stringify(value)
      : (value as string | Uint8Array);
  }

  return {
    ...base,
    async get(key: string) {
      const result = await base.get(key);
      if (result.error || result.data === null) return result;
      return parseEntry(key, result.data);
    },
    async set(key: string, value: unknown, setOpts?: SetOptions) {
      const ttl = setOpts?.ttl ?? defaultTtl;
      const mergedOpts = ttl != null ? { ...setOpts, ttl } : setOpts;
      return base.set(key, serializeValue(key, value), mergedOpts);
    },
    async getAndSet(
      key: string,
      value: unknown,
      getAndSetOpts?: GetAndSetOptions,
    ) {
      const result = await base.getAndSet(
        key,
        serializeValue(key, value),
        getAndSetOpts,
      );
      if (result.error || result.data === null) return result;
      return parseEntry(key, result.data);
    },
    async getAndDelete(key: string) {
      const result = await base.getAndDelete(key);
      if (result.error || result.data === null) return result;
      return parseEntry(key, result.data);
    },
    async delete(key: string, opts?: DeleteOptions) {
      if (opts?.returnOld) {
        const result = await base.delete(
          key,
          opts as DeleteOptions & { returnOld: true },
        );
        if (result.error || !result.data) return result;
        return parseEntry(key, result.data);
      }
      return base.delete(key, opts);
    },
    async cas(
      key: string,
      value: unknown,
      revision: number,
      casOpts?: CasOptions,
    ) {
      return base.cas(key, serializeValue(key, value), revision, casOpts);
    },
    async batchGet(keys: readonly string[]) {
      const result = await base.batchGet(keys);
      if (result.error) return result;
      const parsed: unknown[] = [];
      for (let i = 0; i < result.data.length; i++) {
        const entry = result.data[i]!;
        if (entry === null) {
          parsed.push(null);
          continue;
        }
        const schema = findSchema(keys[i]!);
        if (!schema) {
          parsed.push(entry);
          continue;
        }
        try {
          parsed.push(schema.parse(entry.json()));
        } catch (err) {
          return {
            data: undefined,
            error: new KvError(
              "schema_error",
              err instanceof Error ? err.message : String(err),
              422,
            ),
          };
        }
      }
      return { data: parsed, error: undefined };
    },
    async batchSet(entries: readonly MSetEntry[]) {
      const wire: MSetEntry[] = entries.map(({ key, value, opts }) => {
        const ttl = opts?.ttl ?? defaultTtl;
        const schema = findSchema(key);
        const entry: MSetEntry = {
          key,
          value: schema
            ? JSON.stringify(value)
            : (value as string | Uint8Array),
        };
        const mergedOpts = ttl != null ? { ...opts, ttl } : opts;
        if (mergedOpts) entry.opts = mergedOpts;
        return entry;
      });
      return base.batchSet(wire);
    },
    async batch(ops: readonly BatchOp[]) {
      const wireOps: BatchOp[] = ops.map((op) => {
        if (op.op === "set") {
          const ttl = op.opts?.ttl ?? defaultTtl;
          if (ttl == null) return op;
          return { ...op, opts: { ...op.opts, ttl } } as BatchOp;
        }
        return op;
      });
      const result = await base.batch(wireOps);
      if (result.error) return result;
      const parsed: unknown[] = [];
      for (let i = 0; i < result.data.length; i++) {
        const op = ops[i]!;
        const item = (result.data as unknown[])[i];
        if (op.op === "get" && item !== null) {
          const schema = findSchema(op.key);
          if (schema) {
            try {
              parsed.push(schema.parse((item as Entry).json()));
            } catch (err) {
              return {
                data: undefined,
                error: new KvError(
                  "schema_error",
                  err instanceof Error ? err.message : String(err),
                  422,
                ),
              };
            }
            continue;
          }
        }
        parsed.push(item);
      }
      return { data: parsed, error: undefined };
    },
    async *watch(key: string, watchOpts?: WatchOptions) {
      const dec = new TextDecoder();
      for await (const event of base.watch(key, watchOpts)) {
        if (event.type !== "set") {
          yield event;
          continue;
        }
        const schema = findSchema(event.key);
        if (!schema) {
          yield event;
          continue;
        }
        try {
          yield {
            ...event,
            value: schema.parse(JSON.parse(dec.decode(event.value))),
          };
        } catch {
          yield event;
        }
      }
    },
  } as unknown as KvSchemaClient<Map>;
}

function globToRegex(pattern: string): RegExp {
  return new RegExp(
    "^"
      + pattern
        .split("*")
        .map((s) => s.replace(/[.+^${}()|[\]\\]/g, "\\$&"))
        .join(".*")
      + "$",
  );
}

function specificity(pattern: string): number {
  return pattern.replace(/\*/g, "").length;
}

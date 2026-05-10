import type { KvClient } from "./client.js";
import { kv } from "./index.js";
import type { BatchOp, BatchSetOpts, Entry } from "./kv-types.js";

// ── Public types ──────────────────────────────────────────────────────────────

/**
 * Options for a cached function created with {@link cache} or {@link createCache}.
 *
 * @typeParam TArgs - Argument tuple of the wrapped fetcher function.
 */
export type CacheOptions<TArgs extends unknown[]> = {
  /**
   * Cache entry lifetime in seconds.
   *
   * When combined with `swr`, the entry is stored for `ttl + swr` seconds
   * total: `ttl` seconds of freshness followed by `swr` seconds of
   * stale-while-revalidate.
   *
   * Omit to cache without expiry.
   */
  ttl?: number;
  /**
   * Stale-while-revalidate window in **seconds**, appended after `ttl`.
   *
   * While in this window the cached (stale) value is returned immediately and
   * the fetcher is called in the background to refresh the entry.
   *
   * @example
   * ```ts
   * // Serve fresh for 60s, serve stale for up to 1hr while refreshing.
   * const getUser = cache(fetchUser, { ttl: 60, swr: 3600 })
   * ```
   */
  swr?: number;
  /**
   * KV key or key derivation function.
   *
   * **Omit** to derive the key automatically from the fetcher's function name
   * and JSON-serialised arguments: `"fnName:${JSON.stringify(args)}"`.
   * Anonymous functions cannot be used without an explicit `key` — they will
   * throw at definition time.
   *
   * @example Static key (no-arg fetcher):
   * ```ts
   * const getConfig = cache(fetchConfig, { key: 'app:config', ttl: 300 })
   * ```
   *
   * @example Dynamic key derived from arguments:
   * ```ts
   * const getUser = cache(fetchUser, { key: (id: string) => `users:${id}`, ttl: 60 })
   * ```
   */
  key?: string | ((...args: TArgs) => string);
};

/**
 * A cached async function returned by {@link cache} or {@link createCache}.
 *
 * Call it like the original function — it returns a `Promise<TReturn>`.
 * Use `.delete(...args)` to invalidate a specific cached entry.
 *
 * @typeParam TArgs - Argument tuple of the wrapped fetcher function.
 * @typeParam TReturn - Return type of the wrapped fetcher function.
 */
export type CacheHandle<TArgs extends unknown[], TReturn> = {
  (...args: TArgs): Promise<TReturn>;
  /** Invalidate the cached entry for these arguments. */
  delete(...args: TArgs): Promise<void>;
};

type CacheFn = <TArgs extends unknown[], TReturn>(
  fetcher: (...args: TArgs) => Promise<TReturn>,
  options?: CacheOptions<TArgs>,
) => CacheHandle<TArgs, TReturn>;

// ── Per-client batch state ────────────────────────────────────────────────────
//
// All cache handles sharing the same KvClient share one BatchState, so
// concurrent reads and writes from different handles collapse into a single
// client.batch() call per microtask tick.

type Waiter = {
  resolve(e: Entry | null): void;
  reject(e: unknown): void;
};

type PendingGet = { key: string; waiters: Waiter[] };
type PendingSet = {
  key: string;
  value: string;
  opts?: BatchSetOpts;
  resolve(): void;
  reject(e: unknown): void;
};

type BatchState = {
  gets: Map<string, PendingGet>;
  sets: PendingSet[];
  scheduled: boolean;
};

const stateByClient = new WeakMap<KvClient, BatchState>();

function getState(client: KvClient): BatchState {
  let s = stateByClient.get(client);
  if (!s) {
    s = { gets: new Map(), sets: [], scheduled: false };
    stateByClient.set(client, s);
  }
  return s;
}

function scheduleFlush(client: KvClient, state: BatchState): void {
  if (state.scheduled) return;
  state.scheduled = true;
  Promise.resolve().then(() => flush(client, state));
}

async function flush(client: KvClient, state: BatchState): Promise<void> {
  state.scheduled = false;

  const gets = [...state.gets.values()];
  const sets = [...state.sets];
  state.gets.clear();
  state.sets.length = 0;

  if (!gets.length && !sets.length) return;

  const ops: BatchOp[] = [
    ...gets.map((g) => ({ op: "get" as const, key: g.key })),
    ...sets.map((s) =>
      s.opts !== undefined
        ? { op: "set" as const, key: s.key, value: s.value, opts: s.opts }
        : { op: "set" as const, key: s.key, value: s.value }
    ),
  ];

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const result = await client.batch(ops as any);

  if (result.error) {
    for (const g of gets) g.waiters.forEach((w) => w.reject(result.error));
    for (const s of sets) s.reject(result.error);
    return;
  }

  const data = result.data as Array<Entry | null | undefined>;
  for (let i = 0; i < gets.length; i++) {
    const entry = data[i] ?? null;
    gets[i]!.waiters.forEach((w) => w.resolve(entry as Entry | null));
  }
  for (const s of sets) s.resolve();
}

function enqueueGet(client: KvClient, key: string): Promise<Entry | null> {
  const state = getState(client);
  return new Promise<Entry | null>((resolve, reject) => {
    let pending = state.gets.get(key);
    if (!pending) {
      pending = { key, waiters: [] };
      state.gets.set(key, pending);
    }
    pending.waiters.push({ resolve, reject });
    scheduleFlush(client, state);
  });
}

function enqueueSet(
  client: KvClient,
  key: string,
  value: string,
  opts?: BatchSetOpts,
): Promise<void> {
  const state = getState(client);
  return new Promise<void>((resolve, reject) => {
    const entry: PendingSet = { key, value, resolve, reject };
    if (opts !== undefined) entry.opts = opts;
    state.sets.push(entry);
    scheduleFlush(client, state);
  });
}

// ── Cache factory ─────────────────────────────────────────────────────────────

/**
 * Create a cache factory bound to a specific KV client.
 *
 * Use this when you need a client other than the default one configured via
 * `BEYOND_KV_URL`. The returned function has the same signature as {@link cache}.
 *
 * @example
 * ```ts
 * import { createCache } from '@beyond.dev/kv/cache'
 * import { createKvClient } from '@beyond.dev/kv'
 *
 * const myKv = createKvClient({ url: 'https://my-cluster.beyond.dev' })
 * const cache = createCache(myKv)
 *
 * const getUser = cache(fetchUser, { ttl: 60 })
 * ```
 */
export function createCache(client: KvClient): CacheFn {
  return function cache<TArgs extends unknown[], TReturn>(
    fetcher: (...args: TArgs) => Promise<TReturn>,
    options: CacheOptions<TArgs> = {},
  ): CacheHandle<TArgs, TReturn> {
    const { ttl, swr = 0, key: keyOpt } = options;
    const storageTtl = ttl != null ? ttl + swr : undefined;

    if (!keyOpt && !fetcher.name) {
      throw new Error(
        "cache: cannot derive key from anonymous function — provide a `key` option",
      );
    }

    // Per-handle in-flight map: stampede protection across microtask ticks.
    // A second caller for the same key while a fetch is in-flight joins the
    // existing Promise instead of launching a duplicate request.
    const inFlight = new Map<string, Promise<TReturn>>();

    function resolveKey(args: TArgs): string {
      if (!keyOpt) return `${fetcher.name}:${JSON.stringify(args)}`;
      if (typeof keyOpt === "string") return keyOpt;
      return keyOpt(...args);
    }

    async function fetchAndStore(kvKey: string, args: TArgs): Promise<TReturn> {
      const existing = inFlight.get(kvKey);
      if (existing) return existing;

      const p = (async () => {
        try {
          const value = await fetcher(...args);
          await enqueueSet(
            client,
            kvKey,
            JSON.stringify(value),
            storageTtl != null ? { ttl: storageTtl } : undefined,
          );
          return value;
        } finally {
          inFlight.delete(kvKey);
        }
      })();

      inFlight.set(kvKey, p);
      return p;
    }

    const handle = async function(...args: TArgs): Promise<TReturn> {
      const kvKey = resolveKey(args);
      const entry = await enqueueGet(client, kvKey);

      if (entry === null) {
        return fetchAndStore(kvKey, args);
      }

      if (swr > 0 && entry.ttlMs != null && entry.ttlMs <= swr * 1000) {
        // Within the SWR window — return stale value immediately and refresh
        // in the background so the next caller gets fresh data.
        fetchAndStore(kvKey, args).catch(() => {});
        return entry.json<TReturn>();
      }

      return entry.json<TReturn>();
    } as CacheHandle<TArgs, TReturn>;

    handle.delete = async function(...args: TArgs): Promise<void> {
      const { error } = await client.delete(resolveKey(args));
      if (error) throw error;
    };

    return handle;
  };
}

/**
 * Wrap an async function with KV-backed caching.
 *
 * Uses the default KV client configured via `BEYOND_KV_URL`. Pass a custom
 * client with {@link createCache} instead.
 *
 * **Automatic key derivation** — omit `key` and the cache key is derived from
 * the function's name and JSON-serialised arguments:
 * `"fnName:${JSON.stringify(args)}"`. The function must be named; anonymous
 * functions throw at definition time.
 *
 * **Coalescing** — concurrent calls within the same microtask tick are
 * collapsed into a single `batch()` round-trip regardless of which handle they
 * come from. Reads and writes share the same batch.
 *
 * **Stampede protection** — concurrent callers for the same key while a fetch
 * is in-flight all receive the same Promise. The upstream is called exactly
 * once.
 *
 * **Stale-while-revalidate** — set `swr` to serve a stale cached value
 * immediately while refreshing in the background.
 *
 * @example Basic usage — key derived from function name + args:
 * ```ts
 * import { cache } from '@beyond.dev/kv/cache'
 *
 * const getUser = cache(fetchUser, { ttl: 60 })
 * const user = await getUser('user_123') // type: User
 * await getUser.delete('user_123')       // invalidate
 * ```
 *
 * @example Explicit key (required for anonymous functions or custom shapes):
 * ```ts
 * const getUser = cache(fetchUser, {
 *   key: (id: string) => `users:${id}`,
 *   ttl: 60,
 * })
 * ```
 *
 * @example Stale-while-revalidate — serve cached data instantly, refresh in background:
 * ```ts
 * const getPost = cache(fetchPost, {
 *   key: (id: string) => `posts:${id}`,
 *   ttl: 60,    // fresh for 60s
 *   swr: 3600,  // serve stale for up to 1hr while refreshing
 * })
 * ```
 *
 * @example Static key for a no-arg fetcher:
 * ```ts
 * const getConfig = cache(fetchConfig, { key: 'app:config', ttl: 300 })
 * const config = await getConfig()
 * ```
 *
 * @example Concurrent calls coalesce automatically — one round-trip to KV:
 * ```ts
 * const [a, b, c] = await Promise.all([
 *   getUser('1'),
 *   getUser('2'),
 *   getUser('3'),
 * ])
 * ```
 */
export const cache: CacheFn = createCache(kv);

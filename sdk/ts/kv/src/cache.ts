import type { KvClient } from "./client.js";
import { kv } from "./index.js";

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
   * the fetcher is called in the background to refresh the entry. If the
   * background refresh fails, `onRefreshError` is called (if provided) and
   * stale data continues to be served until the entry expires.
   *
   * @example
   * ```ts
   * // Serve fresh for 60s, serve stale for up to 1hr while refreshing.
   * const getUser = cache(fetchUser, { ttl: 60, swr: 3600 })
   * ```
   */
  swr?: number;
  /**
   * Called when a background SWR refresh fails. Stale data continues to be
   * served regardless — this is purely for observability (logging, metrics).
   *
   * @example
   * ```ts
   * const getUser = cache(fetchUser, {
   *   ttl: 60,
   *   swr: 3600,
   *   onRefreshError: (err) => logger.warn('cache refresh failed', err),
   * })
   * ```
   */
  onRefreshError?: (err: unknown) => void;
  /**
   * KV key or key derivation function. Required.
   *
   * Use a static string for no-arg fetchers or shared state. Use a function
   * to derive the key from the fetcher's arguments.
   *
   * > **Note — `JSON.stringify` constraints**: If you derive the key from args
   * > with `JSON.stringify`, object key order must be consistent, `undefined`
   * > values are dropped, `Date` instances become strings, `BigInt` and
   * > circular references throw, and class instances serialise as plain
   * > objects. Use a custom derivation if your args don't satisfy these
   * > constraints.
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
  key: string | ((...args: TArgs) => string);
};

/**
 * A cached async function returned by {@link cache} or {@link createCache}.
 *
 * Call it like the original function — it returns a `Promise<TReturn>`.
 * Use `.delete(...args)` to invalidate a specific cached entry, or
 * `.refresh(...args)` to force an immediate re-fetch and update the cache.
 *
 * @typeParam TArgs - Argument tuple of the wrapped fetcher function.
 * @typeParam TReturn - Return type of the wrapped fetcher function.
 */
export type CacheHandle<TArgs extends unknown[], TReturn> = {
  (...args: TArgs): Promise<TReturn>;
  /** Invalidate the cached entry for these arguments. */
  delete(...args: TArgs): Promise<void>;
  /**
   * Force a fresh fetch, bypassing the cache, and update the stored value.
   * Returns the new value. Stampede protection applies — concurrent `.refresh()`
   * calls for the same key share one fetch.
   */
  refresh(...args: TArgs): Promise<TReturn>;
};

type CacheFn = <TArgs extends unknown[], TReturn>(
  fetcher: (...args: TArgs) => Promise<TReturn>,
  options: CacheOptions<TArgs>,
) => CacheHandle<TArgs, TReturn>;

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
    options: CacheOptions<TArgs>,
  ): CacheHandle<TArgs, TReturn> {
    const { ttl, swr = 0, key: keyOpt, onRefreshError } = options;
    const storageTtl = ttl != null ? ttl + swr : undefined;

    // Per-handle in-flight map: stampede protection across microtask ticks.
    // A second caller for the same key while a fetch is in-flight joins the
    // existing Promise instead of launching a duplicate request.
    const inFlight = new Map<string, Promise<TReturn>>();

    function resolveKey(args: TArgs): string {
      if (typeof keyOpt === "string") return keyOpt;
      return keyOpt(...args);
    }

    async function fetchAndStore(kvKey: string, args: TArgs): Promise<TReturn> {
      const existing = inFlight.get(kvKey);
      if (existing) return existing;

      const p = (async () => {
        try {
          const value = await fetcher(...args);
          const { error } = await client.set(
            kvKey,
            JSON.stringify(value),
            storageTtl != null ? { ttl: storageTtl } : undefined,
          );
          if (error) throw error;
          return value;
        } finally {
          inFlight.delete(kvKey);
        }
      })();

      inFlight.set(kvKey, p);
      return p;
    }

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const handle = async function(...args: TArgs): Promise<TReturn> {
      const kvKey = resolveKey(args);
      const { data: entry, error } = await client.get(kvKey);
      if (error) throw error;

      if (entry === null) {
        return fetchAndStore(kvKey, args);
      }

      if (swr > 0 && entry.ttlMs != null && entry.ttlMs <= swr * 1000) {
        // Within the SWR window — return stale value immediately and refresh
        // in the background so the next caller gets fresh data.
        fetchAndStore(kvKey, args).catch(onRefreshError ?? (() => {}));
        return entry.json<TReturn>();
      }

      return entry.json<TReturn>();
    } as CacheHandle<TArgs, TReturn>;

    handle.delete = async function(...args: TArgs): Promise<void> {
      const { error } = await client.delete(resolveKey(args));
      if (error) throw error;
    };

    handle.refresh = function(...args: TArgs): Promise<TReturn> {
      return fetchAndStore(resolveKey(args), args);
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
 * **Explicit key required** — provide a static string for no-arg fetchers or
 * a derivation function for parameterised ones. This keeps keys stable across
 * bundler minification and refactors.
 *
 * **Coalescing** — concurrent `get` and `set` calls within the same microtask
 * tick are collapsed into a single `batch()` round-trip at the client level,
 * so any combination of cache handles and direct client calls coalesce together.
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
 * await getUser.refresh('user_123')      // force re-fetch
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
 *   onRefreshError: (err) => logger.warn('cache refresh failed', err),
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
export function cache<TArgs extends unknown[], TReturn>(
  fetcher: (...args: TArgs) => Promise<TReturn>,
  options: CacheOptions<TArgs>,
): CacheHandle<TArgs, TReturn> {
  return createCache(kv)(fetcher, options);
}

/**
 * Vercel Flags SDK adapter for `@beyond.dev/flags`.
 *
 * Lets you declare flags with the Vercel [`flags`](https://flags-sdk.dev) SDK
 * (`flag({ key, adapter })`) but resolve them against Beyond KV — the same
 * `flags:def:*` defs, targeting rules, rollout, kill switch, and per-user prefs
 * the native `@beyond.dev/flags` API uses. The host (`flags/next`) owns the
 * request plumbing, toolbar overrides, precompute, and reporting; this module
 * implements the one seam it calls into: the {@link Adapter} contract.
 *
 * @example
 * ```ts
 * // flags.ts
 * import { flag } from 'flags/next'
 * import { createKvClient } from '@beyond.dev/kv'
 * import { beyondAdapter } from '@beyond.dev/flags/adapter'
 *
 * const kv = createKvClient({ url: process.env.BEYOND_KV_URL! })
 * const adapter = beyondAdapter(kv)
 *
 * export const newCheckout = flag<boolean>({
 *   key: 'new-checkout',
 *   defaultValue: false,
 *   adapter,
 *   identify: ({ headers }) => ({ id: headers.get('x-user-id') ?? 'anon', plan: 'free' }),
 * })
 * ```
 *
 * @packageDocumentation
 */

import type { KvClient } from "@beyond.dev/kv";
import type {
  Adapter,
  FlagDefinitionsType,
  Identify,
  Origin,
  ProviderData,
  ReadonlyHeaders,
} from "flags";
import { evaluate } from "./eval.js";
import { fetchUserPrefs } from "./snapshot.js";
import { Snapshot } from "./snapshot.js";
import type {
  FlagContext,
  FlagDef,
  FlagsErrorEvent,
  JsonValue,
  UserPrefs,
} from "./types.js";

const DEF_PREFIX = "flags:def:";

/** Options for {@link beyondAdapter}. The KV client is positional, not here. */
export interface BeyondAdapterOptions {
  /**
   * How defs are read from KV.
   *
   * - `"snapshot"` (default): keep an in-memory snapshot of all `flags:def:*`,
   *   refreshed via `kv.watch()` (or polling). `decide` does zero KV
   *   round-trips. Best for long-lived Node servers.
   * - `"request"`: fetch each def on demand, cached per request (keyed by the
   *   request `headers` object). Survives short-lived edge/serverless functions
   *   that can't hold a persistent watch. One KV read per distinct flag/request.
   */
  mode?: "snapshot" | "request";
  /** snapshot mode: SWR poll interval (seconds) when watch is unavailable. Default 30. */
  refresh?: number;
  /** snapshot mode: use `kv.watch()` for instant invalidation. Default true. */
  watch?: boolean;
  /** Honor per-user prefs (`flags:user:<id>`) in `decide`. Default true. */
  userPrefs?: boolean;
  /**
   * Default `identify` for flags using this adapter. A per-flag `identify` in
   * the `flag({...})` declaration overrides it. Must return an object with a
   * non-empty `id` (the rollout bucket key).
   */
  identify?: Identify<FlagContext>;
  /** Management URL for the toolbar — a string base or a `(key) => url` builder. */
  origin?: string | Origin | ((key: string) => string | Origin | undefined);
  /** Called for snapshot/watch/KV/pref failures. */
  onError?: (event: FlagsErrorEvent) => void;
}

/**
 * The adapter object passed to `flag({ adapter })`, plus discovery/lifecycle
 * helpers. It satisfies the Vercel {@link Adapter} interface; the extra members
 * are ignored by the host.
 */
export interface BeyondAdapter<
  T extends JsonValue = JsonValue,
  E extends FlagContext = FlagContext,
> extends Adapter<T, E> {
  /**
   * Build {@link ProviderData} for the toolbar discovery endpoint
   * (`flags/next`'s `createFlagsDiscoveryEndpoint`). Enumerates `flags:def:*`
   * from KV. Definitions are thin — only what KV stores; merge with the host's
   * `getProviderData(flags)` (via `mergeProviderData`) for code-declared
   * options/description/defaultValue.
   */
  getProviderData(): Promise<ProviderData>;
  /** Stop background syncing (snapshot mode). Does not close the KV client. */
  close(): Promise<void>;
}

/**
 * Per-request def reader. Two strategies behind one interface so `decide`,
 * `bulkDecide`, and prefs caching are mode-agnostic.
 */
interface DefSource {
  ready(): Promise<void>;
  get(key: string, headers: ReadonlyHeaders): Promise<FlagDef | undefined>;
  getMany(
    keys: readonly string[],
    headers: ReadonlyHeaders,
  ): Promise<Map<string, FlagDef | undefined>>;
  close(): void;
}

/** snapshot mode — in-memory, kept live by {@link Snapshot}. Zero per-eval I/O. */
class SnapshotDefSource implements DefSource {
  private readonly snapshot: Snapshot;

  constructor(kv: KvClient, opts: BeyondAdapterOptions) {
    this.snapshot = new Snapshot(kv, {
      refresh: opts.refresh ?? 30,
      watch: opts.watch ?? true,
      ...(opts.onError ? { onError: opts.onError } : {}),
    });
    this.snapshot.start();
  }

  ready(): Promise<void> {
    return this.snapshot.awaitReady();
  }

  async get(key: string): Promise<FlagDef | undefined> {
    return this.snapshot.get(key);
  }

  async getMany(
    keys: readonly string[],
  ): Promise<Map<string, FlagDef | undefined>> {
    const out = new Map<string, FlagDef | undefined>();
    for (const key of keys) out.set(key, this.snapshot.get(key));
    return out;
  }

  close(): void {
    this.snapshot.close();
  }
}

/**
 * request mode — fetch defs on demand, cached per request via a
 * `WeakMap<ReadonlyHeaders, ...>`. The `headers` object is stable for the
 * lifetime of one request (the host passes the same sealed instance to every
 * flag), so it's a natural per-request cache key with no manual cleanup.
 */
class RequestDefSource implements DefSource {
  private readonly kv: KvClient;
  private readonly onError: ((event: FlagsErrorEvent) => void) | undefined;
  private readonly cache = new WeakMap<
    ReadonlyHeaders,
    Map<string, Promise<FlagDef | undefined>>
  >();

  constructor(kv: KvClient, opts: BeyondAdapterOptions) {
    this.kv = kv;
    this.onError = opts.onError;
  }

  ready(): Promise<void> {
    return Promise.resolve();
  }

  private requestCache(
    headers: ReadonlyHeaders,
  ): Map<string, Promise<FlagDef | undefined>> {
    let map = this.cache.get(headers);
    if (!map) {
      map = new Map();
      this.cache.set(headers, map);
    }
    return map;
  }

  get(key: string, headers: ReadonlyHeaders): Promise<FlagDef | undefined> {
    const map = this.requestCache(headers);
    let pending = map.get(key);
    if (!pending) {
      pending = this.fetchOne(key);
      map.set(key, pending);
    }
    return pending;
  }

  private async fetchOne(key: string): Promise<FlagDef | undefined> {
    const { data, error } = await this.kv.get(DEF_PREFIX + key);
    if (error) {
      this.onError?.({ source: "snapshot", error, name: key });
      return undefined;
    }
    if (!data) return undefined;
    return parseDef(data.text(), key, this.onError);
  }

  async getMany(
    keys: readonly string[],
    headers: ReadonlyHeaders,
  ): Promise<Map<string, FlagDef | undefined>> {
    const map = this.requestCache(headers);
    // Batch the keys not already cached into a single round-trip.
    const missing = keys.filter((k) => !map.has(k));
    if (missing.length > 0) {
      const fetched = this.fetchMany(missing);
      for (const key of missing) {
        map.set(
          key,
          fetched.then((m) => m.get(key)),
        );
      }
    }
    const out = new Map<string, FlagDef | undefined>();
    await Promise.all(
      keys.map(async (key) => {
        out.set(key, await (map.get(key) as Promise<FlagDef | undefined>));
      }),
    );
    return out;
  }

  private async fetchMany(
    keys: readonly string[],
  ): Promise<Map<string, FlagDef | undefined>> {
    const out = new Map<string, FlagDef | undefined>();
    const { data, error } = await this.kv.batchGet(
      keys.map((k) => DEF_PREFIX + k),
    );
    if (error) {
      this.onError?.({ source: "snapshot", error });
      for (const key of keys) out.set(key, undefined);
      return out;
    }
    for (let i = 0; i < keys.length; i++) {
      const key = keys[i] as string;
      const entry = data[i];
      out.set(
        key,
        entry ? parseDef(entry.text(), key, this.onError) : undefined,
      );
    }
    return out;
  }

  close(): void {
    // Nothing to release — caches are per-request and GC'd with their headers.
  }
}

/**
 * Parse a `flags:def:*` JSON payload into a {@link FlagDef}. Returns `undefined`
 * (treated as "no def" → flag falls back to its declared default) on malformed
 * input, reporting through `onError` rather than throwing into eval.
 */
function parseDef(
  text: string,
  key: string,
  onError?: (event: FlagsErrorEvent) => void,
): FlagDef | undefined {
  if (text.length === 0) return undefined;
  try {
    const parsed = JSON.parse(text) as unknown;
    if (
      parsed === null
      || typeof parsed !== "object"
      || Array.isArray(parsed)
      || typeof (parsed as Record<string, unknown>)["on"] !== "boolean"
    ) {
      throw new Error("flag def must be a JSON object with a boolean `on`");
    }
    return parsed as FlagDef;
  } catch (err) {
    onError?.({
      source: "snapshot",
      error: err instanceof Error ? err : new Error(String(err)),
      name: key,
    });
    return undefined;
  }
}

/**
 * Create a Beyond KV adapter for the Vercel Flags SDK, bound to a specific
 * {@link KvClient}. Mirrors `createFlags(kv, opts)`.
 *
 * One adapter object owns one `adapterId`, so all flags sharing it are batched
 * together by the host's `evaluate()` through a single `bulkDecide`.
 */
export function beyondAdapter<
  T extends JsonValue = JsonValue,
  E extends FlagContext = FlagContext,
>(kv: KvClient, opts: BeyondAdapterOptions = {}): BeyondAdapter<T, E> {
  const mode = opts.mode ?? "snapshot";
  const useUserPrefs = opts.userPrefs !== false;
  const defSource: DefSource = mode === "request"
    ? new RequestDefSource(kv, opts)
    : new SnapshotDefSource(kv, opts);

  // Stable per-instance id so the host groups this adapter's flags for bulk eval.
  const adapterId = Symbol("beyondAdapter");

  // Per-request pref cache, keyed by the request headers object (stable per
  // request) → one KV read per id regardless of how many flags evaluate.
  const prefsCache = new WeakMap<
    ReadonlyHeaders,
    Map<string, Promise<UserPrefs | null>>
  >();

  function loadPrefs(
    id: string,
    headers: ReadonlyHeaders,
  ): Promise<UserPrefs | null> {
    let byId = prefsCache.get(headers);
    if (!byId) {
      byId = new Map();
      prefsCache.set(headers, byId);
    }
    let pending = byId.get(id);
    if (!pending) {
      pending = fetchUserPrefs(kv, id, opts.onError);
      byId.set(id, pending);
    }
    return pending;
  }

  function resolveOrigin(
    key: string,
  ): string | Origin | undefined {
    const o = opts.origin;
    if (o === undefined) return undefined;
    return typeof o === "function" ? o(key) : o;
  }

  const adapter: BeyondAdapter<T, E> = {
    adapterId,

    ...(opts.identify ? { identify: opts.identify as Identify<E> } : {}),
    ...(opts.origin ? { origin: resolveOrigin } : {}),

    async decide({ key, entities, headers, defaultValue }) {
      const ctx = entities as FlagContext | undefined;
      // No id → can't bucket or look up prefs. Fall back to the declared default.
      if (!ctx?.id) return defaultValue as T;
      await defSource.ready();
      const def = await defSource.get(key, headers);
      const prefs = useUserPrefs ? await loadPrefs(ctx.id, headers) : null;
      return evaluate<T>(
        key,
        defaultValue as T,
        ctx,
        def as FlagDef<T> | undefined,
        prefs,
      ).value;
    },

    async bulkDecide({ flags, entities, headers }) {
      const ctx = entities as FlagContext | undefined;
      const out: Record<string, T> = {};
      if (!ctx?.id) {
        for (const f of flags) out[f.key] = f.defaultValue as T;
        return out;
      }
      await defSource.ready();
      const defs = await defSource.getMany(
        flags.map((f) => f.key),
        headers,
      );
      const prefs = useUserPrefs ? await loadPrefs(ctx.id, headers) : null;
      for (const f of flags) {
        out[f.key] = evaluate<T>(
          f.key,
          f.defaultValue as T,
          ctx,
          defs.get(f.key) as FlagDef<T> | undefined,
          prefs,
        ).value;
      }
      return out;
    },

    async getProviderData(): Promise<ProviderData> {
      try {
        const definitions = await listDefs(kv);
        const out: FlagDefinitionsType = {};
        for (const name of definitions) {
          const origin = opts.origin ? resolveOrigin(name) : undefined;
          out[name] = {
            declaredInCode: false,
            ...(origin !== undefined ? { origin } : {}),
          };
        }
        return { definitions: out, hints: [] };
      } catch (err) {
        const error = err instanceof Error ? err : new Error(String(err));
        opts.onError?.({ source: "snapshot", error });
        return {
          definitions: {},
          hints: [
            {
              key: "beyond-kv",
              text:
                `Failed to load flag definitions from Beyond KV: ${error.message}`,
            },
          ],
        };
      }
    },

    async close(): Promise<void> {
      defSource.close();
    },
  };

  return adapter;
}

/** Alias matching the `createFlags`/`createKvClient` naming. */
export const createBeyondAdapter = beyondAdapter;

/** Enumerate all `flags:def:*` keys, returning the bare flag names. */
async function listDefs(kv: KvClient): Promise<string[]> {
  const names: string[] = [];
  let cursor: string | undefined;
  do {
    const { data, error } = await kv.list(
      cursor === undefined
        ? { prefix: DEF_PREFIX }
        : { prefix: DEF_PREFIX, cursor },
    );
    if (error) throw error;
    for (const k of data.keys) names.push(k.name.slice(DEF_PREFIX.length));
    cursor = data.nextCursor;
  } while (cursor);
  return names;
}

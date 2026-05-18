import { createKvClient, type KvClient } from "@beyond.dev/kv";
import { env } from "std-env";
import { currentScope } from "./als.js";
import {
  type Flag,
  type FlagRuntime,
  makeFlag,
  type VariantsHint,
} from "./flag.js";
import { fetchUserPrefs, Snapshot, userKey } from "./snapshot.js";
import type {
  FlagContext,
  FlagDef,
  FlagEvent,
  FlagsErrorEvent,
  JsonValue,
  UserPrefs,
} from "./types.js";

/** Options for {@link createFlags}. KV client is positional, not in here. */
export interface CreateFlagsOptions {
  /** SWR poll interval in seconds when watch is unavailable. Default: 30. */
  refresh?: number;
  /** Use `kv.watch()` for instant invalidation. Default: true. Falls back to polling on watch failure. */
  watch?: boolean;
  /** Called for every flag eval (success or error). */
  onEvaluate?: (event: FlagEvent) => void;
  /** Called for snapshot/watch/KV failures. */
  onError?: (event: FlagsErrorEvent) => void;
}

/**
 * A factory function that defines a flag — callable as `flagsFn(name, default)`
 * or `flagsFn(name, [variants])`. The returned {@link Flag} is itself callable
 * to evaluate.
 *
 * The factory has no methods; flag-mutation operations live on the returned
 * Flag (`.set`, `.reset`).
 */
export interface FlagsFactory {
  // Boolean flag — always widened to boolean so `.set(ctx, true)` works on a flag declared with `false`.
  (name: string, defaultValue: boolean): Flag<boolean>;
  // String / number flag — type inferred from default.
  <const T extends string | number>(name: string, defaultValue: T): Flag<T>;
  // Variant flag — pass an array literal; default is the first element.
  <
    const A extends
      | VariantsHint<string>
      | VariantsHint<number>
      | VariantsHint<boolean>,
  >(
    name: string,
    variants: A,
  ): Flag<A[number]>;
  // JSON / object flag — pass an explicit T generic.
  <T extends JsonValue>(name: string, defaultValue: T): Flag<T>;
}

/** Return type of {@link createFlags} — the factory plus lifecycle methods. */
export interface FlagsClient extends FlagsFactory {
  /**
   * Resolves once the initial snapshot load has completed (success or failure).
   * Flag evaluations wait for this automatically — calling it is not required
   * for correct behavior. Useful for health checks, warm-up probes, and tests
   * that need to assert snapshot state before evaluating.
   */
  ready(): Promise<void>;
  /** Stop background syncing and release the underlying KV client. */
  close(): Promise<void>;
}

class Runtime implements FlagRuntime {
  private readonly kv: KvClient;
  private readonly snapshot: Snapshot;
  private readonly onEvaluate: ((event: FlagEvent) => void) | undefined;
  private readonly onError: ((event: FlagsErrorEvent) => void) | undefined;

  constructor(kv: KvClient, opts: CreateFlagsOptions) {
    this.kv = kv;
    this.onEvaluate = opts.onEvaluate;
    this.onError = opts.onError;
    this.snapshot = new Snapshot(kv, {
      refresh: opts.refresh ?? 30,
      watch: opts.watch ?? true,
      ...(opts.onError ? { onError: opts.onError } : {}),
    });
    this.snapshot.start();
  }

  ready(): Promise<void> {
    return this.snapshot.ready();
  }

  awaitReady(): Promise<void> {
    return this.snapshot.awaitReady();
  }

  getDef<T extends JsonValue>(name: string): FlagDef<T> | undefined {
    return this.snapshot.get(name) as FlagDef<T> | undefined;
  }

  emit(event: FlagEvent): void {
    if (!this.onEvaluate) return;
    try {
      this.onEvaluate(event);
    } catch {
      // Observability hooks must not crash eval. Swallow.
    }
  }

  reportError(
    error: Error,
    ctx: { source: "set" | "reset"; name: string; id: string },
  ): void {
    this.onError?.({ source: ctx.source, error, name: ctx.name, id: ctx.id });
  }

  async loadPrefsForScope(): Promise<UserPrefs | null> {
    const scope = currentScope();
    if (!scope) return null;
    if (scope.prefs !== undefined) return scope.prefs;
    if (!scope.prefsPromise) {
      scope.prefsPromise = fetchUserPrefs(
        this.kv,
        scope.context.id,
        this.onError,
      );
    }
    const prefs = await scope.prefsPromise;
    scope.prefs = prefs;
    return prefs;
  }

  loadPrefsForId(id: string): Promise<UserPrefs | null> {
    return fetchUserPrefs(this.kv, id, this.onError);
  }

  async setUserPref(
    name: string,
    id: string,
    value: JsonValue,
  ): Promise<void> {
    await mutateUserPrefs(this.kv, id, (prefs) => {
      prefs[name] = value;
      return prefs;
    });
  }

  async resetUserPref(name: string, id: string): Promise<void> {
    await mutateUserPrefs(this.kv, id, (prefs) => {
      delete prefs[name];
      return prefs;
    });
  }

  async close(): Promise<void> {
    this.snapshot.close();
    await this.kv.close();
  }
}

/**
 * Read-modify-write a per-id pref bundle. Uses `cas` when the key already
 * exists to avoid clobbering concurrent writes; falls back to `set` for the
 * absent case. If the resulting bundle is empty, the key is deleted.
 */
async function mutateUserPrefs(
  kv: KvClient,
  id: string,
  mutator: (prefs: UserPrefs) => UserPrefs,
): Promise<void> {
  const key = userKey(id);
  const maxAttempts = 10;
  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    const { data: entry, error } = await kv.get(key);
    if (error) throw error;

    const current: UserPrefs = entry ? entry.json<UserPrefs>() : {};
    const next = mutator({ ...current });

    if (Object.keys(next).length === 0) {
      if (!entry) return; // Already absent, no-op.
      const { error: delErr } = await kv.delete(key);
      if (delErr) throw delErr;
      return;
    }

    const value = JSON.stringify(next);
    let casErr;
    if (entry) {
      ({ error: casErr } = await kv.cas(key, value, entry.revision));
    } else {
      ({ error: casErr } = await kv.set(key, value, { ifAbsent: true }));
    }
    if (!casErr) return;
    if (casErr.status !== 409) throw casErr;
    // Jittered exponential backoff: with N concurrent writers, microtask
    // scheduling can keep them re-racing at exactly the same tick. The
    // 0–10ms × 2^attempt window (capped at 100ms) spreads them out.
    const backoffMs = Math.min(100, Math.random() * 10 * 2 ** attempt);
    await new Promise<void>((r) => setTimeout(r, backoffMs));
  }
  throw new Error(
    `flags: failed to update prefs for id="${id}" after ${maxAttempts} retries (concurrent contention)`,
  );
}

/**
 * Create a flags client bound to a specific {@link KvClient}. Use this when
 * you need a non-default KV connection (e.g. multi-tenant separation, custom
 * retry/timeout, or a separately-typed `Context`).
 *
 * Mirrors `createCache(kv)` and `createKvClient` — the kv client is the
 * dependency, not an option.
 */
export function createFlags<_Ctx extends FlagContext = FlagContext>(
  kv: KvClient,
  opts: CreateFlagsOptions = {},
): FlagsClient {
  const runtime = new Runtime(kv, opts);

  const factory =
    ((name: string, defaultOrVariants: unknown): Flag<JsonValue> => {
      if (Array.isArray(defaultOrVariants)) {
        const variants = defaultOrVariants as readonly JsonValue[];
        if (variants.length === 0) {
          throw new Error(
            `flags("${name}", []) — variants array must have at least one entry`,
          );
        }
        return makeFlag(runtime, name, variants[0] as JsonValue, variants);
      }
      return makeFlag(runtime, name, defaultOrVariants as JsonValue);
    }) as FlagsClient;

  factory.ready = () => runtime.ready();
  factory.close = () => runtime.close();
  return factory;
}

// ── Default lazy singleton ────────────────────────────────────────────────────
//
// Mirrors the `kv` lazy-init pattern: the first call to `flags(...)` constructs
// a default runtime from `BEYOND_KV_URL`, and that runtime is reused for the
// lifetime of the process.

let _default: FlagsClient | undefined;

function getDefault(): FlagsClient {
  if (_default) return _default;
  const url = env["BEYOND_KV_URL"];
  if (!url) {
    throw new Error(
      "BEYOND_KV_URL is required for the default `flags` client. Set it, or use `createFlags(kv)` with a custom client.",
    );
  }
  const kv = createKvClient({ url });
  _default = createFlags(kv);
  return _default;
}

/**
 * Default flags client — lazily initialized from `BEYOND_KV_URL` on first call.
 * Mirrors {@link import('@beyond.dev/kv').kv}.
 *
 * @example
 * ```ts
 * import { flags } from '@beyond.dev/flags'
 *
 * export const newCheckout = flags('new-checkout', false)
 * export const aiSearch    = flags('ai-search', ['off', 'v1', 'v2'])
 *
 * if (await newCheckout()) renderNew()
 * ```
 */
export const flags: FlagsFactory = ((
  name: string,
  defaultOrVariants: unknown,
) => {
  return getDefault()(name as never, defaultOrVariants as never);
}) as FlagsFactory;

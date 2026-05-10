import { AsyncLocalStorage } from "node:async_hooks";
import type { FlagContext, UserPrefs } from "./types.js";

/**
 * Per-request scope shared across flag evaluations. Set by framework
 * middleware (`flags(...)` from `@beyond.dev/flags/{hono,express,...}`) and
 * read by zero-arg `await flag()` calls downstream.
 *
 * `prefs` is the lazily-loaded per-id pref bundle for `context.id`.
 * Subsequent flag evals within the same scope reuse it — one KV round-trip
 * per request regardless of how many flags get evaluated.
 */
export interface FlagsScope {
  context: FlagContext;
  /** Resolved on first eval. `null` means "fetched, no prefs exist". */
  prefs?: UserPrefs | null;
  /** In-flight pref fetch promise — used to coalesce concurrent first evals. */
  prefsPromise?: Promise<UserPrefs | null>;
}

const storage = new AsyncLocalStorage<FlagsScope>();

/** Get the current scope, or `undefined` if not inside one. */
export function currentScope(): FlagsScope | undefined {
  return storage.getStore();
}

/**
 * Run `fn` with `context` as the ambient flags scope. Used by middleware
 * adapters whose framework offers a wrap-the-chain primitive (Hono, Express).
 */
export function runWithScope<T>(
  context: FlagContext,
  fn: () => T | Promise<T>,
): Promise<T> {
  const scope: FlagsScope = { context };
  return Promise.resolve(storage.run(scope, fn));
}

/**
 * Set the scope for the current async context — for frameworks (Fastify,
 * Next.js) that don't expose a wrap-the-chain primitive in their hooks.
 *
 * Caveat: `enterWith` cannot be undone within an async context; that's by
 * design here, since we want the scope to live for the entire request.
 */
export function enterScope(context: FlagContext): void {
  storage.enterWith({ context });
}

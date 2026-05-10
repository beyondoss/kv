import { currentScope } from "./als.js";
import { FlagError } from "./errors.js";
import { evaluate, type Override } from "./eval.js";
import type {
  FlagContext,
  FlagDef,
  FlagEvent,
  JsonValue,
  UserPrefs,
} from "./types.js";

/**
 * Variant mapping passed to {@link makeFlag} when the user provided a literal
 * array of allowed values (e.g. `flags('ai-search', ['off','v1','v2'])`).
 * The first element is the default; the array is the runtime variant list.
 */
export type VariantsHint<T extends JsonValue> = readonly [T, ...T[]];

/**
 * A defined flag — callable to evaluate, with chainable methods for code
 * overrides (`.when()`) and end-user opt-in/out (`.set()`, `.reset()`).
 *
 * Two call shapes:
 *   - `await flag()` reads context from the ALS scope set by the framework adapter.
 *   - `await flag(ctx)` uses an explicit context (workflows, cron, tests).
 */
export interface Flag<T extends JsonValue> {
  /** Eval using the current ALS scope's context. Throws if no scope is active. */
  (): Promise<T>;
  /** Eval with an explicit context. */
  (ctx: FlagContext): Promise<T>;

  /** Flag name (the `key` in `flags:def:<name>`). */
  readonly name: string;
  /** The default returned when no rule, override, rollout, or user pref matches. */
  readonly default: T;
  /** Allowed variants when declared via the array form, otherwise `undefined`. */
  readonly variants?: readonly T[];

  /**
   * Set a code override that runs after the kill switch and end-user pref
   * checks but before rules and rollout. Return `undefined` to fall through
   * to the next layer. Mutates and returns the same flag for chaining.
   */
  when(override: Override<T>): Flag<T>;

  /** Attach a human description (used by tooling). Mutates and returns the flag. */
  desc(text: string): Flag<T>;

  /**
   * Set this flag's value for one user. Writes `flags:user:<id>` in KV.
   * Idempotent; safe to call repeatedly.
   */
  set(ctxOrId: FlagContext | { id: string }, value: T): Promise<void>;

  /** Remove this user's override; the flag reverts to rules/rollout/default. */
  reset(ctxOrId: FlagContext | { id: string }): Promise<void>;
}

/**
 * Internal facade the runtime exposes to a Flag — separates the public Flag
 * surface from the runtime's snapshot/kv/observability machinery.
 */
export interface FlagRuntime {
  awaitReady(): Promise<void>;
  getDef<T extends JsonValue>(name: string): FlagDef<T> | undefined;
  emit(event: FlagEvent): void;
  reportError(
    error: Error,
    ctx: { source: "set" | "reset"; name: string; id: string },
  ): void;
  setUserPref(name: string, id: string, value: JsonValue): Promise<void>;
  resetUserPref(name: string, id: string): Promise<void>;
  /** Resolve the per-id pref bundle for the current request scope (cached). */
  loadPrefsForScope(): Promise<UserPrefs | null>;
  /** Fetch the per-id pref bundle for an explicit id (no scope caching). */
  loadPrefsForId(id: string): Promise<UserPrefs | null>;
}

/**
 * Build a `Flag<T>` bound to a runtime. Most callers reach this through
 * `flags(name, default)` rather than constructing it directly.
 */
export function makeFlag<T extends JsonValue>(
  runtime: FlagRuntime,
  name: string,
  defaultValue: T,
  variants?: readonly T[],
): Flag<T> {
  let override: Override<T> | undefined;

  const callable = ((ctx?: FlagContext): Promise<T> => {
    return runEval(ctx);
  }) as Flag<T>;

  async function runEval(explicit?: FlagContext): Promise<T> {
    const start = performance.now();
    let context: FlagContext;
    let prefs: UserPrefs | null = null;

    if (explicit) {
      context = explicit;
      if (!context.id) {
        const err = new FlagError(
          "missing_id",
          `flag "${name}" called with an empty id. Ensure the context always provides a non-empty id.`,
        );
        emitError(err, start);
        throw err;
      }
      // Explicit context skips the per-request scope — lookup user prefs
      // directly. Callers that want caching across many evals should run
      // inside a scope (via the framework adapter).
      prefs = await runtime.loadPrefsForId(context.id);
    } else {
      const scope = currentScope();
      if (!scope) {
        const err = new FlagError(
          "no_context",
          `flag "${name}" called with no context. Pass a context arg or wrap with a flags middleware.`,
        );
        emitError(err, start);
        throw err;
      }
      context = scope.context;
      if (!context.id) {
        const err = new FlagError(
          "missing_id",
          `flag "${name}" called with an empty id in the current scope. Ensure your middleware sets a non-empty id.`,
        );
        emitError(err, start);
        throw err;
      }
      prefs = await runtime.loadPrefsForScope();
    }

    await runtime.awaitReady();
    const state = runtime.getDef<T>(name);
    const result = evaluate(
      name,
      defaultValue,
      context,
      state,
      prefs,
      override,
    );
    const event: FlagEvent = {
      name,
      value: result.value,
      reason: result.reason,
      durationMs: performance.now() - start,
      id: context.id,
    };
    if (result.ruleIndex !== undefined) event.ruleIndex = result.ruleIndex;
    runtime.emit(event);
    return result.value;
  }

  function emitError(error: Error, start: number): void {
    runtime.emit({
      name,
      value: undefined,
      reason: "error",
      durationMs: performance.now() - start,
      error,
    });
  }

  Object.defineProperty(callable, "name", { value: name, configurable: false });
  Object.defineProperty(callable, "default", {
    value: defaultValue,
    configurable: false,
  });
  if (variants !== undefined) {
    Object.defineProperty(callable, "variants", {
      value: variants,
      configurable: false,
    });
  }

  callable.when = (next: Override<T>) => {
    override = next;
    return callable;
  };

  callable.desc = (_text: string) => {
    // Reserved for future tooling integration; intentionally a no-op at runtime.
    return callable;
  };

  callable.set = async (
    ctxOrId: FlagContext | { id: string },
    value: T,
  ): Promise<void> => {
    const id = ctxOrId.id;
    if (!id) {
      throw new FlagError(
        "missing_id",
        `flag.set requires an id (got "${id}").`,
      );
    }
    try {
      await runtime.setUserPref(name, id, value);
    } catch (err) {
      const error = err instanceof Error ? err : new Error(String(err));
      runtime.reportError(error, { source: "set", name, id });
      throw error;
    }
  };

  callable.reset = async (
    ctxOrId: FlagContext | { id: string },
  ): Promise<void> => {
    const id = ctxOrId.id;
    if (!id) {
      throw new FlagError(
        "missing_id",
        `flag.reset requires an id (got "${id}").`,
      );
    }
    try {
      await runtime.resetUserPref(name, id);
    } catch (err) {
      const error = err instanceof Error ? err : new Error(String(err));
      runtime.reportError(error, { source: "reset", name, id });
      throw error;
    }
  };

  return callable;
}

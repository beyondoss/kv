import { bucket } from "./hash.js";
import type {
  EvalReason,
  FlagContext,
  FlagDef,
  JsonValue,
  Rule,
  UserPrefs,
} from "./types.js";

/** Override callback set via `flag.when(...)`. */
export type Override<T> = (params: {
  context: FlagContext;
}) => T | undefined;

/** Result of {@link evaluate} — the value plus the reason it was returned. */
export interface EvalResult<T> {
  value: T;
  reason: EvalReason;
  /** Index of the matched rule when `reason === "rule"`. */
  ruleIndex?: number;
}

/**
 * Pure eval engine. Order:
 *   0. no snapshot        → default                (cold-boot / unknown flag)
 *   1. on === false       → default                (kill switch)
 *   2. user pref set      → return that            (end-user opt-in)
 *   3. override defined   → return override(ctx)   (code escape hatch)
 *   4. walk rules         → first when-match wins  (ops targeting)
 *   5. apply rollout      → bucketed               (ops rollout)
 *   6. default
 *
 * Note: steps 2 and 3 apply even when no snapshot state exists for the flag,
 * because user prefs and code overrides are independent of ops state.
 */
export function evaluate<T extends JsonValue>(
  name: string,
  defaultValue: T,
  context: FlagContext,
  state: FlagDef<T> | undefined,
  prefs: UserPrefs | null,
  override?: Override<T>,
): EvalResult<T> {
  // (1) Kill switch — checked first because nothing should override a kill.
  if (state && state.on === false) {
    return { value: defaultValue, reason: "off" };
  }

  // (2) End-user pref — applies even when KV has no def for this flag,
  // because the user's explicit choice is independent of ops state.
  if (prefs && Object.prototype.hasOwnProperty.call(prefs, name)) {
    return { value: prefs[name] as T, reason: "user-pref" };
  }

  // (3) Code override — same reasoning as user prefs: applies without a def.
  if (override) {
    const overridden = override({ context });
    if (overridden !== undefined) {
      return { value: overridden, reason: "override" };
    }
  }

  // (4) No def in snapshot — nothing more to evaluate.
  if (!state) {
    return { value: defaultValue, reason: "no-snapshot" };
  }

  // (5) Targeting rules.
  if (state.rules?.length) {
    for (let i = 0; i < state.rules.length; i++) {
      const rule = state.rules[i] as Rule<T>;
      if (matches(rule.when, context)) {
        return { value: rule.value, reason: "rule", ruleIndex: i };
      }
    }
  }

  // (6) Percentage rollout.
  if (state.rollout && state.rollout.percent > 0) {
    const pct = clamp(state.rollout.percent, 0, 100);
    if (bucket(context.id, name) < pct) {
      const value = (state.rollout.value ?? (true as unknown as T)) as T;
      return { value, reason: "rollout" };
    }
  }

  // (7) Fall through to default.
  return { value: defaultValue, reason: "default" };
}

/**
 * Match a rule's `when` clause against the context. A rule matches when every
 * key in `when` matches the context's value for that key.
 *
 * - Single value → strict equality.
 * - Array value  → any-of (the context's value must equal one of them).
 * - Missing key on context → no match (rule is more specific than the context).
 */
function matches(
  when: Rule<unknown>["when"],
  context: FlagContext,
): boolean {
  for (const key of Object.keys(when) as (keyof typeof when)[]) {
    const constraint = when[key];
    if (constraint === undefined) continue;
    const actual =
      (context as unknown as Record<string, unknown>)[key as string];
    if (Array.isArray(constraint)) {
      if (!constraint.some((v) => v === actual)) return false;
    } else if (constraint !== actual) {
      return false;
    }
  }
  return true;
}

function clamp(n: number, lo: number, hi: number): number {
  return n < lo ? lo : n > hi ? hi : n;
}

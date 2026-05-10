/**
 * JSON-serializable value. Used for flag values that aren't booleans, strings,
 * numbers, or string variants — typically configuration objects.
 */
export type JsonValue =
  | string
  | number
  | boolean
  | null
  | JsonValue[]
  | { [key: string]: JsonValue };

/**
 * Base shape every flag context must satisfy. `id` is the stable identifier
 * used for deterministic rollout bucketing and per-id pref lookups. Apps
 * decide what `id` actually represents (user id, hashed IP, session, device,
 * workflow run, etc.) when they build the context.
 */
export interface BaseContext {
  id: string;
}

/**
 * Augmentable context interface. Apps add their own attributes by declaring:
 *
 * ```ts
 * declare module '@beyond.dev/flags' {
 *   interface Context {
 *     plan: 'free' | 'pro' | 'enterprise'
 *     country: string
 *   }
 * }
 * ```
 *
 * `id: string` is built into {@link BaseContext} — don't redeclare it.
 */
// biome-ignore lint/suspicious/noEmptyInterface: augmentable
export interface Context {}

/** Combined runtime context type — base fields merged with the augmented {@link Context}. */
export type FlagContext = BaseContext & Context;

/**
 * A targeting rule. `when` is a partial match against the context — every key
 * present must equal the context's value (array values are any-of within a
 * key). Rules are walked in order; the first match wins.
 */
export type Rule<T, Ctx extends BaseContext = FlagContext> = {
  when: { [K in keyof Ctx]?: Ctx[K] | readonly Ctx[K][] };
  value: T;
};

/**
 * Rollout descriptor. The bucket key is always `ctx.id`; the percentage is
 * computed deterministically as `fnv1a(ctx.id + flag.name) % 100 < percent`.
 * Same id + same flag → same answer, always.
 */
export type Rollout<T> = {
  /** 0..100. Out-of-range values are clamped. */
  percent: number;
  /** Returned on hit. Defaults to `true` for boolean flags. */
  value?: T;
};

/**
 * Flag definition state stored in KV at `flags:def:<name>`. Mutated by ops via
 * the CLI; never directly by application code.
 */
export interface FlagDef<T = JsonValue> {
  /** Master kill switch. When `false`, eval always returns the flag's `default`. */
  on: boolean;
  /** First-match-wins targeting rules. */
  rules?: Rule<T>[];
  /** Deterministic % rollout for contexts that match no rule. */
  rollout?: Rollout<T>;
}

/**
 * Per-id pref bundle stored at `flags:user:<id>`. Sparse — only fields that
 * differ from the flag's default are stored. Field key is the flag name.
 */
export type UserPrefs = Record<string, JsonValue>;

/** Reasons surfaced to {@link FlagEvent.reason} for observability. */
export type EvalReason =
  | "default"
  | "off"
  | "user-pref"
  | "override"
  | "rule"
  | "rollout"
  | "no-snapshot"
  | "error";

/** Observability event emitted for every {@link Flag} call. */
export interface FlagEvent {
  /** Flag name. */
  name: string;
  /** Resolved value, or `undefined` if eval threw. */
  value: unknown;
  /** Why this value was returned. */
  reason: EvalReason;
  /** Wall-clock duration of the eval in milliseconds. */
  durationMs: number;
  /** Id used for bucketing/pref lookup, if available. */
  id?: string;
  /** Index of the matched rule, when `reason === "rule"`. */
  ruleIndex?: number;
  /** Error if eval threw or any subsystem failed. */
  error?: Error;
}

/** Surfaced via `onError` for snapshot/watch/KV failures (eval-time errors flow through `onEvaluate`). */
export interface FlagsErrorEvent {
  /** Where the error originated. */
  source: "snapshot" | "watch" | "user-prefs" | "set" | "reset";
  /** Underlying error. */
  error: Error;
  /** Optional flag name when the error is scoped to one. */
  name?: string;
  /** Optional id when the error is scoped to a user pref op. */
  id?: string;
}

/**
 * `@beyond.dev/flags` — typed feature flags backed by `@beyond.dev/kv`.
 *
 * Code declares the contract (name, type, default). KV stores the state
 * (kill switch, rules, rollout). The eval engine combines them with a
 * per-request {@link Context} to produce a typed value.
 *
 * @packageDocumentation
 */

export { currentScope, enterScope, runWithScope } from "./als.js";
export type { FlagsScope } from "./als.js";
export { FlagError } from "./errors.js";
export type { Flag, FlagRuntime, VariantsHint } from "./flag.js";
export {
  createFlags,
  type CreateFlagsOptions,
  flags,
  type FlagsClient,
  type FlagsFactory,
} from "./flags.js";
export type {
  BaseContext,
  Context,
  EvalReason,
  FlagContext,
  FlagDef,
  FlagEvent,
  FlagsErrorEvent,
  JsonValue,
  Rollout,
  Rule,
  UserPrefs,
} from "./types.js";

/**
 * SDK-agnostic glue shared by the OpenFeature server and web providers.
 *
 * This module maps between OpenFeature's evaluation model and Beyond's pure
 * {@link evaluate} engine. It imports only from `@openfeature/core` (the shared
 * base both the server and web SDKs depend on), so it can be bundled into either
 * provider entry point without pulling the other SDK.
 *
 * @packageDocumentation
 */

import {
  ErrorCode,
  type EvaluationContext,
  type ResolutionDetails,
  type ResolutionReason,
  StandardResolutionReasons,
} from "@openfeature/core";
import type { EvalResult } from "../eval.js";
import type { EvalReason, FlagContext, JsonValue } from "../types.js";

/** The four flag value shapes OpenFeature resolves, used for type checking. */
export type ExpectedType = "boolean" | "string" | "number" | "object";

/**
 * Turn an OpenFeature {@link EvaluationContext} into Beyond's {@link FlagContext}.
 *
 * `targetingKey` becomes the rollout/pref bucket key `id`; every other attribute
 * is carried through for targeting-rule matching. A missing `targetingKey` maps
 * to an empty `id` — targeting rules still match on the other attributes, while
 * rollout bucketing and per-user prefs are simply skipped (an empty id can't be
 * bucketed or looked up). This mirrors the Vercel adapter's "no id → default"
 * behavior without throwing.
 */
export function toFlagContext(context: EvaluationContext): FlagContext {
  const { targetingKey, ...rest } = context;
  return { id: targetingKey ?? "", ...rest } as unknown as FlagContext;
}

/** Whether the context can drive rollout bucketing / pref lookup. */
export function hasTargetingKey(context: EvaluationContext): boolean {
  return typeof context.targetingKey === "string"
    && context.targetingKey !== "";
}

/**
 * Map Beyond's {@link EvalReason} to an OpenFeature {@link ResolutionReason}.
 * `no-snapshot` reports `STALE` before the provider has loaded (the def may yet
 * arrive) and `DEFAULT` afterward (the flag genuinely has no def in KV).
 */
export function mapReason(
  reason: EvalReason,
  ready: boolean,
): ResolutionReason {
  switch (reason) {
    case "off":
      return StandardResolutionReasons.DISABLED;
    case "user-pref":
    case "override":
    case "rule":
      return StandardResolutionReasons.TARGETING_MATCH;
    case "rollout":
      return StandardResolutionReasons.SPLIT;
    case "no-snapshot":
      return ready
        ? StandardResolutionReasons.DEFAULT
        : StandardResolutionReasons.STALE;
    case "error":
      return StandardResolutionReasons.ERROR;
    default:
      return StandardResolutionReasons.DEFAULT;
  }
}

function typeMatches(value: JsonValue, expected: ExpectedType): boolean {
  switch (expected) {
    case "boolean":
      return typeof value === "boolean";
    case "string":
      return typeof value === "string";
    case "number":
      return typeof value === "number";
    case "object":
      // JSON objects and arrays are both valid OpenFeature object flags.
      return typeof value === "object" && value !== null;
  }
}

/**
 * Build an OpenFeature {@link ResolutionDetails} from an {@link EvalResult}.
 *
 * Enforces the OpenFeature type contract: if the resolved value doesn't match
 * the requested flag type, the declared `defaultValue` is returned with a
 * `TYPE_MISMATCH` error code instead of coercing. `flagMetadata` carries the
 * native `beyondReason` (and `ruleIndex` when a targeting rule matched) for
 * debugging.
 */
export function toResolution<T extends JsonValue>(
  result: EvalResult<JsonValue>,
  defaultValue: T,
  expected: ExpectedType,
  ready: boolean,
): ResolutionDetails<T> {
  if (!typeMatches(result.value, expected)) {
    return {
      value: defaultValue,
      reason: StandardResolutionReasons.ERROR,
      errorCode: ErrorCode.TYPE_MISMATCH,
      errorMessage: `flag resolved to ${
        describe(result.value)
      }, expected ${expected}`,
      flagMetadata: { beyondReason: result.reason },
    };
  }
  const flagMetadata: Record<string, string | number | boolean> = {
    beyondReason: result.reason,
  };
  if (result.ruleIndex !== undefined) {
    flagMetadata["ruleIndex"] = result.ruleIndex;
  }
  return {
    value: result.value as T,
    reason: mapReason(result.reason, ready),
    flagMetadata,
  };
}

function describe(value: JsonValue): string {
  if (value === null) return "null";
  if (Array.isArray(value)) return "array";
  return typeof value;
}

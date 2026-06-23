/**
 * OpenFeature **web** (client-side) provider for `@beyond.dev/flags`.
 *
 * The web SDK resolves flags **synchronously** against a single, static
 * evaluation context. This provider satisfies that by keeping an in-memory,
 * watch-synced {@link Snapshot} of all `flags:def:*` and pre-fetching the active
 * context's per-user prefs on `initialize`/`onContextChange`. Each resolution is
 * then a synchronous {@link evaluate} over in-memory state — zero I/O.
 *
 * Live KV changes are surfaced as `PROVIDER_CONFIGURATION_CHANGED`, prompting the
 * SDK to re-resolve.
 *
 * @example
 * ```ts
 * import { OpenFeature } from '@openfeature/web-sdk'
 * import { createKvClient } from '@beyond.dev/kv'
 * import { BeyondWebProvider } from '@beyond.dev/flags/openfeature/web'
 *
 * const kv = createKvClient({ url: '...' })
 * await OpenFeature.setContext({ targetingKey: 'user-123', plan: 'pro' })
 * await OpenFeature.setProviderAndWait(new BeyondWebProvider(kv))
 *
 * const enabled = OpenFeature.getClient().getBooleanValue('new-checkout', false)
 * ```
 *
 * @packageDocumentation
 */

import type { KvClient } from "@beyond.dev/kv";
import {
  type EvaluationContext,
  type JsonValue as OFJsonValue,
  type Logger,
  OpenFeatureEventEmitter,
  type Provider,
  type ProviderMetadata,
  ProviderEvents,
  type ResolutionDetails,
} from "@openfeature/web-sdk";
import { evaluate } from "../eval.js";
import { fetchUserPrefs, Snapshot } from "../snapshot.js";
import type { FlagsErrorEvent, JsonValue, UserPrefs } from "../types.js";
import {
  type ExpectedType,
  toFlagContext,
  toResolution,
} from "./shared.js";

/** Options for {@link BeyondWebProvider}. The KV client is passed positionally. */
export interface BeyondWebProviderOptions {
  /** SWR poll interval (seconds) when watch is unavailable. Default 30. */
  refresh?: number;
  /** Use `kv.watch()` for instant invalidation + change events. Default true. */
  watch?: boolean;
  /** Honor per-user prefs (`flags:user:<id>`) for the active context. Default true. */
  userPrefs?: boolean;
  /** Called for snapshot/watch/KV/pref failures. */
  onError?: (event: FlagsErrorEvent) => void;
}

/**
 * An OpenFeature web {@link Provider} backed by Beyond KV. Resolves
 * synchronously against an in-memory snapshot; prefs for the active context are
 * pre-fetched on context change.
 */
export class BeyondWebProvider implements Provider {
  readonly metadata: ProviderMetadata = { name: "beyond-kv" };
  readonly runsOn = "client";
  readonly events = new OpenFeatureEventEmitter();

  private readonly snapshot: Snapshot;
  private readonly kv: KvClient;
  private readonly useUserPrefs: boolean;
  private readonly onError: ((e: FlagsErrorEvent) => void) | undefined;
  private ready = false;
  private cachedPrefs: UserPrefs | null = null;
  private cachedPrefsId = "";

  constructor(kv: KvClient, opts: BeyondWebProviderOptions = {}) {
    this.kv = kv;
    this.useUserPrefs = opts.userPrefs !== false;
    this.onError = opts.onError;
    this.snapshot = new Snapshot(kv, {
      refresh: opts.refresh ?? 30,
      watch: opts.watch ?? true,
      onChange: (flagsChanged) => {
        this.events.emit(ProviderEvents.ConfigurationChanged, { flagsChanged });
      },
      ...(opts.onError ? { onError: opts.onError } : {}),
    });
  }

  /** Load the snapshot and the initial context's prefs. SDK emits `PROVIDER_READY`. */
  async initialize(context?: EvaluationContext): Promise<void> {
    this.snapshot.start();
    await this.snapshot.awaitReady();
    await this.loadPrefs(context?.targetingKey ?? "");
    this.ready = true;
  }

  /** Re-fetch prefs for the new static context. Async → SDK reconciles + re-resolves. */
  async onContextChange(
    _oldContext: EvaluationContext,
    newContext: EvaluationContext,
  ): Promise<void> {
    await this.loadPrefs(newContext.targetingKey ?? "");
  }

  /** Stop background syncing. Does not close the KV client (caller owns it). */
  async onClose(): Promise<void> {
    this.snapshot.close();
  }

  private async loadPrefs(id: string): Promise<void> {
    if (!this.useUserPrefs || id === "") {
      this.cachedPrefs = null;
      this.cachedPrefsId = id;
      return;
    }
    this.cachedPrefs = await fetchUserPrefs(this.kv, id, this.onError);
    this.cachedPrefsId = id;
  }

  resolveBooleanEvaluation(
    flagKey: string,
    defaultValue: boolean,
    context: EvaluationContext,
    _logger: Logger,
  ): ResolutionDetails<boolean> {
    return this.resolve(flagKey, defaultValue, context, "boolean");
  }

  resolveStringEvaluation(
    flagKey: string,
    defaultValue: string,
    context: EvaluationContext,
    _logger: Logger,
  ): ResolutionDetails<string> {
    return this.resolve(flagKey, defaultValue, context, "string");
  }

  resolveNumberEvaluation(
    flagKey: string,
    defaultValue: number,
    context: EvaluationContext,
    _logger: Logger,
  ): ResolutionDetails<number> {
    return this.resolve(flagKey, defaultValue, context, "number");
  }

  resolveObjectEvaluation<T extends OFJsonValue>(
    flagKey: string,
    defaultValue: T,
    context: EvaluationContext,
    _logger: Logger,
  ): ResolutionDetails<T> {
    return this.resolve(
      flagKey,
      defaultValue as JsonValue,
      context,
      "object",
    ) as ResolutionDetails<T>;
  }

  private resolve<T extends JsonValue>(
    flagKey: string,
    defaultValue: T,
    context: EvaluationContext,
    expected: ExpectedType,
  ): ResolutionDetails<T> {
    const ctx = toFlagContext(context);
    // Prefs are pre-fetched per context; use them only when they match the
    // context being resolved (the SDK keeps these in lockstep).
    const prefs = ctx.id === this.cachedPrefsId ? this.cachedPrefs : null;
    const result = evaluate<JsonValue>(
      flagKey,
      defaultValue,
      ctx,
      this.snapshot.get(flagKey),
      prefs,
    );
    return toResolution(result, defaultValue, expected, this.ready);
  }
}

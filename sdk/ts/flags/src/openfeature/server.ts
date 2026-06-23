/**
 * OpenFeature **server** provider for `@beyond.dev/flags`.
 *
 * Resolves OpenFeature flag evaluations against Beyond KV using the same
 * `flags:def:*` defs, targeting rules, rollout, kill switch, and per-user prefs
 * as the native `@beyond.dev/flags` API and the Vercel adapter — through the
 * same pure {@link evaluate} engine.
 *
 * @example
 * ```ts
 * import { OpenFeature } from '@openfeature/server-sdk'
 * import { createKvClient } from '@beyond.dev/kv'
 * import { BeyondProvider } from '@beyond.dev/flags/openfeature/server'
 *
 * const kv = createKvClient({ url: process.env.BEYOND_KV_URL! })
 * await OpenFeature.setProviderAndWait(new BeyondProvider(kv))
 *
 * const client = OpenFeature.getClient()
 * const enabled = await client.getBooleanValue('new-checkout', false, {
 *   targetingKey: 'user-123',
 *   plan: 'pro',
 * })
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
} from "@openfeature/server-sdk";
import { evaluate } from "../eval.js";
import { defKey, fetchUserPrefs, Snapshot } from "../snapshot.js";
import type {
  FlagDef,
  FlagsErrorEvent,
  JsonValue,
  UserPrefs,
} from "../types.js";
import {
  type ExpectedType,
  hasTargetingKey,
  toFlagContext,
  toResolution,
} from "./shared.js";

/** Options for {@link BeyondProvider}. The KV client is passed positionally. */
export interface BeyondProviderOptions {
  /**
   * How defs are read from KV.
   *
   * - `"snapshot"` (default): keep an in-memory snapshot of all `flags:def:*`,
   *   refreshed via `kv.watch()` (or polling). Resolve does zero KV round-trips
   *   for defs and surfaces live changes as `PROVIDER_CONFIGURATION_CHANGED`.
   * - `"fetch"`: read each def from KV on every evaluation. No background sync,
   *   no change events — for environments that can't hold a persistent watch.
   */
  mode?: "snapshot" | "fetch";
  /** snapshot mode: SWR poll interval (seconds) when watch is unavailable. Default 30. */
  refresh?: number;
  /** snapshot mode: use `kv.watch()` for instant invalidation + events. Default true. */
  watch?: boolean;
  /**
   * Honor per-user prefs (`flags:user:<id>`) during resolution. Default true.
   * Each resolution costs one KV read for the pref bundle; disable for zero
   * per-eval I/O in snapshot mode.
   */
  userPrefs?: boolean;
  /** Called for snapshot/watch/KV/pref failures. */
  onError?: (event: FlagsErrorEvent) => void;
}

/** Reads flag defs from KV — snapshot-backed or per-eval fetch. */
interface DefReader {
  ready(): Promise<void>;
  get(key: string): Promise<FlagDef | undefined>;
  close(): void;
}

class SnapshotReader implements DefReader {
  private readonly snapshot: Snapshot;

  constructor(
    kv: KvClient,
    opts: BeyondProviderOptions,
    onChange: (names: string[]) => void,
  ) {
    this.snapshot = new Snapshot(kv, {
      refresh: opts.refresh ?? 30,
      watch: opts.watch ?? true,
      onChange,
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

  close(): void {
    this.snapshot.close();
  }
}

class FetchReader implements DefReader {
  private readonly kv: KvClient;
  private readonly onError: ((e: FlagsErrorEvent) => void) | undefined;

  constructor(kv: KvClient, opts: BeyondProviderOptions) {
    this.kv = kv;
    this.onError = opts.onError;
  }

  ready(): Promise<void> {
    return Promise.resolve();
  }

  async get(key: string): Promise<FlagDef | undefined> {
    const { data, error } = await this.kv.get(defKey(key));
    if (error) {
      this.onError?.({ source: "snapshot", error, name: key });
      return undefined;
    }
    if (!data) return undefined;
    return parseDef(data.text(), key, this.onError);
  }

  close(): void {
    // Nothing to release.
  }
}

/**
 * An OpenFeature {@link Provider} backed by Beyond KV.
 *
 * Long-lived: register once via `OpenFeature.setProviderAndWait(...)`. In the
 * default snapshot mode it holds an in-memory, watch-synced copy of all defs and
 * emits `PROVIDER_CONFIGURATION_CHANGED` when KV changes.
 */
export class BeyondProvider implements Provider {
  readonly metadata: ProviderMetadata = { name: "beyond-kv" };
  readonly runsOn = "server";
  readonly events = new OpenFeatureEventEmitter();

  private readonly reader: DefReader;
  private readonly kv: KvClient;
  private readonly useUserPrefs: boolean;
  private readonly onError: ((e: FlagsErrorEvent) => void) | undefined;
  private ready = false;

  constructor(kv: KvClient, opts: BeyondProviderOptions = {}) {
    this.kv = kv;
    this.useUserPrefs = opts.userPrefs !== false;
    this.onError = opts.onError;
    this.reader =
      (opts.mode ?? "snapshot") === "fetch"
        ? new FetchReader(kv, opts)
        : new SnapshotReader(kv, opts, (flagsChanged) => {
            this.events.emit(ProviderEvents.ConfigurationChanged, {
              flagsChanged,
            });
          });
  }

  /** Await the initial snapshot load. The SDK emits `PROVIDER_READY` on resolve. */
  async initialize(): Promise<void> {
    await this.reader.ready();
    this.ready = true;
  }

  /** Stop background syncing. Does not close the KV client (caller owns it). */
  async onClose(): Promise<void> {
    this.reader.close();
  }

  resolveBooleanEvaluation(
    flagKey: string,
    defaultValue: boolean,
    context: EvaluationContext,
    _logger: Logger,
  ): Promise<ResolutionDetails<boolean>> {
    return this.resolve(flagKey, defaultValue, context, "boolean");
  }

  resolveStringEvaluation(
    flagKey: string,
    defaultValue: string,
    context: EvaluationContext,
    _logger: Logger,
  ): Promise<ResolutionDetails<string>> {
    return this.resolve(flagKey, defaultValue, context, "string");
  }

  resolveNumberEvaluation(
    flagKey: string,
    defaultValue: number,
    context: EvaluationContext,
    _logger: Logger,
  ): Promise<ResolutionDetails<number>> {
    return this.resolve(flagKey, defaultValue, context, "number");
  }

  resolveObjectEvaluation<T extends OFJsonValue>(
    flagKey: string,
    defaultValue: T,
    context: EvaluationContext,
    _logger: Logger,
  ): Promise<ResolutionDetails<T>> {
    return this.resolve(flagKey, defaultValue as JsonValue, context, "object") as
      Promise<ResolutionDetails<T>>;
  }

  private async resolve<T extends JsonValue>(
    flagKey: string,
    defaultValue: T,
    context: EvaluationContext,
    expected: ExpectedType,
  ): Promise<ResolutionDetails<T>> {
    const ctx = toFlagContext(context);
    const def = await this.reader.get(flagKey);
    const prefs: UserPrefs | null =
      this.useUserPrefs && hasTargetingKey(context)
        ? await fetchUserPrefs(this.kv, ctx.id, this.onError)
        : null;
    const result = evaluate<JsonValue>(flagKey, defaultValue, ctx, def, prefs);
    return toResolution(result, defaultValue, expected, this.ready);
  }
}

/**
 * Parse a `flags:def:*` JSON payload into a {@link FlagDef}. Returns `undefined`
 * (treated as "no def" → flag falls back to its declared default) on malformed
 * input, reporting through `onError` rather than throwing into resolution.
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
      parsed === null ||
      typeof parsed !== "object" ||
      Array.isArray(parsed) ||
      typeof (parsed as Record<string, unknown>)["on"] !== "boolean"
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

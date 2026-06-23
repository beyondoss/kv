import type { KvClient, WatchOptions } from "@beyond.dev/kv";
import { FlagError } from "./errors.js";
import type {
  FlagDef,
  FlagsErrorEvent,
  JsonValue,
  UserPrefs,
} from "./types.js";

const DEF_PREFIX = "flags:def:";
const USER_PREFIX = "flags:user:";

const decoder = new TextDecoder();

/**
 * In-memory snapshot of all `flags:def:*` entries. Loaded once at boot, then
 * kept in sync via `kv.watch()` (preferred) or polling (fallback).
 *
 * Reads are O(1) Map lookups — zero KV round-trips per flag eval.
 */
export class Snapshot {
  /**
   * Parsed def plus the exact KV bytes it was decoded from. The raw bytes are
   * the source of truth for change detection (see {@link applyValue}) — keeping
   * them avoids re-serializing parsed objects to compare.
   */
  private state = new Map<string, { def: FlagDef; raw: Uint8Array }>();
  private readyPromise: Promise<void>;
  private resolveReady!: () => void;
  private watchAbort: AbortController | undefined;
  private pollTimer: ReturnType<typeof setInterval> | undefined;
  private closed = false;
  isReady = false;
  /**
   * Highest revision observed from any read or watch event. Used to resume the
   * watch from where it left off after a hard reconnect (and to start the first
   * watch from the point the initial load saw), so deltas that land while the
   * stream is down are replayed rather than lost.
   */
  private lastRevision = 0;

  readonly kv: KvClient;
  readonly opts: {
    refresh: number;
    watch: boolean;
    onError?: (e: FlagsErrorEvent) => void;
    onChange?: (names: string[]) => void;
  };

  constructor(
    kv: KvClient,
    opts: {
      refresh: number;
      watch: boolean;
      onError?: (e: FlagsErrorEvent) => void;
      /**
       * Called after the *initial* load with the names of flags whose def
       * changed (added, updated, or removed) via a watch delta or a poll
       * reload. Never fired during the initial load. Used to surface live
       * config changes (e.g. OpenFeature `PROVIDER_CONFIGURATION_CHANGED`).
       */
      onChange?: (names: string[]) => void;
    },
  ) {
    this.kv = kv;
    this.opts = opts;
    this.readyPromise = new Promise<void>((r) => {
      this.resolveReady = r;
    });
  }

  /** Resolves once the initial snapshot load has finished (success or failure). */
  ready(): Promise<void> {
    return this.readyPromise;
  }

  /** Returns a resolved promise if already loaded, otherwise the ready promise. */
  awaitReady(): Promise<void> {
    return this.isReady ? Promise.resolve() : this.readyPromise;
  }

  /** Lookup the current state for a flag. Returns `undefined` if absent. */
  get(name: string): FlagDef | undefined {
    return this.state.get(name)?.def;
  }

  /** Start the snapshot: initial load + background sync (watch or poll). */
  start(): void {
    void this.runInitialLoad();
  }

  private async runInitialLoad(): Promise<void> {
    try {
      await this.loadAll();
    } catch (err) {
      this.reportError("snapshot", err);
    } finally {
      this.isReady = true;
      this.resolveReady();
    }

    if (this.closed) return;

    if (this.opts.watch) {
      void this.runWatch();
    } else {
      this.startPolling();
    }
  }

  private async loadAll(): Promise<void> {
    let cursor: string | undefined;
    const seen = new Set<string>();
    const changed: string[] = [];
    do {
      const { data, error } = await this.kv.list(
        cursor === undefined
          ? { prefix: DEF_PREFIX }
          : { prefix: DEF_PREFIX, cursor },
      );
      if (error) throw error;
      const names = data.keys.map((k) => k.name);
      if (names.length > 0) {
        const entries = await this.kv.batchGet(names);
        if (entries.error) throw entries.error;
        for (let i = 0; i < names.length; i++) {
          const fullKey = names[i] as string;
          const entry = entries.data[i];
          if (!entry) continue;
          const flagName = fullKey.slice(DEF_PREFIX.length);
          seen.add(flagName);
          if (entry.revision > this.lastRevision) {
            this.lastRevision = entry.revision;
          }
          if (this.applyValue(flagName, entry.value)) changed.push(flagName);
        }
      }
      cursor = data.nextCursor;
    } while (cursor);

    // Drop any flag that's no longer in KV.
    for (const name of this.state.keys()) {
      if (!seen.has(name)) {
        this.state.delete(name);
        changed.push(name);
      }
    }

    this.notifyChange(changed);
  }

  private async runWatch(): Promise<void> {
    let attempt = 0;
    while (!this.closed) {
      this.watchAbort = new AbortController();
      // Resume from the last revision we saw so deltas that arrived while the
      // stream was down are replayed (the server treats `since` as exclusive).
      const opts: WatchOptions = {
        prefix: true,
        signal: this.watchAbort.signal,
      };
      if (this.lastRevision > 0) opts.since = this.lastRevision;
      const sessionStart = Date.now();
      try {
        for await (const event of this.kv.watch(DEF_PREFIX, opts)) {
          if (this.closed) return;
          if (event.type === "ready") continue;
          // Advance the resume point on every delta, even ones the byte-compare
          // dedups, so a reconnect never re-requests already-applied revisions.
          if (event.revision > this.lastRevision) {
            this.lastRevision = event.revision;
          }
          if (event.type === "set") {
            const flagName = event.key.slice(DEF_PREFIX.length);
            if (this.applyValue(flagName, event.value)) {
              this.notifyChange([flagName]);
            }
          } else if (event.type === "del") {
            const flagName = event.key.slice(DEF_PREFIX.length);
            if (this.state.delete(flagName)) this.notifyChange([flagName]);
          }
        }
        // Stream ended cleanly (server close, LB timeout, graceful restart).
        // Treat as a transient disconnect and reconnect with backoff.
      } catch (err) {
        if (this.closed) return;
        this.reportError("watch", err);
      }

      if (this.closed) return;

      // A long-lived session indicates the server is healthy; reset backoff.
      if (Date.now() - sessionStart > 30_000) attempt = 0;

      // Poll while waiting to reconnect so evals don't freeze on a stale snapshot.
      this.startPolling();

      const delayMs = Math.min(1_000 * 2 ** attempt, 60_000);
      attempt++;
      await new Promise<void>((r) => setTimeout(r, delayMs));
      if (this.closed) return;

      // Stop polling before re-establishing watch so we don't run both at once.
      if (this.pollTimer !== undefined) {
        clearInterval(this.pollTimer);
        this.pollTimer = undefined;
      }
    }
  }

  private startPolling(): void {
    if (this.closed || this.pollTimer) return;
    const intervalMs = Math.max(1, this.opts.refresh) * 1000;
    this.pollTimer = setInterval(() => {
      this.loadAll().catch((err) => this.reportError("snapshot", err));
    }, intervalMs);
    // Don't keep the event loop alive for our own polling.
    if (typeof this.pollTimer === "object" && "unref" in this.pollTimer) {
      (this.pollTimer as { unref: () => void }).unref();
    }
  }

  /**
   * Decode and store a flag def. Returns `true` if the stored value actually
   * changed (newly added, or its KV bytes differ from the prior value) so
   * callers can fire change notifications without spurious events on no-op
   * re-reads — every poll re-reads every def, so an unchanged re-read must stay
   * silent.
   *
   * Change is decided by exact equality of the raw KV bytes, not by comparing
   * parsed objects: bytes are the authoritative representation, the check
   * short-circuits on length, and it needs no parse→serialize round-trip. A
   * semantically-identical rewrite with reordered keys counts as a change here
   * (different bytes) — that only triggers a harmless re-resolve downstream, so
   * canonicalizing is deliberately not worth the cost.
   */
  private applyValue(flagName: string, raw: Uint8Array): boolean {
    let parsed: FlagDef | undefined;
    try {
      parsed = decodeFlagDef(raw);
    } catch (err) {
      this.reportError("snapshot", err, flagName);
      return false;
    }
    if (!parsed) return false;
    const prev = this.state.get(flagName);
    if (prev && bytesEqual(prev.raw, raw)) return false;
    // Copy the bytes: the KV client's buffer is not guaranteed to outlive this
    // call unmutated, and this map retains it as the change-detection baseline.
    this.state.set(flagName, { def: parsed, raw: raw.slice() });
    return true;
  }

  /**
   * Fire the `onChange` callback for actually-changed flags. Suppressed during
   * the initial load (only live deltas/reloads notify) and for empty sets.
   */
  private notifyChange(names: string[]): void {
    if (!this.isReady || names.length === 0 || !this.opts.onChange) return;
    this.opts.onChange(names);
  }

  private reportError(
    source: FlagsErrorEvent["source"],
    err: unknown,
    name?: string,
  ): void {
    if (!this.opts.onError) return;
    const event: FlagsErrorEvent = {
      source,
      error: err instanceof Error
        ? err
        : new FlagError("kv_error", String(err)),
    };
    if (name !== undefined) event.name = name;
    this.opts.onError(event);
  }

  /** Stop background sync and release resources. Idempotent. */
  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.watchAbort?.abort();
    this.watchAbort = undefined;
    if (this.pollTimer) clearInterval(this.pollTimer);
    this.pollTimer = undefined;
  }
}

// ── User pref helpers (per-id KV keys, fetched once per request) ──────────────

/** Build the KV key for a flag definition. */
export function defKey(flagName: string): string {
  return DEF_PREFIX + flagName;
}

/** Build the KV key for a per-id pref bundle. */
export function userKey(id: string): string {
  return USER_PREFIX + id;
}

/**
 * Fetch the per-id pref bundle for `id`. Returns `null` if the user has no
 * prefs. Errors are reported through `onError` and surfaced as `null` so
 * eval falls through to defaults rather than crashing.
 */
export async function fetchUserPrefs(
  kv: KvClient,
  id: string,
  onError?: (e: FlagsErrorEvent) => void,
): Promise<UserPrefs | null> {
  const { data, error } = await kv.get(userKey(id));
  if (error) {
    onError?.({ source: "user-prefs", error, id });
    return null;
  }
  if (!data) return null;
  try {
    return data.json<UserPrefs>();
  } catch (err) {
    onError?.({
      source: "user-prefs",
      error: err instanceof Error ? err : new Error(String(err)),
      id,
    });
    return null;
  }
}

/** Exact byte equality. Short-circuits on length; no allocation. */
function bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
  if (a === b) return true;
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) return false;
  }
  return true;
}

function decodeFlagDef(value: Uint8Array): FlagDef | undefined {
  const text = decoder.decode(value);
  if (text.length === 0) return undefined;
  const parsed = JSON.parse(text) as JsonValue;
  if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new FlagError("invalid_state", "Flag def must be a JSON object");
  }
  if (typeof (parsed as Record<string, unknown>)["on"] !== "boolean") {
    throw new FlagError(
      "invalid_state",
      "FlagDef.on must be a boolean (got: "
        + JSON.stringify((parsed as Record<string, unknown>)["on"]) + ")",
    );
  }
  return parsed as unknown as FlagDef;
}

/**
 * Unit coverage for the OpenFeature **web** provider. Web resolution is
 * synchronous against an in-memory snapshot, with per-context prefs pre-fetched
 * on initialize/onContextChange. Tests drive the provider methods directly.
 *
 * Flag/user names are uuid-suffixed (the test KV keyspace is shared).
 */
import type { KvClient } from "@beyond.dev/kv";
import {
  type EvaluationContext,
  type Logger,
  ProviderEvents,
} from "@openfeature/web-sdk";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { BeyondWebProvider } from "../openfeature/web.js";
import { kvClient, writeDef } from "./harness.js";
import "./test-context.js";

const uid = () => crypto.randomUUID();

const logger: Logger = {
  error() {},
  warn() {},
  info() {},
  debug() {},
};

function ctx(id: string, extra: EvaluationContext = {}): EvaluationContext {
  return { targetingKey: id, ...extra };
}

describe("BeyondWebProvider — synchronous resolution", () => {
  let kv: KvClient;
  let provider: BeyondWebProvider;

  beforeEach(() => {
    kv = kvClient();
  });

  afterEach(async () => {
    await provider?.onClose();
  });

  async function start(context: EvaluationContext): Promise<void> {
    provider = new BeyondWebProvider(kv, { watch: true, refresh: 2 });
    await provider.initialize(context);
  }

  it("returns the default with reason DEFAULT when no def exists", async () => {
    await start(ctx("u"));
    const res = provider.resolveBooleanEvaluation(
      uid(),
      false,
      ctx("u"),
      logger,
    );
    expect(res.value).toBe(false);
    expect(res.reason).toBe("DEFAULT");
  });

  it("resolves a def loaded before initialize", async () => {
    const key = uid();
    await writeDef(kv, key, { on: true, rollout: { percent: 100 } });
    await start(ctx("u"));
    const res = provider.resolveBooleanEvaluation(key, false, ctx("u"), logger);
    expect(res.value).toBe(true);
    expect(res.reason).toBe("SPLIT");
  });

  it("matches targeting rules from the static context", async () => {
    const key = uid();
    await writeDef(kv, key, {
      on: true,
      rules: [{ when: { plan: "pro" }, value: true }],
    });
    await start(ctx("u", { plan: "pro" }));
    const res = provider.resolveBooleanEvaluation(
      key,
      false,
      ctx("u", { plan: "pro" }),
      logger,
    );
    expect(res.value).toBe(true);
    expect(res.reason).toBe("TARGETING_MATCH");
  });

  it("applies pre-fetched per-user prefs for the active context", async () => {
    const key = uid();
    const id = uid();
    await writeDef(kv, key, { on: true, rollout: { percent: 0 } });
    const { error } = await kv.set(
      `flags:user:${id}`,
      JSON.stringify({ [key]: true }),
    );
    if (error) throw error;
    await start(ctx(id)); // prefetches id's prefs
    const res = provider.resolveBooleanEvaluation(key, false, ctx(id), logger);
    expect(res.value).toBe(true);
    expect(res.flagMetadata?.beyondReason).toBe("user-pref");
  });

  it("re-fetches prefs on context change", async () => {
    const key = uid();
    const id1 = uid();
    const id2 = uid();
    await writeDef(kv, key, { on: true, rollout: { percent: 0 } });
    const { error } = await kv.set(
      `flags:user:${id1}`,
      JSON.stringify({ [key]: true }),
    );
    if (error) throw error;
    await start(ctx(id1)); // id1 → pref true
    expect(
      provider.resolveBooleanEvaluation(key, false, ctx(id1), logger).value,
    ).toBe(true);

    // Switch to id2 (no pref) → falls through to the 0% rollout = false.
    await provider.onContextChange(ctx(id1), ctx(id2));
    expect(
      provider.resolveBooleanEvaluation(key, false, ctx(id2), logger).value,
    ).toBe(false);
  });

  it("returns TYPE_MISMATCH for a wrong-typed value", async () => {
    const key = uid();
    await writeDef(kv, key, {
      on: true,
      rollout: { percent: 100, value: "nope" },
    });
    await start(ctx("u"));
    const res = provider.resolveBooleanEvaluation(key, false, ctx("u"), logger);
    expect(res.value).toBe(false);
    expect(res.errorCode).toBe("TYPE_MISMATCH");
  });

  it("emits PROVIDER_CONFIGURATION_CHANGED on a live def change", async () => {
    const key = uid();
    await start(ctx("u"));
    const changed = new Promise<string[]>((resolve) => {
      provider.events.addHandler(ProviderEvents.ConfigurationChanged, (e) => {
        resolve((e?.flagsChanged as string[]) ?? []);
      });
    });
    await writeDef(kv, key, { on: true, rollout: { percent: 100 } });
    const flagsChanged = await Promise.race([
      changed,
      new Promise<string[]>((_, reject) =>
        setTimeout(() => reject(new Error("timed out")), 10_000)
      ),
    ]);
    expect(flagsChanged).toContain(key);
    expect(
      provider.resolveBooleanEvaluation(key, false, ctx("u"), logger).value,
    ).toBe(true);
  });
});

/**
 * Unit coverage for the OpenFeature **server** provider. Drives the provider's
 * resolve methods directly (the SDK calls these) against real beyond-kv, and
 * asserts the full {@link ResolutionDetails} — value, reason, errorCode, and
 * flagMetadata — for every evaluation path.
 *
 * The test KV keyspace is shared across tests, so every flag/user name is
 * uuid-suffixed to keep tests independent (mirrors `adapter.test.ts`).
 */
import type { KvClient } from "@beyond.dev/kv";
import {
  type EvaluationContext,
  type Logger,
  ProviderEvents,
} from "@openfeature/server-sdk";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { BeyondProvider } from "../openfeature/server.js";
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

describe("BeyondProvider (server) — fetch mode resolution", () => {
  let kv: KvClient;
  let provider: BeyondProvider;

  beforeEach(async () => {
    kv = kvClient();
    provider = new BeyondProvider(kv, { mode: "fetch" });
    await provider.initialize();
  });

  afterEach(async () => {
    await provider.onClose();
  });

  it("returns the default with reason DEFAULT when no def exists", async () => {
    const res = await provider.resolveBooleanEvaluation(
      uid(),
      false,
      ctx("u"),
      logger,
    );
    expect(res.value).toBe(false);
    expect(res.reason).toBe("DEFAULT");
    expect(res.errorCode).toBeUndefined();
  });

  it("resolves a 100% rollout to true with reason SPLIT", async () => {
    const key = uid();
    await writeDef(kv, key, { on: true, rollout: { percent: 100 } });
    const res = await provider.resolveBooleanEvaluation(
      key,
      false,
      ctx("u"),
      logger,
    );
    expect(res.value).toBe(true);
    expect(res.reason).toBe("SPLIT");
  });

  it("honors the kill switch with reason DISABLED", async () => {
    const key = uid();
    await writeDef(kv, key, { on: false, rollout: { percent: 100 } });
    const res = await provider.resolveBooleanEvaluation(
      key,
      false,
      ctx("u"),
      logger,
    );
    expect(res.value).toBe(false);
    expect(res.reason).toBe("DISABLED");
  });

  it("matches a targeting rule with reason TARGETING_MATCH + ruleIndex", async () => {
    const key = uid();
    await writeDef(kv, key, {
      on: true,
      rules: [
        { when: { plan: "free" }, value: false },
        { when: { plan: "pro" }, value: true },
      ],
    });
    const res = await provider.resolveBooleanEvaluation(
      key,
      false,
      ctx("u", { plan: "pro" }),
      logger,
    );
    expect(res.value).toBe(true);
    expect(res.reason).toBe("TARGETING_MATCH");
    expect(res.flagMetadata?.ruleIndex).toBe(1);
    expect(res.flagMetadata?.beyondReason).toBe("rule");
  });

  it("applies a per-user pref with reason TARGETING_MATCH", async () => {
    const key = uid();
    const id = uid();
    await writeDef(kv, key, { on: true, rollout: { percent: 0 } });
    const { error } = await kv.set(
      `flags:user:${id}`,
      JSON.stringify({ [key]: true }),
    );
    if (error) throw error;
    const res = await provider.resolveBooleanEvaluation(
      key,
      false,
      ctx(id),
      logger,
    );
    expect(res.value).toBe(true);
    expect(res.reason).toBe("TARGETING_MATCH");
    expect(res.flagMetadata?.beyondReason).toBe("user-pref");
  });

  it("resolves string and number flags", async () => {
    const sKey = uid();
    const nKey = uid();
    await writeDef(kv, sKey, {
      on: true,
      rollout: { percent: 100, value: "dark" },
    });
    await writeDef(kv, nKey, {
      on: true,
      rollout: { percent: 100, value: 42 },
    });
    const s = await provider.resolveStringEvaluation(
      sKey,
      "light",
      ctx("u"),
      logger,
    );
    const n = await provider.resolveNumberEvaluation(nKey, 0, ctx("u"), logger);
    expect(s.value).toBe("dark");
    expect(n.value).toBe(42);
  });

  it("resolves an object flag", async () => {
    const key = uid();
    await writeDef(kv, key, {
      on: true,
      rollout: { percent: 100, value: { mode: "x", n: 3 } },
    });
    const res = await provider.resolveObjectEvaluation(
      key,
      {},
      ctx("u"),
      logger,
    );
    expect(res.value).toEqual({ mode: "x", n: 3 });
  });

  it("returns TYPE_MISMATCH when the resolved value is the wrong type", async () => {
    const key = uid();
    // Boolean flag whose KV value resolves to a string.
    await writeDef(kv, key, {
      on: true,
      rollout: { percent: 100, value: "not-a-bool" },
    });
    const res = await provider.resolveBooleanEvaluation(
      key,
      false,
      ctx("u"),
      logger,
    );
    expect(res.value).toBe(false); // declared default, not coerced
    expect(res.reason).toBe("ERROR");
    expect(res.errorCode).toBe("TYPE_MISMATCH");
  });

  it("matches attribute rules even without a targetingKey", async () => {
    const key = uid();
    await writeDef(kv, key, {
      on: true,
      rules: [{ when: { plan: "pro" }, value: true }],
    });
    const res = await provider.resolveBooleanEvaluation(
      key,
      false,
      { plan: "pro" }, // no targetingKey
      logger,
    );
    expect(res.value).toBe(true);
    expect(res.reason).toBe("TARGETING_MATCH");
  });
});

describe("BeyondProvider (server) — snapshot mode", () => {
  let kv: KvClient;

  beforeEach(() => {
    kv = kvClient();
  });

  it("resolves from the in-memory snapshot after initialize", async () => {
    const key = uid();
    await writeDef(kv, key, { on: true, rollout: { percent: 100 } });
    const provider = new BeyondProvider(kv, { mode: "snapshot", watch: false });
    await provider.initialize();
    try {
      const res = await provider.resolveBooleanEvaluation(
        key,
        false,
        ctx("u"),
        logger,
      );
      expect(res.value).toBe(true);
    } finally {
      await provider.onClose();
    }
  });

  it("reports reason STALE when resolving an unknown flag before init completes", async () => {
    // No def for this key, and initialize() is never called → not-ready + absent
    // def deterministically yields STALE (vs DEFAULT once ready).
    const provider = new BeyondProvider(kv, { mode: "snapshot", watch: false });
    try {
      const res = await provider.resolveBooleanEvaluation(
        uid(),
        false,
        ctx("u"),
        logger,
      );
      expect(res.value).toBe(false); // declared default
      expect(res.reason).toBe("STALE");
    } finally {
      await provider.onClose();
    }
  });

  it("emits PROVIDER_CONFIGURATION_CHANGED when a watched def changes", async () => {
    const key = uid();
    const provider = new BeyondProvider(kv, {
      mode: "snapshot",
      watch: true,
      refresh: 2,
    });
    await provider.initialize();
    try {
      const changed = new Promise<string[]>((resolve) => {
        provider.events.addHandler(ProviderEvents.ConfigurationChanged, (e) => {
          resolve((e?.flagsChanged as string[]) ?? []);
        });
      });
      await writeDef(kv, key, { on: true, rollout: { percent: 100 } });
      const flagsChanged = await withTimeout(
        changed,
        10_000,
        "ConfigurationChanged",
      );
      expect(flagsChanged).toContain(key);

      // And the new value is now resolvable from the snapshot.
      const res = await provider.resolveBooleanEvaluation(
        key,
        false,
        ctx("u"),
        logger,
      );
      expect(res.value).toBe(true);
    } finally {
      await provider.onClose();
    }
  });
});

function withTimeout<T>(p: Promise<T>, ms: number, label: string): Promise<T> {
  return Promise.race([
    p,
    new Promise<T>((_, reject) =>
      setTimeout(() => reject(new Error(`timed out waiting for ${label}`)), ms)
    ),
  ]);
}

/**
 * End-to-end proof that `@beyond.dev/flags/openfeature/server` is a real
 * OpenFeature provider.
 *
 * These tests import the REAL host SDK (`OpenFeature` from
 * `@openfeature/server-sdk`) and drive evaluation through it — we register the
 * provider with `setProviderAndWait`, get a real `Client`, and call
 * `getBooleanValue`/`getBooleanDetails` exactly as an application would. We never
 * call the provider's `resolve*` methods ourselves; the SDK does. The full chain
 * is exercised:
 *
 *   OpenFeature.getClient().getBooleanValue()
 *     → host hooks/context merge → OUR BeyondProvider.resolve* → real beyond-kv
 *
 * Assertions are toggle-based: flipping the def in live KV flips the value the
 * HOST returns. That can only pass if the entire chain works. Flag names are
 * uuid-suffixed because the test KV keyspace is shared across tests.
 */
import { createKvClient, type KvClient } from "@beyond.dev/kv";
import { OpenFeature, ProviderEvents } from "@openfeature/server-sdk";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { BeyondProvider } from "../openfeature/server.js";
import type { FlagsErrorEvent } from "../types.js";
import { deleteDef, kvClient, writeDef } from "./harness.js";
import "./test-context.js";

const uid = () => crypto.randomUUID();

function withTimeout<T>(p: Promise<T>, ms: number, label: string): Promise<T> {
  return Promise.race([
    p,
    new Promise<T>((_, reject) =>
      setTimeout(() => reject(new Error(`timed out waiting for ${label}`)), ms),
    ),
  ]);
}

describe("e2e: real @openfeature/server-sdk → BeyondProvider → real KV", () => {
  let kv: KvClient;

  beforeEach(() => {
    kv = kvClient();
  });

  afterEach(async () => {
    // Closes and detaches all registered providers (calls onClose()).
    await OpenFeature.clearProviders();
  });

  it("host-returned value tracks live KV state (the irrefutable toggle)", async () => {
    const key = uid();
    await OpenFeature.setProviderAndWait(new BeyondProvider(kv, { mode: "fetch" }));
    const client = OpenFeature.getClient();
    const eval_ = () => client.getBooleanValue(key, false, { targetingKey: "u1" });

    // No def yet → declared default.
    expect(await eval_()).toBe(false);

    // Turn it on at 100% → host now returns true.
    await writeDef(kv, key, { on: true, rollout: { percent: 100 } });
    expect(await eval_()).toBe(true);

    // Kill switch → back to default.
    await writeDef(kv, key, { on: false });
    expect(await eval_()).toBe(false);

    // Re-enable → true again. The flips prove resolve reads live KV each call.
    await writeDef(kv, key, { on: true, rollout: { percent: 100 } });
    expect(await eval_()).toBe(true);
  });

  it("targeting rule resolves through the host by context", async () => {
    const key = uid();
    await writeDef(kv, key, {
      on: true,
      rules: [{ when: { plan: "pro" }, value: true }],
    });
    await OpenFeature.setProviderAndWait(new BeyondProvider(kv, { mode: "fetch" }));
    const client = OpenFeature.getClient();

    expect(
      await client.getBooleanValue(key, false, { targetingKey: "u1", plan: "pro" }),
    ).toBe(true);
    expect(
      await client.getBooleanValue(key, false, { targetingKey: "u2", plan: "free" }),
    ).toBe(false);
  });

  it("per-user pref resolves through the host", async () => {
    const key = uid();
    const id = uid();
    await writeDef(kv, key, { on: true, rollout: { percent: 0 } });
    const { error } = await kv.set(`flags:user:${id}`, JSON.stringify({ [key]: true }));
    if (error) throw error;
    await OpenFeature.setProviderAndWait(new BeyondProvider(kv, { mode: "fetch" }));
    const client = OpenFeature.getClient();

    expect(await client.getBooleanValue(key, false, { targetingKey: id })).toBe(true);
    expect(await client.getBooleanValue(key, false, { targetingKey: uid() })).toBe(false);
  });

  it("getBooleanDetails surfaces reason and flagMetadata through the host", async () => {
    const key = uid();
    await writeDef(kv, key, {
      on: true,
      rules: [{ when: { plan: "pro" }, value: true }],
    });
    await OpenFeature.setProviderAndWait(new BeyondProvider(kv, { mode: "fetch" }));
    const client = OpenFeature.getClient();

    const details = await client.getBooleanDetails(key, false, {
      targetingKey: "u1",
      plan: "pro",
    });
    expect(details.value).toBe(true);
    expect(details.reason).toBe("TARGETING_MATCH");
    expect(details.flagMetadata?.ruleIndex).toBe(0);
    expect(details.flagMetadata?.beyondReason).toBe("rule");
  });

  it("type mismatch surfaces TYPE_MISMATCH and the default through the host", async () => {
    const key = uid();
    await writeDef(kv, key, {
      on: true,
      rollout: { percent: 100, value: "not-a-bool" },
    });
    await OpenFeature.setProviderAndWait(new BeyondProvider(kv, { mode: "fetch" }));
    const client = OpenFeature.getClient();

    const details = await client.getBooleanDetails(key, false, { targetingKey: "u1" });
    expect(details.value).toBe(false);
    expect(details.reason).toBe("ERROR");
    expect(details.errorCode).toBe("TYPE_MISMATCH");
  });

  it("emits PROVIDER_CONFIGURATION_CHANGED through the host on a live change", async () => {
    const key = uid();
    await OpenFeature.setProviderAndWait(
      new BeyondProvider(kv, { mode: "snapshot", watch: true }),
    );
    const client = OpenFeature.getClient();

    const changed = new Promise<string[]>((resolve) => {
      client.addHandler(ProviderEvents.ConfigurationChanged, (e) => {
        resolve((e?.flagsChanged as string[]) ?? []);
      });
    });

    await writeDef(kv, key, { on: true, rollout: { percent: 100 } });

    const flagsChanged = await Promise.race([
      changed,
      new Promise<string[]>((_, reject) =>
        setTimeout(() => reject(new Error("timed out waiting for event")), 15_000),
      ),
    ]);
    expect(flagsChanged).toContain(key);

    // The host now resolves the new value from the live snapshot.
    expect(await client.getBooleanValue(key, false, { targetingKey: "u1" })).toBe(true);
  });

  it("resolves string, number, and object flags through the host", async () => {
    const sKey = uid();
    const nKey = uid();
    const oKey = uid();
    await writeDef(kv, sKey, { on: true, rollout: { percent: 100, value: "dark" } });
    await writeDef(kv, nKey, { on: true, rollout: { percent: 100, value: 42 } });
    await writeDef(kv, oKey, {
      on: true,
      rollout: { percent: 100, value: { a: 1, b: ["x"] } },
    });
    await OpenFeature.setProviderAndWait(new BeyondProvider(kv, { mode: "fetch" }));
    const client = OpenFeature.getClient();
    const id = { targetingKey: "u1" };

    expect(await client.getStringValue(sKey, "light", id)).toBe("dark");
    expect(await client.getNumberValue(nKey, 0, id)).toBe(42);
    expect(await client.getObjectValue(oKey, {}, id)).toEqual({ a: 1, b: ["x"] });

    // Details path is typed end-to-end too.
    const sd = await client.getStringDetails(sKey, "light", id);
    expect(sd.value).toBe("dark");
    expect(sd.reason).toBe("SPLIT");
    const od = await client.getObjectDetails(oKey, {}, id);
    expect(od.value).toEqual({ a: 1, b: ["x"] });
  });

  it("type mismatch is enforced for string and object flags too", async () => {
    const sKey = uid();
    const oKey = uid();
    // String flag resolving to a number, object flag resolving to a string.
    await writeDef(kv, sKey, { on: true, rollout: { percent: 100, value: 7 } });
    await writeDef(kv, oKey, { on: true, rollout: { percent: 100, value: "scalar" } });
    await OpenFeature.setProviderAndWait(new BeyondProvider(kv, { mode: "fetch" }));
    const client = OpenFeature.getClient();
    const id = { targetingKey: "u1" };

    const s = await client.getStringDetails(sKey, "fallback", id);
    expect(s.value).toBe("fallback");
    expect(s.errorCode).toBe("TYPE_MISMATCH");

    const o = await client.getObjectDetails(oKey, { ok: true }, id);
    expect(o.value).toEqual({ ok: true });
    expect(o.errorCode).toBe("TYPE_MISMATCH");
  });

  it("snapshot mode resolves through the host (def written before init)", async () => {
    const key = uid();
    await writeDef(kv, key, { on: true, rollout: { percent: 100 } });
    await OpenFeature.setProviderAndWait(
      new BeyondProvider(kv, { mode: "snapshot", watch: false }),
    );
    const client = OpenFeature.getClient();
    expect(await client.getBooleanValue(key, false, { targetingKey: "u1" })).toBe(true);
  });

  it("flag deletion propagates back to the default via watch", async () => {
    const key = uid();
    await writeDef(kv, key, { on: true, rollout: { percent: 100 } });
    await OpenFeature.setProviderAndWait(
      new BeyondProvider(kv, { mode: "snapshot", watch: true }),
    );
    const client = OpenFeature.getClient();
    expect(await client.getBooleanValue(key, false, { targetingKey: "u1" })).toBe(true);

    const changed = new Promise<string[]>((resolve) => {
      client.addHandler(ProviderEvents.ConfigurationChanged, (e) => {
        resolve((e?.flagsChanged as string[]) ?? []);
      });
    });
    await deleteDef(kv, key);
    expect(await withTimeout(changed, 15_000, "delete event")).toContain(key);

    // With the def gone, the host falls back to the declared default.
    expect(await client.getBooleanValue(key, false, { targetingKey: "u1" })).toBe(false);
  });

  it("partial rollout is deterministic and roughly split through the host", async () => {
    const key = uid();
    await writeDef(kv, key, { on: true, rollout: { percent: 50 } });
    await OpenFeature.setProviderAndWait(new BeyondProvider(kv, { mode: "fetch" }));
    const client = OpenFeature.getClient();

    const ids = Array.from({ length: 200 }, (_, i) => `user-${i}`);
    const first = await Promise.all(
      ids.map((id) => client.getBooleanValue(key, false, { targetingKey: id })),
    );
    const second = await Promise.all(
      ids.map((id) => client.getBooleanValue(key, false, { targetingKey: id })),
    );

    expect(second).toEqual(first); // same id → same bucket, always
    const trueCount = first.filter(Boolean).length;
    expect(trueCount).toBeGreaterThan(60); // ~100 expected; wide bounds = no flake
    expect(trueCount).toBeLessThan(140);
  });

  it("ignores per-user prefs when userPrefs is disabled", async () => {
    const key = uid();
    const id = uid();
    await writeDef(kv, key, { on: true, rollout: { percent: 0 } });
    const { error } = await kv.set(`flags:user:${id}`, JSON.stringify({ [key]: true }));
    if (error) throw error;
    await OpenFeature.setProviderAndWait(
      new BeyondProvider(kv, { mode: "fetch", userPrefs: false }),
    );
    const client = OpenFeature.getClient();
    // Pref says true, but userPrefs:false ignores it → 0% rollout = false.
    expect(await client.getBooleanValue(key, false, { targetingKey: id })).toBe(false);
  });

  it("degrades to the default and reports onError on malformed KV data", async () => {
    const key = uid();
    // Write invalid JSON directly (bypassing writeDef's JSON.stringify).
    const { error } = await kv.set(`flags:def:${key}`, "{ not valid json");
    if (error) throw error;
    const errors: FlagsErrorEvent[] = [];
    await OpenFeature.setProviderAndWait(
      new BeyondProvider(kv, { mode: "fetch", onError: (e) => errors.push(e) }),
    );
    const client = OpenFeature.getClient();

    // Malformed def is treated as absent → declared default is returned.
    expect(await client.getBooleanValue(key, true, { targetingKey: "u1" })).toBe(true);
    expect(errors.length).toBeGreaterThan(0);
    expect(errors[0]?.source).toBe("snapshot");
  });

  it("degrades to the default and reports onError when KV is unreachable", async () => {
    const key = uid();
    // Nothing listens on port 1 → ECONNREFUSED. Bounded retries/timeout keep it fast.
    const deadKv = createKvClient({
      url: "http://127.0.0.1:1",
      namespace: "dead",
      retries: 0,
      timeout: 1_000,
    });
    const errors: FlagsErrorEvent[] = [];
    await OpenFeature.setProviderAndWait(
      new BeyondProvider(deadKv, { mode: "fetch", onError: (e) => errors.push(e) }),
    );
    const client = OpenFeature.getClient();

    expect(await client.getBooleanValue(key, false, { targetingKey: "u1" })).toBe(false);
    expect(errors.length).toBeGreaterThan(0);
  });
});

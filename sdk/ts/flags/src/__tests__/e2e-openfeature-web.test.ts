/**
 * End-to-end proof that `@beyond.dev/flags/openfeature/web` is a real OpenFeature
 * web (client-side) provider.
 *
 * These tests import the REAL host SDK (`OpenFeature` from `@openfeature/web-sdk`)
 * and drive evaluation through it: set a static context, register the provider
 * with `setProviderAndWait`, get a real `Client`, and call the SYNCHRONOUS
 * `getBooleanValue`/`getBooleanDetails`. The SDK calls our provider's sync
 * `resolve*` against its in-memory snapshot. The full chain is exercised:
 *
 *   OpenFeature.setContext() / getClient().getBooleanValue()
 *     → host static-context machinery → OUR BeyondWebProvider.resolve* (sync)
 *     → in-memory snapshot kept live by real beyond-kv watch
 *
 * Toggle-based: flipping the def in live KV (propagated via watch) flips the
 * value the HOST returns. Flag names are uuid-suffixed (shared test keyspace).
 */
import type { KvClient } from "@beyond.dev/kv";
import { OpenFeature, ProviderEvents } from "@openfeature/web-sdk";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { BeyondWebProvider } from "../openfeature/web.js";
import { deleteDef, kvClient, writeDef } from "./harness.js";
import "./test-context.js";

const uid = () => crypto.randomUUID();

describe("e2e: real @openfeature/web-sdk → BeyondWebProvider → real KV", () => {
  let kv: KvClient;

  beforeEach(() => {
    kv = kvClient();
  });

  afterEach(async () => {
    await OpenFeature.clearProviders();
    await OpenFeature.setContext({});
  });

  it("host returns live value after a watched def change (the irrefutable toggle)", async () => {
    const key = uid();
    await OpenFeature.setContext({ targetingKey: "u1" });
    await OpenFeature.setProviderAndWait(new BeyondWebProvider(kv, { watch: true }));
    const client = OpenFeature.getClient();

    // No def yet → declared default (sync call, implicit static context).
    expect(client.getBooleanValue(key, false)).toBe(false);

    const changed = onceConfigChanged(client, key);
    await writeDef(kv, key, { on: true, rollout: { percent: 100 } });
    await changed; // watch → snapshot → PROVIDER_CONFIGURATION_CHANGED

    expect(client.getBooleanValue(key, false)).toBe(true);
  });

  it("targeting rule resolves from the static context", async () => {
    const key = uid();
    await writeDef(kv, key, {
      on: true,
      rules: [{ when: { plan: "pro" }, value: true }],
    });
    await OpenFeature.setContext({ targetingKey: "u1", plan: "pro" });
    await OpenFeature.setProviderAndWait(new BeyondWebProvider(kv, { watch: true }));
    const client = OpenFeature.getClient();

    expect(client.getBooleanValue(key, false)).toBe(true);
  });

  it("context change re-resolves with the new targeting context", async () => {
    const key = uid();
    await writeDef(kv, key, {
      on: true,
      rules: [{ when: { plan: "pro" }, value: true }],
    });
    await OpenFeature.setContext({ targetingKey: "u1", plan: "free" });
    await OpenFeature.setProviderAndWait(new BeyondWebProvider(kv, { watch: true }));
    const client = OpenFeature.getClient();

    expect(client.getBooleanValue(key, false)).toBe(false); // free

    // Changing the global context reconciles the provider (onContextChange).
    await OpenFeature.setContext({ targetingKey: "u2", plan: "pro" });
    expect(client.getBooleanValue(key, false)).toBe(true); // pro
  });

  it("per-user pref resolves for the active static context", async () => {
    const key = uid();
    const id = uid();
    await writeDef(kv, key, { on: true, rollout: { percent: 0 } });
    const { error } = await kv.set(`flags:user:${id}`, JSON.stringify({ [key]: true }));
    if (error) throw error;
    await OpenFeature.setContext({ targetingKey: id });
    await OpenFeature.setProviderAndWait(new BeyondWebProvider(kv, { watch: true }));
    const client = OpenFeature.getClient();

    expect(client.getBooleanValue(key, false)).toBe(true);

    await OpenFeature.setContext({ targetingKey: uid() });
    expect(client.getBooleanValue(key, false)).toBe(false); // no pref → 0% rollout
  });

  it("getBooleanDetails surfaces reason + flagMetadata; type mismatch returns default", async () => {
    const okKey = uid();
    const badKey = uid();
    await writeDef(kv, okKey, {
      on: true,
      rules: [{ when: { plan: "pro" }, value: true }],
    });
    await writeDef(kv, badKey, { on: true, rollout: { percent: 100, value: "x" } });
    await OpenFeature.setContext({ targetingKey: "u1", plan: "pro" });
    await OpenFeature.setProviderAndWait(new BeyondWebProvider(kv, { watch: true }));
    const client = OpenFeature.getClient();

    const ok = client.getBooleanDetails(okKey, false);
    expect(ok.value).toBe(true);
    expect(ok.reason).toBe("TARGETING_MATCH");
    expect(ok.flagMetadata?.ruleIndex).toBe(0);

    const bad = client.getBooleanDetails(badKey, false);
    expect(bad.value).toBe(false);
    expect(bad.errorCode).toBe("TYPE_MISMATCH");
  });

  it("resolves string, number, and object flags synchronously through the host", async () => {
    const sKey = uid();
    const nKey = uid();
    const oKey = uid();
    await writeDef(kv, sKey, { on: true, rollout: { percent: 100, value: "dark" } });
    await writeDef(kv, nKey, { on: true, rollout: { percent: 100, value: 42 } });
    await writeDef(kv, oKey, {
      on: true,
      rollout: { percent: 100, value: { a: 1, b: ["x"] } },
    });
    await OpenFeature.setContext({ targetingKey: "u1" });
    await OpenFeature.setProviderAndWait(new BeyondWebProvider(kv, { watch: true }));
    const client = OpenFeature.getClient();

    expect(client.getStringValue(sKey, "light")).toBe("dark");
    expect(client.getNumberValue(nKey, 0)).toBe(42);
    expect(client.getObjectValue(oKey, {})).toEqual({ a: 1, b: ["x"] });
  });

  it("flag deletion propagates back to the default via watch", async () => {
    const key = uid();
    await writeDef(kv, key, { on: true, rollout: { percent: 100 } });
    await OpenFeature.setContext({ targetingKey: "u1" });
    await OpenFeature.setProviderAndWait(new BeyondWebProvider(kv, { watch: true }));
    const client = OpenFeature.getClient();
    expect(client.getBooleanValue(key, false)).toBe(true);

    const changed = onceConfigChanged(client, key);
    await deleteDef(kv, key);
    await changed; // watch del → snapshot drop → PROVIDER_CONFIGURATION_CHANGED

    expect(client.getBooleanValue(key, false)).toBe(false);
  });

  it("polling fallback (watch:false) still picks up live changes", async () => {
    const key = uid();
    await OpenFeature.setContext({ targetingKey: "u1" });
    // watch disabled → the snapshot polls every `refresh` seconds instead.
    await OpenFeature.setProviderAndWait(
      new BeyondWebProvider(kv, { watch: false, refresh: 1 }),
    );
    const client = OpenFeature.getClient();
    expect(client.getBooleanValue(key, false)).toBe(false);

    const changed = onceConfigChanged(client, key);
    await writeDef(kv, key, { on: true, rollout: { percent: 100 } });
    await changed; // arrives via the poll loop, not watch

    expect(client.getBooleanValue(key, false)).toBe(true);
  });

  it("ignores per-user prefs when userPrefs is disabled", async () => {
    const key = uid();
    const id = uid();
    await writeDef(kv, key, { on: true, rollout: { percent: 0 } });
    const { error } = await kv.set(`flags:user:${id}`, JSON.stringify({ [key]: true }));
    if (error) throw error;
    await OpenFeature.setContext({ targetingKey: id });
    await OpenFeature.setProviderAndWait(
      new BeyondWebProvider(kv, { watch: true, userPrefs: false }),
    );
    const client = OpenFeature.getClient();
    expect(client.getBooleanValue(key, false)).toBe(false); // pref ignored → 0% rollout
  });
});

/** Resolve once the provider reports `name` changed (with a bounded timeout). */
function onceConfigChanged(
  // biome-ignore lint/suspicious/noExplicitAny: web Client type is verbose here
  client: any,
  name: string,
): Promise<void> {
  return Promise.race([
    new Promise<void>((resolve) => {
      // biome-ignore lint/suspicious/noExplicitAny: event payload
      const handler = (e: any) => {
        if (((e?.flagsChanged as string[]) ?? []).includes(name)) resolve();
      };
      client.addHandler(ProviderEvents.ConfigurationChanged, handler);
    }),
    new Promise<void>((_, reject) =>
      setTimeout(() => reject(new Error("timed out waiting for change")), 15_000),
    ),
  ]);
}

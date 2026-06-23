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
import { deleteDef, kvClient, sleep, writeDef } from "./harness.js";
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
    await OpenFeature.setProviderAndWait(
      new BeyondWebProvider(kv, { watch: true, refresh: 2 }),
    );
    const client = OpenFeature.getClient();

    // No def yet → declared default (sync call, implicit static context).
    expect(client.getBooleanValue(key, false)).toBe(false);

    const changes = collectChanges(client);
    await writeDef(kv, key, { on: true, rollout: { percent: 100 } });
    // Wait on the observable value, not a single event within a fixed window.
    await waitUntil(
      () => client.getBooleanValue(key, false) === true,
      "value→true",
    );

    expect(client.getBooleanValue(key, false)).toBe(true);
    await waitUntil(
      () => changes().includes(key),
      "config-changed event",
      10_000,
    );
  });

  it("targeting rule resolves from the static context", async () => {
    const key = uid();
    await writeDef(kv, key, {
      on: true,
      rules: [{ when: { plan: "pro" }, value: true }],
    });
    await OpenFeature.setContext({ targetingKey: "u1", plan: "pro" });
    await OpenFeature.setProviderAndWait(
      new BeyondWebProvider(kv, { watch: true, refresh: 2 }),
    );
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
    await OpenFeature.setProviderAndWait(
      new BeyondWebProvider(kv, { watch: true, refresh: 2 }),
    );
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
    const { error } = await kv.set(
      `flags:user:${id}`,
      JSON.stringify({ [key]: true }),
    );
    if (error) throw error;
    await OpenFeature.setContext({ targetingKey: id });
    await OpenFeature.setProviderAndWait(
      new BeyondWebProvider(kv, { watch: true, refresh: 2 }),
    );
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
    await writeDef(kv, badKey, {
      on: true,
      rollout: { percent: 100, value: "x" },
    });
    await OpenFeature.setContext({ targetingKey: "u1", plan: "pro" });
    await OpenFeature.setProviderAndWait(
      new BeyondWebProvider(kv, { watch: true, refresh: 2 }),
    );
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
    await writeDef(kv, sKey, {
      on: true,
      rollout: { percent: 100, value: "dark" },
    });
    await writeDef(kv, nKey, {
      on: true,
      rollout: { percent: 100, value: 42 },
    });
    await writeDef(kv, oKey, {
      on: true,
      rollout: { percent: 100, value: { a: 1, b: ["x"] } },
    });
    await OpenFeature.setContext({ targetingKey: "u1" });
    await OpenFeature.setProviderAndWait(
      new BeyondWebProvider(kv, { watch: true, refresh: 2 }),
    );
    const client = OpenFeature.getClient();

    expect(client.getStringValue(sKey, "light")).toBe("dark");
    expect(client.getNumberValue(nKey, 0)).toBe(42);
    expect(client.getObjectValue(oKey, {})).toEqual({ a: 1, b: ["x"] });
  });

  it("flag deletion propagates back to the default via poll reconcile", async () => {
    const key = uid();
    await writeDef(kv, key, { on: true, rollout: { percent: 100 } });
    await OpenFeature.setContext({ targetingKey: "u1" });
    // Poll mode reconciles continuously (watch mode only polls during reconnect
    // gaps), so a deletion is caught deterministically. Watch-driven deletion is
    // covered in snapshot.test.ts.
    await OpenFeature.setProviderAndWait(
      new BeyondWebProvider(kv, { watch: false, refresh: 1 }),
    );
    const client = OpenFeature.getClient();
    expect(client.getBooleanValue(key, false)).toBe(true);

    const changes = collectChanges(client);
    await deleteDef(kv, key);
    await waitUntil(
      () => client.getBooleanValue(key, false) === false,
      "value→false",
    );

    expect(client.getBooleanValue(key, false)).toBe(false);
    await waitUntil(
      () => changes().includes(key),
      "del config-changed",
      10_000,
    );
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

    await writeDef(kv, key, { on: true, rollout: { percent: 100 } });
    // Arrives via the poll loop, not watch — wait on the value with headroom.
    await waitUntil(
      () => client.getBooleanValue(key, false) === true,
      "poll→true",
    );

    expect(client.getBooleanValue(key, false)).toBe(true);
  });

  it("ignores per-user prefs when userPrefs is disabled", async () => {
    const key = uid();
    const id = uid();
    await writeDef(kv, key, { on: true, rollout: { percent: 0 } });
    const { error } = await kv.set(
      `flags:user:${id}`,
      JSON.stringify({ [key]: true }),
    );
    if (error) throw error;
    await OpenFeature.setContext({ targetingKey: id });
    await OpenFeature.setProviderAndWait(
      new BeyondWebProvider(kv, { watch: true, userPrefs: false }),
    );
    const client = OpenFeature.getClient();
    expect(client.getBooleanValue(key, false)).toBe(false); // pref ignored → 0% rollout
  });
});

/**
 * Poll a predicate until it holds, or fail after `ms`. Waiting on observable
 * state (rather than a single event within a fixed window) keeps these e2e tests
 * robust when the shared test keyspace is large and a snapshot reload or event
 * dispatch is briefly slow.
 */
async function waitUntil(
  pred: () => boolean | Promise<boolean>,
  what: string,
  ms = 20_000,
): Promise<void> {
  const deadline = Date.now() + ms;
  while (Date.now() < deadline) {
    if (await pred()) return;
    await sleep(50);
  }
  throw new Error(`condition not met within ${ms}ms: ${what}`);
}

/** Accumulate the flag names reported via PROVIDER_CONFIGURATION_CHANGED. */
function collectChanges(
  // biome-ignore lint/suspicious/noExplicitAny: web Client type is verbose here
  client: any,
): () => string[] {
  const seen: string[] = [];
  client.addHandler(
    ProviderEvents.ConfigurationChanged,
    (e: { flagsChanged?: string[] }) => {
      seen.push(...(e?.flagsChanged ?? []));
    },
  );
  return () => seen;
}

/**
 * End-to-end proof that `@beyond.dev/flags/adapter` is a real Vercel Flags SDK
 * adapter.
 *
 * These tests import the REAL host SDK (`flag`, `evaluate`, `getProviderData`
 * from `flags/next` v4) and drive evaluation through it. We never call
 * `adapter.decide`/`adapter.bulkDecide` ourselves — the host does, exactly as it
 * would inside a Next.js request. The full chain is exercised:
 *
 *   real flag()  →  host identify/dedupe/override  →  OUR adapter.decide  →  real beyond-kv
 *
 * The host's Pages-Router call shape `flag(request)` reads request data from the
 * passed object instead of `next/headers`, so it runs headless under vitest
 * (confirmed against flags@4.2.0 dist/next.js: `if ("headers" in args[0])`).
 *
 * Assertions are toggle-based: flipping the def in KV flips the value the HOST
 * returns. That can only pass if the entire chain works.
 */
import type { KvClient } from "@beyond.dev/kv";
import { evaluate, flag, getProviderData } from "flags/next";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { beyondAdapter } from "../adapter.js";
import type { FlagContext } from "../types.js";
import { kvClient, writeDef } from "./harness.js";
import "./test-context.js";

// The test KV server shares one keyspace (see http.ts `nsToIndex`), so use
// per-test UUIDs for flag keys and ids to avoid cross-test/cross-file collisions.
const uid = () => crypto.randomUUID();

/**
 * Minimal Pages-Router request the host accepts (`"headers" in request`). The
 * host only reads `.headers`, so a bare object suffices; typed `any` because it
 * stands in for both `PagesRouterRequest` (flag) and `EvaluateRequest`
 * (evaluate).
 */
// biome-ignore lint/suspicious/noExplicitAny: minimal headless request stub
function request(headers: Record<string, string> = {}): any {
  return { headers };
}

describe("e2e: real flags/next host → beyond adapter → real KV", () => {
  let kv: KvClient;

  beforeEach(() => {
    kv = kvClient();
  });

  afterEach(async () => {
    // adapters are created per-test; nothing global to tear down here.
  });

  it("host-returned value tracks live KV state (the irrefutable toggle)", async () => {
    const key = uid();
    const adapter = beyondAdapter<boolean>(kv, { mode: "request" });
    const newCheckout = flag<boolean>({
      key,
      defaultValue: false,
      adapter,
      identify: () => ({ id: uid() }),
    });
    try {
      // No def yet → host applies the declared defaultValue.
      expect(await newCheckout(request())).toBe(false);

      // Turn it on at 100% → host now returns true (value came from KV via decide).
      await writeDef(kv, key, { on: true, rollout: { percent: 100 } });
      expect(await newCheckout(request())).toBe(true);

      // Kill switch → back to default.
      await writeDef(kv, key, { on: false });
      expect(await newCheckout(request())).toBe(false);

      // Re-enable → true again. The flips prove decide reads live KV each call.
      await writeDef(kv, key, { on: true, rollout: { percent: 100 } });
      expect(await newCheckout(request())).toBe(true);
    } finally {
      await adapter.close();
    }
  });

  it("identify → entities → KV targeting rule, end to end", async () => {
    const key = uid();
    await writeDef(kv, key, {
      on: true,
      rules: [{ when: { plan: "pro" }, value: true }],
    });
    const adapter = beyondAdapter<boolean>(kv, { mode: "request" });

    // identify pulls the plan from the request headers the host sealed for us.
    const aiSearch = flag<boolean, FlagContext>({
      key,
      defaultValue: false,
      adapter,
      identify: ({ headers }) => ({
        id: headers.get("x-user-id") ?? "anon",
        plan: (headers.get("x-plan") as FlagContext["plan"]) ?? "free",
      }),
    });
    try {
      expect(await aiSearch(request({ "x-user-id": "u1", "x-plan": "pro" })))
        .toBe(
          true,
        );
      expect(
        await aiSearch(request({ "x-user-id": "u2", "x-plan": "free" })),
      ).toBe(false);
    } finally {
      await adapter.close();
    }
  });

  it("per-user pref resolves end to end", async () => {
    const key = uid();
    const id = uid();
    await writeDef(kv, key, { on: true, rollout: { percent: 0 } });
    const { error } = await kv.set(
      `flags:user:${id}`,
      JSON.stringify({ [key]: true }),
    );
    if (error) throw error;

    const adapter = beyondAdapter<boolean>(kv, { mode: "request" });
    const beta = flag<boolean>({
      key,
      defaultValue: false,
      adapter,
      identify: ({ headers }) => ({ id: headers.get("x-user-id") ?? "anon" }),
    });
    try {
      expect(await beta(request({ "x-user-id": id }))).toBe(true); // pref
      expect(await beta(request({ "x-user-id": uid() }))).toBe(false); // 0% rollout
    } finally {
      await adapter.close();
    }
  });

  it("snapshot mode resolves through the host too", async () => {
    // Write before creating the adapter; the first decide awaits initial load.
    const key = uid();
    await writeDef(kv, key, { on: true, rollout: { percent: 100 } });
    const adapter = beyondAdapter<boolean>(kv, {
      mode: "snapshot",
      watch: false,
    });
    const snap = flag<boolean>({
      key,
      defaultValue: false,
      adapter,
      identify: () => ({ id: uid() }),
    });
    try {
      expect(await snap(request())).toBe(true);
    } finally {
      await adapter.close();
    }
  });

  it("host evaluate() batches through our bulkDecide", async () => {
    const ka = uid();
    const kb = uid();
    const kc = uid();
    await writeDef(kv, ka, { on: true, rollout: { percent: 100 } });
    await writeDef(kv, kb, { on: false });
    await writeDef(kv, kc, {
      on: true,
      rules: [{ when: { plan: "pro" }, value: true }],
    });
    const adapter = beyondAdapter<boolean>(kv, { mode: "request" });
    const bulkSpy = vi.spyOn(adapter, "bulkDecide");

    // The host groups flags by (adapterId, identify reference). Sharing ONE
    // adapter instance AND one identify function collapses all three into a
    // single bulk group → a single bulkDecide call.
    const id = uid();
    const identify = (): FlagContext => ({ id, plan: "pro" });
    const mk = (key: string) =>
      flag<boolean, FlagContext>({
        key,
        defaultValue: false,
        adapter,
        identify,
      });
    const a = mk(ka);
    const b = mk(kb);
    const c = mk(kc);
    try {
      const result = await evaluate([a, b, c], request());
      expect(result).toEqual([true, false, true]);
      // Proves the host routed through bulkDecide, not per-flag decide.
      expect(bulkSpy).toHaveBeenCalledTimes(1);
      expect(bulkSpy.mock.calls[0]?.[0].flags.map((f) => f.key)).toEqual([
        ka,
        kb,
        kc,
      ]);
    } finally {
      await adapter.close();
    }
  });

  it("getProviderData merges adapter (KV) + host (code) definitions", async () => {
    const key = uid();
    await writeDef(kv, key, { on: true });
    const adapter = beyondAdapter<boolean>(kv, {
      mode: "request",
      origin: (k) => `https://flags.example/${k}`,
    });
    const shipped = flag<boolean>({
      key,
      defaultValue: false,
      description: "A shipped feature",
      adapter,
      identify: () => ({ id: uid() }),
    });
    try {
      // Host builds code-side definitions (description/defaultValue/options).
      // biome-ignore lint/suspicious/noExplicitAny: host typing friction under exactOptionalPropertyTypes
      const codeData = getProviderData({ shipped } as any);
      // Adapter builds provider-side definitions (what exists in KV + origin).
      const kvData = await adapter.getProviderData();

      expect(codeData.definitions[key]?.description).toBe("A shipped feature");
      expect(kvData.definitions[key]?.declaredInCode).toBe(false);
      expect(kvData.definitions[key]?.origin).toBe(
        `https://flags.example/${key}`,
      );
      expect(kvData.hints).toEqual([]);
    } finally {
      await adapter.close();
    }
  });
});

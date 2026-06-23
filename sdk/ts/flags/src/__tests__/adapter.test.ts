import type { KvClient } from "@beyond.dev/kv";
import type { ReadonlyHeaders, ReadonlyRequestCookies } from "flags";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { type BeyondAdapter, beyondAdapter } from "../adapter.js";
import type { FlagContext } from "../types.js";
import { kvClient, writeDef } from "./harness.js";
import "./test-context.js";

// NOTE: the test KV server addresses namespaces by a small db index, so the
// harness's `uniqueNs()` does NOT isolate keys across tests — every client
// shares one keyspace (see http.ts `nsToIndex`). Tests therefore use unique
// flag keys and ids per case to avoid cross-test pollution, matching the
// pattern in integration.test.ts.
const uid = () => crypto.randomUUID();

// The adapter only uses `headers` as a per-request WeakMap key and never reads
// `cookies`, so plain objects suffice as stand-ins for the sealed Next types.
function reqHeaders(): ReadonlyHeaders {
  return new Headers() as unknown as ReadonlyHeaders;
}
const cookies = {} as unknown as ReadonlyRequestCookies;

async function writePrefs(
  kv: KvClient,
  id: string,
  prefs: Record<string, unknown>,
): Promise<void> {
  const { error } = await kv.set(`flags:user:${id}`, JSON.stringify(prefs));
  if (error) throw error;
}

// Run the whole suite against both read strategies. snapshot mode loads defs at
// construction, so each test seeds KV first and *then* builds the adapter via
// `build()` (whose first `decide` awaits the initial load). request mode reads
// on demand, so it works the same way.
for (const mode of ["snapshot", "request"] as const) {
  describe(`beyondAdapter — decide (${mode} mode)`, () => {
    let kv: KvClient;
    let adapter: BeyondAdapter<boolean, FlagContext>;

    const build = (opts: { userPrefs?: boolean } = {}) => {
      adapter = beyondAdapter<boolean>(kv, {
        mode,
        watch: false,
        refresh: 1,
        ...opts,
      });
      return adapter;
    };

    beforeEach(() => {
      kv = kvClient();
    });

    afterEach(async () => {
      await adapter?.close();
    });

    const decide = (
      key: string,
      entities: FlagContext | undefined,
      defaultValue: boolean,
    ) =>
      adapter.decide({
        key,
        ...(entities !== undefined ? { entities } : {}),
        headers: reqHeaders(),
        cookies,
        defaultValue,
      });

    it("returns the rollout value for a 100% flag", async () => {
      const key = uid();
      await writeDef(kv, key, { on: true, rollout: { percent: 100 } });
      build();
      expect(await decide(key, { id: uid() }, false)).toBe(true);
    });

    it("kill switch (on:false) returns the declared default", async () => {
      const key = uid();
      await writeDef(kv, key, {
        on: false,
        rollout: { percent: 100 },
        rules: [{ when: {}, value: true }],
      });
      build();
      expect(await decide(key, { id: uid() }, false)).toBe(false);
    });

    it("0% rollout returns the default", async () => {
      const key = uid();
      await writeDef(kv, key, { on: true, rollout: { percent: 0 } });
      build();
      expect(await decide(key, { id: uid() }, false)).toBe(false);
    });

    it("targeting rule matches on an augmented context field", async () => {
      const key = uid();
      await writeDef(kv, key, {
        on: true,
        rules: [{ when: { plan: "pro" }, value: true }],
      });
      build();
      expect(await decide(key, { id: uid(), plan: "pro" }, false)).toBe(true);
      expect(await decide(key, { id: uid(), plan: "free" }, false)).toBe(false);
    });

    it("user pref overrides rules/rollout", async () => {
      const key = uid();
      const id = uid();
      await writeDef(kv, key, { on: true, rollout: { percent: 0 } });
      await writePrefs(kv, id, { [key]: true });
      build();
      expect(await decide(key, { id }, false)).toBe(true);
      // A different id without the pref still falls through to default.
      expect(await decide(key, { id: uid() }, false)).toBe(false);
    });

    it("userPrefs:false ignores stored prefs", async () => {
      const key = uid();
      const id = uid();
      await writeDef(kv, key, { on: true, rollout: { percent: 0 } });
      await writePrefs(kv, id, { [key]: true });
      build({ userPrefs: false });
      expect(await decide(key, { id }, false)).toBe(false);
    });

    it("unknown flag returns the default", async () => {
      build();
      expect(await decide(uid(), { id: uid() }, false)).toBe(false);
    });

    it("missing id returns the default (cannot bucket)", async () => {
      const key = uid();
      await writeDef(kv, key, { on: true, rollout: { percent: 100 } });
      build();
      expect(await decide(key, undefined, false)).toBe(false);
      expect(await decide(key, { id: "" }, false)).toBe(false);
    });

    it("rollout bucketing is deterministic for the same id", async () => {
      const key = uid();
      const id = uid();
      await writeDef(kv, key, { on: true, rollout: { percent: 50 } });
      build();
      const a = await decide(key, { id }, false);
      const b = await decide(key, { id }, false);
      expect(a).toBe(b);
    });
  });

  describe(`beyondAdapter — bulkDecide (${mode} mode)`, () => {
    let kv: KvClient;
    let adapter: BeyondAdapter<boolean, FlagContext>;

    beforeEach(() => {
      kv = kvClient();
    });
    afterEach(async () => {
      await adapter?.close();
    });

    it("resolves many flags in one call", async () => {
      const a = uid();
      const b = uid();
      const c = uid();
      const missing = uid();
      await writeDef(kv, a, { on: true, rollout: { percent: 100 } });
      await writeDef(kv, b, { on: false });
      await writeDef(kv, c, {
        on: true,
        rules: [{ when: { plan: "pro" }, value: true }],
      });
      adapter = beyondAdapter<boolean>(kv, { mode, watch: false, refresh: 1 });
      const out = await adapter.bulkDecide?.({
        flags: [
          { key: a, defaultValue: false },
          { key: b, defaultValue: false },
          { key: c, defaultValue: false },
          { key: missing, defaultValue: false },
        ],
        entities: { id: uid(), plan: "pro" },
        headers: reqHeaders(),
        cookies,
      });
      expect(out).toEqual({
        [a]: true,
        [b]: false,
        [c]: true,
        [missing]: false,
      });
    });

    it("missing id falls every flag back to its default", async () => {
      const a = uid();
      const b = uid();
      adapter = beyondAdapter<boolean>(kv, { mode, watch: false, refresh: 1 });
      const out = await adapter.bulkDecide?.({
        flags: [
          { key: a, defaultValue: false },
          { key: b, defaultValue: true },
        ],
        headers: reqHeaders(),
        cookies,
      });
      expect(out).toEqual({ [a]: false, [b]: true });
    });
  });
}

describe("beyondAdapter — request mode caching", () => {
  it("reads each def at most once per request (shared headers)", async () => {
    const kv = kvClient();
    const a = uid();
    const b = uid();
    const id = uid();
    await writeDef(kv, a, { on: true, rollout: { percent: 100 } });
    await writeDef(kv, b, { on: true, rollout: { percent: 100 } });
    const getSpy = vi.spyOn(kv, "get");
    const batchSpy = vi.spyOn(kv, "batchGet");
    const adapter = beyondAdapter<boolean>(kv, { mode: "request" });
    try {
      const headers = reqHeaders();
      // Evaluate the same two flags twice within one request.
      for (let i = 0; i < 2; i++) {
        for (const key of [a, b]) {
          await adapter.decide({
            key,
            entities: { id },
            headers,
            cookies,
            defaultValue: false,
          });
        }
      }
      // Two distinct def keys → at most two def reads total (get or batchGet),
      // plus one prefs read for the id. Re-evaluations hit the per-request cache.
      const defReads = getSpy.mock.calls.filter((c) =>
        String(c[0]).startsWith("flags:def:")
      )
        .length + batchSpy.mock.calls.length;
      const prefReads = getSpy.mock.calls.filter((c) =>
        String(c[0]).startsWith("flags:user:")
      ).length;
      expect(defReads).toBe(2);
      expect(prefReads).toBe(1);
    } finally {
      await adapter.close();
    }
  });
});

describe("beyondAdapter — getProviderData", () => {
  it("lists declared flags from KV", async () => {
    const kv = kvClient();
    const alpha = uid();
    const beta = uid();
    await writeDef(kv, alpha, { on: true });
    await writeDef(kv, beta, { on: false });
    const adapter = beyondAdapter(kv, { mode: "request" });
    try {
      const data = await adapter.getProviderData();
      // Shared keyspace: assert our defs are present (subset), not exact set.
      expect(data.definitions[alpha]).toBeDefined();
      expect(data.definitions[beta]).toBeDefined();
      expect(data.definitions[alpha]?.declaredInCode).toBe(false);
      expect(data.hints).toEqual([]);
    } finally {
      await adapter.close();
    }
  });

  it("returns a hint instead of throwing on KV failure", async () => {
    const kv = kvClient();
    vi.spyOn(kv, "list").mockResolvedValue({
      data: undefined,
      // biome-ignore lint/suspicious/noExplicitAny: minimal error stub
      error: { message: "boom" } as any,
    });
    const adapter = beyondAdapter(kv, { mode: "request" });
    try {
      const data = await adapter.getProviderData();
      expect(data.definitions).toEqual({});
      expect(data.hints).toHaveLength(1);
      expect(data.hints[0]?.key).toBe("beyond-kv");
    } finally {
      await adapter.close();
    }
  });
});

describe("beyondAdapter — adapter identity", () => {
  it("exposes a stable adapterId and origin resolver", async () => {
    const kv = kvClient();
    const adapter = beyondAdapter(kv, {
      mode: "request",
      origin: (key) => `https://flags.example/${key}`,
    });
    try {
      expect(typeof adapter.adapterId).toBe("symbol");
      const origin = adapter.origin;
      const resolved = typeof origin === "function"
        ? origin("my-flag")
        : origin;
      expect(resolved).toBe("https://flags.example/my-flag");
    } finally {
      await adapter.close();
    }
  });
});

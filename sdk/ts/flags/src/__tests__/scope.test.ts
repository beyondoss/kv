import type { KvClient } from "@beyond.dev/kv";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { runWithScope } from "../als.js";
import { createFlags, type FlagsClient } from "../flags.js";
import type { FlagEvent } from "../types.js";
import { kvClient, writeDef } from "./harness.js";

describe("ALS scope — per-request user-pref caching", () => {
  let kv: KvClient;
  let getSpyCount = 0;
  let realGet: KvClient["get"];
  let flags: FlagsClient;

  beforeEach(() => {
    kv = kvClient();
    realGet = kv.get.bind(kv);
    getSpyCount = 0;
    kv.get = ((key: string) => {
      if (key.startsWith("flags:user:")) getSpyCount++;
      return realGet(key);
    }) as KvClient["get"];
    flags = createFlags(kv, { watch: false, refresh: 30 });
  });

  afterEach(async () => {
    await flags.close();
  });

  it("zero-arg eval inside runWithScope fetches user prefs exactly once", async () => {
    const a = flags("scope-a", false);
    const b = flags("scope-b", false);
    const c = flags("scope-c", false);
    await a.set({ id: "u_scope_1" }, true);
    getSpyCount = 0;

    const result = await runWithScope({ id: "u_scope_1" }, async () => {
      const r1 = await a();
      const r2 = await b();
      const r3 = await c();
      return [r1, r2, r3];
    });

    expect(result).toEqual([true, false, false]);
    expect(getSpyCount).toBe(1);
  });

  it("zero-arg eval throws when no scope is active", async () => {
    const f = flags("no-scope", false);
    await expect(f()).rejects.toThrow(/no context/i);
  });

  it("two parallel scopes do not cross-contaminate", async () => {
    const f = flags("multi-scope", false);
    await f.set({ id: "u_alpha" }, true);
    await f.set({ id: "u_beta" }, false);

    const [a, b] = await Promise.all([
      runWithScope({ id: "u_alpha" }, () => f()),
      runWithScope({ id: "u_beta" }, () => f()),
    ]);
    expect(a).toBe(true);
    expect(b).toBe(false);
  });

  it("explicit-context eval inside a scope still works (and bypasses scope cache)", async () => {
    const f = flags("explicit-in-scope", false);
    await f.set({ id: "u_x" }, true);
    await f.set({ id: "u_y" }, false);

    const [x, y] = await runWithScope({ id: "u_x" }, async () => {
      return [await f(), await f({ id: "u_y" })];
    });
    expect(x).toBe(true);
    expect(y).toBe(false);
  });
});

describe("createFlags — error observability", () => {
  let kv: KvClient;

  beforeEach(() => {
    kv = kvClient();
  });

  it("eval error fires an 'error' reason event via onEvaluate", async () => {
    const events: FlagEvent[] = [];
    const flags = createFlags(kv, {
      watch: false,
      onEvaluate: (e) => events.push(e),
    });
    const f = flags("error-event", false);

    // No scope active — throws no_context.
    await expect(f()).rejects.toThrow(/no context/i);

    expect(events).toHaveLength(1);
    expect(events[0]?.reason).toBe("error");
    expect(events[0]?.error).toBeDefined();
    expect(events[0]?.value).toBeUndefined();
    await flags.close();
  });

  it("onEvaluate hook throwing does not crash eval", async () => {
    const flags = createFlags(kv, {
      watch: false,
      onEvaluate: () => {
        throw new Error("hook failure");
      },
    });
    const f = flags("swallow-hook", false);
    await expect(f({ id: "u_swallow" })).resolves.toBe(false);
    await flags.close();
  });
});

describe("default flags singleton", () => {
  it("throws with a clear message when BEYOND_KV_URL is unset", async () => {
    const saved = process.env["BEYOND_KV_URL"];
    delete process.env["BEYOND_KV_URL"];
    vi.resetModules();
    const { flags: freshFlags } = await import("../flags.js");
    try {
      expect(() => freshFlags("x", false)).toThrow(/BEYOND_KV_URL/);
    } finally {
      if (saved !== undefined) process.env["BEYOND_KV_URL"] = saved;
      vi.resetModules();
    }
  });
});

describe("createFlags — onEvaluate observability", () => {
  let kv: KvClient;
  let flags: FlagsClient;

  beforeEach(() => {
    kv = kvClient();
  });

  afterEach(async () => {
    await flags?.close();
  });

  it("emits an event for each eval with reason populated", async () => {
    const events: { name: string; reason: string }[] = [];
    // Write def BEFORE createFlags so the initial snapshot load picks it up.
    await writeDef(kv, "obs", { on: true, rollout: { percent: 100 } });
    flags = createFlags(kv, {
      watch: false,
      refresh: 30,
      onEvaluate: (e) => events.push({ name: e.name, reason: e.reason }),
    });
    await flags.ready();
    const f = flags("obs", false);
    expect(await f({ id: "u_obs" })).toBe(true);

    events.length = 0;
    await f({ id: "u_obs" });
    expect(events.length).toBe(1);
    expect(events[0]?.name).toBe("obs");
    expect(events[0]?.reason).toBe("rollout");
  });
});

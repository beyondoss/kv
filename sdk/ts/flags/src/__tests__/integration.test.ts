import { KvError } from "@beyond.dev/kv";
import type { KvClient } from "@beyond.dev/kv";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { createFlags, type FlagsClient } from "../flags.js";
import { deleteDef, kvClient, readPrefs, sleep, writeDef } from "./harness.js";
import "./test-context.js";
import type { FlagsErrorEvent } from "../types.js";

describe("createFlags — explicit-context eval against real KV", () => {
  let kv: KvClient;
  let flags: FlagsClient;

  beforeEach(() => {
    kv = kvClient();
    flags = createFlags(kv, { watch: false, refresh: 1 });
  });

  afterEach(async () => {
    await flags.close();
  });

  it("returns the default when the flag has no def in KV", async () => {
    const newCheckout = flags("new-checkout-missing", false);
    expect(await newCheckout({ id: "u_1" })).toBe(false);
  });

  it("returns rollout=true at 100% once the def is written", async () => {
    await writeDef(kv, "rollout-100", { on: true, rollout: { percent: 100 } });
    const f = flags("rollout-100", false);

    // Wait until snapshot has it (poll mode every 1s).
    const deadline = Date.now() + 5_000;
    while (Date.now() < deadline) {
      if ((await f({ id: "u_x" })) === true) break;
      await sleep(100);
    }
    expect(await f({ id: "u_x" })).toBe(true);
  });

  it("kill switch (on:false) returns default even with rollout 100", async () => {
    await writeDef(kv, "killed", { on: false, rollout: { percent: 100 } });
    const f = flags("killed", false);

    const deadline = Date.now() + 5_000;
    let attempts = 0;
    while (Date.now() < deadline && attempts < 50) {
      attempts++;
      const v = await f({ id: "u_x" });
      // Once the def is loaded, value stays `false` because of the kill switch.
      // We just need to know the snapshot has loaded — confirm via def lookup.
      if (v === false) {
        await sleep(100);
        if ((await f({ id: "u_x" })) === false) break;
      }
    }
    expect(await f({ id: "u_x" })).toBe(false);
  });

  it("variant flag returns rollout.value when matched", async () => {
    await writeDef(kv, "variant-rollout", {
      on: true,
      rollout: { percent: 100, value: "v2" },
    });
    const f = flags("variant-rollout", ["off", "v1", "v2"]);

    const deadline = Date.now() + 5_000;
    while (Date.now() < deadline) {
      if ((await f({ id: "u_v" })) === "v2") break;
      await sleep(100);
    }
    expect(await f({ id: "u_v" })).toBe("v2");
  });
});

describe("flag.set / flag.reset — real KV CAS round-trip", () => {
  let kv: KvClient;
  let flags: FlagsClient;

  beforeEach(() => {
    kv = kvClient();
    flags = createFlags(kv, { watch: false, refresh: 30 });
  });

  afterEach(async () => {
    await flags.close();
  });

  it("set writes the per-id pref bundle and reset clears it", async () => {
    const f = flags("opt-in", false);
    await f.set({ id: "u_1" }, true);

    const prefs1 = await readPrefs(kv, "u_1");
    expect(prefs1).toEqual({ "opt-in": true });

    // user-pref beats default with no def in KV
    expect(await f({ id: "u_1" })).toBe(true);
    expect(await f({ id: "u_2" })).toBe(false);

    await f.reset({ id: "u_1" });
    const prefs2 = await readPrefs(kv, "u_1");
    expect(prefs2).toBeNull();
    expect(await f({ id: "u_1" })).toBe(false);
  });

  it("set merges multiple flags into the same per-id bundle", async () => {
    const a = flags("flag-a", false);
    const b = flags("flag-b", "off" as string);

    await a.set({ id: "u_merge" }, true);
    await b.set({ id: "u_merge" }, "v2");

    const prefs = await readPrefs(kv, "u_merge");
    expect(prefs).toEqual({ "flag-a": true, "flag-b": "v2" });

    await a.reset({ id: "u_merge" });
    expect(await readPrefs(kv, "u_merge")).toEqual({ "flag-b": "v2" });

    await b.reset({ id: "u_merge" });
    expect(await readPrefs(kv, "u_merge")).toBeNull();
  });

  it("reset on absent prefs is a no-op (idempotent)", async () => {
    const f = flags("absent-flag", false);
    await f.reset({ id: "u_never_set" });
    expect(await readPrefs(kv, "u_never_set")).toBeNull();
  });

  it("user pref overrides rollout via real KV state", async () => {
    await writeDef(kv, "rolled", { on: true, rollout: { percent: 0 } });
    const f = flags("rolled", false);
    await f.set({ id: "u_pref" }, true);

    const deadline = Date.now() + 5_000;
    while (Date.now() < deadline) {
      if ((await f({ id: "u_pref" })) === true) break;
      await sleep(100);
    }
    expect(await f({ id: "u_pref" })).toBe(true);
    await f.reset({ id: "u_pref" });
    await deleteDef(kv, "rolled");
  });
});

describe("createFlags — input validation", () => {
  let kv: KvClient;
  let flags: FlagsClient;

  beforeEach(() => {
    kv = kvClient();
    flags = createFlags(kv, { watch: false, refresh: 30 });
  });

  afterEach(async () => {
    await flags.close();
  });

  it("empty variants array throws synchronously", () => {
    expect(() => flags("no-variants", [] as never)).toThrow(/variants array/i);
  });

  it("explicit context with empty id throws FlagError missing_id", async () => {
    const f = flags("empty-id-explicit", false);
    await expect(f({ id: "" })).rejects.toMatchObject({ code: "missing_id" });
  });

  it("flag.set with empty id throws FlagError missing_id", async () => {
    const f = flags("set-empty-id", false);
    await expect(f.set({ id: "" }, true)).rejects.toMatchObject({
      code: "missing_id",
    });
  });

  it("flag.reset with empty id throws FlagError missing_id", async () => {
    const f = flags("reset-empty-id", false);
    await expect(f.reset({ id: "" })).rejects.toMatchObject({
      code: "missing_id",
    });
  });
});

describe("createFlags — KV error resilience", () => {
  let kv: KvClient;

  afterEach(async () => {
    await kv.close();
  });

  it("KV error fetching user prefs triggers onError and falls through to default", async () => {
    kv = kvClient();
    const errors: FlagsErrorEvent[] = [];
    const realGet = kv.get.bind(kv);
    kv.get = async (key: string) => {
      if (key.startsWith("flags:user:")) {
        return {
          data: undefined,
          error: new KvError("unavailable", "KV unavailable", 503),
        };
      }
      return realGet(key);
    };

    const flagsClient = createFlags(kv, {
      watch: false,
      refresh: 30,
      onError: (e) => errors.push(e),
    });
    const f = flagsClient("kv-err-flag", "default-value" as string);
    expect(await f({ id: "u_1" })).toBe("default-value");
    expect(errors.some((e) => e.source === "user-prefs")).toBe(true);
    await flagsClient.close();
  });
});

describe("flag.set — CAS contention", () => {
  let kv: KvClient;
  let flags: FlagsClient;

  beforeEach(() => {
    kv = kvClient();
    flags = createFlags(kv, { watch: false, refresh: 30 });
  });

  afterEach(async () => {
    await flags.close();
  });

  it("concurrent set calls on the same user all succeed (CAS retry handles contention)", async () => {
    // n=5 concurrent writers on the same key. mutateUserPrefs retries
    // with jittered backoff up to 10 times, which is comfortably above
    // the worst-case scheduling unfairness for this n.
    const n = 5;
    const flagsList = Array.from(
      { length: n },
      (_, i) => flags(`cas-flag-${i}`, false),
    );
    await Promise.all(
      flagsList.map((f) => f.set({ id: "u_concurrent" }, true)),
    );

    const prefs = await readPrefs(kv, "u_concurrent");
    expect(Object.keys(prefs ?? {}).length).toBe(n);
  });

  it("set throws a clear error after exhausting CAS retries", async () => {
    // Pre-populate so the code takes the CAS (not ifAbsent) path.
    await kv.set("flags:user:u_exhaust", JSON.stringify({ existing: true }));

    const realCas = kv.cas.bind(kv);
    let casCount = 0;
    kv.cas = async (...args: Parameters<typeof kv.cas>) => {
      casCount++;
      // Simulate a perpetual 409 by returning the error without actually writing.
      void realCas(...args); // fire-and-forget to avoid interfering with retry gets
      return {
        data: undefined,
        error: new KvError("conflict", "conflict", 409),
      };
    };

    const f = flags("exhaust-flag", false);
    await expect(f.set({ id: "u_exhaust" }, true)).rejects.toThrow(/retries/);
    expect(casCount).toBe(10);
  });
});

describe("watch — kill-switch propagates to live evaluator", () => {
  let kv: KvClient;
  let flags: FlagsClient;

  beforeEach(() => {
    kv = kvClient();
    flags = createFlags(kv, { watch: true, refresh: 30 });
  });

  afterEach(async () => {
    await flags.close();
  });

  it("flipping on:true → on:false in KV is observed by eval", async () => {
    await writeDef(kv, "live", { on: true, rollout: { percent: 100 } });
    const f = flags("live", false);

    const onDeadline = Date.now() + 5_000;
    while (Date.now() < onDeadline) {
      if ((await f({ id: "u" })) === true) break;
      await sleep(50);
    }
    expect(await f({ id: "u" })).toBe(true);

    await writeDef(kv, "live", { on: false, rollout: { percent: 100 } });
    const offDeadline = Date.now() + 5_000;
    while (Date.now() < offDeadline) {
      if ((await f({ id: "u" })) === false) break;
      await sleep(50);
    }
    expect(await f({ id: "u" })).toBe(false);
  });
});

import { KvError } from "@beyond.dev/kv";
import type { KvClient, WatchEvent, WatchOptions } from "@beyond.dev/kv";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { fetchUserPrefs, Snapshot } from "../snapshot.js";
import { deleteDef, kvClient, sleep, writeDef } from "./harness.js";

const encode = (v: unknown) => new TextEncoder().encode(JSON.stringify(v));

describe("snapshot — real KV", () => {
  let kv: KvClient;
  let snap: Snapshot;

  beforeEach(() => {
    kv = kvClient();
  });

  afterEach(async () => {
    snap?.close();
    await kv.close();
  });

  it("loads existing flags:def:* on boot", async () => {
    await writeDef(kv, "boot-a", { on: true, rollout: { percent: 100 } });
    await writeDef(kv, "boot-b", { on: false });

    snap = new Snapshot(kv, { refresh: 30, watch: false });
    snap.start();
    await snap.ready();

    expect(snap.get("boot-a")).toEqual({ on: true, rollout: { percent: 100 } });
    expect(snap.get("boot-b")).toEqual({ on: false });
    expect(snap.get("does-not-exist")).toBeUndefined();
  });

  it("watch mode picks up writes after boot", async () => {
    snap = new Snapshot(kv, { refresh: 30, watch: true });
    snap.start();
    await snap.ready();
    expect(snap.get("watched")).toBeUndefined();

    await writeDef(kv, "watched", { on: true });

    // Watch event needs a moment to propagate.
    const deadline = Date.now() + 5_000;
    while (Date.now() < deadline && snap.get("watched") === undefined) {
      await sleep(50);
    }
    expect(snap.get("watched")).toEqual({ on: true });

    await deleteDef(kv, "watched");
    const deadline2 = Date.now() + 5_000;
    while (Date.now() < deadline2 && snap.get("watched") !== undefined) {
      await sleep(50);
    }
    expect(snap.get("watched")).toBeUndefined();
  });

  it("polling fallback picks up writes when watch is disabled", async () => {
    snap = new Snapshot(kv, { refresh: 1, watch: false });
    snap.start();
    await snap.ready();
    expect(snap.get("polled")).toBeUndefined();

    await writeDef(kv, "polled", { on: true });

    const deadline = Date.now() + 5_000;
    while (Date.now() < deadline && snap.get("polled") === undefined) {
      await sleep(100);
    }
    expect(snap.get("polled")).toEqual({ on: true });
  });

  it("fires onChange once per real edit, never for unchanged re-reads (byte dedup)", async () => {
    const name = `dedup-${crypto.randomUUID()}`;
    await writeDef(kv, name, { on: true, rollout: { percent: 50 } });

    const changes: string[][] = [];
    const ours = () => changes.flat().filter((n) => n === name);
    snap = new Snapshot(kv, {
      refresh: 1, // poll every second
      watch: false,
      onChange: (names) => changes.push(names),
    });
    snap.start();
    await snap.ready(); // initial load — onChange suppressed

    // Several poll cycles re-read the identical def. Same bytes → no events.
    await sleep(2_200);
    expect(ours()).toEqual([]);

    // A genuine edit is detected on the next poll — exactly once.
    await writeDef(kv, name, { on: false });
    const deadline = Date.now() + 5_000;
    while (Date.now() < deadline && ours().length === 0) {
      await sleep(100);
    }
    expect(ours()).toEqual([name]);

    // Re-reading the now-stable value must not keep firing.
    await sleep(2_200);
    expect(ours()).toEqual([name]);
  });

  it("ignores keys outside flags:def: prefix", async () => {
    const { error } = await kv.set("not-a-flag", JSON.stringify({ on: true }));
    expect(error).toBeUndefined();
    await writeDef(kv, "real", { on: true });

    snap = new Snapshot(kv, { refresh: 30, watch: false });
    snap.start();
    await snap.ready();

    expect(snap.get("real")).toBeDefined();
    expect(snap.get("not-a-flag")).toBeUndefined();
  });

  it("reports decode errors via onError without crashing", async () => {
    const errors: unknown[] = [];
    const { error } = await kv.set("flags:def:bad-json", "{ this is not json");
    expect(error).toBeUndefined();

    snap = new Snapshot(kv, {
      refresh: 30,
      watch: false,
      onError: (e) => errors.push(e),
    });
    snap.start();
    await snap.ready();

    expect(errors.length).toBeGreaterThan(0);
    expect(snap.get("bad-json")).toBeUndefined();
  });

  it("close() called twice is a no-op (idempotent)", async () => {
    snap = new Snapshot(kv, { refresh: 30, watch: false });
    snap.start();
    await snap.ready();
    snap.close();
    expect(() => snap.close()).not.toThrow();
  });

  it("decodeFlagDef: rejects valid JSON that is an array", async () => {
    const errors: unknown[] = [];
    await kv.set("flags:def:array-def", JSON.stringify([1, 2, 3]));

    snap = new Snapshot(kv, {
      refresh: 30,
      watch: false,
      onError: (e) => errors.push(e),
    });
    snap.start();
    await snap.ready();

    expect(snap.get("array-def")).toBeUndefined();
    expect(errors.length).toBeGreaterThan(0);
  });

  it("decodeFlagDef: rejects object missing the on field", async () => {
    const errors: unknown[] = [];
    await kv.set(
      "flags:def:no-on",
      JSON.stringify({ rollout: { percent: 100 } }),
    );

    snap = new Snapshot(kv, {
      refresh: 30,
      watch: false,
      onError: (e) => errors.push(e),
    });
    snap.start();
    await snap.ready();

    expect(snap.get("no-on")).toBeUndefined();
    expect(errors.length).toBeGreaterThan(0);
  });

  it("decodeFlagDef: rejects object where on is not a boolean", async () => {
    const errors: unknown[] = [];
    await kv.set("flags:def:bad-on", JSON.stringify({ on: "yes" }));

    snap = new Snapshot(kv, {
      refresh: 30,
      watch: false,
      onError: (e) => errors.push(e),
    });
    snap.start();
    await snap.ready();

    expect(snap.get("bad-on")).toBeUndefined();
    expect(errors.length).toBeGreaterThan(0);
  });

  it("loadAll traverses cursor pagination to load all flags", async () => {
    await writeDef(kv, "paged-a", { on: true });
    await writeDef(kv, "paged-b", { on: false });

    // Capture real keys so we can split them across mocked pages.
    const { data: realData } = await kv.list({ prefix: "flags:def:" });
    const allKeys = realData!.keys;
    expect(allKeys.length).toBeGreaterThanOrEqual(2);

    let listCalls = 0;
    kv.list = async () => {
      listCalls++;
      if (listCalls === 1) {
        return {
          data: { keys: allKeys.slice(0, 1), nextCursor: "page2" },
          error: undefined,
        };
      }
      return { data: { keys: allKeys.slice(1) }, error: undefined };
    };

    snap = new Snapshot(kv, { refresh: 30, watch: false });
    snap.start();
    await snap.ready();

    expect(snap.get("paged-a")).toBeDefined();
    expect(snap.get("paged-b")).toBeDefined();
    expect(listCalls).toBe(2);
  });

  it("watch failure triggers onError and polling fallback picks up new writes", async () => {
    await writeDef(kv, "pre-fail", { on: true });

    const errors: unknown[] = [];
    let watchCalls = 0;
    const realWatch = kv.watch.bind(kv);

    kv.watch =
      (async function*(key: string, opts: Parameters<typeof kv.watch>[1]) {
        if (watchCalls++ === 0) {
          throw new Error("simulated watch disconnection");
        }
        yield* realWatch(key, opts);
      }) as typeof kv.watch;

    snap = new Snapshot(kv, {
      refresh: 1,
      watch: true,
      onError: (e) => errors.push(e),
    });
    snap.start();
    await snap.ready();

    expect(snap.get("pre-fail")).toBeDefined();

    // Write a flag after boot — polling fallback must pick it up during reconnect backoff.
    await writeDef(kv, "post-fail", { on: true });

    const deadline = Date.now() + 6_000;
    while (Date.now() < deadline && snap.get("post-fail") === undefined) {
      await sleep(100);
    }
    expect(snap.get("post-fail")).toBeDefined();
    expect(errors.length).toBeGreaterThan(0);
  });
});

describe("snapshot — watch resume after hard reconnect", () => {
  it("resumes from the last revision and replays the delta missed while down", async () => {
    const sinceSeen: (number | undefined)[] = [];
    let watchCalls = 0;

    // Fake client: empty initial load; a watch that delivers one set then
    // hard-errors (a dropped stream), and on reconnect replays a *different*
    // delta — exactly what the server does for everything after `since`.
    const fakeKv = {
      async list() {
        return { data: { keys: [] as { name: string }[] }, error: undefined };
      },
      async batchGet() {
        return { data: [] as never[], error: undefined };
      },
      async *watch(_key: string, opts?: WatchOptions): AsyncGenerator<WatchEvent> {
        watchCalls++;
        sinceSeen.push(opts?.since);
        if (watchCalls === 1) {
          yield { type: "ready" };
          yield { type: "set", key: "flags:def:a", value: encode({ on: true }), revision: 5 };
          throw new KvError("sse_error", "stream dropped", 500); // hard disconnect
        }
        // Reconnect: replay the delta that landed while we were down.
        yield { type: "set", key: "flags:def:b", value: encode({ on: false }), revision: 7 };
        await new Promise<void>((resolve) => {
          opts?.signal?.addEventListener("abort", () => resolve(), { once: true });
        });
      },
    } as unknown as KvClient;

    const snap = new Snapshot(fakeKv, { refresh: 30, watch: true });
    snap.start();
    await snap.ready();

    // First session applied A.
    {
      const deadline = Date.now() + 2_000;
      while (Date.now() < deadline && snap.get("a") === undefined) await sleep(25);
    }
    expect(snap.get("a")).toEqual({ on: true });

    // After the hard error + backoff, the reconnect must carry since=5 and the
    // replayed delta for B must land — proving no gap loss.
    {
      const deadline = Date.now() + 5_000;
      while (Date.now() < deadline && snap.get("b") === undefined) await sleep(25);
    }
    expect(snap.get("b")).toEqual({ on: false });

    expect(sinceSeen[0]).toBeUndefined(); // first connect starts from current state
    expect(sinceSeen[1]).toBe(5); // reconnect resumes exactly where it left off

    snap.close();
  });
});

describe("fetchUserPrefs — real KV", () => {
  let kv: KvClient;

  beforeEach(() => {
    kv = kvClient();
  });

  afterEach(async () => {
    await kv.close();
  });

  it("KV error calls onError and returns null instead of throwing", async () => {
    const errors: unknown[] = [];
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

    const result = await fetchUserPrefs(kv, "u_kv_err", (e) => errors.push(e));
    expect(result).toBeNull();
    expect(errors.length).toBe(1);
    expect((errors[0] as { source: string }).source).toBe("user-prefs");
  });

  it("malformed JSON in pref bundle calls onError and returns null", async () => {
    const errors: unknown[] = [];
    await kv.set("flags:user:u_bad_json", "{ not valid json");

    const result = await fetchUserPrefs(
      kv,
      "u_bad_json",
      (e) => errors.push(e),
    );
    expect(result).toBeNull();
    expect(errors.length).toBe(1);
    expect((errors[0] as { source: string }).source).toBe("user-prefs");
  });
});

import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { createKvClient } from "../client.js";
import type { KvSchemaMap, SchemaAwareWatchEvent } from "../client.js";
import { getHttpUrl, getRespUrl, uniqueKey, uniqueNs } from "./harness.js";

async function watchCollect<Map extends KvSchemaMap, K extends string>(
  factory: (
    signal: AbortSignal,
  ) => AsyncGenerator<SchemaAwareWatchEvent<K, Map>>,
  predicate: (events: SchemaAwareWatchEvent<K, Map>[]) => boolean,
  act: () => Promise<unknown>,
): Promise<SchemaAwareWatchEvent<K, Map>[]> {
  const ac = new AbortController();
  const gen = factory(ac.signal);
  const events: SchemaAwareWatchEvent<K, Map>[] = [];
  let readyResolve!: () => void;
  const ready = new Promise<void>((r) => {
    readyResolve = r;
  });
  const timeout = setTimeout(() => ac.abort(), 5_000);
  const collect = (async () => {
    for await (const ev of gen) {
      events.push(ev);
      if (ev.type === "ready") readyResolve();
      if (predicate(events)) {
        ac.abort();
        break;
      }
    }
  })();
  collect.then(readyResolve, readyResolve);
  await ready;
  await act();
  await collect.catch(() => {});
  clearTimeout(timeout);
  return events;
}

const schema = {
  "users:*": {
    parse(input: unknown): { username: string } {
      if (
        typeof input === "object"
        && input !== null
        && "username" in input
        && typeof (input as any).username === "string"
      ) return input as { username: string };
      throw new Error("invalid user");
    },
  },
  "counters:*": {
    parse(input: unknown): { value: number } {
      if (
        typeof input === "object"
        && input !== null
        && "value" in input
        && typeof (input as any).value === "number"
      ) return input as { value: number };
      throw new Error("invalid counter");
    },
  },
};

for (const backend of ["http", "resp"] as const) {
  function baseOpts() {
    return backend === "http"
      ? { url: getHttpUrl(), namespace: uniqueNs() }
      : { url: getRespUrl(), db: 14 };
  }

  describe(`schema client — ${backend}`, () => {
    let key: string;

    beforeEach(() => {
      key = uniqueKey();
    });
    afterEach(async () => {
      const kv = createKvClient(baseOpts());
      await kv.delete(key);
      await kv.close();
    });

    it("get returns null for missing key", async () => {
      const kv = createKvClient({ ...baseOpts(), schema });
      const { data, error } = await kv.get(`users:${key}`);
      expect(error).toBeUndefined();
      expect(data).toBeNull();
      await kv.close();
    });

    it("set + get round-trip parses through matched schema", async () => {
      const kv = createKvClient({ ...baseOpts(), schema });
      await kv.set(`users:${key}`, { username: "alice" });
      const { data, error } = await kv.get(`users:${key}`);
      expect(error).toBeUndefined();
      expect(data).toEqual({ username: "alice" });
      await kv.close();
    });

    it("different key patterns use different schemas", async () => {
      const kv = createKvClient({ ...baseOpts(), schema });
      await kv.set(`counters:${key}`, { value: 42 });
      const { data } = await kv.get(`counters:${key}`);
      expect(data).toEqual({ value: 42 });
      await kv.close();
    });

    it("unmatched key returns raw Entry", async () => {
      const kv = createKvClient({ ...baseOpts(), schema });
      const raw = createKvClient(baseOpts());
      await raw.set(`raw:${key}`, "hello");
      await raw.close();
      const { data } = await kv.get(`raw:${key}`);
      expect(data?.text()).toBe("hello");
      await kv.close();
    });

    it("get returns schema_error when stored value fails validation", async () => {
      const raw = createKvClient(baseOpts());
      await raw.set(`users:${key}`, JSON.stringify({ wrong: true }));
      await raw.close();

      const kv = createKvClient({ ...baseOpts(), schema });
      const { data, error } = await kv.get(`users:${key}`);
      expect(data).toBeUndefined();
      expect(error?.code).toBe("schema_error");
      expect(error?.status).toBe(422);
      await kv.close();
    });

    it("default ttl applied on set", async () => {
      const kv = createKvClient({ ...baseOpts(), schema, ttl: 60 });
      await kv.set(`users:${key}`, { username: "bob" });
      const { data: entry } = await createKvClient(baseOpts()).get(
        `users:${key}`,
      );
      expect(entry?.ttl).toBeGreaterThan(0);
      expect(entry?.ttl).toBeLessThanOrEqual(60);
      await kv.close();
    });

    it("per-call ttl overrides default", async () => {
      const kv = createKvClient({ ...baseOpts(), schema, ttl: 3600 });
      await kv.set(`users:${key}`, { username: "carol" }, { ttl: 30 });
      const { data: entry } = await createKvClient(baseOpts()).get(
        `users:${key}`,
      );
      expect(entry?.ttl).toBeLessThanOrEqual(30);
      await kv.close();
    });

    it("ttl-only (no schema) applies default to raw set", async () => {
      const kv = createKvClient({ ...baseOpts(), ttl: 60 });
      await kv.set(key, "raw-value");
      const { data: entry } = await createKvClient(baseOpts()).get(key);
      expect(entry?.ttl).toBeGreaterThan(0);
      expect(entry?.ttl).toBeLessThanOrEqual(60);
      await kv.close();
    });

    it("no-schema path is unchanged", async () => {
      const kv = createKvClient(baseOpts());
      await kv.set(key, "plain");
      const { data: entry, error } = await kv.get(key);
      expect(error).toBeUndefined();
      expect(entry?.text()).toBe("plain");
      await kv.close();
    });

    it("batchGet returns typed values per key", async () => {
      const kv = createKvClient({ ...baseOpts(), schema });
      const k2 = uniqueKey();
      await kv.set(`users:${key}`, { username: "alice" });
      await kv.set(`counters:${k2}`, { value: 7 });
      const { data, error } = await kv.batchGet(
        [`users:${key}`, `counters:${k2}`] as const,
      );
      expect(error).toBeUndefined();
      expect(data![0]).toEqual({ username: "alice" });
      expect(data![1]).toEqual({ value: 7 });
      await kv.delete(`counters:${k2}`);
      await kv.close();
    });

    it("batchGet returns null for missing key", async () => {
      const kv = createKvClient({ ...baseOpts(), schema });
      const { data } = await kv.batchGet([`users:${key}`] as const);
      expect(data![0]).toBeNull();
      await kv.close();
    });

    it("batchSet serializes typed values", async () => {
      const kv = createKvClient({ ...baseOpts(), schema });
      // dynamic key loses literal inference — cast required at call site
      await kv.batchSet([
        { key: `users:${key}`, value: { username: "bob" } } as any,
      ]);
      const { data } = await kv.get(`users:${key}`);
      expect(data).toEqual({ username: "bob" });
      await kv.close();
    });

    it("batch get op returns typed value", async () => {
      const kv = createKvClient({ ...baseOpts(), schema });
      await kv.set(`users:${key}`, { username: "carol" });
      const { data, error } = await kv.batch(
        [{ op: "get", key: `users:${key}` }] as const,
      );
      expect(error).toBeUndefined();
      expect(data![0]).toEqual({ username: "carol" });
      await kv.close();
    });

    it("batch set op applies default ttl", async () => {
      const kv = createKvClient({ ...baseOpts(), schema, ttl: 60 });
      await kv.batch(
        [{
          op: "set",
          key: `users:${key}`,
          value: JSON.stringify({ username: "dan" }),
        }] as const,
      );
      const { data: entry } = await createKvClient(baseOpts()).get(
        `users:${key}`,
      );
      expect(entry?.ttl).toBeGreaterThan(0);
      expect(entry?.ttl).toBeLessThanOrEqual(60);
      await kv.close();
    });

    if (backend === "http") {
      it("watch emits typed set event for matched key", async () => {
        const kv = createKvClient({ ...baseOpts(), schema });
        const events = await watchCollect(
          (signal) => kv.watch(`users:${key}`, { signal }),
          (evs) => evs.some((e) => e.type === "set"),
          async () => kv.set(`users:${key}`, { username: "watchUser" } as any),
        );
        const setEvent = events.find((e) => e.type === "set");
        expect(setEvent).toBeDefined();
        if (setEvent!.type === "set") {
          expect(setEvent!.value).toEqual({ username: "watchUser" });
        }
        await kv.close();
      });

      it("watch emits raw entry for unmatched key", async () => {
        const opts = baseOpts();
        const kv = createKvClient({ ...opts, schema });
        const rawKey = `raw:${key}`;
        const raw = createKvClient(opts);
        const events = await watchCollect(
          (signal) => kv.watch(rawKey, { signal }),
          (evs) => evs.some((e) => e.type === "set"),
          async () => {
            await raw.set(rawKey, "hello");
          },
        );
        await raw.delete(rawKey);
        await raw.close();
        const setEvent = events.find((e) => e.type === "set");
        expect(setEvent).toBeDefined();
        if (setEvent!.type === "set") {
          expect(setEvent!.value).toBeInstanceOf(Uint8Array);
        }
        await kv.close();
      });

      it("prefix watch emits typed set events", async () => {
        const opts = baseOpts();
        const kv = createKvClient({ ...opts, schema });
        // Use key as a sub-prefix so the watch is scoped to this test only.
        // All HTTP tests share namespace 0 (nsToIndex maps arbitrary names to 0),
        // so a bare "users:" prefix would pick up other tests' keys.
        const pfx = `users:${key}:`;
        const events = await watchCollect(
          (signal) => kv.watch(pfx, { prefix: true, signal }),
          (evs) => evs.filter((e) => e.type === "set").length >= 2,
          async () => {
            await kv.set(`${pfx}a`, { username: "pfxA" } as any);
            await kv.set(`${pfx}b`, { username: "pfxB" } as any);
          },
        );
        const setEvents = events.filter((e) => e.type === "set");
        expect(setEvents.length).toBeGreaterThanOrEqual(2);
        for (const ev of setEvents) {
          if (ev.type === "set") {
            expect(ev.value).toHaveProperty("username");
          }
        }
        await kv.delete(`${pfx}a`);
        await kv.delete(`${pfx}b`);
        await kv.close();
      });
    }
  });
}

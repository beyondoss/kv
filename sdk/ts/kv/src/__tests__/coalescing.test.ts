import { afterEach, describe, expect, it } from "vitest";
import {
  createKvClient,
  type KvClient,
  type KvRequestEvent,
} from "../client.js";
import { dec, getHttpUrl, getRespUrl, uniqueKey, uniqueNs } from "./harness.js";

// Collects onRequest events so tests can assert which commands were fired and how many.
function tracker() {
  const events: KvRequestEvent[] = [];
  return {
    events,
    onRequest: (e: KvRequestEvent) => events.push({ ...e }),
    reset: () => (events.length = 0),
  };
}

// ── HTTP backend ──────────────────────────────────────────────────────────────

describe("HTTP backend — coalescing", () => {
  it("concurrent gets collapse into one BATCH", async () => {
    const t = tracker();
    const kv = createKvClient({
      url: getHttpUrl(),
      namespace: uniqueNs(),
      onRequest: t.onRequest,
    });
    const [k1, k2, k3] = [uniqueKey("c"), uniqueKey("c"), uniqueKey("c")];
    await kv.set(k1, "alpha");
    await kv.set(k2, "beta");
    t.reset();

    const [r1, r2, r3] = await Promise.all([
      kv.get(k1),
      kv.get(k2),
      kv.get(k3),
    ]);

    expect(t.events).toHaveLength(1);
    expect(t.events[0]!.command).toBe("BATCH");
    expect(t.events[0]!.keyCount).toBe(3);
    expect(dec(r1.data!.value)).toBe("alpha");
    expect(dec(r2.data!.value)).toBe("beta");
    expect(r3.data).toBeNull();
  });

  it("concurrent sets collapse into one BATCH", async () => {
    const t = tracker();
    const kv = createKvClient({
      url: getHttpUrl(),
      namespace: uniqueNs(),
      onRequest: t.onRequest,
    });
    const [k1, k2] = [uniqueKey("c"), uniqueKey("c")];

    const [r1, r2] = await Promise.all([
      kv.set(k1, "v1"),
      kv.set(k2, "v2"),
    ]);

    expect(t.events).toHaveLength(1);
    expect(t.events[0]!.command).toBe("BATCH");
    expect(t.events[0]!.keyCount).toBe(2);
    expect(r1.error).toBeUndefined();
    expect(r2.error).toBeUndefined();
    expect(dec((await kv.get(k1)).data!.value)).toBe("v1");
    expect(dec((await kv.get(k2)).data!.value)).toBe("v2");
  });

  it("mixed gets and sets collapse into one BATCH", async () => {
    const t = tracker();
    const kv = createKvClient({
      url: getHttpUrl(),
      namespace: uniqueNs(),
      onRequest: t.onRequest,
    });
    const [k1, k2, k3] = [uniqueKey("c"), uniqueKey("c"), uniqueKey("c")];
    await kv.set(k1, "existing");
    t.reset();

    const [getResult, setResult, getMissResult] = await Promise.all([
      kv.get(k1),
      kv.set(k2, "new"),
      kv.get(k3),
    ]);

    expect(t.events).toHaveLength(1);
    expect(t.events[0]!.command).toBe("BATCH");
    expect(t.events[0]!.keyCount).toBe(3);
    expect(dec(getResult.data!.value)).toBe("existing");
    expect(setResult.error).toBeUndefined();
    expect(getMissResult.data).toBeNull();
    expect(dec((await kv.get(k2)).data!.value)).toBe("new");
  });

  it("duplicate gets for the same key deduplicate to one round-trip", async () => {
    const t = tracker();
    const kv = createKvClient({
      url: getHttpUrl(),
      namespace: uniqueNs(),
      onRequest: t.onRequest,
    });
    const key = uniqueKey("dup");
    await kv.set(key, "shared");
    t.reset();

    const [r1, r2, r3] = await Promise.all([
      kv.get(key),
      kv.get(key),
      kv.get(key),
    ]);

    // Deduplicated to a single GET (3 waiters share 1 request — no batch needed for 1 unique key)
    expect(t.events).toHaveLength(1);
    expect(t.events[0]!.command).toBe("GET");
    expect(t.events[0]!.keyCount).toBe(1);
    // All three callers get the value
    expect(dec(r1.data!.value)).toBe("shared");
    expect(dec(r2.data!.value)).toBe("shared");
    expect(dec(r3.data!.value)).toBe("shared");
  });

  it("gets in different ticks do NOT coalesce", async () => {
    const t = tracker();
    const kv = createKvClient({
      url: getHttpUrl(),
      namespace: uniqueNs(),
      onRequest: t.onRequest,
    });
    const [k1, k2] = [uniqueKey("c"), uniqueKey("c")];
    await kv.set(k1, "a");
    await kv.set(k2, "b");
    t.reset();

    await kv.get(k1);
    await kv.get(k2);

    // Two separate ticks → two separate GET commands (not batched)
    expect(t.events).toHaveLength(2);
    expect(t.events.every((e) => e.command === "GET")).toBe(true);
  });

  it("set with TTL coalesces and preserves expiry", async () => {
    const t = tracker();
    const kv = createKvClient({
      url: getHttpUrl(),
      namespace: uniqueNs(),
      onRequest: t.onRequest,
    });
    const [k1, k2] = [uniqueKey("ttl"), uniqueKey("ttl")];

    await Promise.all([
      kv.set(k1, "v1", { ttl: 60 }),
      kv.set(k2, "v2", { ttl: 120 }),
    ]);

    expect(t.events).toHaveLength(1);
    expect(t.events[0]!.command).toBe("BATCH");

    const e1 = (await kv.get(k1)).data!;
    const e2 = (await kv.get(k2)).data!;
    expect(dec(e1.value)).toBe("v1");
    expect(e1.ttl).toBeDefined();
    expect(e1.ttl!).toBeGreaterThan(0);
    expect(e1.ttl!).toBeLessThanOrEqual(60);
    expect(dec(e2.value)).toBe("v2");
    expect(e2.ttl).toBeDefined();
    expect(e2.ttl!).toBeGreaterThan(0);
    expect(e2.ttl!).toBeLessThanOrEqual(120);
  });

  it("conditional set (ifAbsent) bypasses coalescing — errors are independent", async () => {
    const t = tracker();
    const kv = createKvClient({
      url: getHttpUrl(),
      namespace: uniqueNs(),
      onRequest: t.onRequest,
    });
    const [k1, k2] = [uniqueKey("cond"), uniqueKey("cond")];
    await kv.set(k1, "original");
    t.reset();

    // k1 already exists so ifAbsent will fail; k2 is a concurrent plain get.
    // The conditional set bypasses coalescing — each op is independent.
    const [setResult, getResult] = await Promise.all([
      kv.set(k1, "new", { ifAbsent: true }),
      kv.get(k2),
    ]);

    expect(setResult.error).toBeDefined();
    expect(setResult.error!.status).toBe(409);
    // The get is unaffected by the set's failure
    expect(getResult.error).toBeUndefined();
    expect(getResult.data).toBeNull();
    // Original value untouched
    expect(dec((await kv.get(k1)).data!.value)).toBe("original");
  });

  it("large concurrent fan-out all resolve correctly", async () => {
    const kv = createKvClient({ url: getHttpUrl(), namespace: uniqueNs() });
    const keys = Array.from({ length: 20 }, () => uniqueKey("fan"));
    await Promise.all(keys.map((k, i) => kv.set(k, `v${i}`)));

    const results = await Promise.all(keys.map((k) => kv.get(k)));
    results.forEach((r, i) => {
      expect(r.error).toBeUndefined();
      expect(dec(r.data!.value)).toBe(`v${i}`);
    });
  });
});

// ── RESP backend ──────────────────────────────────────────────────────────────

describe("RESP backend — coalescing", () => {
  // Use a single client shared across RESP tests; close at the end.
  // Each test gets unique keys so there's no cross-test state.
  let kv: KvClient | undefined;

  afterEach(async () => {
    await kv?.close();
    kv = undefined;
  });

  it("concurrent gets collapse into one BATCH", async () => {
    const t = tracker();
    kv = createKvClient({ url: getRespUrl(), db: 2, onRequest: t.onRequest });
    const [k1, k2, k3] = [uniqueKey("c"), uniqueKey("c"), uniqueKey("c")];
    await kv.set(k1, "alpha");
    await kv.set(k2, "beta");
    t.reset();

    const [r1, r2, r3] = await Promise.all([
      kv.get(k1),
      kv.get(k2),
      kv.get(k3),
    ]);

    expect(t.events).toHaveLength(1);
    expect(t.events[0]!.command).toBe("BATCH");
    expect(t.events[0]!.keyCount).toBe(3);
    expect(dec(r1.data!.value)).toBe("alpha");
    expect(dec(r2.data!.value)).toBe("beta");
    expect(r3.data).toBeNull();
  });

  it("concurrent sets collapse into one BATCH", async () => {
    const t = tracker();
    kv = createKvClient({ url: getRespUrl(), db: 2, onRequest: t.onRequest });
    const [k1, k2] = [uniqueKey("c"), uniqueKey("c")];

    const [r1, r2] = await Promise.all([
      kv.set(k1, "v1"),
      kv.set(k2, "v2"),
    ]);

    expect(t.events).toHaveLength(1);
    expect(t.events[0]!.command).toBe("BATCH");
    expect(t.events[0]!.keyCount).toBe(2);
    expect(r1.error).toBeUndefined();
    expect(r2.error).toBeUndefined();
    expect(dec((await kv.get(k1)).data!.value)).toBe("v1");
    expect(dec((await kv.get(k2)).data!.value)).toBe("v2");
  });

  it("mixed gets and sets collapse into one BATCH", async () => {
    const t = tracker();
    kv = createKvClient({ url: getRespUrl(), db: 2, onRequest: t.onRequest });
    const [k1, k2, k3] = [uniqueKey("c"), uniqueKey("c"), uniqueKey("c")];
    await kv.set(k1, "existing");
    t.reset();

    const [getResult, setResult, getMissResult] = await Promise.all([
      kv.get(k1),
      kv.set(k2, "new"),
      kv.get(k3),
    ]);

    expect(t.events).toHaveLength(1);
    expect(t.events[0]!.command).toBe("BATCH");
    expect(t.events[0]!.keyCount).toBe(3);
    expect(dec(getResult.data!.value)).toBe("existing");
    expect(setResult.error).toBeUndefined();
    expect(getMissResult.data).toBeNull();
    expect(dec((await kv.get(k2)).data!.value)).toBe("new");
  });

  it("duplicate gets for the same key deduplicate to one round-trip", async () => {
    const t = tracker();
    kv = createKvClient({ url: getRespUrl(), db: 2, onRequest: t.onRequest });
    const key = uniqueKey("dup");
    await kv.set(key, "shared");
    t.reset();

    const [r1, r2, r3] = await Promise.all([
      kv.get(key),
      kv.get(key),
      kv.get(key),
    ]);

    // Deduplicated to a single GET (3 waiters share 1 request — no batch needed for 1 unique key)
    expect(t.events).toHaveLength(1);
    expect(t.events[0]!.command).toBe("GET");
    expect(t.events[0]!.keyCount).toBe(1);
    expect(dec(r1.data!.value)).toBe("shared");
    expect(dec(r2.data!.value)).toBe("shared");
    expect(dec(r3.data!.value)).toBe("shared");
  });

  it("gets in different ticks do NOT coalesce", async () => {
    const t = tracker();
    kv = createKvClient({ url: getRespUrl(), db: 2, onRequest: t.onRequest });
    const [k1, k2] = [uniqueKey("c"), uniqueKey("c")];
    await kv.set(k1, "a");
    await kv.set(k2, "b");
    t.reset();

    await kv.get(k1);
    await kv.get(k2);

    // Two separate ticks → two separate GET commands (not batched)
    expect(t.events).toHaveLength(2);
    expect(t.events.every((e) => e.command === "GET")).toBe(true);
  });

  it("set with TTL coalesces and preserves expiry", async () => {
    const t = tracker();
    kv = createKvClient({ url: getRespUrl(), db: 2, onRequest: t.onRequest });
    const [k1, k2] = [uniqueKey("ttl"), uniqueKey("ttl")];

    await Promise.all([
      kv.set(k1, "v1", { ttl: 60 }),
      kv.set(k2, "v2", { ttl: 120 }),
    ]);

    expect(t.events).toHaveLength(1);
    expect(t.events[0]!.command).toBe("BATCH");

    const e1 = (await kv.get(k1)).data!;
    const e2 = (await kv.get(k2)).data!;
    expect(dec(e1.value)).toBe("v1");
    expect(e1.ttl).toBeDefined();
    expect(e1.ttl!).toBeGreaterThan(0);
    expect(e1.ttl!).toBeLessThanOrEqual(60);
    expect(dec(e2.value)).toBe("v2");
    expect(e2.ttl).toBeDefined();
    expect(e2.ttl!).toBeGreaterThan(0);
    expect(e2.ttl!).toBeLessThanOrEqual(120);
  });

  it("large concurrent fan-out all resolve correctly", async () => {
    kv = createKvClient({ url: getRespUrl(), db: 2 });
    const keys = Array.from({ length: 20 }, () => uniqueKey("fan"));
    await Promise.all(keys.map((k, i) => kv!.set(k, `v${i}`)));

    const results = await Promise.all(keys.map((k) => kv!.get(k)));
    results.forEach((r, i) => {
      expect(r.error).toBeUndefined();
      expect(dec(r.data!.value)).toBe(`v${i}`);
    });
  });
});

// ── Schema client ─────────────────────────────────────────────────────────────
//
// Schema wrapper sits on top of the coalescing client — verifies that the extra
// layer of parse/serialize doesn't break coalescing.

const userSchema = {
  parse(input: unknown): { name: string } {
    if (typeof input === "object" && input !== null && "name" in input) {
      return input as { name: string };
    }
    throw new Error("invalid user");
  },
};

describe("schema client — coalescing", () => {
  it("concurrent gets through schema client coalesce into one BATCH", async () => {
    const t = tracker();
    const kv = createKvClient({
      url: getHttpUrl(),
      namespace: uniqueNs(),
      schema: { "u:*": userSchema },
      onRequest: t.onRequest,
    });
    await kv.set("u:alice", { name: "Alice" });
    await kv.set("u:bob", { name: "Bob" });
    t.reset();

    const [r1, r2, r3] = await Promise.all([
      kv.get("u:alice"),
      kv.get("u:bob"),
      kv.get("u:missing" as "u:alice"),
    ]);

    expect(t.events).toHaveLength(1);
    expect(t.events[0]!.command).toBe("BATCH");
    expect(t.events[0]!.keyCount).toBe(3);
    expect(r1.data).toEqual({ name: "Alice" });
    expect(r2.data).toEqual({ name: "Bob" });
    expect(r3.data).toBeNull();
  });

  it("concurrent sets through schema client coalesce and serialize correctly", async () => {
    const t = tracker();
    const kv = createKvClient({
      url: getHttpUrl(),
      namespace: uniqueNs(),
      schema: { "u:*": userSchema },
      onRequest: t.onRequest,
    });
    const [k1, k2] = ["u:alice" as const, "u:bob" as const];

    await Promise.all([
      kv.set(k1, { name: "Alice" }),
      kv.set(k2, { name: "Bob" }),
    ]);

    expect(t.events).toHaveLength(1);
    expect(t.events[0]!.command).toBe("BATCH");
    expect(kv.get(k1).then((r) => r.data)).resolves.toEqual({ name: "Alice" });
    expect(kv.get(k2).then((r) => r.data)).resolves.toEqual({ name: "Bob" });
  });

  it("mixed schema + unmatched keys coalesce — unmatched returns raw Entry", async () => {
    const t = tracker();
    const kv = createKvClient({
      url: getHttpUrl(),
      namespace: uniqueNs(),
      schema: { "u:*": userSchema },
      onRequest: t.onRequest,
    });
    const rawKey = uniqueKey("raw");
    await kv.set("u:carol" as "u:alice", { name: "Carol" });
    await kv.set(rawKey, "plaintext");
    t.reset();

    const [typed, raw] = await Promise.all([
      kv.get("u:carol" as "u:alice"),
      kv.get(rawKey),
    ]);

    expect(t.events).toHaveLength(1);
    expect(t.events[0]!.command).toBe("BATCH");
    expect(typed.data).toEqual({ name: "Carol" });
    // Unmatched key — raw Entry (has .text())
    expect(dec((raw.data as any).value)).toBe("plaintext");
  });
});

// ── batchGet / batchSet do not coalesce with individual get / set ─────────────
//
// batchGet and batchSet bypass the coalescing queue (they call the underlying
// transport directly). This verifies they still return correct results when
// called concurrently with coalesced ops.

describe("HTTP backend — batchGet/batchSet fire independently", () => {
  it("batchGet concurrent with get fires as two separate commands", async () => {
    const t = tracker();
    const kv = createKvClient({
      url: getHttpUrl(),
      namespace: uniqueNs(),
      onRequest: t.onRequest,
    });
    const [k1, k2, k3] = [uniqueKey("bg"), uniqueKey("bg"), uniqueKey("bg")];
    await kv.set(k1, "v1");
    await kv.set(k2, "v2");
    await kv.set(k3, "v3");
    t.reset();

    const [batchResult, getResult] = await Promise.all([
      kv.batchGet([k1, k2]),
      kv.get(k3),
    ]);

    // batchGet fires immediately through the underlying client; get goes through coalescing.
    // Both complete but as independent commands (2 total).
    expect(t.events).toHaveLength(2);
    expect(batchResult.error).toBeUndefined();
    expect(dec(batchResult.data![0]!.value)).toBe("v1");
    expect(dec(batchResult.data![1]!.value)).toBe("v2");
    expect(getResult.error).toBeUndefined();
    expect(dec(getResult.data!.value)).toBe("v3");
  });

  it("batchSet concurrent with set both write correctly", async () => {
    const t = tracker();
    const kv = createKvClient({
      url: getHttpUrl(),
      namespace: uniqueNs(),
      onRequest: t.onRequest,
    });
    const [k1, k2, k3] = [uniqueKey("bs"), uniqueKey("bs"), uniqueKey("bs")];

    await Promise.all([
      kv.batchSet([{ key: k1, value: "v1" }, { key: k2, value: "v2" }]),
      kv.set(k3, "v3"),
    ]);

    expect(t.events).toHaveLength(2);
    expect(dec((await kv.get(k1)).data!.value)).toBe("v1");
    expect(dec((await kv.get(k2)).data!.value)).toBe("v2");
    expect(dec((await kv.get(k3)).data!.value)).toBe("v3");
  });
});

// ── Conditional writes never coalesce ─────────────────────────────────────────
//
// ifAbsent / ifPresent / ifMatch bypass coalescing so a conflict on one
// operation cannot poison unrelated concurrent ops.

describe("HTTP backend — conditional writes bypass coalescing", () => {
  it("ifAbsent, ifPresent, and ifMatch all fire as direct SETs", async () => {
    const t = tracker();
    const kv = createKvClient({
      url: getHttpUrl(),
      namespace: uniqueNs(),
      onRequest: t.onRequest,
    });
    const [k1, k2, k3] = [uniqueKey("cw"), uniqueKey("cw"), uniqueKey("cw")];
    await kv.set(k2, "existing");
    const { data: entry } = await kv.get(k2);
    t.reset();

    await Promise.all([
      kv.set(k1, "v", { ifAbsent: true }),
      kv.set(k2, "v", { ifPresent: true }),
      kv.set(k3, "v", { ifMatch: entry!.revision }),
    ]);

    // Three conditional sets → three separate SET commands (not one BATCH)
    expect(t.events).toHaveLength(3);
    expect(t.events.every((e) => e.command === "SET")).toBe(true);
  });

  it("ifAbsent conflict does not affect a concurrent plain get", async () => {
    const t = tracker();
    const kv = createKvClient({
      url: getHttpUrl(),
      namespace: uniqueNs(),
      onRequest: t.onRequest,
    });
    const [existing, other] = [uniqueKey("cw"), uniqueKey("cw")];
    await kv.set(existing, "original");
    await kv.set(other, "untouched");
    t.reset();

    const [setResult, getResult] = await Promise.all([
      kv.set(existing, "new", { ifAbsent: true }), // 409 — key already exists
      kv.get(other), // plain get — independent
    ]);

    expect(setResult.error?.status).toBe(409);
    expect(getResult.error).toBeUndefined();
    expect(dec(getResult.data!.value)).toBe("untouched");
  });
});

// ── Batch error propagation ───────────────────────────────────────────────────
//
// When the underlying batch call fails, the same KvError is forwarded to every
// coalesced waiter. Use a dead port so the real HTTP client gets a connection
// error — no mocks needed.

describe("HTTP backend — batch error propagation", () => {
  // Port 1 is reserved and always connection-refused on any OS.
  const deadKv = () =>
    createKvClient({
      url: "http://127.0.0.1:1",
      namespace: uniqueNs(),
      retries: 0,
    });

  it("connection error on batch propagates to all coalesced gets", async () => {
    const kv = deadKv();
    const [k1, k2, k3] = [uniqueKey("e"), uniqueKey("e"), uniqueKey("e")];

    const [r1, r2, r3] = await Promise.all([
      kv.get(k1),
      kv.get(k2),
      kv.get(k3),
    ]);

    expect(r1.error).toBeDefined();
    expect(r1.error).toBe(r2.error);
    expect(r2.error).toBe(r3.error);
  });

  it("connection error on batch propagates to mixed get+set callers", async () => {
    const kv = deadKv();
    const [k1, k2] = [uniqueKey("e"), uniqueKey("e")];

    const [getResult, setResult] = await Promise.all([
      kv.get(k1),
      kv.set(k2, "v"),
    ]);

    expect(getResult.error).toBeDefined();
    expect(setResult.error).toBeDefined();
    expect(getResult.error).toBe(setResult.error);
  });
});

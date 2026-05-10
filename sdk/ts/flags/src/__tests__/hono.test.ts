import type { KvClient } from "@beyond.dev/kv";
import { Hono } from "hono";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { createFlags, type FlagsClient } from "../flags.js";
import { flags as flagsMiddleware } from "../middleware/hono.js";
import { kvClient, sleep, writeDef } from "./harness.js";

describe("hono adapter — ALS scope through real request", () => {
  let kv: KvClient;
  let flags: FlagsClient;

  beforeEach(() => {
    kv = kvClient();
    // Use watch mode so writes after createFlags propagate quickly.
    flags = createFlags(kv, { watch: true, refresh: 30 });
  });

  afterEach(async () => {
    await flags.close();
  });

  it("zero-arg flag eval inside a route reads context from middleware", async () => {
    const f = flags("hono-zero-arg", false);
    await f.set({ id: "u_hono_1" }, true);

    const app = new Hono();
    app.use(
      "*",
      flagsMiddleware({
        context: (c) => ({ id: c.req.header("x-user-id") ?? "anon" }),
      }),
    );
    app.get("/", async (c) => c.json({ value: await f() }));

    const res1 = await app.request("/", {
      headers: { "x-user-id": "u_hono_1" },
    });
    expect(res1.status).toBe(200);
    expect(await res1.json()).toEqual({ value: true });

    const res2 = await app.request("/", {
      headers: { "x-user-id": "u_hono_2" },
    });
    expect(await res2.json()).toEqual({ value: false });
  });

  it("rules in KV evaluate against the per-request context", async () => {
    await writeDef(kv, "hono-rules", {
      on: true,
      rules: [{ when: { id: "u_match" }, value: true }],
    });
    const f = flags("hono-rules", false);

    const deadline = Date.now() + 5_000;
    while (Date.now() < deadline) {
      if ((await f({ id: "u_match" })) === true) break;
      await sleep(100);
    }

    const app = new Hono();
    app.use(
      "*",
      flagsMiddleware({
        context: (c) => ({ id: c.req.header("x-user-id") ?? "anon" }),
      }),
    );
    app.get("/", async (c) => c.json({ value: await f() }));

    const matched = await app.request("/", {
      headers: { "x-user-id": "u_match" },
    });
    expect(await matched.json()).toEqual({ value: true });

    const missed = await app.request("/", {
      headers: { "x-user-id": "u_other" },
    });
    expect(await missed.json()).toEqual({ value: false });
  });

  it("skip option bypasses the middleware (zero-arg eval throws)", async () => {
    const f = flags("hono-skip", false);

    const app = new Hono();
    app.use(
      "*",
      flagsMiddleware({
        context: (c) => ({ id: c.req.header("x-user-id") ?? "anon" }),
        skip: (c) => c.req.path === "/skip",
      }),
    );
    app.get("/skip", async (c) => {
      try {
        await f();
        return c.json({ ok: true });
      } catch (err) {
        return c.json({ error: (err as Error).message }, 500);
      }
    });
    app.get("/run", async (c) => c.json({ value: await f() }));

    const skipped = await app.request("/skip", {
      headers: { "x-user-id": "u_a" },
    });
    expect(skipped.status).toBe(500);

    const ran = await app.request("/run", {
      headers: { "x-user-id": "u_a" },
    });
    expect(await ran.json()).toEqual({ value: false });
  });
});

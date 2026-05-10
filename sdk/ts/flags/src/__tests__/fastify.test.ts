import type { KvClient } from "@beyond.dev/kv";
import Fastify from "fastify";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { createFlags, type FlagsClient } from "../flags.js";
import { flags as flagsPlugin } from "../middleware/fastify.js";
import { kvClient, sleep, writeDef } from "./harness.js";

describe("fastify adapter — ALS scope through real request", () => {
  let kv: KvClient;
  let flags: FlagsClient;

  beforeEach(() => {
    kv = kvClient();
    flags = createFlags(kv, { watch: true, refresh: 30 });
  });

  afterEach(async () => {
    await flags.close();
  });

  it("zero-arg flag eval inside a route reads context from plugin", async () => {
    const f = flags("fastify-zero-arg", false);
    await f.set({ id: "u_fastify_1" }, true);

    const app = Fastify({ logger: false });
    await app.register(flagsPlugin, {
      context: (req) => ({
        id: (req.headers["x-user-id"] as string) ?? "anon",
      }),
    });
    app.get("/", async () => ({ value: await f() }));

    const r1 = await app.inject({
      method: "GET",
      url: "/",
      headers: { "x-user-id": "u_fastify_1" },
    });
    expect(JSON.parse(r1.body)).toEqual({ value: true });

    const r2 = await app.inject({
      method: "GET",
      url: "/",
      headers: { "x-user-id": "u_fastify_2" },
    });
    expect(JSON.parse(r2.body)).toEqual({ value: false });

    await app.close();
  });

  it("rules in KV evaluate against the per-request context", async () => {
    await writeDef(kv, "fastify-rules", {
      on: true,
      rules: [{ when: { id: "u_fastify_match" }, value: true }],
    });
    const f = flags("fastify-rules", false);

    const deadline = Date.now() + 5_000;
    while (Date.now() < deadline) {
      if ((await f({ id: "u_fastify_match" })) === true) break;
      await sleep(100);
    }

    const app = Fastify({ logger: false });
    await app.register(flagsPlugin, {
      context: (req) => ({
        id: (req.headers["x-user-id"] as string) ?? "anon",
      }),
    });
    app.get("/", async () => ({ value: await f() }));

    const matched = await app.inject({
      method: "GET",
      url: "/",
      headers: { "x-user-id": "u_fastify_match" },
    });
    expect(JSON.parse(matched.body)).toEqual({ value: true });

    const missed = await app.inject({
      method: "GET",
      url: "/",
      headers: { "x-user-id": "u_fastify_other" },
    });
    expect(JSON.parse(missed.body)).toEqual({ value: false });

    await app.close();
  });

  it("skip option bypasses the plugin (zero-arg eval throws)", async () => {
    const f = flags("fastify-skip", false);

    const app = Fastify({ logger: false });
    await app.register(flagsPlugin, {
      context: (req) => ({
        id: (req.headers["x-user-id"] as string) ?? "anon",
      }),
      skip: (req) => req.url === "/skip",
    });
    // No scope on /skip — zero-arg eval throws no_context → Fastify returns 500.
    app.get("/skip", async () => {
      await f();
    });
    app.get("/run", async () => ({ value: await f() }));

    const skipped = await app.inject({
      method: "GET",
      url: "/skip",
      headers: { "x-user-id": "u_a" },
    });
    expect(skipped.statusCode).toBe(500);

    const ran = await app.inject({
      method: "GET",
      url: "/run",
      headers: { "x-user-id": "u_a" },
    });
    expect(JSON.parse(ran.body)).toEqual({ value: false });

    await app.close();
  });
});

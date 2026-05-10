import type { KvClient } from "@beyond.dev/kv";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

// Mock next/server before any imports that depend on it. Vitest hoists vi.mock
// calls so this takes effect before the middleware module loads.
vi.mock("next/server", () => ({
  NextResponse: {
    next: () =>
      new Response(null, {
        status: 200,
        headers: { "x-middleware-next": "1" },
      }),
    redirect: (url: URL) => Response.redirect(url.href, 302),
  },
}));

import { createFlags, type FlagsClient } from "../flags.js";
import { flags as nextMiddleware } from "../middleware/next-middleware.js";
import { withFlags } from "../middleware/next.js";
import { kvClient } from "./harness.js";

describe("next — withFlags helper", () => {
  let kv: KvClient;
  let flags: FlagsClient;

  beforeEach(() => {
    kv = kvClient();
    flags = createFlags(kv, { watch: false, refresh: 30 });
  });

  afterEach(async () => {
    await flags.close();
  });

  it("zero-arg eval inside withFlags reads context from wrapper", async () => {
    const f = flags("next-wf-1", false);
    await f.set({ id: "u_next_1" }, true);

    const result = await withFlags({ id: "u_next_1" }, () => f());
    expect(result).toBe(true);
  });

  it("multiple flags inside withFlags share the same scope (one pref fetch)", async () => {
    const a = flags("next-wf-a", false);
    const b = flags("next-wf-b", "off" as string);
    await a.set({ id: "u_next_multi" }, true);
    await b.set({ id: "u_next_multi" }, "v2");

    const [ra, rb] = await withFlags(
      { id: "u_next_multi" },
      async () => Promise.all([a(), b()]),
    );
    expect(ra).toBe(true);
    expect(rb).toBe("v2");
  });

  it("zero-arg eval outside withFlags throws no_context", async () => {
    const f = flags("next-wf-noscope", false);
    await expect(f()).rejects.toThrow(/no context/i);
  });
});

describe("next/middleware — edge middleware factory", () => {
  let kv: KvClient;
  let flags: FlagsClient;

  beforeEach(() => {
    kv = kvClient();
    flags = createFlags(kv, { watch: false, refresh: 30 });
  });

  afterEach(async () => {
    await flags.close();
  });

  it("establishes scope and calls handler with the resolved context", async () => {
    const f = flags("next-mw-handler", false);
    await f.set({ id: "u_next_mw" }, true);

    const middleware = nextMiddleware({
      context: (req) => ({ id: req.headers.get("x-user-id") ?? "anon" }),
      handler: async () => {
        const value = await f();
        return new Response(JSON.stringify({ value }), {
          status: 200,
          headers: { "content-type": "application/json" },
        });
      },
    });

    const req = new Request("https://example.com/", {
      headers: { "x-user-id": "u_next_mw" },
    });
    const res = await middleware(req as never, {} as never);
    expect(res?.status).toBe(200);
    expect(await res?.json()).toEqual({ value: true });
  });

  it("without a handler returns NextResponse.next()", async () => {
    const middleware = nextMiddleware({
      context: () => ({ id: "u_1" }),
    });

    const req = new Request("https://example.com/");
    const res = await middleware(req as never, {} as never);
    expect(res?.status).toBe(200);
    expect(res?.headers.get("x-middleware-next")).toBe("1");
  });
});

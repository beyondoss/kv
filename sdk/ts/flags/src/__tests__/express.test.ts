import type { KvClient } from "@beyond.dev/kv";
import type { NextFunction, Request, Response } from "express";
import express from "express";
import type { AddressInfo } from "node:net";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { createFlags, type FlagsClient } from "../flags.js";
import { flags as flagsMiddleware } from "../middleware/express.js";
import { kvClient } from "./harness.js";

describe("express adapter — ALS scope through real request", () => {
  let kv: KvClient;
  let flags: FlagsClient;

  beforeEach(() => {
    kv = kvClient();
    flags = createFlags(kv, { watch: false, refresh: 30 });
  });

  afterEach(async () => {
    await flags.close();
  });

  async function withApp(
    app: express.Application,
    fn: (base: string) => Promise<void>,
  ): Promise<void> {
    const server = await new Promise<ReturnType<typeof app.listen>>((res) => {
      const s = app.listen(0, () => res(s));
    });
    const { port } = server.address() as AddressInfo;
    try {
      await fn(`http://127.0.0.1:${port}`);
    } finally {
      server.closeAllConnections?.();
      await new Promise<void>((res) => server.close(() => res()));
    }
  }

  it("zero-arg flag eval inside a route reads context from middleware", async () => {
    const f = flags("express-zero-arg", false);
    await f.set({ id: "u_express_1" }, true);

    const app = express();
    app.use(
      flagsMiddleware({
        context: (req) => ({ id: req.header("x-user-id") ?? "anon" }),
      }),
    );
    app.get("/", async (_req: Request, res: Response) => {
      res.json({ value: await f() });
    });

    await withApp(app, async (base) => {
      const r1 = await fetch(`${base}/`, {
        headers: { "x-user-id": "u_express_1" },
      });
      expect(await r1.json()).toEqual({ value: true });

      const r2 = await fetch(`${base}/`, {
        headers: { "x-user-id": "u_express_2" },
      });
      expect(await r2.json()).toEqual({ value: false });
    });
  });

  it("skip option bypasses the middleware (zero-arg eval throws)", async () => {
    const f = flags("express-skip", false);

    const app = express();
    app.use(
      flagsMiddleware({
        context: (req) => ({ id: req.header("x-user-id") ?? "anon" }),
        skip: (req) => req.path === "/skip",
      }),
    );
    app.get(
      "/skip",
      async (_req: Request, res: Response, next: NextFunction) => {
        try {
          await f();
          res.json({ ok: true });
        } catch (err) {
          next(err);
        }
      },
    );
    app.get("/run", async (_req: Request, res: Response) => {
      res.json({ value: await f() });
    });
    // Error handler must come after routes in Express.
    app.use(
      (_err: Error, _req: Request, res: Response, _next: NextFunction) => {
        res.status(500).json({ error: "caught" });
      },
    );

    await withApp(app, async (base) => {
      const skipped = await fetch(`${base}/skip`, {
        headers: { "x-user-id": "u_a" },
      });
      expect(skipped.status).toBe(500);

      const ran = await fetch(`${base}/run`, {
        headers: { "x-user-id": "u_a" },
      });
      expect(await ran.json()).toEqual({ value: false });
    });
  });
});

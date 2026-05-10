import type { NextFunction, Request, RequestHandler, Response } from "express";
import { runWithScope } from "../als.js";
import type { FlagContext } from "../types.js";

export interface FlagsMiddlewareOptions {
  /** Build the per-request flag context. Required. */
  context: (req: Request) => FlagContext | Promise<FlagContext>;
  /** Skip the middleware for matching requests. */
  skip?: (req: Request) => boolean | Promise<boolean>;
}

/**
 * Express middleware that establishes the per-request flag context. After
 * installation, `await flag()` (zero-arg) reads from this context anywhere
 * downstream.
 *
 * @example
 * ```ts
 * import express from 'express'
 * import { flags } from '@beyond.dev/flags/express'
 *
 * const app = express()
 * app.use(flags({
 *   context: (req) => ({
 *     id: req.header('x-user-id') ?? 'anon',
 *     plan: req.header('x-plan') ?? 'free',
 *   }),
 * }))
 * ```
 */
export function flags(opts: FlagsMiddlewareOptions): RequestHandler {
  const { context, skip } = opts;
  return async (req: Request, _res: Response, next: NextFunction) => {
    try {
      if (skip && (await skip(req))) {
        next();
        return;
      }
      const ctx = await context(req);
      // Express dispatches the rest of the chain synchronously inside `next()`,
      // so any async work it kicks off inherits the AsyncLocalStorage scope.
      await runWithScope(ctx, () => {
        next();
      });
    } catch (err) {
      next(err);
    }
  };
}

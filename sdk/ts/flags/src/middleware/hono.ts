import type { Context, MiddlewareHandler } from "hono";
import { runWithScope } from "../als.js";
import type { FlagContext } from "../types.js";

export interface FlagsMiddlewareOptions {
  /** Build the per-request flag context. Required. */
  context: (c: Context) => FlagContext | Promise<FlagContext>;
  /** Skip the middleware for matching requests. */
  skip?: (c: Context) => boolean | Promise<boolean>;
}

/**
 * Hono middleware that establishes the per-request flag context. After
 * installation, `await flag()` (zero-arg) reads from this context anywhere
 * downstream.
 *
 * @example
 * ```ts
 * import { Hono } from 'hono'
 * import { flags } from '@beyond.dev/flags/hono'
 *
 * const app = new Hono()
 * app.use('*', flags({
 *   context: (c) => ({
 *     id: c.req.header('x-user-id') ?? 'anon',
 *     plan: c.req.header('x-plan') ?? 'free',
 *   }),
 * }))
 * ```
 */
export function flags(opts: FlagsMiddlewareOptions): MiddlewareHandler {
  const { context, skip } = opts;
  return async (c, next) => {
    if (skip && (await skip(c))) return next();
    const ctx = await context(c);
    await runWithScope(ctx, () => next());
  };
}

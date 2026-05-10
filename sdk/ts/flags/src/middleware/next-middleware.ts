import type { NextMiddleware, NextRequest } from "next/server";
import { NextResponse } from "next/server";
import { runWithScope } from "../als.js";
import type { FlagContext } from "../types.js";

export interface FlagsMiddlewareOptions {
  /** Build the per-request flag context. Required. */
  context: (req: NextRequest) => FlagContext | Promise<FlagContext>;
  /**
   * Custom middleware body. Receives the active scope; return a `NextResponse`.
   * If omitted, the middleware just establishes the scope and returns
   * `NextResponse.next()` so a downstream handler can read flags via
   * per-call eval (`await flag(ctx)`).
   */
  handler?: (req: NextRequest) => Response | Promise<Response>;
}

/**
 * Edge middleware factory for Next.js App Router. Establishes a flags scope
 * for the duration of the middleware handler so flag evals inside `handler`
 * can use the zero-arg form. Note: this scope does **not** propagate into
 * route handlers — Next dispatches them in a separate context. For route
 * handlers, use per-call `await flag(ctx)`.
 *
 * @example
 * ```ts
 * // middleware.ts
 * import { flags } from '@beyond.dev/flags/next/middleware'
 * import { newCheckout } from '@/flags'
 *
 * export default flags({
 *   context: (req) => ({
 *     id: req.headers.get('x-user-id') ?? 'anon',
 *     plan: 'free',
 *   }),
 *   handler: async (req) => {
 *     if (await newCheckout()) {
 *       return NextResponse.redirect(new URL('/checkout-v2', req.url))
 *     }
 *     return NextResponse.next()
 *   },
 * })
 * ```
 */
export function flags(opts: FlagsMiddlewareOptions): NextMiddleware {
  const { context, handler } = opts;
  return async (req) => {
    const ctx = await context(req);
    return runWithScope<Response>(ctx, async () => {
      if (handler) return handler(req);
      return NextResponse.next();
    });
  };
}

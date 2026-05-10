import { runWithScope } from "../als.js";
import type { FlagContext } from "../types.js";

/**
 * RSC helper: run `body` with `context` as the ambient flags scope so any
 * `await flag()` calls inside it (zero-arg) read this context.
 *
 * Most of the time you can just pass an explicit context to the flag itself
 * (`await newCheckout(ctx)`) — that's the simpler shape. Reach for this when
 * a single page or route handler evaluates many flags and you want to avoid
 * threading `ctx` through every call.
 *
 * @example
 * ```ts
 * // app/page.tsx (Server Component)
 * import { headers } from 'next/headers'
 * import { withFlags } from '@beyond.dev/flags/next'
 * import { newCheckout, aiSearch } from '@/flags'
 *
 * export default async function Page() {
 *   const h = await headers()
 *   return withFlags(
 *     {
 *       id: h.get('x-user-id') ?? 'anon',
 *       plan: 'free',
 *     },
 *     async () => {
 *       const showNew = await newCheckout()
 *       const search  = await aiSearch()
 *       return <View showNew={showNew} search={search} />
 *     },
 *   )
 * }
 * ```
 */
export function withFlags<T>(
  context: FlagContext,
  body: () => T | Promise<T>,
): Promise<T> {
  return runWithScope(context, body);
}

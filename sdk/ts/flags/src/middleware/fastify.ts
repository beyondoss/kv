import type {
  FastifyPluginCallback,
  FastifyReply,
  FastifyRequest,
  HookHandlerDoneFunction,
} from "fastify";
import fp from "fastify-plugin";
import { runWithScope } from "../als.js";
import type { FlagContext } from "../types.js";

export interface FlagsPluginOptions {
  /** Build the per-request flag context. Required. */
  context: (req: FastifyRequest) => FlagContext | Promise<FlagContext>;
  /** Skip the middleware for matching requests. */
  skip?: (req: FastifyRequest) => boolean | Promise<boolean>;
}

const plugin: FastifyPluginCallback<FlagsPluginOptions> = (
  fastify,
  opts,
  done,
) => {
  const { context, skip } = opts;

  // Use the callback form (done) so hookDone() fires inside storage.run(),
  // making the route handler an async descendant of that scope. The async hook
  // form resolves into Fastify's own awaiting context (a sibling, not a child),
  // which is why we don't use it. runWithScope (storage.run) is preferred over
  // enterWith because it's bounded to its callback's descendants — it doesn't
  // permanently modify the current async context, which prevents scope leakage
  // between requests (and between tests that share an async context).
  fastify.addHook(
    "onRequest",
    (
      req: FastifyRequest,
      _reply: FastifyReply,
      hookDone: HookHandlerDoneFunction,
    ) => {
      const run = async () => {
        if (skip && (await skip(req))) {
          hookDone();
          return;
        }
        const ctx = await context(req);
        runWithScope(ctx, () => hookDone());
      };
      run().catch(hookDone);
    },
  );

  done();
};

/**
 * Fastify plugin that establishes the per-request flag context. After
 * registration, `await flag()` (zero-arg) reads from this context anywhere
 * inside the route handler.
 *
 * @example
 * ```ts
 * import Fastify from 'fastify'
 * import { flags } from '@beyond.dev/flags/fastify'
 *
 * const app = Fastify()
 * await app.register(flags, {
 *   context: (req) => ({
 *     id: (req.headers['x-user-id'] as string) ?? 'anon',
 *     plan: (req.headers['x-plan'] as string) ?? 'free',
 *   }),
 * })
 * ```
 */
export const flags = fp(plugin, {
  name: "@beyond.dev/flags",
  fastify: ">=4",
});

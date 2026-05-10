import { createKvClient, type KvClient } from "./client.js";

let _kv: KvClient | undefined;

/**
 * Default KV client, lazily initialized from environment variables.
 *
 * - `BEYOND_KV_URL` *(required)* — server URL. The scheme determines the backend:
 *   - `redis://` or `rediss://` → RESP (recommended for server-side use)
 *   - `http://` or `https://` → HTTP (works in edge/browser runtimes)
 * - `BEYOND_KV_DB` — RESP database number (0–15, default `0`)
 * - `BEYOND_KV_NAMESPACE` — HTTP namespace name (default `"default"`)
 *
 * The client is created on the first method call and reused for the lifetime
 * of the process. For custom configuration or schema-typed access use
 * {@link createKvClient} directly.
 *
 * @example
 * ```ts
 * import { kv } from '@beyond.dev/kv'
 *
 * await kv.set('hits', '0')
 * const { data } = await kv.get('hits')
 * console.log(data?.text()) // "0"
 *
 * await kv.incr('hits')    // atomic increment → 1
 * await kv.delete('hits')
 * ```
 */
export const kv: KvClient = new Proxy({} as KvClient, {
  get(_, prop) {
    _kv ??= createKvClient();
    return (_kv as unknown as Record<string | symbol, unknown>)[prop];
  },
});

export {
  type components,
  createClient,
  createKvClient,
  type KvClient,
  type KvClientOptions,
  type KvHttpClient,
  type KvHttpClientOptions,
  type KvHttpResult,
  type KvRawClientOptions,
  type KvRequestEvent,
  type KvRespClientOptions,
  type KvResponseEvent,
  type KvResult,
  type KvSchema,
  type KvSchemaClient,
  type KvSchemaMap,
  type KvSchemaType,
  type operations,
  type paths,
  type SchemaAwareBatchResults,
  type SchemaAwareWatchEvent,
} from "./client.js";
export { KvError } from "./errors.js";
export { createHttpKvClient } from "./http.js";
export type {
  BatchOp,
  BatchResults,
  BatchSetOpts,
  CasOptions,
  DeleteOptions,
  Entry,
  ExpiryOptions,
  GetAndSetOptions,
  ListKey,
  ListOptions,
  ListResult,
  Lock,
  LockOptions,
  MSetEntry,
  SetOptions,
  WatchEvent,
  WatchOptions,
} from "./kv-types.js";
export { createRespKvClient } from "./resp.js";

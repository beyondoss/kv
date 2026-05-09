import { createKvClient, type KvClient } from "./client.js";

let _kv: KvClient | undefined;

/**
 * Default KV client configured from environment variables.
 * Reads `BEYOND_KV_URL` (required), `BEYOND_KV_DB` (RESP), and `BEYOND_KV_NAMESPACE` (HTTP).
 * Initialized lazily on first method call.
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
  MSetEntry,
  SetOptions,
  WatchEvent,
  WatchOptions,
} from "./kv-types.js";
export { createRespKvClient } from "./resp.js";

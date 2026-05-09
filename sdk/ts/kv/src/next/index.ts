import {
  createKvClient,
  type KvClient,
  type KvSchemaClient,
  type KvSchemaMap,
} from "../client.js";

/**
 * Creates a KV client configured from environment variables. Intended for
 * Next.js Server Components, Route Handlers, and Server Actions.
 *
 * Required env var:
 * - `BEYOND_KV_URL` — server URL. Scheme selects the backend automatically:
 *   - `redis://localhost:6379` → RESP (recommended)
 *   - `http://localhost:4869` → HTTP
 *
 * Optional env vars:
 * - `BEYOND_KV_DB` — database number for RESP (0–15, default 0)
 * - `BEYOND_KV_NAMESPACE` — namespace name for HTTP (default `"default"`)
 *
 * @example
 * ```ts
 * // app/api/route.ts
 * import { createServerKvClient } from "@beyond.dev/kv/next"
 *
 * export async function GET() {
 *   const kv = createServerKvClient()
 *   const entry = await kv.get("my-key")
 *   return Response.json({ value: entry ? new TextDecoder().decode(entry.value) : null })
 * }
 * ```
 */
export function createServerKvClient(): KvClient;
export function createServerKvClient<Map extends KvSchemaMap>(opts: {
  schema: Map;
  ttl?: number;
}): KvSchemaClient<Map>;
export function createServerKvClient<Map extends KvSchemaMap>(opts?: {
  schema?: Map;
  ttl?: number;
}): KvClient | KvSchemaClient<Map> {
  return createKvClient(
    opts?.schema != null
      ? {
        schema: opts.schema,
        ...(opts.ttl != null && { ttl: opts.ttl }),
      } as Parameters<typeof createKvClient<Map>>[0]
      : undefined,
  );
}

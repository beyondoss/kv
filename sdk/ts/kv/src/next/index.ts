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
 * - `KV_URL` — server URL. Scheme selects the backend automatically:
 *   - `redis://localhost:6379` → RESP (recommended)
 *   - `http://localhost:4869` → HTTP
 *
 * Optional env vars:
 * - `KV_DB` — database number for RESP (0–15, default 0)
 * - `KV_NAMESPACE` — namespace name for HTTP (default `"default"`)
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
  const url = process.env["KV_URL"];
  if (!url) {
    throw new Error(
      "KV_URL environment variable is not set. "
        + "Set it to your beyond-kv endpoint, e.g. redis://localhost:6379",
    );
  }

  const dbStr = process.env["KV_DB"];
  const namespace = process.env["KV_NAMESPACE"];

  return createKvClient(
    {
      url,
      ...(dbStr != null && { db: Number(dbStr) }),
      ...(namespace != null && { namespace }),
      ...(opts?.schema != null && { schema: opts.schema }),
      ...(opts?.ttl != null && { ttl: opts.ttl }),
    } as Parameters<typeof createKvClient>[0],
  );
}

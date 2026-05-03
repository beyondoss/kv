import { createKvClient, type KvClient } from "../client.js";

/**
 * Creates a KV client configured from the `KV_URL` and `KV_NAMESPACE`
 * environment variables. Intended for Next.js Server Components, Route
 * Handlers, and Server Actions where env vars are available at runtime.
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
export function createServerKvClient(): KvClient {
  const baseUrl = process.env["KV_URL"];
  if (!baseUrl) {
    throw new Error(
      "KV_URL environment variable is not set. " +
        "Set it to your beyond-kv HTTP endpoint, e.g. http://localhost:4869",
    );
  }
  const namespace = process.env["KV_NAMESPACE"];
  return createKvClient({ baseUrl, ...(namespace != null && { namespace }) });
}

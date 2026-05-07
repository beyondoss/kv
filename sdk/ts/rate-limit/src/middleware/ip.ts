/** Minimal headers interface — satisfied by both Web `Headers` and any object with a `.get()` method. */
export interface HeadersLike {
  get(name: string): string | null | undefined;
}

/**
 * Extract the client IP from request headers.
 *
 * Priority:
 *   1. `x-beyond-ip`      — set by the Beyond edge proxy; always trustworthy
 *   2. `x-real-ip`        — set by the Beyond edge (nginx-style single value)
 *   3. `x-forwarded-for`  — first token; fallback for non-Beyond deployments
 *   4. `socketIp`         — raw TCP socket IP passed by the framework
 *   5. `"unknown"`        — final fallback
 */
export function extractIp(headers: HeadersLike, socketIp?: string): string {
  const xff = headers.get("x-forwarded-for");
  const firstXff = xff ? xff.split(",")[0]?.trim() : undefined;
  return (
    headers.get("x-beyond-ip")
      ?? headers.get("x-real-ip")
      ?? firstXff
      ?? socketIp
      ?? "unknown"
  );
}

/** Adapter: wrap a Node.js `IncomingHttpHeaders`-style object so `extractIp` can read it. */
export function nodeHeaders(
  headers: Record<string, string | string[] | undefined>,
): HeadersLike {
  return {
    get(name) {
      const val = headers[name.toLowerCase()];
      return Array.isArray(val) ? (val[0] ?? null) : (val ?? null);
    },
  };
}

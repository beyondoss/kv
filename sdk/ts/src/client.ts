import { KvError, KvNotFoundError } from "./errors.js";
import type {
  KvEntry,
  KvListOptions,
  KvListResult,
  KvSetOptions,
} from "./types.js";

export interface KvClientOptions {
  baseUrl: string;
  namespace?: string;
}

async function parseError(res: Response): Promise<KvError> {
  let code = "internal_error";
  let message = res.statusText;
  try {
    const body = (await res.json()) as { error?: string; message?: string };
    if (body.error) code = body.error;
    if (body.message) message = body.message;
  } catch {
    // ignore parse failure
  }
  return new KvError(code, message, res.status);
}

export function createKvClient(opts: KvClientOptions) {
  const base = opts.baseUrl.replace(/\/+$/, "");
  const ns = opts.namespace ?? "default";

  function valueUrl(key: string): string {
    return `${base}/namespaces/${ns}/values/${encodeURIComponent(key)}`;
  }

  function keysUrl(params?: KvListOptions): string {
    const url = new URL(`${base}/namespaces/${ns}/keys`);
    if (params?.prefix) url.searchParams.set("prefix", params.prefix);
    if (params?.cursor) url.searchParams.set("cursor", params.cursor);
    if (params?.limit != null)
      url.searchParams.set("limit", String(params.limit));
    return url.toString();
  }

  return {
    async get(key: string): Promise<KvEntry | null> {
      const res = await fetch(valueUrl(key));
      if (res.status === 404) return null;
      if (!res.ok) throw await parseError(res);

      const value = new Uint8Array(await res.arrayBuffer());
      const ttlHeader = res.headers.get("x-kv-ttl");
      const metaHeader = res.headers.get("x-kv-metadata");

      const entry: KvEntry = { value };
      if (ttlHeader != null) entry.ttl = Number(ttlHeader);
      if (metaHeader != null) {
        try {
          entry.metadata = JSON.parse(metaHeader) as unknown;
        } catch {
          // ignore malformed metadata
        }
      }
      return entry;
    },

    async getOrThrow(key: string): Promise<KvEntry> {
      const entry = await this.get(key);
      if (entry == null) throw new KvNotFoundError(key);
      return entry;
    },

    async set(
      key: string,
      value: string | Uint8Array,
      opts?: KvSetOptions,
    ): Promise<void> {
      const headers: Record<string, string> = {};
      if (opts?.ttl != null) headers["x-kv-ttl"] = String(opts.ttl);
      if (opts?.metadata != null)
        headers["x-kv-metadata"] = JSON.stringify(opts.metadata);

      const url = opts?.nx
        ? `${valueUrl(key)}?nx=1`
        : valueUrl(key);

      const body: BodyInit =
        typeof value === "string"
          ? value
          : new Blob([new Uint8Array(value)]);

      const res = await fetch(url, { method: "PUT", headers, body });
      if (!res.ok) throw await parseError(res);
    },

    async delete(key: string): Promise<void> {
      const res = await fetch(valueUrl(key), { method: "DELETE" });
      if (!res.ok) throw await parseError(res);
    },

    async list(opts?: KvListOptions): Promise<KvListResult> {
      const res = await fetch(keysUrl(opts));
      if (!res.ok) throw await parseError(res);
      return (await res.json()) as KvListResult;
    },
  };
}

export type KvClient = ReturnType<typeof createKvClient>;

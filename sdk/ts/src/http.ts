import type { KvClient, KvClientOptions } from "./client.js";
import { KvError, KvNotFoundError } from "./errors.js";
import type { KvEntry, KvListOptions, KvListResult, KvMSetEntry, KvSetOptions } from "./types.js";

export function createHttpKvClient(opts: KvClientOptions): KvClient {
  const base = opts.url.replace(/\/+$/, "");
  const ns = opts.namespace ?? "default";
  const retries = opts.retries ?? 2;
  const { timeout, onCommand, onResponse, onMetadataParseError } = opts;
  const fetchFn = opts.fetch ?? globalThis.fetch;

  function valueUrl(key: string): string {
    return `${base}/namespaces/${ns}/values/${encodeURIComponent(key)}`;
  }

  function keysUrl(params?: KvListOptions): string {
    const url = new URL(`${base}/namespaces/${ns}/keys`);
    if (params?.prefix) url.searchParams.set("prefix", params.prefix);
    if (params?.cursor) url.searchParams.set("cursor", params.cursor);
    if (params?.limit != null) url.searchParams.set("limit", String(params.limit));
    return url.toString();
  }

  async function request(command: string, keyCount: number, url: string, init: RequestInit): Promise<Response> {
    onCommand?.({ command, keyCount });
    const start = Date.now();

    for (let attempt = 0; attempt <= retries; attempt++) {
      if (attempt > 0) {
        await new Promise<void>((r) => setTimeout(r, 100 * 2 ** (attempt - 1)));
      }
      const signal = timeout != null ? AbortSignal.timeout(timeout) : undefined;
      let res: Response;
      try {
        res = await fetchFn(url, { ...init, ...(signal != null && { signal }) });
      } catch (err) {
        if (attempt >= retries) {
          onResponse?.({ command, keyCount, durationMs: Date.now() - start });
          throw err;
        }
        continue;
      }
      if (res.status >= 500 && attempt < retries) {
        await res.body?.cancel();
        continue;
      }
      onResponse?.({ command, keyCount, durationMs: Date.now() - start });
      return res;
    }
    throw new Error("unreachable");
  }

  async function parseError(res: Response): Promise<KvError> {
    let code = "internal_error";
    let message = res.statusText;
    try {
      const body = (await res.json()) as { error?: string; message?: string };
      if (body.error) code = body.error;
      if (body.message) message = body.message;
    } catch { /* ignore */ }
    return new KvError(code, message, res.status);
  }

  async function get(key: string): Promise<KvEntry | null> {
    const res = await request("GET", 1, valueUrl(key), {});
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
      } catch (err) {
        onMetadataParseError?.(key, metaHeader, err);
      }
    }
    return entry;
  }

  async function set(key: string, value: string | Uint8Array, setOpts?: KvSetOptions): Promise<void> {
    const headers: Record<string, string> = {};
    if (setOpts?.ttl != null) headers["x-kv-ttl"] = String(setOpts.ttl);
    if (setOpts?.metadata != null) headers["x-kv-metadata"] = JSON.stringify(setOpts.metadata);

    const url = setOpts?.nx
      ? `${valueUrl(key)}?nx=1`
      : setOpts?.xx
        ? `${valueUrl(key)}?xx=1`
        : valueUrl(key);

    const body: BodyInit =
      typeof value === "string" ? value : new Blob([new Uint8Array(value)]);

    const res = await request("SET", 1, url, { method: "PUT", headers, body });
    if (!res.ok) throw await parseError(res);
  }

  return {
    get,

    async getOrThrow(key: string): Promise<KvEntry> {
      const entry = await get(key);
      if (entry == null) throw new KvNotFoundError(key);
      return entry;
    },

    set,

    async delete(key: string): Promise<void> {
      const res = await request("DEL", 1, valueUrl(key), { method: "DELETE" });
      if (!res.ok) throw await parseError(res);
    },

    async list(listOpts?: KvListOptions): Promise<KvListResult> {
      const res = await request("SCAN", 1, keysUrl(listOpts), {});
      if (!res.ok) throw await parseError(res);
      return (await res.json()) as KvListResult;
    },

    async mget(keys: string[]): Promise<(KvEntry | null)[]> {
      if (keys.length === 0) return [];
      return Promise.all(keys.map(get));
    },

    async mset(entries: KvMSetEntry[]): Promise<void> {
      if (entries.length === 0) return;
      await Promise.all(entries.map(({ key, value, opts }) => set(key, value, opts)));
    },

    close(): Promise<void> {
      return Promise.resolve();
    },
  };
}

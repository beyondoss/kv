import type { KvClient, KvClientOptions } from "./client.js";
import { KvError, KvNotFoundError } from "./errors.js";
import type {
  KvEntry,
  KvListOptions,
  KvListResult,
  KvMSetEntry,
  KvSetOptions,
  KvWatchEvent,
  KvWatchOptions,
} from "./types.js";

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
    if (params?.limit != null) {
      url.searchParams.set("limit", String(params.limit));
    }
    return url.toString();
  }

  async function request(
    command: string,
    keyCount: number,
    url: string,
    init: RequestInit,
  ): Promise<Response> {
    onCommand?.({ command, keyCount });
    const start = Date.now();

    for (let attempt = 0; attempt <= retries; attempt++) {
      if (attempt > 0) {
        await new Promise<void>((r) => setTimeout(r, 100 * 2 ** (attempt - 1)));
      }
      const signal = timeout != null ? AbortSignal.timeout(timeout) : undefined;
      let res: Response;
      try {
        res = await fetchFn(url, {
          ...init,
          ...(signal != null && { signal }),
        });
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

  async function set(
    key: string,
    value: string | Uint8Array,
    setOpts?: KvSetOptions,
  ): Promise<void> {
    const headers: Record<string, string> = {};
    if (setOpts?.ttl != null) headers["x-kv-ttl"] = String(setOpts.ttl);
    if (setOpts?.metadata != null) {
      headers["x-kv-metadata"] = JSON.stringify(setOpts.metadata);
    }

    const url = setOpts?.nx
      ? `${valueUrl(key)}?nx=1`
      : setOpts?.xx
      ? `${valueUrl(key)}?xx=1`
      : valueUrl(key);

    const body: BodyInit = typeof value === "string"
      ? value
      : new Blob([new Uint8Array(value)]);

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
      await Promise.all(
        entries.map(({ key, value, opts }) => set(key, value, opts)),
      );
    },

    close(): Promise<void> {
      return Promise.resolve();
    },

    watch(
      key: string,
      watchOpts?: KvWatchOptions,
    ): AsyncGenerator<KvWatchEvent> {
      return watchSse(base, ns, key, watchOpts, fetchFn);
    },
  };
}

async function* watchSse(
  base: string,
  ns: string,
  key: string,
  opts: KvWatchOptions | undefined,
  fetchFn: typeof globalThis.fetch,
): AsyncGenerator<KvWatchEvent> {
  let lastRevision = opts?.since ?? 0;
  const signal = opts?.signal;

  while (true) {
    if (signal?.aborted) return;

    const url = new URL(
      opts?.prefix
        ? `${base}/namespaces/${ns}/watch`
        : `${base}/namespaces/${ns}/watch/${encodeURIComponent(key)}`,
    );
    if (opts?.prefix) url.searchParams.set("prefix", key);
    if (lastRevision > 0) url.searchParams.set("since", String(lastRevision));

    let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
    try {
      const init: RequestInit = { headers: { Accept: "text/event-stream" } };
      if (signal != null) init.signal = signal;
      const res = await fetchFn(url.toString(), init);

      if (!res.ok || res.body == null) {
        throw new KvError(
          "sse_error",
          `SSE watch failed: ${res.status}`,
          res.status,
        );
      }

      reader = res.body.getReader();
      const dec = new TextDecoder();
      let buf = "";

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buf += dec.decode(value, { stream: true });
        const parts = buf.split("\n\n");
        buf = parts.pop() ?? "";
        for (const part of parts) {
          if (!part.trim() || part.startsWith(":")) continue; // heartbeat comments
          const data = part
            .split("\n")
            .filter((l) => l.startsWith("data: "))
            .map((l) => l.slice(6))
            .join("");
          if (!data) continue;
          const event = parseSseEvent(data);
          if (event != null) {
            if (event.revision > 0) lastRevision = event.revision;
            yield event;
          }
        }
      }
    } catch (err) {
      if (signal?.aborted) return;
      if (err instanceof KvError) throw err;
      // Transient error — reconnect after a brief delay.
      await new Promise<void>((r) => setTimeout(r, 1000));
      continue;
    } finally {
      reader?.cancel().catch(() => undefined);
    }
    // Clean stream end — reconnect to continue watching.
    await new Promise<void>((r) => setTimeout(r, 100));
  }
}

function parseSseEvent(data: string): KvWatchEvent | null {
  try {
    const obj = JSON.parse(data) as Record<string, unknown>;
    const type = obj["type"] as string;
    if (type === "ready") {
      return { type: "ready", revision: 0 };
    }
    if (type === "set") {
      const raw = obj["value"];
      const value = typeof raw === "string"
        ? decodeBase64(raw)
        : new Uint8Array(0);
      const event: KvWatchEvent = {
        type: "set",
        key: String(obj["key"]),
        value,
        revision: Number(obj["revision"]),
      };
      if (obj["metadata"] !== undefined) event.metadata = obj["metadata"];
      if (typeof obj["ttl"] === "number") event.ttl = obj["ttl"];
      return event;
    }
    if (type === "del") {
      return {
        type: "del",
        key: String(obj["key"]),
        revision: Number(obj["revision"]),
      };
    }
    return null;
  } catch {
    return null;
  }
}

function decodeBase64(b64: string): Uint8Array {
  if (typeof Buffer !== "undefined") {
    return new Uint8Array(Buffer.from(b64, "base64"));
  }
  const bin = atob(b64);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

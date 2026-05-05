import type { KvClient, KvHttpClientOptions } from "./client.js";
import { KvError, KvNotFoundError } from "./errors.js";
import type {
  KvBatchOp,
  KvBatchResults,
  KvDeleteOptions,
  KvEntry,
  KvListKey,
  KvListOptions,
  KvListResult,
  KvMSetEntry,
  KvSetOptions,
  KvWatchEvent,
  KvWatchOptions,
} from "./kv-types.js";
import { makeEntry } from "./kv-types.js";
import type { components } from "./types.js";

function nsToIndex(ns: string): number {
  if (ns === "default") return 0;
  const m = /^db(\d+)$/.exec(ns);
  return m != null ? Math.min(parseInt(m[1]!, 10), 15) : 0;
}

export function createHttpKvClient(opts: KvHttpClientOptions): KvClient {
  const base = opts.url.replace(/\/+$/, "");
  const nsIdx = nsToIndex(opts.namespace ?? "default");
  const retries = opts.retries ?? 2;
  const { timeout, onCommand, onResponse, onMetadataParseError } = opts;
  const fetchFn = opts.fetch ?? globalThis.fetch;

  function valueUrl(key: string): string {
    return `${base}/v1/kv/${encodeURIComponent(key)}?ns=${nsIdx}`;
  }

  function keysUrl(params?: KvListOptions): string {
    const url = new URL(`${base}/v1/kv`);
    url.searchParams.set("ns", String(nsIdx));
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
      const body = (await res.json()) as components["schemas"]["ErrorResponse"];
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
    const revHeader = res.headers.get("x-kv-revision");
    const metaHeader = res.headers.get("x-kv-metadata");

    const raw: {
      value: Uint8Array;
      ttl?: number;
      metadata?: unknown;
      revision: number;
    } = {
      value,
      revision: revHeader != null ? Number(revHeader) : 0,
    };
    if (ttlHeader != null) raw.ttl = Number(ttlHeader);
    if (metaHeader != null) {
      try {
        raw.metadata = JSON.parse(metaHeader) as unknown;
      } catch (err) {
        onMetadataParseError?.(key, metaHeader, err);
      }
    }
    return makeEntry(raw);
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
    if (setOpts?.ifMatch != null) {
      headers["if-match"] = String(setOpts.ifMatch);
    }

    // valueUrl already contains `?ns=N`, so additional flags use `&`.
    const url = setOpts?.ifAbsent
      ? `${valueUrl(key)}&nx=1`
      : setOpts?.ifPresent
      ? `${valueUrl(key)}&xx=1`
      : valueUrl(key);

    const body: BodyInit = typeof value === "string"
      ? value
      : new Blob([new Uint8Array(value)]);

    const res = await request("SET", 1, url, { method: "PUT", headers, body });
    if (!res.ok) throw await parseError(res);
  }

  async function incr(key: string, delta: number = 1): Promise<number> {
    const url = `${base}/v1/kv/${encodeURIComponent(key)}/incr?ns=${nsIdx}${
      delta !== 1 ? `&delta=${delta}` : ""
    }`;
    const res = await request("INCR", 1, url, { method: "POST" });
    if (!res.ok) throw await parseError(res);
    const body = (await res.json()) as components["schemas"]["IncrResponse"];
    return body.value;
  }

  async function deleteOp(key: string, opts?: KvDeleteOptions): Promise<void> {
    const headers: Record<string, string> = {};
    if (opts?.ifMatch != null) headers["if-match"] = String(opts.ifMatch);
    const res = await request("DEL", 1, valueUrl(key), {
      method: "DELETE",
      headers,
    });
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
    incr,

    delete: deleteOp,

    async list(listOpts?: KvListOptions): Promise<KvListResult> {
      const res = await request("SCAN", 1, keysUrl(listOpts), {});
      if (!res.ok) throw await parseError(res);
      const body = (await res.json()) as components["schemas"]["ListResponse"];
      const result: KvListResult = { keys: body.keys as KvListKey[] };
      if (body.cursor) result.nextCursor = body.cursor;
      return result;
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

    async batch<T extends readonly KvBatchOp[]>(
      ops: T,
    ): Promise<KvBatchResults<T>> {
      if (ops.length === 0) return [] as unknown as KvBatchResults<T>;
      const results = await Promise.all(
        ops.map((op) => {
          if (op.op === "get") return get(op.key);
          if (op.op === "set") return set(op.key, op.value, op.opts);
          if (op.op === "delete") return deleteOp(op.key, op.opts);
          return incr(op.key, op.delta ?? 1);
        }),
      );
      return results as unknown as KvBatchResults<T>;
    },

    close(): Promise<void> {
      return Promise.resolve();
    },

    watch(
      key: string,
      watchOpts?: KvWatchOptions,
    ): AsyncGenerator<KvWatchEvent> {
      return watchSse(base, nsIdx, key, watchOpts, fetchFn);
    },
  };
}

async function* watchSse(
  base: string,
  nsIdx: number,
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
        ? `${base}/v1/watch`
        : `${base}/v1/watch/${encodeURIComponent(key)}`,
    );
    url.searchParams.set("ns", String(nsIdx));
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
            if (event.type !== "ready" && event.revision > 0) {
              lastRevision = event.revision;
            }
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
      return { type: "ready" };
    }
    if (type === "set") {
      const raw = obj["value"];
      const value = typeof raw === "string"
        ? decodeBase64(raw)
        : new Uint8Array(0);
      const event: Extract<KvWatchEvent, { type: "set" }> = {
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

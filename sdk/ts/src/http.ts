import type { KvHttpClient, KvHttpClientOptions } from "./client.js";
import { KvError } from "./errors.js";
import type {
  BatchOp,
  BatchResults,
  BatchSetOpts,
  CasOptions,
  DeleteOptions,
  Entry,
  ExpiryOptions,
  GetAndSetOptions,
  KvHttpResult,
  ListKey,
  ListOptions,
  ListResult,
  MSetEntry,
  SetOptions,
  WatchEvent,
  WatchOptions,
} from "./kv-types.js";
import { makeEntry } from "./kv-types.js";
import type { components } from "./types.js";
import { camelize } from "./utils/camelize.js";

interface BatchGetResult {
  value: string;
  revision?: number;
  ttl?: number;
  ttlMs?: number;
  metadata?: unknown;
}

function encodeBase64(bytes: Uint8Array): string {
  if (typeof Buffer !== "undefined") {
    return Buffer.from(bytes).toString("base64url");
  }
  let bin = "";
  for (let i = 0; i < bytes.length; i++) bin += String.fromCharCode(bytes[i]!);
  return btoa(bin).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

function parseBatchEntry(raw: unknown): Entry {
  const r = camelize(raw) as BatchGetResult;
  const value = decodeBase64(r.value);
  const entry: {
    value: Uint8Array;
    ttl?: number;
    ttlMs?: number;
    metadata?: unknown;
    revision: number;
  } = {
    value,
    revision: r.revision ?? 0,
  };
  if (r.ttl != null) entry.ttl = r.ttl;
  if (r.ttlMs != null) entry.ttlMs = r.ttlMs;
  if (r.metadata !== undefined) entry.metadata = r.metadata;
  return makeEntry(entry);
}

function nsToIndex(ns: string): number {
  if (ns === "default") return 0;
  const m = /^db(\d+)$/.exec(ns);
  return m != null ? Math.min(parseInt(m[1]!, 10), 15) : 0;
}

function toKvError(err: unknown): KvError {
  if (err instanceof KvError) return err;
  return new KvError(
    "internal_error",
    err instanceof Error ? err.message : String(err),
    500,
  );
}

export function createHttpKvClient(opts: KvHttpClientOptions): KvHttpClient {
  const base = opts.url.replace(/\/+$/, "");
  const nsIdx = nsToIndex(opts.namespace ?? "default");
  const retries = opts.retries ?? 2;
  const { timeout, onCommand, onResponse, onMetadataParseError } = opts;
  const fetchFn = opts.fetch ?? globalThis.fetch;

  function valueUrl(key: string): string {
    return `${base}/v1/kv/${encodeURIComponent(key)}?ns=${nsIdx}`;
  }

  function batchUrl(): string {
    return `${base}/v1/kv/batch?ns=${nsIdx}`;
  }

  function keysUrl(params?: ListOptions): string {
    const url = new URL(`${base}/v1/kv`);
    url.searchParams.set("ns", String(nsIdx));
    if (params?.prefix) url.searchParams.set("prefix", params.prefix);
    if (params?.cursor) url.searchParams.set("cursor", params.cursor);
    if (params?.limit != null) {
      url.searchParams.set("limit", String(params.limit));
    }
    return url.toString();
  }

  function countUrl(): string {
    return `${base}/v1/kv?ns=${nsIdx}&count=1`;
  }

  function flushUrl(): string {
    return `${base}/v1/kv?ns=${nsIdx}`;
  }

  function compactUrl(): string {
    return `${base}/v1/admin/compact?ns=${nsIdx}`;
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
    } catch {
      /* ignore */
    }
    return new KvError(code, message, res.status, res);
  }

  async function batchRequest(
    ops: unknown[],
    keyCount: number,
  ): Promise<[(unknown | null)[], Response]> {
    const res = await request("BATCH", keyCount, batchUrl(), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(ops),
    });
    if (!res.ok) throw await parseError(res);
    return [(await res.json()) as (unknown | null)[], res];
  }

  function parseEntryHeaders(
    res: Response,
    key: string,
    value: Uint8Array,
  ): Entry {
    const ttlHeader = res.headers.get("x-kv-ttl");
    const ttlMsHeader = res.headers.get("x-kv-ttl-ms");
    const revHeader = res.headers.get("x-kv-revision");
    const metaHeader = res.headers.get("x-kv-metadata");

    const raw: {
      value: Uint8Array;
      ttl?: number;
      ttlMs?: number;
      metadata?: unknown;
      revision: number;
    } = {
      value,
      revision: revHeader != null ? Number(revHeader) : 0,
    };
    if (ttlHeader != null) raw.ttl = Number(ttlHeader);
    if (ttlMsHeader != null) raw.ttlMs = Number(ttlMsHeader);
    if (metaHeader != null) {
      try {
        raw.metadata = JSON.parse(metaHeader) as unknown;
      } catch (err) {
        onMetadataParseError?.(key, metaHeader, err);
      }
    }
    return makeEntry(raw);
  }

  async function _get(key: string): Promise<[Entry | null, Response]> {
    const res = await request("GET", 1, valueUrl(key), {});
    if (res.status === 404) return [null, res];
    if (!res.ok) throw await parseError(res);
    const value = new Uint8Array(await res.arrayBuffer());
    return [parseEntryHeaders(res, key, value), res];
  }

  async function _set(
    key: string,
    value: string | Uint8Array,
    setOpts?: SetOptions,
  ): Promise<[undefined, Response]> {
    const headers: Record<string, string> = {};
    if (setOpts?.keepTtl) {
      headers["x-kv-keepttl"] = "1";
    } else if (setOpts?.ttl != null) {
      headers["x-kv-ttl"] = String(setOpts.ttl);
    }
    if (setOpts?.metadata != null) {
      headers["x-kv-metadata"] = JSON.stringify(setOpts.metadata);
    }
    if (setOpts?.ifMatch != null) {
      headers["if-match"] = String(setOpts.ifMatch);
    }

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
    return [undefined, res];
  }

  async function _exists(key: string): Promise<[boolean, Response]> {
    const res = await request("EXISTS", 1, valueUrl(key), { method: "HEAD" });
    if (res.status === 404) return [false, res];
    if (res.status === 200) return [true, res];
    throw await parseError(res);
  }

  async function _getAndSet(
    key: string,
    value: string | Uint8Array,
    getAndSetOpts?: GetAndSetOptions,
  ): Promise<[Entry | null, Response]> {
    const headers: Record<string, string> = { "x-kv-return-old": "1" };
    if (getAndSetOpts?.ttl != null) {
      headers["x-kv-ttl"] = String(getAndSetOpts.ttl);
    }
    if (getAndSetOpts?.metadata != null) {
      headers["x-kv-metadata"] = JSON.stringify(getAndSetOpts.metadata);
    }

    const body: BodyInit = typeof value === "string"
      ? value
      : new Blob([new Uint8Array(value)]);

    const res = await request("GETSET", 1, valueUrl(key), {
      method: "PUT",
      headers,
      body,
    });
    if (res.status === 204) return [null, res];
    if (!res.ok) throw await parseError(res);
    const oldValue = new Uint8Array(await res.arrayBuffer());
    return [parseEntryHeaders(res, key, oldValue), res];
  }

  async function _expire(
    key: string,
    expireOpts: ExpiryOptions,
  ): Promise<[Entry | null, Response]> {
    const url = new URL(`${base}/v1/kv/${encodeURIComponent(key)}`);
    url.searchParams.set("ns", String(nsIdx));
    if (expireOpts.ttl != null) {
      url.searchParams.set("ttl", String(expireOpts.ttl));
    } else if (expireOpts.ttlMs != null) {
      url.searchParams.set("ttl_ms", String(expireOpts.ttlMs));
    } else if (expireOpts.ttlAt != null) {
      url.searchParams.set("ttl_at", String(expireOpts.ttlAt));
    } else if (expireOpts.ttlAtMs != null) {
      url.searchParams.set("ttl_at_ms", String(expireOpts.ttlAtMs));
    } else if (expireOpts.persist) {
      url.searchParams.set("persist", "1");
    }

    const headers: Record<string, string> = {};
    if (expireOpts.returnValue) {
      headers["x-kv-return-value"] = "1";
    }

    const res = await request("EXPIRE", 1, url.toString(), {
      method: "PATCH",
      headers,
    });
    if (res.status === 404) {
      throw new KvError("not_found", `key not found: ${key}`, 404, res);
    }
    if (res.status === 204) return [null, res];
    if (!res.ok) throw await parseError(res);
    const value = new Uint8Array(await res.arrayBuffer());
    return [parseEntryHeaders(res, key, value), res];
  }

  async function _cas(
    key: string,
    value: string | Uint8Array,
    revision: number,
    casOpts?: CasOptions,
  ): Promise<[number, Response]> {
    const headers: Record<string, string> = {
      "if-match": String(revision),
    };
    if (casOpts?.ttl != null) headers["x-kv-ttl"] = String(casOpts.ttl);

    const body: BodyInit = typeof value === "string"
      ? value
      : new Blob([new Uint8Array(value)]);

    const res = await request("CAS", 1, valueUrl(key), {
      method: "PUT",
      headers,
      body,
    });
    if (!res.ok) throw await parseError(res);
    const revHeader = res.headers.get("x-kv-revision");
    return [revHeader != null ? Number(revHeader) : 0, res];
  }

  async function _getAndDelete(key: string): Promise<[Entry | null, Response]> {
    const headers: Record<string, string> = { "x-kv-return-old": "1" };
    const res = await request("GETDEL", 1, valueUrl(key), {
      method: "DELETE",
      headers,
    });
    if (res.status === 204) return [null, res];
    if (!res.ok) throw await parseError(res);
    const value = new Uint8Array(await res.arrayBuffer());
    return [parseEntryHeaders(res, key, value), res];
  }

  async function _delete(
    key: string,
    opts?: DeleteOptions,
  ): Promise<[Entry | null | undefined, Response]> {
    const headers: Record<string, string> = {};
    if (opts?.ifMatch != null) headers["if-match"] = String(opts.ifMatch);
    if (opts?.returnOld) headers["x-kv-return-old"] = "1";

    const res = await request("DEL", 1, valueUrl(key), {
      method: "DELETE",
      headers,
    });
    if (!res.ok) throw await parseError(res);
    if (opts?.returnOld) {
      if (res.status === 204) return [null, res];
      const value = new Uint8Array(await res.arrayBuffer());
      return [parseEntryHeaders(res, key, value), res];
    }
    return [undefined, res];
  }

  async function _incr(
    key: string,
    delta: number = 1,
  ): Promise<[number, Response]> {
    const url = `${base}/v1/kv/${encodeURIComponent(key)}/incr?ns=${nsIdx}${
      delta !== 1 ? `&delta=${delta}` : ""
    }`;
    const res = await request("INCR", 1, url, { method: "POST" });
    if (!res.ok) throw await parseError(res);
    const body = (await res.json()) as components["schemas"]["IncrResponse"];
    return [body.value, res];
  }

  async function _list(
    listOpts?: ListOptions,
  ): Promise<[ListResult, Response]> {
    const res = await request("SCAN", 1, keysUrl(listOpts), {});
    if (!res.ok) throw await parseError(res);
    const body = (await res.json()) as components["schemas"]["ListResponse"];
    const result: ListResult = { keys: body.keys as ListKey[] };
    if (body.cursor) result.nextCursor = body.cursor;
    return [result, res];
  }

  async function _count(): Promise<[number, Response]> {
    const res = await request("DBSIZE", 1, countUrl(), {});
    if (!res.ok) throw await parseError(res);
    const body = (await res.json()) as components["schemas"]["CountResponse"];
    return [body.count, res];
  }

  async function _flush(): Promise<[undefined, Response]> {
    const res = await request("FLUSHDB", 1, flushUrl(), { method: "DELETE" });
    if (!res.ok) throw await parseError(res);
    return [undefined, res];
  }

  async function _compact(): Promise<[undefined, Response]> {
    const res = await request("BGREWRITEAOF", 1, compactUrl(), {
      method: "POST",
    });
    if (!res.ok) throw await parseError(res);
    return [undefined, res];
  }

  async function _mget(keys: string[]): Promise<[(Entry | null)[], Response]> {
    const ops = keys.map((key) => ({ op: "get" as const, key }));
    const [results, res] = await batchRequest(ops, keys.length);
    return [
      results.map((r) =>
        r == null || typeof r !== "object" || !("value" in r)
          ? null
          : parseBatchEntry(r)
      ),
      res,
    ];
  }

  function batchSetWireOp(
    key: string,
    value: string | Uint8Array,
    opts?: BatchSetOpts,
  ): Record<string, unknown> {
    const bytes = typeof value === "string"
      ? new TextEncoder().encode(value)
      : value;
    return {
      op: "set",
      key,
      value: encodeBase64(bytes),
      ...(opts?.ttlMs != null
        ? { ttlMs: opts.ttlMs }
        : opts?.ttl != null && { ttl: opts.ttl }),
      ...(opts?.metadata != null && { metadata: opts.metadata }),
      ...(opts?.ifAbsent === true && { nx: true }),
      ...(opts?.ifPresent === true && { xx: true }),
      ...(opts?.ifMatch != null && { ifMatch: opts.ifMatch }),
      ...(opts?.keepTtl === true && { keepTtl: true }),
    };
  }

  async function _mset(entries: MSetEntry[]): Promise<[undefined, Response]> {
    const ops = entries.map(({ key, value, opts }) =>
      batchSetWireOp(key, value, opts)
    );
    const [, res] = await batchRequest(ops, entries.length);
    return [undefined, res];
  }

  async function _batch<T extends readonly BatchOp[]>(
    ops: T,
  ): Promise<[BatchResults<T>, Response]> {
    const wireOps = ops.map((op) => {
      if (op.op === "get") return { op: "get" as const, key: op.key };
      if (op.op === "set") {
        return batchSetWireOp(op.key, op.value, op.opts);
      }
      if (op.op === "delete") {
        return {
          op: "delete" as const,
          key: op.key,
          ...(op.opts?.ifMatch != null && { ifMatch: op.opts.ifMatch }),
          ...(op.opts?.returnOld === true && { returnOld: true }),
        };
      }
      if (op.op === "exists") {
        return { op: "exists" as const, key: op.key };
      }
      return { op: "incr" as const, key: op.key, delta: op.delta ?? 1 };
    });
    const [raw, res] = await batchRequest(wireOps, ops.length);
    const results = ops.map((op, i) => {
      const r = raw[i];
      if (op.op === "get") {
        return r == null || typeof r !== "object" || !("value" in r)
          ? null
          : parseBatchEntry(r as BatchGetResult);
      }
      if (op.op === "incr") {
        return r != null && typeof r === "object" && "value" in r
          ? (r as { value: number }).value
          : 0;
      }
      if (op.op === "exists") {
        return typeof r === "boolean" ? r : false;
      }
      if (op.op === "delete" && op.opts?.returnOld === true) {
        return r == null || typeof r !== "object" || !("value" in r)
          ? null
          : parseBatchEntry(r as BatchGetResult);
      }
      return undefined;
    });
    return [results as unknown as BatchResults<T>, res];
  }

  function ok<T>(data: T, response: Response): KvHttpResult<T> {
    return { data, error: undefined, response };
  }

  function fail(err: unknown): KvHttpResult<never> {
    const error = toKvError(err);
    return { data: undefined, error, response: error.response };
  }

  return {
    async get(key) {
      try {
        const [data, response] = await _get(key);
        return ok(data, response);
      } catch (err) {
        return fail(err);
      }
    },

    async set(key, value, opts) {
      try {
        const [, response] = await _set(key, value, opts);
        return ok(undefined, response);
      } catch (err) {
        return fail(err);
      }
    },

    async exists(key) {
      try {
        const [data, response] = await _exists(key);
        return ok(data, response);
      } catch (err) {
        return fail(err);
      }
    },

    async getAndSet(key, value, opts) {
      try {
        const [data, response] = await _getAndSet(key, value, opts);
        return ok(data, response);
      } catch (err) {
        return fail(err);
      }
    },

    async expire(key, expireOpts) {
      try {
        const [data, response] = await _expire(key, expireOpts);
        return ok(data, response);
      } catch (err) {
        return fail(err);
      }
    },

    async delete(key: string, opts?: DeleteOptions) {
      try {
        const [old, response] = await _delete(key, opts);
        if (opts?.returnOld) {
          return ok(old ?? null, response) as KvHttpResult<Entry | null>;
        }
        return ok(undefined, response) as KvHttpResult<void>;
      } catch (err) {
        return fail(err) as KvHttpResult<never>;
      }
    },

    async list(listOpts) {
      try {
        const [data, response] = await _list(listOpts);
        return ok(data, response);
      } catch (err) {
        return fail(err);
      }
    },

    async count() {
      try {
        const [data, response] = await _count();
        return ok(data, response);
      } catch (err) {
        return fail(err);
      }
    },

    async flush() {
      try {
        const [, response] = await _flush();
        return ok(undefined, response);
      } catch (err) {
        return fail(err);
      }
    },

    async compact() {
      try {
        const [, response] = await _compact();
        return ok(undefined, response);
      } catch (err) {
        return fail(err);
      }
    },

    async multiGet(keys) {
      if (keys.length === 0) return ok([], new Response(null, { status: 200 }));
      try {
        const [data, response] = await _mget(keys);
        return ok(data, response);
      } catch (err) {
        return fail(err);
      }
    },

    async multiSet(entries) {
      if (entries.length === 0) {
        return ok(undefined, new Response(null, { status: 200 }));
      }
      try {
        const [, response] = await _mset(entries);
        return ok(undefined, response);
      } catch (err) {
        return fail(err);
      }
    },

    async incr(key, delta) {
      try {
        const [data, response] = await _incr(key, delta);
        return ok(data, response);
      } catch (err) {
        return fail(err);
      }
    },

    async decr(key, delta = 1) {
      try {
        const [data, response] = await _incr(key, -delta);
        return ok(data, response);
      } catch (err) {
        return fail(err);
      }
    },

    async cas(key, value, revision, casOpts) {
      try {
        const [data, response] = await _cas(key, value, revision, casOpts);
        return ok(data, response);
      } catch (err) {
        return fail(err);
      }
    },

    async getAndDelete(key) {
      try {
        const [data, response] = await _getAndDelete(key);
        return ok(data, response);
      } catch (err) {
        return fail(err);
      }
    },

    async batch(ops) {
      if (ops.length === 0) {
        return ok(
          [] as unknown as BatchResults<typeof ops>,
          new Response(null, { status: 200 }),
        );
      }
      try {
        const [data, response] = await _batch(ops);
        return ok(data, response);
      } catch (err) {
        return fail(err);
      }
    },

    close(): Promise<void> {
      return Promise.resolve();
    },

    watch(key: string, watchOpts?: WatchOptions): AsyncGenerator<WatchEvent> {
      return watchSse(base, nsIdx, key, watchOpts, fetchFn);
    },
  } as KvHttpClient;
}

async function* watchSse(
  base: string,
  nsIdx: number,
  key: string,
  opts: WatchOptions | undefined,
  fetchFn: typeof globalThis.fetch,
): AsyncGenerator<WatchEvent> {
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
          if (!part.trim() || part.startsWith(":")) continue;
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
      await new Promise<void>((r) => setTimeout(r, 1000));
      continue;
    } finally {
      reader?.cancel().catch(() => undefined);
    }
    await new Promise<void>((r) => setTimeout(r, 100));
  }
}

function parseSseEvent(data: string): WatchEvent | null {
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
      const event: Extract<WatchEvent, { type: "set" }> = {
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

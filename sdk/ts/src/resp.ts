import Redis from "ioredis";
import * as net from "node:net";

import type { KvClient, KvRespClientOptions } from "./client.js";
import { KvError, KvNotFoundError } from "./errors.js";
import type {
  KvBatchOp,
  KvBatchResults,
  KvDeleteOptions,
  KvEntry,
  KvListOptions,
  KvListResult,
  KvMSetEntry,
  KvSetOptions,
  KvWatchEvent,
  KvWatchOptions,
} from "./types.js";
import { makeEntry } from "./types.js";

export function createRespKvClient(opts: KvRespClientOptions): KvClient {
  const redis = new Redis(opts.url, {
    db: opts.db ?? 0,
    commandTimeout: opts.timeout,
    maxRetriesPerRequest: opts.retries ?? 2,
    enableReadyCheck: false,
    lazyConnect: false,
  });

  redis.defineCommand("revision", { numberOfKeys: 1 });
  redis.defineCommand("setrev", { numberOfKeys: 1 });

  const { onCommand, onResponse } = opts;

  function track<T>(
    command: string,
    keyCount: number,
    fn: () => Promise<T>,
  ): Promise<T> {
    onCommand?.({ command, keyCount });
    const start = Date.now();
    return fn().then(
      (v) => {
        onResponse?.({ command, keyCount, durationMs: Date.now() - start });
        return v;
      },
      (e) => {
        onResponse?.({ command, keyCount, durationMs: Date.now() - start });
        throw e;
      },
    );
  }

  async function get(key: string): Promise<KvEntry | null> {
    return track("GET", 1, async () => {
      const pipeline = redis.pipeline();
      pipeline.getBuffer(key);
      (pipeline as any).revision(key);
      pipeline.ttl(key);
      const [[, valueBuf], [, revision], [, ttlSecs]] = await pipeline
        .exec() as [
          [null, Buffer | null],
          [null, number],
          [null, number],
        ];

      if (valueBuf === null) return null;
      return makeEntry({
        value: new Uint8Array(valueBuf),
        revision: revision > 0 ? revision : 0,
        ...(ttlSecs >= 0 ? { ttl: ttlSecs } : {}),
      });
    });
  }

  async function set(
    key: string,
    value: string | Uint8Array,
    setOpts?: KvSetOptions,
  ): Promise<void> {
    return track("SET", 1, async () => {
      const buf = value instanceof Uint8Array
        ? Buffer.from(value)
        : Buffer.from(value);
      const ttl = setOpts?.ttl;
      const ifAbsent = setOpts?.ifAbsent ?? false;
      const ifPresent = setOpts?.ifPresent ?? false;
      const ifMatch = setOpts?.ifMatch;

      if (ifMatch != null) {
        const args: (string | Buffer)[] = [key, buf, String(ifMatch)];
        if (ttl != null) {
          args.push("EX", String(ttl));
        }
        try {
          await (redis as any).setrev(...args);
        } catch (e: unknown) {
          if (isConflictError(e)) {
            throw new KvError("conflict", "revision mismatch", 409);
          }
          throw e;
        }
        return;
      }

      let result: "OK" | null;
      if (ttl != null && ifAbsent) {
        result = await redis.set(key, buf, "EX", ttl, "NX");
      } else if (ttl != null && ifPresent) {
        result = await redis.set(key, buf, "EX", ttl, "XX");
      } else if (ttl != null) {
        result = await redis.set(key, buf, "EX", ttl);
      } else if (ifAbsent) {
        result = await redis.set(key, buf, "NX");
      } else if (ifPresent) {
        result = await redis.set(key, buf, "XX");
      } else {
        result = await redis.set(key, buf);
      }

      if (result === null) {
        if (ifAbsent) throw new KvError("conflict", "key already exists", 409);
        if (ifPresent) throw new KvError("conflict", "key does not exist", 409);
      }
    });
  }

  return {
    get,

    async getOrThrow(key: string): Promise<KvEntry> {
      const entry = await get(key);
      if (entry == null) throw new KvNotFoundError(key);
      return entry;
    },

    set,

    async incr(key: string, delta: number = 1): Promise<number> {
      return track("INCR", 1, () => redis.incrby(key, delta));
    },

    async delete(key: string, _opts?: KvDeleteOptions): Promise<void> {
      return track("DEL", 1, () => redis.del(key).then(() => undefined));
    },

    async list(listOpts?: KvListOptions): Promise<KvListResult> {
      return track("SCAN", 1, async () => {
        const cursor = listOpts?.cursor ?? "0";
        const count = listOpts?.limit ?? 100;
        const [scanCursor, keys] = listOpts?.prefix
          ? await redis.scan(
            cursor,
            "MATCH",
            `${listOpts.prefix}*`,
            "COUNT",
            count,
          )
          : await redis.scan(cursor, "COUNT", count);

        const done = scanCursor === "0";
        const result: KvListResult = { keys: keys.map((name) => ({ name })) };
        if (!done) result.nextCursor = scanCursor;
        return result;
      });
    },

    async mget(keys: string[]): Promise<(KvEntry | null)[]> {
      if (keys.length === 0) return [];
      return track("MGET", keys.length, async () => {
        // Pipeline getBuffer+revision+ttl triples per key so revision and TTL
        // are returned for each key, matching the behaviour of the single
        // get() and the HTTP backend.
        const pipeline = redis.pipeline();
        for (const key of keys) {
          pipeline.getBuffer(key);
          (pipeline as any).revision(key);
          pipeline.ttl(key);
        }
        const results = await pipeline.exec() as Array<
          [null, Buffer | null | number]
        >;
        const out: (KvEntry | null)[] = [];
        for (let i = 0; i < keys.length; i++) {
          const valueBuf = results[i * 3]![1] as Buffer | null;
          const revision = results[i * 3 + 1]![1] as number;
          const ttlSecs = results[i * 3 + 2]![1] as number;
          if (valueBuf === null) {
            out.push(null);
          } else {
            out.push(makeEntry({
              value: new Uint8Array(valueBuf),
              revision: revision > 0 ? revision : 0,
              ...(ttlSecs >= 0 ? { ttl: ttlSecs } : {}),
            }));
          }
        }
        return out;
      });
    },

    async mset(entries: KvMSetEntry[]): Promise<void> {
      if (entries.length === 0) return;
      return track("MSET", entries.length, async () => {
        const withTtl = entries.filter((e) => e.opts?.ttl != null);
        const plain = entries.filter((e) => e.opts?.ttl == null);

        const pipeline = redis.pipeline();

        if (plain.length > 0) {
          const pairs: (string | Buffer)[] = [];
          for (const { key, value } of plain) {
            pairs.push(
              key,
              value instanceof Uint8Array
                ? Buffer.from(value)
                : Buffer.from(value),
            );
          }
          pipeline.mset(...(pairs as [string, string, ...string[]]));
        }
        for (const { key, value, opts } of withTtl) {
          const buf = value instanceof Uint8Array
            ? Buffer.from(value)
            : Buffer.from(value);
          pipeline.set(key, buf, "EX", opts!.ttl!);
        }

        await pipeline.exec();
      });
    },

    async batch<T extends readonly KvBatchOp[]>(
      ops: T,
    ): Promise<KvBatchResults<T>> {
      if (ops.length === 0) return [] as unknown as KvBatchResults<T>;
      return track("BATCH", ops.length, async () => {
        const pipeline = redis.pipeline();
        // Track how many pipeline slots each op occupies (get = 3, others = 1).
        const offsets: number[] = [];
        let offset = 0;
        for (const op of ops) {
          offsets.push(offset);
          if (op.op === "get") {
            pipeline.getBuffer(op.key);
            (pipeline as any).revision(op.key);
            pipeline.ttl(op.key);
            offset += 3;
          } else if (op.op === "set") {
            const buf = op.value instanceof Uint8Array
              ? Buffer.from(op.value)
              : Buffer.from(op.value);
            if (op.opts?.ifMatch != null) {
              const args: (string | Buffer)[] = [
                op.key,
                buf,
                String(op.opts.ifMatch),
              ];
              if (op.opts.ttl != null) args.push("EX", String(op.opts.ttl));
              (pipeline as any).setrev(...args);
            } else if (op.opts?.ttl != null && op.opts.ifAbsent) {
              pipeline.set(op.key, buf, "EX", op.opts.ttl, "NX");
            } else if (op.opts?.ttl != null && op.opts.ifPresent) {
              pipeline.set(op.key, buf, "EX", op.opts.ttl, "XX");
            } else if (op.opts?.ttl != null) {
              pipeline.set(op.key, buf, "EX", op.opts.ttl);
            } else if (op.opts?.ifAbsent) {
              pipeline.set(op.key, buf, "NX");
            } else if (op.opts?.ifPresent) {
              pipeline.set(op.key, buf, "XX");
            } else {
              pipeline.set(op.key, buf);
            }
            offset += 1;
          } else if (op.op === "delete") {
            pipeline.del(op.key);
            offset += 1;
          } else {
            pipeline.incrby(op.key, (op as any).delta ?? 1);
            offset += 1;
          }
        }

        const results = await pipeline.exec() as Array<[Error | null, unknown]>;

        return ops.map((op, i) => {
          const off = offsets[i]!;
          if (op.op === "get") {
            const valueBuf = results[off]![1] as Buffer | null;
            if (valueBuf === null) return null;
            const revision = results[off + 1]![1] as number;
            const ttlSecs = results[off + 2]![1] as number;
            return makeEntry({
              value: new Uint8Array(valueBuf),
              revision: revision > 0 ? revision : 0,
              ...(ttlSecs >= 0 ? { ttl: ttlSecs } : {}),
            });
          } else if (op.op === "set") {
            const [err] = results[off]!;
            if (err) {
              if (isConflictError(err)) {
                throw new KvError("conflict", "revision mismatch", 409);
              }
              if (op.opts?.ifAbsent) {
                throw new KvError("conflict", "key already exists", 409);
              }
              if (op.opts?.ifPresent) {
                throw new KvError("conflict", "key does not exist", 409);
              }
              throw err;
            }
          } else if (op.op === "incr") {
            const [err, n] = results[off]!;
            if (err) throw err;
            return n as number;
          }
          return undefined; // delete / set with no return value
        }) as unknown as KvBatchResults<T>;
      });
    },

    async *watch(
      key: string,
      watchOpts?: KvWatchOptions,
    ): AsyncGenerator<KvWatchEvent> {
      const url = new URL(opts.url);
      const host = url.hostname;
      const port = parseInt(url.port, 10);
      const db = opts.db ?? 0;
      const prefix = watchOpts?.prefix ?? false;
      const signal = watchOpts?.signal;
      let lastRevision = watchOpts?.since ?? 0;

      while (true) {
        if (signal?.aborted) return;

        let conn: Resp3Conn | undefined;
        try {
          conn = await openResp3Conn(host, port, signal);
          await conn.hello3();
          if (db !== 0) {
            await conn.sendAndRecv(encodeRespArgs("SELECT", String(db)));
          }
          const watchCmd = prefix ? "PWATCH" : "WATCH";
          const args: string[] = [watchCmd, key];
          if (lastRevision > 0) {
            args.push("SINCE", String(lastRevision));
          }
          conn.send(encodeRespArgs(...args));

          while (true) {
            if (signal?.aborted) {
              conn.close();
              return;
            }
            const frame = await conn.recvPush(signal);
            const event = pushToEvent(frame);
            if (event == null) continue;
            if (event.type !== "ready" && event.revision > 0) {
              lastRevision = event.revision;
            }
            yield event;
          }
        } catch {
          if (signal?.aborted) {
            conn?.close();
            return;
          }
          conn?.close();
          // Sleep 1s before reconnect.
          await sleep(1000, signal);
          if (signal?.aborted) return;
          // loop, reconnect with SINCE lastRevision
        }
      }
    },

    close(): Promise<void> {
      return redis.quit().then(() => undefined);
    },
  };
}

function isConflictError(e: unknown): boolean {
  return e instanceof Error && e.message.startsWith("CONFLICT");
}

// ── RESP3 watch internals ───────────────────────────────────────────────────

type RespValue =
  | string
  | number
  | null
  | Uint8Array
  | RespValue[]
  | Map<RespValue, RespValue>
  | { push: RespValue[] };

function encodeRespArgs(...args: string[]): Buffer {
  const parts: string[] = [`*${args.length}\r\n`];
  for (const a of args) parts.push(`$${Buffer.byteLength(a)}\r\n${a}\r\n`);
  return Buffer.from(parts.join(""));
}

class NeedMore extends Error {}

class Resp3Reader {
  private buf = Buffer.alloc(0);
  private pos = 0;

  feed(chunk: Buffer): void {
    this.buf = Buffer.concat([this.buf.subarray(this.pos), chunk]);
    this.pos = 0;
  }

  tryRead(): RespValue | undefined {
    const saved = this.pos;
    try {
      return this.readValue();
    } catch (e) {
      if (e instanceof NeedMore) {
        this.pos = saved;
        return undefined;
      }
      throw e;
    }
  }

  private readLine(): string {
    const i = this.buf.indexOf("\r\n", this.pos);
    if (i === -1) throw new NeedMore();
    const line = this.buf.toString("utf8", this.pos, i);
    this.pos = i + 2;
    return line;
  }

  private readValue(): RespValue {
    if (this.pos >= this.buf.length) throw new NeedMore();
    const type = String.fromCharCode(this.buf[this.pos]!);
    this.pos++;
    switch (type) {
      case "+":
        return this.readLine();
      case "-":
        throw new Error(this.readLine());
      case ":":
        return parseInt(this.readLine(), 10);
      case "_": {
        this.readLine();
        return null;
      }
      case "$": {
        const len = parseInt(this.readLine(), 10);
        if (len === -1) return null;
        if (this.pos + len + 2 > this.buf.length) throw new NeedMore();
        const bytes = this.buf.subarray(this.pos, this.pos + len);
        this.pos += len + 2;
        return new Uint8Array(bytes);
      }
      case "*": {
        const count = parseInt(this.readLine(), 10);
        if (count === -1) return null;
        const arr: RespValue[] = [];
        for (let i = 0; i < count; i++) arr.push(this.readValue());
        return arr;
      }
      case "%": {
        const count = parseInt(this.readLine(), 10);
        const map = new Map<RespValue, RespValue>();
        for (let i = 0; i < count; i++) {
          const k = this.readValue();
          const v = this.readValue();
          map.set(k, v);
        }
        return map;
      }
      case ">": {
        const count = parseInt(this.readLine(), 10);
        const arr: RespValue[] = [];
        for (let i = 0; i < count; i++) arr.push(this.readValue());
        return { push: arr };
      }
      default:
        throw new Error(`unknown RESP3 type: ${type}`);
    }
  }
}

type Waiter = {
  resolve: (v: RespValue) => void;
  reject: (e: unknown) => void;
};

class Resp3Conn {
  private sock: net.Socket;
  private reader = new Resp3Reader();
  private pending: Waiter[] = [];
  private closed = false;
  private error: unknown = null;

  constructor(sock: net.Socket) {
    this.sock = sock;
    sock.on("data", (chunk: Buffer) => {
      this.reader.feed(chunk);
      this.drain();
    });
    sock.on("error", (err) => {
      this.error = err;
      this.failPending(err);
    });
    sock.on("close", () => {
      this.closed = true;
      this.failPending(this.error ?? new Error("connection closed"));
    });
  }

  private failPending(err: unknown): void {
    while (this.pending.length > 0) {
      this.pending.shift()!.reject(err);
    }
  }

  private drain(): void {
    while (this.pending.length > 0) {
      let val: RespValue | undefined;
      try {
        val = this.reader.tryRead();
      } catch (e) {
        const { reject } = this.pending.shift()!;
        reject(e);
        continue;
      }
      if (val === undefined) break;
      const { resolve } = this.pending.shift()!;
      resolve(val);
    }
  }

  send(buf: Buffer): void {
    this.sock.write(buf);
  }

  recv(): Promise<RespValue> {
    return new Promise((resolve, reject) => {
      if (this.closed) {
        reject(this.error ?? new Error("connection closed"));
        return;
      }
      let val: RespValue | undefined;
      try {
        val = this.reader.tryRead();
      } catch (e) {
        reject(e);
        return;
      }
      if (val !== undefined) {
        resolve(val);
      } else {
        this.pending.push({ resolve, reject });
      }
    });
  }

  async sendAndRecv(buf: Buffer): Promise<RespValue> {
    this.send(buf);
    return this.recv();
  }

  async hello3(): Promise<void> {
    this.send(encodeRespArgs("HELLO", "3"));
    await this.recv();
  }

  async recvPush(signal?: AbortSignal): Promise<{ push: RespValue[] }> {
    while (true) {
      const v = await raceAbort(this.recv(), signal);
      if (v && typeof v === "object" && "push" in (v as object)) {
        return v as { push: RespValue[] };
      }
      // Skip non-push frames (shouldn't happen during watch stream).
    }
  }

  close(): void {
    this.sock.destroy();
  }
}

function openResp3Conn(
  host: string,
  port: number,
  signal?: AbortSignal,
): Promise<Resp3Conn> {
  return new Promise((resolve, reject) => {
    const sock = net.createConnection(port, host, () => {
      sock.removeListener("error", reject);
      resolve(new Resp3Conn(sock));
    });
    sock.once("error", reject);
    if (signal) {
      const onAbort = () => {
        sock.destroy();
        reject(new Error("aborted"));
      };
      if (signal.aborted) {
        onAbort();
      } else {
        signal.addEventListener("abort", onAbort, { once: true });
      }
    }
  });
}

function respToString(v: RespValue): string {
  if (v instanceof Uint8Array) return Buffer.from(v).toString("utf8");
  if (typeof v === "string") return v;
  if (typeof v === "number") return String(v);
  throw new Error(`expected string-like, got ${typeof v}`);
}

function pushToEvent(frame: { push: RespValue[] }): KvWatchEvent | null {
  const arr = frame.push;
  if (arr.length < 2) return null;
  const ns = respToString(arr[0]!);
  if (ns !== "watch") return null;
  const kind = respToString(arr[1]!);
  if (kind === "ready") {
    return { type: "ready" };
  }
  if (kind === "set") {
    if (arr.length < 5) return null;
    const key = respToString(arr[2]!);
    const valRaw = arr[3]!;
    const value = valRaw instanceof Uint8Array
      ? valRaw
      : new TextEncoder().encode(respToString(valRaw));
    const revision = typeof arr[4] === "number"
      ? arr[4]
      : parseInt(respToString(arr[4]!), 10);
    return { type: "set", key, value, revision };
  }
  if (kind === "del") {
    if (arr.length < 4) return null;
    const key = respToString(arr[2]!);
    const revision = typeof arr[3] === "number"
      ? arr[3]
      : parseInt(respToString(arr[3]!), 10);
    return { type: "del", key, revision };
  }
  return null;
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    const t = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    const onAbort = () => {
      clearTimeout(t);
      resolve();
    };
    if (signal) {
      if (signal.aborted) {
        clearTimeout(t);
        resolve();
        return;
      }
      signal.addEventListener("abort", onAbort, { once: true });
    }
  });
}

function raceAbort<T>(p: Promise<T>, signal?: AbortSignal): Promise<T> {
  if (!signal) return p;
  return new Promise<T>((resolve, reject) => {
    const onAbort = () => reject(new Error("aborted"));
    if (signal.aborted) {
      reject(new Error("aborted"));
      return;
    }
    signal.addEventListener("abort", onAbort, { once: true });
    p.then(
      (v) => {
        signal.removeEventListener("abort", onAbort);
        resolve(v);
      },
      (e) => {
        signal.removeEventListener("abort", onAbort);
        reject(e);
      },
    );
  });
}

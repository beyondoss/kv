import Redis from "ioredis";

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

export function createRespKvClient(opts: KvClientOptions): KvClient {
  const redis = new Redis(opts.url, {
    db: opts.db ?? 0,
    commandTimeout: opts.timeout,
    maxRetriesPerRequest: opts.retries ?? 2,
    enableReadyCheck: false,
    lazyConnect: false,
  });

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
      const [[, valueBuf], [, ttlSecs]] = await redis
        .pipeline()
        .getBuffer(key)
        .ttl(key)
        .exec() as [[null, Buffer | null], [null, number]];

      if (valueBuf === null) return null;
      const entry: KvEntry = { value: new Uint8Array(valueBuf) };
      if (ttlSecs >= 0) entry.ttl = ttlSecs;
      return entry;
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
      const nx = setOpts?.nx ?? false;
      const xx = setOpts?.xx ?? false;

      let result: "OK" | null;
      if (ttl != null && nx) {
        result = await redis.set(key, buf, "EX", ttl, "NX");
      } else if (ttl != null && xx) {
        result = await redis.set(key, buf, "EX", ttl, "XX");
      } else if (ttl != null) {
        result = await redis.set(key, buf, "EX", ttl);
      } else if (nx) {
        result = await redis.set(key, buf, "NX");
      } else if (xx) {
        result = await redis.set(key, buf, "XX");
      } else {
        result = await redis.set(key, buf);
      }

      if (result === null) {
        if (nx) throw new KvError("conflict", "key already exists", 409);
        if (xx) throw new KvError("conflict", "key does not exist", 409);
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

    async delete(key: string): Promise<void> {
      return track("DEL", 1, () => redis.del(key).then(() => undefined));
    },

    async list(listOpts?: KvListOptions): Promise<KvListResult> {
      return track("SCAN", 1, async () => {
        const cursor = listOpts?.cursor ?? "0";
        const count = listOpts?.limit ?? 100;
        const [nextCursor, keys] = listOpts?.prefix
          ? await redis.scan(
            cursor,
            "MATCH",
            `${listOpts.prefix}*`,
            "COUNT",
            count,
          )
          : await redis.scan(cursor, "COUNT", count);

        const done = nextCursor === "0";
        const result: KvListResult = {
          keys: keys.map((name) => ({ name })),
          complete: done,
        };
        if (!done) result.cursor = nextCursor;
        return result;
      });
    },

    async mget(keys: string[]): Promise<(KvEntry | null)[]> {
      if (keys.length === 0) return [];
      return track("MGET", keys.length, async () => {
        // Pipeline getBuffer+ttl pairs so TTL is returned for each key, matching
        // the behaviour of the single get() and the HTTP backend.
        const pipeline = redis.pipeline();
        for (const key of keys) {
          pipeline.getBuffer(key);
          pipeline.ttl(key);
        }
        const results = await pipeline.exec() as Array<
          [null, Buffer | null | number]
        >;
        const out: (KvEntry | null)[] = [];
        for (let i = 0; i < keys.length; i++) {
          const valueBuf = results[i * 2]![1] as Buffer | null;
          const ttlSecs = results[i * 2 + 1]![1] as number;
          if (valueBuf === null) {
            out.push(null);
          } else {
            const entry: KvEntry = { value: new Uint8Array(valueBuf) };
            if (ttlSecs >= 0) entry.ttl = ttlSecs;
            out.push(entry);
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

    // eslint-disable-next-line require-yield
    async *watch(
      _key: string,
      _opts?: KvWatchOptions,
    ): AsyncGenerator<KvWatchEvent> {
      throw new KvError(
        "not_supported",
        "WATCH is not supported by the RESP backend; use the HTTP backend",
        501,
      );
    },

    close(): Promise<void> {
      return redis.quit().then(() => undefined);
    },
  };
}

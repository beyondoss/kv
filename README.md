# beyond/kv

A Redis-compatible key-value store with two-level storage: in-memory S3-FIFO cache backed by a log-structured disk engine over io_uring. Commands arrive via RESP (TCP) or HTTP. Each OS thread runs a fully isolated shard — no cross-thread locking.

## Quick Start

**Run the server:**

```sh
cargo build -p beyond-kv --release
./target/release/beyond-kv \
  --data-dir /var/lib/beyond/kv \
  --resp-port 6379 \
  --http-port 4869
```

**Connect with any Redis client:**

```sh
redis-cli -p 6379 SET greeting world
redis-cli -p 6379 GET greeting
```

**Or use the TypeScript SDK:**

```sh
npm install @beyond.dev/kv
```

```ts
import { createKvClient } from "@beyond.dev/kv";

const kv = createKvClient({ url: "redis://localhost:6379" });

await kv.set("user:1", JSON.stringify({ name: "Alice" }), { ttl: 3600 });
const entry = await kv.get("user:1"); // { value, ttl? }
await kv.delete("user:1");
await kv.close();
```

## Operations

| Operation      | RESP command                                                                               | HTTP                                                                                   |
| -------------- | ------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------- |
| Get            | `GET key`                                                                                  | `GET /namespaces/{ns}/values/{key}`                                                    |
| Set            | `SET key value [EX n] [NX\|XX]`                                                            | `PUT /namespaces/{ns}/values/{key}`                                                    |
| Set if absent  | `SETNX key value`                                                                          | `PUT /namespaces/{ns}/values/{key}?nx=1`                                               |
| Revision (get) | `REVISION key`                                                                             | `X-KV-Revision` response header on GET                                                 |
| Compare+set    | `SETREV key value revision [EX n]`                                                         | `PUT /namespaces/{ns}/values/{key}` + `if-match: <revision>`                           |
| Delete         | `DEL key`                                                                                  | `DELETE /namespaces/{ns}/values/{key}`                                                 |
| Exists         | `EXISTS key`                                                                               | —                                                                                      |
| Get+set        | `GETSET key value`                                                                         | —                                                                                      |
| Get+delete     | `GETDEL key`                                                                               | —                                                                                      |
| Get+expire     | `GETEX key [EX n \| PERSIST]`                                                              | —                                                                                      |
| Increment      | `INCR key` / `INCRBY key n`                                                                | `POST /namespaces/{ns}/values/{key}/incr?delta=n`                                      |
| Decrement      | `DECR key` / `DECRBY key n`                                                                | `POST /namespaces/{ns}/values/{key}/incr?delta=-n`                                     |
| Bulk get       | `MGET k1 k2 ...`                                                                           | parallel requests                                                                      |
| Bulk set       | `MSET k1 v1 k2 v2 ...`                                                                     | parallel requests                                                                      |
| Scan           | `SCAN cursor [MATCH pat] [COUNT n]`                                                        | `GET /namespaces/{ns}/keys?cursor=0&prefix=p`                                          |
| Keys           | `KEYS pattern`                                                                             | —                                                                                      |
| TTL (get)      | `TTL key` / `PTTL key`                                                                     | `X-KV-TTL` response header                                                             |
| TTL (set)      | `EXPIRE key n` / `PEXPIRE key ms` / `EXPIREAT key ts` / `PEXPIREAT key ts` / `PERSIST key` | `X-KV-TTL` request header                                                              |
| Watch          | `WATCH key ...` / `PWATCH prefix ...` / `UNWATCH`                                          | `GET /namespaces/{ns}/watch/{key}` (SSE) / `GET /namespaces/{ns}/watch?prefix=p` (SSE) |
| Namespace      | `SELECT 0–15`                                                                              | path: `/namespaces/{name}/...`                                                         |
| Count          | `DBSIZE`                                                                                   | —                                                                                      |
| Flush          | `FLUSHDB`                                                                                  | —                                                                                      |

TTL is stored as an absolute millisecond timestamp. `EXPIRE`/`PERSIST` update a sidecar map without rewriting the value. Expiry is lazy on access; a background sweep handles L1 eviction.

`INCR`/`INCRBY`/`DECR`/`DECRBY` interpret the stored value as a UTF-8 decimal integer and return the new value as an integer reply. The operation is atomic per shard. Over HTTP, pass a negative `delta` to decrement.

Compare-and-swap: read the current revision with `REVISION key` (RESP) or from the `X-KV-Revision` response header (HTTP), then write conditionally with `SETREV key value revision [EX n]` (RESP) or `PUT` + `if-match: <revision>` (HTTP). The write is atomic — nil on mismatch, the new revision integer on success. The TypeScript SDK exposes this as `kv.set(key, value, { ifMatch: entry.revision })` on both backends.

`WATCH key [SINCE revision]` / `PWATCH prefix [SINCE revision]` deliver RESP3 push messages on key writes — send `HELLO 3` first; `UNWATCH` cancels all subscriptions. `SINCE` replays all mutations recorded after that revision from the append-only log, so reconnecting clients never miss events. Over HTTP, the SSE endpoints do the same via `?since=revision`; closing the connection is equivalent to `UNWATCH`. The TypeScript SDK's `kv.watch(key)` / `kv.watch(prefix, { prefix: true })` works on both backends with automatic reconnect and `since` tracking built in.

## TypeScript SDK

The SDK selects the backend from the URL scheme: `redis://` or `rediss://` → RESP (recommended); `http://` or `https://` → HTTP.

```ts
// RESP — single connection, pipelined MGET/MSET
const kv = createKvClient({ url: "redis://localhost:6379", db: 0 });

// HTTP — stateless, uses fetch
const kv = createKvClient({
  url: "http://localhost:4869",
  namespace: "default",
});
```

**Next.js** — reads `KV_URL` and `KV_DB`/`KV_NAMESPACE` from the environment:

```ts
import { createServerKvClient } from "@beyond.dev/kv/next";

const kv = createServerKvClient();
```

**Pagination:**

```ts
let cursor = "0";
do {
  const result = await kv.list({ prefix: "user:", limit: 100, cursor });
  for (const key of result.keys) { /* ... */ }
  cursor = result.cursor;
} while (cursor !== "0");
```

**Metadata** (HTTP backend only):

```ts
await kv.set("key", "value", { metadata: { tags: ["a", "b"] } });
const entry = await kv.get("key"); // entry.metadata === { tags: ["a", "b"] }
```

## Configuration

| Flag                         | Env var                       | Default     | Description                                                    |
| ---------------------------- | ----------------------------- | ----------- | -------------------------------------------------------------- |
| `--data-dir`                 | `KV_DATA_DIR`                 | —           | Log file directory (required)                                  |
| `--resp-port`                | `KV_RESP_PORT`                | `6379`      | RESP TCP port                                                  |
| `--http-port`                | `KV_HTTP_PORT`                | `4869`      | HTTP port                                                      |
| `--threads`                  | `KV_THREADS`                  | CPU count   | Worker threads (= shards)                                      |
| `--memory-bytes`             | `KV_MEMORY_BYTES`             | `268435456` | Total L1 cache; split evenly across shards                     |
| `--max-value-bytes`          | `KV_MAX_VALUE_BYTES`          | `67108864`  | Max value size                                                 |
| `--max-conns-per-shard`      | `KV_MAX_CONNS_PER_SHARD`      | `10000`     | Concurrent connections per shard                               |
| `--idle-timeout-secs`        | `KV_IDLE_TIMEOUT_SECS`        | `60`        | Close idle connections after N seconds                         |
| `--reclaim-sealed-threshold` | `KV_RECLAIM_SEALED_THRESHOLD` | `4`         | Auto-reclaim when sealed file count exceeds this; `0` disables |
| `--reclaim-interval-secs`    | `KV_RECLAIM_INTERVAL_SECS`    | `300`       | Seconds between auto-reclaim scans                             |

### Reclaim

The log engine appends writes and tombstones sealed files. Reclaim compacts live records from sealed files and unlinks the originals. Trigger manually:

```sh
redis-cli -p 6379 BGREWRITEAOF
```

Or set `--reclaim-sealed-threshold` > 0 for automatic reclaim.

## Namespaces

RESP databases 0–15 map to namespaces: `0` → `default`, `1` → `db1`, …, `15` → `db15`. Each namespace has its own log and index; flush, reclaim, and SCAN are namespace-scoped.

## Benchmarks

Both Beyond KV and Redis ran inside the same Docker container with `--network none` (loopback only). Durability is matched: Beyond uses a WAL without per-write fsync (kernel-flushed); Redis uses AOF `appendfsync everysec`. Each process gets equal memory.

**Environment**

|        |                                           |
| ------ | ----------------------------------------- |
| Host   | Docker Desktop on Apple Silicon (aarch64) |
| Kernel | Linux 6.12.54-linuxkit                    |
| vCPUs  | 8                                         |
| Redis  | 7.0.15                                    |

The benchmark uses an **open-loop load generator** with coordinated-omission correction: requests arrive at a fixed Poisson rate regardless of outstanding responses, so queueing shows up in latency rather than being hidden by a closed-loop driver. _Service latency_ is measured server-side. _Response latency_ includes client-side queueing and is what an application actually observes.

---

### Single-key GET / SET — 1 shard

80% GET / 20% SET · 500k distinct keys · 256-byte values · uniform distribution · 64 connections · 512 MiB memory · response latency (includes client-side queueing)

| Target rate | Beyond p50 | Beyond p99 | Redis p50 | Redis p99 |
| ----------- | ---------: | ---------: | --------: | --------: |
| 10k ops/s   |   1,448 µs |   2,921 µs |  1,530 µs |  4,443 µs |
| 25k ops/s   |   1,397 µs |   8,191 µs |  1,496 µs |  9,375 µs |
| 50k ops/s   |   1,391 µs |   5,847 µs |  1,348 µs |  3,575 µs |
| 100k ops/s  |   1,265 µs |   2,927 µs |  1,310 µs | 29,407 µs |
| 200k ops/s  |   1,463 µs |  83,071 µs |  1,216 µs |  2,849 µs |

Both sustain 200k ops/s. At 100k ops/s, Beyond's response p99 is 2.9 ms vs Redis's 29.4 ms — a 10× advantage as Redis's write tail spikes under mixed load. At 200k ops/s the positions reverse: Redis holds a 2.8 ms p99 while Beyond's queueing grows to 83 ms, approaching single-shard saturation.

---

### Batch MGET / MSET (100 keys/call) — 1 shard

80% MGET / 20% MSET · 500k distinct keys · 256-byte values · 64 connections · 512 MiB memory · response latency (includes client-side queueing)

| Target rate | Beyond calls/s | Beyond keys/s | Beyond rsp p99 | Redis calls/s | Redis keys/s | Redis rsp p99 |
| ----------- | -------------: | ------------: | -------------: | ------------: | -----------: | ------------: |
| 1k calls/s  |            998 |         ~100k |       5,455 µs |           998 |        ~100k |      5,095 µs |
| 3k calls/s  |          3,006 |         ~301k |       5,975 µs |         3,006 |        ~301k |      5,147 µs |
| 6k calls/s  |          6,011 |         ~601k |      12,319 µs |         6,011 |        ~601k |      8,695 µs |
| 10k calls/s |          9,990 |         ~999k |     128,895 µs |         9,989 |        ~999k |     64,191 µs |
| **peak**    |     **13,637** |    **~1.36M** |   3,510,271 µs |        11,844 |       ~1.18M |  8,171,519 µs |

Beyond peaks at **~13.6k calls/s (~1.36M keys/s)** vs Redis's ~11.8k — ~15% more throughput. The tradeoff: Beyond is closer to its ceiling at 10k calls/s, so response tail latency is higher there (129 ms vs 64 ms for Redis) even though service latency is lower. At saturation, both blow out; Beyond blows out ~15% later.

---

### Batch MGET / MSET (100 keys/call) — 4 shards, transparent fan-out

80% MGET / 20% MSET · 500k distinct keys · 256-byte values · 64 connections · 512 MiB memory\
Multi-key commands spanning shards are handled transparently by the server; the client sends standard MGET/MSET.

| Target rate    | Beyond calls/s | Beyond keys/s | Beyond rsp p99 | Redis calls/s | Redis keys/s | Redis rsp p99 |
| -------------- | -------------: | ------------: | -------------: | ------------: | -----------: | ------------: |
| 1k calls/s     |            998 |         ~100k |       6,079 µs |           998 |        ~100k |      5,711 µs |
| 3k calls/s     |          3,006 |         ~301k |      71,743 µs |         3,006 |        ~301k |      4,975 µs |
| 6k calls/s     |          6,010 |         ~601k |      17,855 µs |         6,009 |        ~601k |      5,895 µs |
| 10k calls/s    |          9,990 |         ~999k |      81,407 µs |         9,990 |        ~999k |     59,455 µs |
| 15k calls/s    |         14,973 |         ~1.5M |     138,367 µs |        11,712 |       ~1.17M |  4,263,935 µs |
| **20k (peak)** |     **19,658** |    **~1.97M** |     582,143 µs |        11,318 |       ~1.13M | 10,551,295 µs |

Beyond sustains up to **~15k calls/s (~1.5M keys/s)** before response latency climbs significantly. Redis saturates at ~10k calls/s (~1M keys/s) — at 15k target it delivers only 11.7k calls/s and response p99 blows out to **4.3 seconds**. Beyond delivers **~68% more throughput** at saturation.

---

## Architecture

See [ARCHITECTURE.md](ARCHITECTURE.md) for storage format, data flows, reclaim state machine, and failure modes.

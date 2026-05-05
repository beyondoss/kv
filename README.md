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

| Operation      | RESP command                                                                               | HTTP                                                                   |
| -------------- | ------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------- |
| Get            | `GET key`                                                                                  | `GET /v1/kv/{key}?ns=N`                                                |
| Set            | `SET key value [EX n] [NX\|XX]`                                                            | `PUT /v1/kv/{key}?ns=N`                                                |
| Set if absent  | `SETNX key value`                                                                          | `PUT /v1/kv/{key}?ns=N&nx=1`                                           |
| Revision (get) | `REVISION key`                                                                             | `X-KV-Revision` response header on GET                                 |
| Compare+set    | `SETREV key value revision [EX n]`                                                         | `PUT /v1/kv/{key}?ns=N` + `If-Match: <revision>`                       |
| Delete         | `DEL key`                                                                                  | `DELETE /v1/kv/{key}?ns=N`                                             |
| Get+delete     | `GETDEL key`                                                                               | `DELETE /v1/kv/{key}?ns=N` + `X-KV-Return-Old: 1`                      |
| Exists         | `EXISTS key`                                                                               | `HEAD /v1/kv/{key}?ns=N` (200 / 404)                                   |
| Get+set        | `GETSET key value`                                                                         | `PUT /v1/kv/{key}?ns=N` + `X-KV-Return-Old: 1`                         |
| Get+expire     | `GETEX key [EX n \| PERSIST]`                                                              | `PATCH /v1/kv/{key}?ns=N&ttl=n` + `X-KV-Return-Value: 1`               |
| Increment      | `INCR key` / `INCRBY key n`                                                                | `POST /v1/kv/{key}/incr?ns=N&delta=n`                                  |
| Decrement      | `DECR key` / `DECRBY key n`                                                                | `POST /v1/kv/{key}/incr?ns=N&delta=-n`                                 |
| Bulk get       | `MGET k1 k2 ...`                                                                           | `POST /v1/kv/batch?ns=N`                                               |
| Bulk set       | `MSET k1 v1 k2 v2 ...`                                                                     | `POST /v1/kv/batch?ns=N`                                               |
| Scan           | `SCAN cursor [MATCH pat] [COUNT n]`                                                        | `GET /v1/kv?ns=N&cursor=0&prefix=p`                                    |
| Keys           | `KEYS pattern`                                                                             | `GET /v1/kv?ns=N&prefix=p` (cursor-paginated)                          |
| TTL (get)      | `TTL key` / `PTTL key`                                                                     | `X-KV-TTL` response header on GET / HEAD                               |
| TTL (set)      | `EXPIRE key n` / `PEXPIRE key ms` / `EXPIREAT key ts` / `PEXPIREAT key ts` / `PERSIST key` | `PATCH /v1/kv/{key}?ns=N&ttl=n` (or `ttl_ms`, `ttl_at`, `persist=1`)   |
| Watch          | `WATCH key ...` / `PWATCH prefix ...` / `UNWATCH`                                          | `GET /v1/watch/{key}?ns=N` (SSE) / `GET /v1/watch?ns=N&prefix=p` (SSE) |
| Namespace      | `SELECT 0–15`                                                                              | `?ns=N` query param (0=`default`, 1=`db1`, …, 15=`db15`)               |
| Count          | `DBSIZE`                                                                                   | `GET /v1/kv?ns=N&count=1`                                              |
| Flush          | `FLUSHDB`                                                                                  | `DELETE /v1/kv?ns=N`                                                   |
| Compact        | `BGREWRITEAOF`                                                                             | `POST /v1/admin/compact?ns=N`                                          |

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
let cursor: string | undefined;
do {
  const result = await kv.list({ prefix: "user:", limit: 100, cursor });
  for (const key of result.keys) { /* ... */ }
  cursor = result.nextCursor;
} while (cursor !== undefined);
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
| 10k ops/s   |   1,441 µs |   3,603 µs |  1,495 µs |  3,743 µs |
| 25k ops/s   |   1,442 µs |   3,925 µs |  1,468 µs |  4,005 µs |
| 50k ops/s   |   1,330 µs |   3,675 µs |  1,351 µs |  3,751 µs |
| 100k ops/s  |   1,291 µs |   3,473 µs |  1,333 µs |  3,493 µs |
| 200k ops/s  |   1,457 µs |   4,599 µs |  1,372 µs |  3,493 µs |

Both sustain 200k ops/s with comparable latency throughout. p50 and p99 track closely at every load point. At 200k ops/s Redis holds a slight p99 edge (3.5 ms vs 4.6 ms).

---

### Batch MGET / MSET (100 keys/call) — 1 shard

80% MGET / 20% MSET · 500k distinct keys · 256-byte values · 64 connections · 512 MiB memory · response latency (includes client-side queueing)

| Target rate    | Beyond calls/s | Beyond keys/s | Beyond rsp p99 | Redis calls/s | Redis keys/s | Redis rsp p99 |
| -------------- | -------------: | ------------: | -------------: | ------------: | -----------: | ------------: |
| 1k calls/s     |            998 |         ~100k |       5,127 µs |           998 |        ~100k |      5,739 µs |
| 3k calls/s     |          3,006 |         ~301k |       6,171 µs |         3,006 |        ~301k |      5,247 µs |
| 6k calls/s     |          6,011 |         ~601k |      19,951 µs |         6,011 |        ~601k |      5,363 µs |
| 10k calls/s    |          9,990 |         ~999k |      71,871 µs |         9,989 |        ~999k |     92,351 µs |
| 15k calls/s    |         14,066 |         ~1.4M |   2,279,423 µs |        12,181 |       ~1.22M |  7,147,519 µs |
| **20k (peak)** |     **14,524** |    **~1.45M** |  11,354,111 µs |        12,189 |       ~1.22M | 19,136,511 µs |

Beyond peaks at **~14.5k calls/s (~1.45M keys/s)** vs Redis's ~12.2k — ~19% more throughput. At 10k calls/s, Beyond's response p99 is 72 ms vs Redis's 92 ms. Beyond's single-shard write path shows queuing earlier (6k), while Redis stays clean there; both saturate above 10k, with Beyond sustaining ~19% more throughput at the ceiling.

---

### Batch MGET / MSET (100 keys/call) — 4 shards, transparent fan-out

80% MGET / 20% MSET · 500k distinct keys · 256-byte values · 64 connections · 512 MiB memory\
Multi-key commands spanning shards are handled transparently by the server; the client sends standard MGET/MSET.

| Target rate    | Beyond calls/s | Beyond keys/s | Beyond rsp p99 | Redis calls/s | Redis keys/s | Redis rsp p99 |
| -------------- | -------------: | ------------: | -------------: | ------------: | -----------: | ------------: |
| 1k calls/s     |            998 |         ~100k |       5,655 µs |           998 |        ~100k |      5,407 µs |
| 3k calls/s     |          3,006 |         ~301k |      10,247 µs |         3,006 |        ~301k |      4,567 µs |
| 6k calls/s     |          6,011 |         ~601k |      12,439 µs |         6,011 |        ~601k |     11,591 µs |
| 10k calls/s    |          9,990 |         ~999k |      23,311 µs |         9,989 |        ~999k |     42,335 µs |
| 15k calls/s    |         14,974 |         ~1.5M |      41,983 µs |        12,125 |       ~1.21M |  7,344,127 µs |
| **20k (peak)** |     **19,941** |     **~2.0M** |     279,807 µs |        12,227 |       ~1.22M | 18,939,903 µs |

Beyond sustains up to **~20k calls/s (~2M keys/s)** before response latency climbs significantly. Redis saturates at ~12k calls/s (~1.2M keys/s) — at 15k target it delivers only 12.1k calls/s and response p99 blows out to **7.3 seconds**. Beyond delivers **~65% more throughput** at saturation.

---

## Architecture

See [ARCHITECTURE.md](ARCHITECTURE.md) for storage format, data flows, reclaim state machine, and failure modes.

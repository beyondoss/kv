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

| Operation | RESP command                        | HTTP                                           |
| --------- | ----------------------------------- | ---------------------------------------------- |
| Get       | `GET key`                           | `GET /namespaces/{ns}/values/{key}`            |
| Set       | `SET key value [EX n] [NX\|XX]`     | `PUT /namespaces/{ns}/values/{key}`            |
| Delete    | `DEL key`                           | `DELETE /namespaces/{ns}/values/{key}`         |
| Bulk get  | `MGET k1 k2 ...`                    | parallel requests                              |
| Bulk set  | `MSET k1 v1 k2 v2 ...`              | parallel requests                              |
| Scan      | `SCAN cursor [MATCH pat] [COUNT n]` | `GET /namespaces/{ns}/keys?cursor=0&pattern=*` |
| TTL (get) | `TTL key` / `PTTL key`              | `X-KV-TTL` response header                     |
| TTL (set) | `EXPIRE key n` / `PERSIST key`      | `X-KV-TTL` request header                      |
| Namespace | `SELECT 0–15`                       | path: `/namespaces/{name}/...`                 |
| Count     | `DBSIZE`                            | —                                              |
| Flush     | `FLUSHDB`                           | —                                              |

TTL is stored as an absolute millisecond timestamp. `EXPIRE`/`PERSIST` update a sidecar map without rewriting the value. Expiry is lazy on access; a background sweep handles L1 eviction.

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

Both Beyond KV and Redis ran inside Docker with `--network none` (loopback only) to eliminate network variance. Durability is matched: Beyond uses a WAL without per-write fsync (kernel-flushed); Redis uses AOF `appendfsync everysec`. Each process was given equal memory.

**Environment**

|        |                                           |
| ------ | ----------------------------------------- |
| Host   | Docker Desktop on Apple Silicon (aarch64) |
| Kernel | Linux 6.12.54-linuxkit                    |
| vCPUs  | 8                                         |
| Redis  | 7.0.15                                    |

The benchmark uses an **open-loop load generator**: requests are issued at a fixed target rate regardless of outstanding responses, so queueing and back-pressure show up as latency rather than being hidden by a closed-loop driver. _Service latency_ measures time inside the server (timestamped by the driver on send/receive at the server side). _Response latency_ includes client-side queuing and is what an application actually observes.

---

### Single-key GET / SET - Single thread

80% GET / 20% SET · 100k distinct keys · 256-byte values · uniform distribution · 64 concurrent connections · 256 MiB memory

| Target rate    |       Beyond p50 | Beyond p99 | Beyond p99.9 |        Redis p50 | Redis p99 | Redis p99.9 |
| -------------- | ---------------: | ---------: | -----------: | ---------------: | --------: | ----------: |
| 10k ops/s      |           202 µs |     969 µs |     3,645 µs |           250 µs |  1,622 µs |    6,255 µs |
| 25k ops/s      |           242 µs |     873 µs |     2,919 µs |           262 µs |  1,700 µs |    5,879 µs |
| 50k ops/s      |           256 µs |     747 µs |     2,851 µs |           244 µs |  1,411 µs |    4,431 µs |
| 100k ops/s     |           187 µs |     797 µs |     3,891 µs |           189 µs |  1,265 µs |    3,767 µs |
| **saturation** |                — |          — |            — |                — |         — |           — |
| **281k ops/s** |           201 µs |     572 µs |     2,479 µs | _(not achieved)_ |           |             |
| **286k ops/s** | _(not achieved)_ |            |              |           188 µs |    761 µs |    3,649 µs |

At 10–100k ops/s Beyond's p99 service latency is **40–55% lower** than Redis. Both reach ~280k ops/s at saturation.

---

### Batch MGET / MSET (100 keys/op) — 4 shards

80% MGET / 20% MSET · batch size 100 · 500k distinct keys · 256-byte values · 64 concurrent connections · 512 MiB memory

| Target rate        |    Beyond keys/s | Beyond p50 | Beyond p99 |     Redis keys/s | Redis p50 | Redis p99 |
| ------------------ | ---------------: | ---------: | ---------: | ---------------: | --------: | --------: |
| 1k ops/s           |            ~100k |     576 µs |   3,955 µs |            ~100k |    816 µs |  3,433 µs |
| 3k ops/s           |            ~300k |     597 µs |   5,495 µs |            ~300k |    758 µs |  3,415 µs |
| 6k ops/s           |            ~600k |     896 µs |  14,367 µs |            ~600k |    930 µs |  7,747 µs |
| 10k ops/s          |            ~960k |   5,075 µs |  13,471 µs |            ~960k |  2,221 µs | 10,167 µs |
| **peak sustained** | **~1.1M keys/s** |   5,159 µs |  13,495 µs | **~970k keys/s** |  5,795 µs | 15,007 µs |

At 1–3k ops/s (100–300k keys/s) Beyond has lower p50 service latency than Redis. Both saturate near 1M keys/s with 4 shards; Beyond edges ahead at peak thanks to lock-free per-shard isolation.

---

## Architecture

See [ARCHITECTURE.md](ARCHITECTURE.md) for storage format, data flows, reclaim state machine, and failure modes.

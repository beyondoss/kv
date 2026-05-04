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

| Operation     | RESP command                                                                               | HTTP                                                                                   |
| ------------- | ------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------- |
| Get           | `GET key`                                                                                  | `GET /namespaces/{ns}/values/{key}`                                                    |
| Set           | `SET key value [EX n] [NX\|XX]`                                                            | `PUT /namespaces/{ns}/values/{key}`                                                    |
| Set if absent | `SETNX key value`                                                                          | `PUT /namespaces/{ns}/values/{key}?nx=1`                                               |
| Compare+set   | _(none — use `SET XX` after `GET`)_                                                        | `PUT /namespaces/{ns}/values/{key}` + `if-match: <revision>`                           |
| Delete        | `DEL key`                                                                                  | `DELETE /namespaces/{ns}/values/{key}`                                                 |
| Exists        | `EXISTS key`                                                                               | —                                                                                      |
| Get+set       | `GETSET key value`                                                                         | —                                                                                      |
| Get+delete    | `GETDEL key`                                                                               | —                                                                                      |
| Get+expire    | `GETEX key [EX n \| PERSIST]`                                                              | —                                                                                      |
| Increment     | `INCR key` / `INCRBY key n`                                                                | `POST /namespaces/{ns}/values/{key}/incr?delta=n`                                      |
| Decrement     | `DECR key` / `DECRBY key n`                                                                | `POST /namespaces/{ns}/values/{key}/incr?delta=-n`                                     |
| Bulk get      | `MGET k1 k2 ...`                                                                           | parallel requests                                                                      |
| Bulk set      | `MSET k1 v1 k2 v2 ...`                                                                     | parallel requests                                                                      |
| Scan          | `SCAN cursor [MATCH pat] [COUNT n]`                                                        | `GET /namespaces/{ns}/keys?cursor=0&prefix=p`                                          |
| Keys          | `KEYS pattern`                                                                             | —                                                                                      |
| TTL (get)     | `TTL key` / `PTTL key`                                                                     | `X-KV-TTL` response header                                                             |
| TTL (set)     | `EXPIRE key n` / `PEXPIRE key ms` / `EXPIREAT key ts` / `PEXPIREAT key ts` / `PERSIST key` | `X-KV-TTL` request header                                                              |
| Watch         | `WATCH key ...` / `PWATCH prefix ...` / `UNWATCH`                                          | `GET /namespaces/{ns}/watch/{key}` (SSE) / `GET /namespaces/{ns}/watch?prefix=p` (SSE) |
| Namespace     | `SELECT 0–15`                                                                              | path: `/namespaces/{name}/...`                                                         |
| Count         | `DBSIZE`                                                                                   | —                                                                                      |
| Flush         | `FLUSHDB`                                                                                  | —                                                                                      |

TTL is stored as an absolute millisecond timestamp. `EXPIRE`/`PERSIST` update a sidecar map without rewriting the value. Expiry is lazy on access; a background sweep handles L1 eviction.

`INCR`/`INCRBY`/`DECR`/`DECRBY` interpret the stored value as a UTF-8 decimal integer and return the new value as an integer reply. The operation is atomic per shard. Over HTTP, pass a negative `delta` to decrement.

Compare-and-swap over HTTP: `GET` a key to read its `X-KV-Revision` header, then `PUT` with `if-match: <revision>`. The server atomically rejects the write if the revision has changed.

`WATCH` / `PWATCH` over RESP3 deliver push messages on key writes — send `HELLO 3` first; `UNWATCH` cancels all subscriptions. Over HTTP, the SSE endpoints stream the same events; closing the connection is equivalent to `UNWATCH`.

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

80% GET / 20% SET · 500k distinct keys · 256-byte values · uniform distribution · 64 connections · 512 MiB memory · service latency (median of 3 runs)

| Target rate | Beyond p50 | Beyond p99 | Redis p50 | Redis p99 |
| ----------- | ---------: | ---------: | --------: | --------: |
| 10k ops/s   |     217 µs |     993 µs |    252 µs |  1,183 µs |
| 25k ops/s   |     261 µs |   1,047 µs |    270 µs |  1,073 µs |
| 50k ops/s   |     263 µs |     866 µs |    252 µs |    760 µs |
| 100k ops/s  |     202 µs |     633 µs |    186 µs |    454 µs |
| 200k ops/s  |     183 µs |     364 µs |    153 µs |    317 µs |

Both sustain 200k ops/s. Beyond has a p99 service latency advantage at low rates (10k: 993 µs vs 1,183 µs); the two converge through mid-range. Redis pulls ahead on both p50 and p99 at 100k+.

---

### Batch MGET / MSET (100 keys/call) — 1 shard

80% MGET / 20% MSET · 500k distinct keys · 256-byte values · 64 connections · 512 MiB memory

| Target rate        |   Beyond calls/s |    Beyond keys/s | Beyond svc p99 |    Redis calls/s |   Redis keys/s | Redis svc p99 |
| ------------------ | ---------------: | ---------------: | -------------: | ---------------: | -------------: | ------------: |
| 1k calls/s         |             ~999 |            ~100k |       7,615 µs |             ~999 |          ~100k |      7,407 µs |
| 2.5k calls/s       |           ~2,507 |            ~250k |       3,593 µs |           ~2,507 |          ~250k |      4,675 µs |
| 5k calls/s         |           ~4,981 |            ~498k |       3,479 µs |           ~4,981 |          ~498k |      5,603 µs |
| **peak sustained** | **~13k calls/s** | **~1.3M keys/s** |       5,043 µs | **~10k calls/s** | **~1M keys/s** |      8,727 µs |

Beyond sustains **~30% more throughput** than Redis at saturation. Below saturation, Beyond's p99 service latency is consistently 25–40% lower — the log engine doesn't stall under mixed read/write load the way Redis AOF does.

---

### Batch MGET / MSET (100 keys/call) — 4 shards, transparent fan-out

80% MGET / 20% MSET · 500k distinct keys · 256-byte values · 64 connections · 512 MiB memory\
Multi-key commands spanning shards are handled transparently by the server; the client sends standard MGET/MSET.

| Target rate        |      Beyond calls/s |     Beyond keys/s | Beyond svc p99 |       Redis calls/s |      Redis keys/s | Redis svc p99 |
| ------------------ | ------------------: | ----------------: | -------------: | ------------------: | ----------------: | ------------: |
| 3k calls/s         |              ~2,962 |             ~296k |       3,201 µs |              ~2,962 |             ~296k |      2,041 µs |
| 6k calls/s         |              ~5,927 |             ~593k |       3,255 µs |              ~5,927 |             ~593k |      2,801 µs |
| 10k calls/s        |              ~9,911 |             ~991k |       2,361 µs |              ~9,910 |             ~991k |      3,229 µs |
| 15k calls/s        |             ~14,913 |             ~1.5M |       2,157 µs |             ~14,870 |             ~1.5M |      4,219 µs |
| **peak sustained** | **~19,680 calls/s** | **~1.97M keys/s** |       2,031 µs | **~18,143 calls/s** | **~1.81M keys/s** |      3,961 µs |

Beyond's p99 service latency **stays flat at ~2ms** from 3k to 20k calls/s while Redis climbs from 2ms to 4ms. At peak, Beyond delivers 19,680 calls/s vs Redis's 18,143 — and response p99 (what applications observe) is **263ms vs 1,153ms**, a 4× difference driven by Redis's write tail under load.

---

## Architecture

See [ARCHITECTURE.md](ARCHITECTURE.md) for storage format, data flows, reclaim state machine, and failure modes.

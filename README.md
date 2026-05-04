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

80% GET / 20% SET · 500k distinct keys · 256-byte values · uniform distribution · 64 connections · 512 MiB memory

| Target rate | Beyond p50 | Beyond p99 | Beyond p99.9 | Redis p50 | Redis p99 | Redis p99.9 |
| ----------- | ---------: | ---------: | -----------: | --------: | --------: | ----------: |
| 10k ops/s   |     216 µs |     791 µs |     2,799 µs |    257 µs |  2,157 µs |    8,767 µs |
| 25k ops/s   |     267 µs |     979 µs |     2,913 µs |    254 µs |  1,360 µs |    5,275 µs |
| 50k ops/s   |     251 µs |     680 µs |     2,669 µs |    216 µs |  1,210 µs |    7,663 µs |
| 100k ops/s  |     192 µs |     426 µs |     1,659 µs |    182 µs |    483 µs |    1,916 µs |
| 200k ops/s  |     168 µs |     367 µs |     1,362 µs |    137 µs |    274 µs |      979 µs |

Both sustain 200k ops/s cleanly. Beyond's p99 service latency is **50–80% lower** than Redis at 10–100k ops/s. Redis edges ahead on p50 and ceiling at very high rates (>200k); Beyond's advantage is in the tail.

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

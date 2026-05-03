# beyond-kv Architecture

A high-performance KV store for the Beyond platform. Every VM gets a Redis-compatible KV service at `localhost:6379` — standard Redis clients work with zero config. An HTTP API mirrors the Cloudflare KV shape for edge functions and HTTP-native clients.

Branching is free: GlideFS snapshots the block device at the storage layer. RocksDB's immutable SST files survive CoW forks cleanly; the WAL handles in-flight writes.

---

## Workspace

```
kv/
├── crates/
│   ├── engine/    # KV engine: tiered cache, TTL, namespaces, RocksDB
│   ├── proto/     # RESP command parsing and response building
│   └── server/    # Binary: monoio thread-per-core, RESP + HTTP listeners
└── sdk/
    └── ts/        # TypeScript SDK (@beyond.dev/kv)
```

---

## Thread-per-core Model

The server spawns one worker thread per CPU core. Each worker:

1. Opens its own RocksDB instance at `<data-dir>/shard-{i}/`
2. Wraps it in `Rc<ShardStore>` (no `Arc`, no locks on the hot path)
3. Runs a monoio (`FusionDriver`: io_uring on Linux, kqueue on macOS) event loop
4. Binds both RESP (`:6379`) and HTTP (`:4869`) listeners with `SO_REUSEPORT` — the kernel distributes connections across threads with zero userspace coordination
5. Spawns a background TTL sweeper task (30s interval)

Because each thread owns its data independently, there is no cross-thread contention on reads or writes. The tradeoff: keys are not replicated across shards; a client hitting shard-0 and shard-1 sees different keyspaces. This is acceptable for the VM-local use case — each VM has a single KV service, not a distributed cluster.

---

## Storage: L1 (MemCache) + L2 (RocksDB)

### L1 — S3-FIFO In-memory Cache

Hand-rolled S3-FIFO (~240 lines, `crates/engine/src/cache.rs`). All operations are O(1). Not `Send`/`Sync` — uses `Cell`/`RefCell` for interior mutability, no locking needed within a single worker thread.

**Structure:**
- **Small queue** (10% of capacity) — new entries enter here
- **Main queue** (90% of capacity) — entries promoted from Small when `freq > 0`
- **Ghost set** — hashed keys of recently evicted Small entries; re-insertion before displacement goes directly to Main

**Eviction:** Walk Small front; `freq == 0` → evict and record in ghost; `freq > 0` → reset freq, promote to Main. If still over capacity, walk Main front; `freq > 0` → reset and requeue; `freq == 0` → evict. Main entries are never added to the ghost set.

**Expiry:** Checked lazily on every `get`. Background sweeper calls `sweep_expired` every 30 seconds to batch-remove expired entries without blocking reads.

### L2 — RocksDB

One column family per namespace within a single DB instance per shard. Namespaces: `default` and `db1`–`db15` (matching Redis `SELECT 0`–`15`). Column families share the WAL and compaction infrastructure — one open DB, zero per-namespace file handle overhead.

**Value encoding:** `StoredValue { value: &[u8], expires_at_ms: Option<u64>, metadata: Option<Vec<u8>> }` serialized with postcard. Compact binary format; `Option` is a single discriminant byte.

**Read path:** L1 hit → return (bump freq). L1 miss → RocksDB point lookup → insert into L1 Small. Lazy expiry check on every read.

**Write path:** Write to RocksDB first (durable), then update L1. `WriteBatch` used for multi-key operations (`MSET`, `DEL`).

**TTL at the storage layer:** Expiry is stored as an absolute Unix millisecond timestamp. RocksDB does not natively expire keys; expiry is enforced lazily on read and by the background sweeper. A compaction filter could be added to clean up expired keys during compaction — not yet implemented.

---

## RESP Server (port 6379)

Uses `monoio-codec` `Framed<TcpStream, RespCodec>` from the `beyond-resp` crate. Supports pipelining naturally — each connection loops: decode a command, dispatch, send response, flush.

`HELLO 3` switches the codec to RESP3 via `framed.codec_mut().set_version(Version::Resp3)`.  
`SELECT {n}` updates per-connection namespace state without any store interaction.

**Supported commands:** `GET`, `SET` (EX/PX/EXAT/PXAT/NX/XX/GET/KEEPTTL), `DEL`, `EXISTS`, `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`, `TTL`, `PTTL`, `PERSIST`, `KEYS`, `SCAN`, `MGET`, `MSET`, `GETSET`, `SETNX`, `GETDEL`, `GETEX`, `DBSIZE`, `FLUSHDB`, `PING`, `HELLO`, `SELECT`, `QUIT`, `RESET`.

---

## HTTP Server (port 4869)

Hand-rolled HTTP/1.1 on `monoio-http`. Uses `ServerCodec<TcpStream>` directly — no framework. Request body is filled from the IO source via `fill_payload()` before processing (required for correct HTTP/1.1 keep-alive pipelining).

**Routes:**

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/namespaces/{ns}/values/{key}` | Read value; `X-KV-TTL` and `X-KV-Metadata` response headers |
| `PUT` | `/namespaces/{ns}/values/{key}` | Write value; TTL from `X-KV-TTL` header or `?ttl=` query; `?nx=1` for set-if-not-exists |
| `DELETE` | `/namespaces/{ns}/values/{key}` | Delete (idempotent) |
| `GET` | `/namespaces/{ns}/keys` | Cursor-paginated key list; `?prefix=`, `?cursor=`, `?limit=` |
| `GET` | `/healthz` | Health check |

**Error shape:** `{ "error": "<code>", "message": "..." }` — codes: `not_found`, `conflict`, `method_not_allowed`, `internal_error`.

**List response** (Cloudflare KV-compatible):
```json
{ "keys": [{ "name": "..." }], "cursor": "42", "complete": false }
```
When `complete: true`, `cursor` is omitted.

---

## TypeScript SDK (`sdk/ts/`, `@beyond.dev/kv`)

Thin fetch wrapper — no generated client, no runtime dependencies.

```ts
import { createKvClient } from "@beyond.dev/kv"

const kv = createKvClient({ baseUrl: "http://localhost:4869", namespace: "default" })

await kv.set("hello", "world", { ttl: 60 })
const entry = await kv.get("hello")     // { value: Uint8Array, ttl: 59 }
await kv.delete("hello")
const page = await kv.list({ prefix: "user:" })
```

**Next.js helper:**
```ts
import { createServerKvClient } from "@beyond.dev/kv/next"

// Reads KV_URL and KV_NAMESPACE from env
const kv = createServerKvClient()
```

**Error types:** `KvError` (non-2xx responses) and `KvNotFoundError extends KvError` (thrown by `getOrThrow`).

---

## Key Design Decisions

**`Rc` not `Arc`:** All per-thread state uses `Rc`. Since monoio is single-threaded per worker, there are zero atomic operations on the hot path.

**`SO_REUSEPORT` over a shared accept loop:** The kernel load-balances connections across threads. No userspace queue, no mutex, no thundering herd.

**S3-FIFO over LRU:** Better hit rate on skewed/Zipf workloads (which real KV traffic resembles) with the same O(1) complexity. The ghost set prevents churn on recently-evicted hot keys.

**Postcard over JSON/protobuf:** Binary, zero-copy-friendly, no schema registration. Smaller wire format than JSON for numeric fields.

**One RocksDB per shard (not one per namespace):** Column families share the block cache, WAL, and compaction threads. Adding a namespace is one `create_cf` call, not a new file descriptor set.

# KV Architecture

A Redis-compatible key-value store that takes commands over RESP (TCP) or REST (HTTP), executes them against a two-level storage hierarchy (in-memory S3-FIFO cache + a log-structured per-namespace engine on local disk via io_uring), and returns results. Each OS thread runs a fully isolated shard — no cross-thread locking, no shared mutable state.

## Data Flow

### RESP Write Path (SET)

```
TCP Client
  │
  ▼
RespCodec (beyond_resp)     ← RESP2/RESP3 framing
  │ RESP Array → Bytes
  ▼
Command::parse()            ← command.rs  — stack-allocated parsing, arity check
  │ Command::Set { key, value, args }
  ▼
dispatch()                  ← dispatch.rs — NX/XX condition, TTL conversion
  │ SetOptions { ttl: Duration, metadata }
  ▼
ShardStore::set()           ← store.rs (async)
  ├─ record::encode(tstamp, flags, expires_at_ms, key, value, metadata)
  ├─ NamespaceLog::put_full → active_file.append(buf) → fsync   ← L2 write (io_uring)
  └─ MemCache::insert(key, value, ...)                          ← L1 write
  │
  ▼
r::ok()                     ← response.rs
  │
  ▼
TCP Client
```

### RESP Read Path (GET)

```
TCP Client
  │
  ▼
Command::Get { key }
  │
  ▼
ShardStore::get() (async)
  ├─ MemCache::get(key, now_ms)  ── hit? ──► check expiry ──► return Entry  (L1 fast path)
  │                                                │ expired
  │                                                ▼
  │                                  remove from L1, append tombstone, return None
  │
  └─ miss? ──► NsIndex::get(key)
                 ├─ None ──────────────────────────────────────────► return None
                 ├─ expired (TTL sidecar) ──► append tombstone ────► None
                 └─ live ──► file.read_at(record_offset, record_size)  (single io_uring SQE)
                                ├─ parse header → slice value/metadata
                                └─ MemCache::insert ──► return Entry
  │
  ▼
r::bulk(entry.value) or r::nil()
  │
  ▼
TCP Client
```

### HTTP Path

```
HTTP Client
  │
  ▼
http.rs router
  ├─ GET    /v1/kv/{key}                        → ShardStore::get()          → 200 + X-KV-Revision / X-KV-TTL / X-KV-TTL-MS / X-KV-Metadata
  ├─ HEAD   /v1/kv/{key}                        → ShardStore::get()          → 200 (headers only) / 404
  ├─ PUT    /v1/kv/{key}                        → ShardStore::set() / setnx() / setxx()
  ├─ PUT    /v1/kv/{key} + If-Match             → ShardStore::setrev()       → 204 + X-KV-Revision / 409 conflict
  ├─ PATCH  /v1/kv/{key}?ttl=n                  → ShardStore::expire()       → 204 / 404
  ├─ PATCH  /v1/kv/{key}?persist=1              → ShardStore::persist()      → 204 / 404
  ├─ PATCH  /v1/kv/{key} + X-KV-Return-Value   → ShardStore::getex()        → 200 with value body
  ├─ DELETE /v1/kv/{key}                        → ShardStore::del()
  ├─ DELETE /v1/kv/{key} + If-Match             → ShardStore::delrev()       → 204 / 409 conflict
  ├─ POST   /v1/kv/{key}/incr?delta=n           → ShardStore::incr()
  ├─ GET    /v1/kv                              → ShardStore::scan() (cursor-paginated)
  ├─ GET    /v1/kv?count=1                      → ShardStore::db_size()
  ├─ DELETE /v1/kv                              → ShardStore::flush_db()
  ├─ POST   /v1/kv/batch                        → mixed ops: get/set/delete/incr/exists (cross-shard fan-out)
  ├─ GET    /v1/watch/{key}                     → SSE stream (exact key)
  ├─ GET    /v1/watch?prefix=…                  → SSE stream (prefix, all shards)
  ├─ POST   /v1/admin/compact                   → ShardStore::reclaim()
  ├─ GET    /livez                             → 200 OK (liveness)
  └─ GET    /readyz                            → 200 OK | 503 degraded (readiness)
  │
  ▼
HTTP Client
```

### TTL Expiry

```
Lazy (on access):
  ShardStore::get/ttl/del
    └─ TTL sidecar: expires_at_ms ≤ now_ms?  ──► append tombstone + evict L1 ──► None

Background (every 30s per thread):
  ShardStore::sweep_cache()
    └─ MemCache::sweep_expired(now_ms)  ← L1-only; on-disk records linger until reclaim
```

### EXPIRE / PERSIST (TTL-update record)

EXPIRE and PERSIST do not rewrite the value. They append a tiny `TTL_UPDATE` record (~50 bytes — header + key only, no value bytes) and update the in-RAM TTL sidecar. On replay, an orphan TTL-update for a key that isn't in the rebuilt index is silently ignored. This makes EXPIRE/PERSIST O(1) regardless of value size, matching Redis semantics.

### SCAN Pagination

Single-shard deployments use a simple per-shard cursor:

```
SCAN 0 MATCH user:* COUNT 100
  │
  ▼
ShardStore::scan(cursor="0", pattern, count=100)
  ├─ "0" → iterator from beginning of keyspace
  ├─ iterate: skip expired, glob-match against pattern
  ├─ collect up to count matching keys
  └─ hit count? → next_cursor = b"\x01" + last_key
     exhausted? → next_cursor = "0"  (signals completion)
  │
  ▼
[cursor_bytes, [key1, key2, ...]]
  │
  ▼
SCAN <next_cursor> MATCH user:* COUNT 100   ← client loops until cursor == "0"
```

Multi-shard deployments use a compound cursor that encodes the target shard index alongside the per-shard cursor:

```
b"0"                       → start of iteration (shard 0, inner cursor "0")
b"\x02" + [shard: u8] + [per-shard cursor bytes]  → continuation
```

SCAN iterates shards sequentially: when a shard's inner cursor returns `"0"` (exhausted), the next call begins at shard+1 with inner cursor `"0"`. When all shards are exhausted the outer cursor returns `"0"`. Single-shard deployments never produce the `\x02` prefix — the cursor format is unchanged for existing single-shard deployments.

`KEYS` and `DBSIZE` fan out to all shards in parallel via `CrossShardRequest::AllKeys` / `CrossShardRequest::DbSize` and merge the results. `FLUSHDB` fans out via `CrossShardRequest::FlushDb` and awaits all shards before returning.

## Concepts & Terminology

| Term                   | What It Controls                                                                                                                                                                                | NOT                                                                                               |
| ---------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| Namespace (`ns`)       | Which `NamespaceLog` (and therefore which on-disk directory) receives reads/writes; set by `SELECT <n>` (RESP, any non-negative integer) or `/namespaces/{ns}/` (HTTP); max 1024 open per shard | Not an auth or tenant boundary                                                                    |
| Shard / ShardStore     | One independent storage unit per OS thread — lazily-opened `NamespaceLog` per namespace + L1 cache                                                                                              | A partition of the keyspace: a key lives on exactly one shard, picked by `FxHash(key) % n_shards` |
| L1 / MemCache          | In-process S3-FIFO cache that short-circuits disk reads                                                                                                                                         | Not write-through durable storage                                                                 |
| L2 / NamespaceLog      | Persistent on-disk store; in-RAM hash index over an append-only log file; authoritative source of truth                                                                                         | Not the hot path for reads after first access                                                     |
| Active file            | The currently-writable log file. Records are appended, fsynced, then made visible via the index                                                                                                 | Not modified in place; only appended                                                              |
| Sealed file            | A previously-active file that has been merged through reclaim. Read-only, has a footer of live entries                                                                                          | Not deleted until reclaim runs again                                                              |
| Ghost Set              | MemCache tracking of recently evicted keys; a ghost hit promotes the next insert directly to the Main queue                                                                                     | Not a tombstone or deletion marker                                                                |
| Cursor `"0"`           | SCAN sentinel meaning "start from beginning" or "scan complete" — the same value signals both states                                                                                            | Not a literal zero integer                                                                        |
| `\x01`-prefixed cursor | Single-shard continuation cursor: `b"\x01"` + last_key from the previous page                                                                                                                   | Not a user-visible value; internal to scan                                                        |
| `\x02`-prefixed cursor | Multi-shard continuation cursor: `b"\x02"` + `[shard_idx: u8]` + per-shard inner cursor; only emitted when `n_shards > 1`                                                                       | Never produced by single-shard deployments; not a user-visible value                              |

## Core Mechanism

### Threading Model

`main.rs` spawns one OS thread per CPU. Each thread:

1. Starts a Monoio async runtime (io-uring on Linux)
2. Opens its own `ShardStore` (separate data directory under `{data_dir}/shard-{n}/{ns}/data-*.log` + 256 MB L1 cache by default)
3. Spawns three tasks: RESP listener, HTTP listener, cache sweeper

```
[OS Thread 0]  Monoio runtime  ┬─ RESP listener :6379
               ShardStore 0    ├─ HTTP listener :4869
                               └─ cache sweeper (30s)

[OS Thread 1]  Monoio runtime  ┬─ RESP listener :6379
               ShardStore 1    ├─ HTTP listener :4869
                               └─ cache sweeper (30s)
... (N threads)
```

`ShardStore` is `!Sync` (via `Rc<>` wrapping). There is no shared mutable state between threads — each shard owns its slice of the keyspace and has no read or write path into another shard's storage.

The accept loop in `main.rs` peeks the first command's key on each new connection (via `routing.rs:peek_resp_key` / `peek_http_key`) and routes it to the owning shard; the connection is then **pinned** to that shard for its lifetime (Redis-cluster-style). If the key cannot be extracted within a 2 ms window (slow client, or single-element commands like `PING`), the connection falls back to round-robin shard assignment. Single-key commands (GET/SET/DEL/EXISTS/...) execute locally on the pinned shard. Multi-key commands (MGET/MSET/DEL/EXISTS) **fan out across shards** transparently — see "Cross-Shard Fan-Out" below.

### Two-Level Storage

Every read checks L1 first. L1 hits avoid all disk I/O. On L1 miss the engine looks up the key in the in-RAM hash index, then issues a single io_uring `read_at(record_offset, record_size)` against the file holding that record. The header carries `key_size`/`val_size`/`meta_size`, so the value and metadata are sliced out in-memory after the read completes.

Writes go to both levels in order: append + fsync to disk first (durable), then L1 (hot set).

### S3-FIFO Cache (`cache.rs`)

S3-FIFO partitions capacity into a Small queue (10%) and a Main queue (90%):

- **Insert:** New keys enter Small. If the key was recently evicted (ghost hit), it goes directly to Main.
- **Eviction:** Small is evicted FIFO. If the entry's `freq == 1` (accessed at least once since insertion), it's promoted to Main instead of discarded. Main is evicted FIFO, but entries with `freq == 1` get one reprieve (freq reset to 0, placed back in Main).
- **Ghost Set:** A bounded `HashSet` (≈10% of capacity) of recently evicted keys. Prevents one-hit wonders from polluting Main; ensures keys with real reuse skip the Small queue on re-insertion.

Memory accounting tracks `key.len() + value.len() + metadata.len()` per entry. Eviction runs until `current_bytes ≤ max_bytes`.

### Log-Structured Storage Format

Each namespace gets its own directory `{data_dir}/shard-{n}/{ns}/`. Files in that directory are named `data-NNNNNNNNNN.log`. The highest-numbered file is the active (writable) one; lower-numbered files are sealed (read-only, immutable until reclaim unlinks them).

**Record format** (every key — full record, tombstone, or TTL-update — uses the same header):

```
| crc64 (8) | tstamp_ms (8) | flags (1) | expires_at_ms (8) |
| key_size (4) | val_size (4) | meta_size (4) |
| key bytes | value bytes | metadata bytes |
```

CRC-64/NVME via `crc-fast` covers everything after the CRC field. `flags` carries `TOMBSTONE` (0x01), `NO_EXPIRY` (0x02), `TTL_UPDATE` (0x04). Tombstone and TTL-update records have `val_size = meta_size = 0`.

**In-RAM index** (per namespace): `FxHashMap<Bytes, IndexEntry>`. `IndexEntry` is 24 bytes:

```rust
struct IndexEntry {
    record_offset: u64,
    record_size: u32,
    file_id: u16,
    tstamp_ms: u64, // revision — enables O(1) CAS checks without a disk read
}
```

Plus a TTL sidecar `FxHashMap<Bytes, u64>` so only TTL'd keys pay the extra 16-byte slot.

**Sealed-file footer** (written when the active file is rotated by reclaim): array of `(key, record_offset, record_size, expires_at_ms, tstamp_ms)` entries followed by a 24-byte trailer (body length + CRC + magic `0x4259_4F4E_445F_4B57`). On startup, recovery reads the footer of each sealed file in O(1) and rebuilds the index without scanning the file body. Sealed files with the older magic (`0x4259_4F4E_445F_4B56`, written before the `tstamp_ms` footer field was added) fall back to a full sequential scan of the file body, reading `tstamp_ms` from each record header — no explicit migration needed. The active file's tail (between its last hint checkpoint and EOF) is replayed record-by-record; first bad CRC truncates the active file at the last good boundary.

**Reclaim**: seal the current active file, walk live index entries, copy live records to a new sealed file, write its footer + fsync, atomic-rename, unlink old sealed files. A fresh active file is opened. Triggered two ways: `BGREWRITEAOF` (current namespace, synchronous from the client's perspective) or the auto-reclaim background task (every `KV_RECLAIM_INTERVAL_SECS`, default 300s) which reclaims any namespace whose sealed file count exceeds `KV_RECLAIM_SEALED_THRESHOLD` (default 4, 0 = disabled).

**FLUSHDB** unlinks-and-recreates the namespace's data files (does NOT truncate in place) so CoW sharing with the parent fork's blocks is preserved.

### Command Parsing (`command.rs`)

RESP arrays are parsed into a `Command` enum with zero heap allocation for command name matching: command names are compared against 16-byte stack buffers. SET option tokens use 7-byte stack buffers. Arity is checked before any further parsing.

### Expiry

Expiry is stored as an absolute Unix timestamp in milliseconds, in the in-RAM TTL sidecar. On every read, the current time is compared against the sidecar entry. If expired:

- The key is removed from the index and TTL sidecar.
- A tombstone record is appended to the log (so a crash before the next reclaim still observes the deletion on replay).
- L1 is evicted.
- The caller receives `None`.

Expired keys that are never accessed accumulate as dead bytes in the log files until reclaim runs.

### MGET batching

`ShardStore::mget` resolves all keys in-RAM (index + L1 lookup), then submits the cold-read futures concurrently via `futures_util::future::join_all`. io_uring sees them as a batch of SQEs and processes them in parallel rather than serialising one round-trip per key. This is the load-bearing optimization for batched-GET throughput; a 100-key MGET completes in ≈ one disk round-trip instead of N.

### Cross-Shard Fan-Out

A connection is pinned to one shard, but multi-key commands (MGET, MSET, DEL, EXISTS) routinely receive keys whose hashes span multiple shards. Rather than reject those with `CROSSSLOT` (Redis Cluster's behavior), the dispatcher transparently fans them out.

- Each shard exposes one inbound `futures_channel::mpsc::Receiver<CrossShardRequest>` (capacity `1024`). Senders are shared across all shards via `Arc<[Sender]>` on `ConnState`.
- `crates/server/src/cross_shard.rs` runs a per-shard task that drains the inbox; each request is `monoio::spawn`ed so a slow store op (e.g. cold MGET reads) doesn't block the next inbound request.
- Reply channel is `futures_channel::oneshot` per request — light, single-use, `Send`. Cross-thread waker support requires monoio's `sync` feature.
- The dispatcher (`crates/server/src/dispatch.rs`) buckets keys by `shard_for_key`. The local subset runs against the pinned shard's `ShardStore`; foreign subsets are sent over the channel. Results are reassembled by original key index for MGET (which must preserve order); DEL/EXISTS reduce to a count on the receiving shard so only the count crosses the channel.
- Fast path: when `n_shards == 1` or every key already hashes to the connection's shard, dispatch skips bucketing and calls the local store directly.

**MSET is not atomic across shards.** A single-shard MSET still uses one fsynced write (atomic), but a cross-shard MSET applies each shard's subset independently — a crash between sub-replies leaves some keys written and others not. This matches Redis Cluster's MSET semantics.

### SCAN Glob Matching

Pattern matching uses a stack-based backtracking algorithm that handles `*` (any sequence) and `?` (single character). No heap allocation; runs inline during RocksDB iteration. See `store.rs:glob_match()`.

### Watch / Subscribe

Clients can subscribe to mutations on a key or a key prefix and receive a live stream of events. The mechanism is the same for both transports; only the framing differs.

**Revision** — every log record's `tstamp_ms` field doubles as a revision ID. No separate counter. Revisions are monotonically increasing per-shard and are included in every `WatchEvent`, enabling resumable subscriptions.

**WatchRegistry** (`engine/src/watch.rs`) — one per `ShardStore`, owned behind `RefCell` (no locking needed; single-threaded per shard). Holds two tables:

- `keys: FxHashMap<(ns, key), Vec<UnboundedSender<WatchEvent>>>` — exact-key watchers
- `prefixes: Vec<((ns, prefix), UnboundedSender<WatchEvent>)>` — prefix watchers scanned linearly on each write

After each successful `set`, `mset`, or `del`, the store calls `WatchRegistry::notify`. Dead senders (disconnected clients) are pruned lazily on the next notify.

**Initial state delivery** (`watch_subscribe`):

- `since == 0` → call `NamespaceLog::current_entries` — reads the live index + fetches values from disk for matching keys. Delivers the current state snapshot immediately.
- `since > 0` → call `NamespaceLog::scan_since` — scans all log files in `file_id` order to replay mutations with `tstamp_ms > since`. Used by clients that reconnect after a brief disconnection to catch up without missing writes.

**RESP3 transport** — `WATCH key [key ...] [SINCE <revision>]` and `PWATCH prefix [SINCE <revision>]` intercept in `handle_conn` before `dispatch()`. They require RESP3 (`HELLO 3` first). Initial events are sent as Push frames, followed by a `>2 watch ready` frame, then a live select loop:

```
>5  watch  set  <key>  <value>  <revision>
>4  watch  del  <key>  ""       <revision>
>2  watch  ready
>2  watch  heartbeat              ← emitted every 30s of silence to keep proxies alive
```

`UNWATCH` breaks the loop. `WATCH` takes over the connection for its lifetime; normal commands cannot be issued while watching.

**Multi-shard fan-out** — `WATCH key1 key2 ...` routes each key to its owning shard via `CrossShardRequest::WatchSubscribe`. `PWATCH prefix` subscribes on every shard (a prefix may match keys on any shard). All per-shard `Receiver<WatchEvent>` streams are merged into a single `SelectAll` and demultiplexed onto the connection. A single-shard deployment (`n_shards == 1`) uses the local path directly without any cross-shard I/O.

**HTTP/SSE transport** — `GET /namespaces/{ns}/watch/{key}` (exact) or `GET /namespaces/{ns}/watch?prefix=...` streams `text/event-stream`. The raw TCP stream is split before any codec is created (`stream.into_split()`), keeping the write half for direct SSE writes. A 25-second heartbeat comment (`: heartbeat`) prevents proxy timeouts. Reconnect with `?since=<revision>` for catch-up replay. SSE event JSON:

```json
{"type":"set","key":"foo","value":"<base64>","ttl":60,"revision":1746312345678}
{"type":"del","key":"foo","revision":1746312345999}
{"type":"ready"}
```

HTTP prefix watch applies the same cross-shard fan-out as RESP3 PWATCH: it subscribes on all shards via `CrossShardRequest::WatchSubscribe` and merges their event streams.

**TypeScript SDK** — `kv.watch(key, opts?)` returns an `AsyncGenerator<KvWatchEvent>`. On reconnect the SDK automatically passes `?since=lastRevision` so no mutations are silently lost. Both the HTTP and RESP3 backends support watch; the TypeScript SDK uses raw TCP with RESP3 push frames for the RESP backend.

**HTTP route table additions:**

```
GET /v1/watch/{key}                → exact-key SSE stream
GET /v1/watch/{key}?since=<rev>    → resumable exact-key stream
GET /v1/watch?prefix=<p>           → prefix SSE stream
GET /v1/watch?prefix=<p>&since=<r> → resumable prefix stream
```

### Compare-And-Swap (CAS)

CAS enables optimistic concurrency control: a write succeeds only if the current revision of the key matches the caller's expected value. Because each shard is single-threaded, the check-then-write is atomic with no race window.

**RESP** — `SET key value REV <n>`:

- If the key exists and its `IndexEntry.tstamp_ms == n` → write proceeds, new revision returned as a RESP integer.
- Otherwise (missing key, expired, or stale revision) → `nil` returned; key unchanged.

**HTTP** — `PUT /namespaces/{ns}/values/{key}` with `If-Match: <n>` request header:

- Match → `204 No Content` + `X-KV-Revision: <new_rev>` response header.
- Mismatch → `409 Conflict` + `{"error":"conflict","message":"revision mismatch"}`.
- `GET` always returns `X-KV-Revision: <n>` so the caller can capture the revision before a CAS write.

**Implementation** — `ShardStore::setrev()`:

1. `ensure_ns()` borrows the in-memory index.
2. Reads `IndexEntry.tstamp_ms` for the key (O(1), no disk read).
3. Expired keys are treated as absent (revision mismatch).
4. On match: write via `put_full()`, notify watchers, return `Ok(Some(new_rev))`.
5. On mismatch: return `Ok(None)` — no write, no disk I/O.

`REV` is mutually exclusive with `NX`/`XX` at the protocol layer.

## State Machines

### Connection Lifecycle (RESP)

```
 accept()
    │
    ▼
┌───────┐
│ OPEN  │ ◄─── default RESP2, ns="default"
└───┬───┘
    │ HELLO n  ──────────► switch codec (RESP2 ↔ RESP3)
    │ SELECT n ──────────► switch ns ("default" | "db1"…"db15")
    │ QUIT     ──────────┐
    │ EOF/error ─────────┤
    ▼                    │
┌────────┐               │
│ CLOSED │ ◄─────────────┘
└────────┘
```

`ConnState` (`resp.rs`) holds `ns`, `resp_version`, `quit`, `shard_idx`, and `n_shards`. The HELLO command is handled before the codec switches so the response uses the old version.

### Key Lifecycle

```
absent ──SET──► live
  live ──GET──► live  (freq bumped in L1; revision returned in X-KV-Revision / Entry.revision)
  live ──DEL──► absent
  live ──expired────► absent  (lazy, on next access or L1 sweep)
  live ──PERSIST──► live (TTL cleared)
  live ──EXPIRE──► live (TTL replaced)
  live ──CAS (rev matches)────► live (new value; revision advances)
  live ──CAS (rev mismatch)───► live (unchanged; 409 / nil returned)
absent ──CAS──────────────────► absent (mismatch; 409 / nil returned)
```

## Why It Behaves This Way

### Why each thread has its own engine instance

Sharing storage across threads would require locking on the index and the active-file write offset. Per-thread instances eliminate that coordination entirely and keep the hot path lock-free. The tradeoff is that the routing layer must pin each client connection to a thread — a key read on thread 0 won't see a write made on thread 1.

Connection routing is built into the server: `peek_resp_key` peeks the first bytes of a new TCP connection (without consuming them), extracts the key from the first command, and runs `FxHash(key) % n_shards` to pick a worker thread. The connection is then pinned to that thread for its lifetime. Multi-key commands (MGET, MSET, DEL, EXISTS) whose keys span shards are transparently fanned out via per-shard request channels (see "Cross-Shard Fan-Out") so the client sees a single response in original key order — no `CROSSSLOT` error.

### Why expiry is lazy rather than proactive

Proactive expiry requires a background scan of all keys, which competes with normal I/O and is expensive at scale. Lazy expiry costs nothing at write time and reclaims memory immediately on access. The background L1 sweep (every 30s) prevents L1 from filling with dead entries; on-disk dead bytes accumulate until reclaim runs.

### Why log-structured (and not RocksDB / LMDB / redb)

The platform runs each instance on a CoW filesystem (GlideFS) where O(1) fork is the load-bearing capability. RocksDB's LSM compaction rewrites SST files in the background regardless of write load — an idle fork diverges from its parent within minutes. mmap-based stores (LMDB, redb) page-fault synchronously on cold reads, stalling the monoio reactor across other tenants on the same core. A custom append-only log + in-RAM index satisfies all nine required properties (idle stability, bounded fork-local growth, async-friendly reads, crash atomicity via per-record CRC, native TTL via the sidecar, single-I/O point lookup, scan without ordering, threshold-triggered reclaim, single-writer-per-shard) without those failure modes.

### Why S3-FIFO instead of LRU

LRU requires updating a linked list on every cache hit (O(1) but with high cache-line contention). S3-FIFO uses FIFO queues (append/pop, no random access) and a single `freq` bit per entry. It performs comparably to LRU on typical access distributions while being significantly cheaper to update under high hit rates.

### Why a hand-rolled record format over postcard / bincode

We control the on-disk format directly because every record gets a fixed-size header and is read via a single `read_at(record_offset, record_size)`. The header carries the CRC, sizes, flags, and TTL inline; downstream parsing is just slicing into the returned buffer. A schema-driven serializer (postcard, bincode) would buy us nothing here and cost an alloc + copy per read. CRC-64/NVME via `crc-fast` is SIMD-accelerated on aarch64/x86_64.

### Why RESP cursor "0" means both start and done

Redis protocol defines SCAN to return "0" when iteration is complete. Reusing "0" as the start sentinel matches the Redis API contract exactly — clients loop `while cursor != "0"` after the first call, which naturally handles both starting and stopping. Internal continuation cursors are prefixed with `\x01` to ensure they can never collide with the literal "0" string.

### Why MSET is atomic (within one shard)

Redis MSET is documented as atomic. Within a single shard this implementation builds one buffer containing every record, calls `write_at(buf, base_offset)` and `fsync()` once, then bulk-updates the index — all keys land or none do. The L1 cache is populated after the disk fsync; in the narrow window between the two, a cache miss correctly falls back to disk and sees all keys.

Across shards (when MSET keys span shard boundaries), atomicity is **not** preserved: each shard's subset commits independently, matching Redis Cluster's semantics.

## Configuration

| CLI Flag / Env Var                                                     | Default              | What It Controls at Runtime                                                                       |
| ---------------------------------------------------------------------- | -------------------- | ------------------------------------------------------------------------------------------------- |
| `--data-dir` / `KV_DATA_DIR`                                           | `/var/lib/beyond/kv` | Root path for all shard directories (`{data_dir}/shard-{n}`)                                      |
| `--resp-port` / `KV_RESP_PORT`                                         | `6379`               | TCP port each thread's RESP listener binds to                                                     |
| `--http-address` / `KV_ADDRESS`                                        | `0.0.0.0:4869`       | Socket address each thread's HTTP listener binds to (full `ip:port`)                              |
| `--threads` / `KV_THREADS`                                             | `num_cpus::get()`    | Number of OS threads (= number of shards)                                                         |
| `--memory-bytes` / `KV_MEMORY_BYTES`                                   | `268435456` (256 MB) | Total L1 cache budget; divided evenly across threads                                              |
| `--max-conns-per-shard` / `KV_MAX_CONNS_PER_SHARD`                     | `10000`              | Per-shard connection cap; connections beyond this are dropped immediately with a busy response    |
| `--idle-timeout-secs` / `KV_IDLE_TIMEOUT_SECS`                         | `60`                 | Seconds of inactivity before a connection is closed                                               |
| `--max-value-bytes` / `KV_MAX_VALUE_BYTES`                             | `67108864` (64 MB)   | Maximum accepted value size; larger bodies are rejected with HTTP 413 or RESP `ERR`               |
| `--reclaim-sealed-threshold` / `KV_RECLAIM_SEALED_THRESHOLD`           | `4`                  | Auto-reclaim a namespace when its sealed file count exceeds this value; `0` disables auto-reclaim |
| `--reclaim-interval-secs` / `KV_RECLAIM_INTERVAL_SECS`                 | `300`                | Seconds between auto-reclaim scans (ignored when threshold is 0)                                  |
| `--readyz-sync-failure-threshold` / `KV_READYZ_SYNC_FAILURE_THRESHOLD` | `3`                  | Consecutive log-sync failures on any shard before `/readyz` returns 503                           |
| `--log-level` / `LOG_LEVEL`                                            | `info`               | `tracing` filter level; set `ENVIRONMENT=development` for pretty-printed logs                     |

## Observability

Metrics are exposed in Prometheus text format at `GET /metrics` on the HTTP port. Each shard registers its own sub-counters; the `Metrics::encode()` call flushes atomic cache counters into the registered `CounterVec` before gathering, so scrape output is always consistent.

| Metric                               | Labels                     | What It Measures                                              |
| ------------------------------------ | -------------------------- | ------------------------------------------------------------- |
| `http_requests_total`                | `method`, `path`, `status` | Total HTTP requests by method, route pattern, and status code |
| `http_request_duration_seconds`      | `method`, `path`           | HTTP request latency histogram (5 ms – 2.5 s buckets)         |
| `kv_ops_total`                       | `op`, `result`             | KV operation count by command name and outcome (ok/err/miss)  |
| `kv_op_duration_seconds`             | `op`                       | KV operation latency histogram (25 µs – 10 s buckets)         |
| `kv_active_connections`              | `shard`, `proto`           | Live client connections per shard and protocol (resp/http)    |
| `kv_cross_shard_ops_total`           | `op`                       | Operations that required cross-shard fan-out                  |
| `kv_cross_shard_op_duration_seconds` | `op`                       | Cross-shard fan-out latency histogram (100 µs – 5 s buckets)  |
| `kv_cache_ops_total`                 | `result` (hit/miss)        | L1 cache lookup outcomes aggregated across all shards         |
| `kv_cache_size_bytes`                | `shard`                    | L1 cache memory in use per shard                              |
| `kv_cache_entries`                   | `shard`                    | L1 cache entry count per shard                                |
| `kv_keys_expired_total`              | `shard`                    | Keys removed by TTL sweep per shard                           |
| `kv_log_sync_failures_total`         | `shard`                    | fsync failures per shard (drives `/readyz` degradation)       |
| `kv_log_sync_duration_seconds`       | `shard`                    | fsync latency histogram (1 ms – 1 s buckets)                  |
| `kv_sealed_segments`                 | `shard`                    | Sealed log files awaiting compaction per shard                |
| `kv_reclaim_runs_total`              | `shard`                    | Completed compaction runs per shard                           |
| `kv_reclaim_files_freed_total`       | `shard`                    | Log files deleted by compaction per shard                     |
| `kv_namespaces_open`                 | `shard`                    | Open namespace count per shard (hard limit: 1024)             |

## Trust Boundaries

**What the server verifies (rejects if invalid):**

- Value size: bodies exceeding `KV_MAX_VALUE_BYTES` are rejected with HTTP 413 or RESP `ERR`.
- Connection count: connections beyond `KV_MAX_CONNS_PER_SHARD` are dropped immediately.
- RESP protocol framing: malformed arrays or bulk strings close the connection.
- CAS preconditions: `If-Match` / `REV` mismatches return 409 / nil without writing.

**What passes through unchecked:**

- Client identity — there is **no authentication**. Any TCP client can read or write any key in any namespace.
- Namespace validity beyond format — `SELECT` accepts any non-negative integer; the namespace directory is created on first write.
- Value content — bytes are stored and returned verbatim; no schema validation.

**Why these boundaries are where they are:**

The server is designed to run inside a trusted network perimeter (the same GlideFS-backed VM host). Authentication, authz, and tenant isolation are delegated to the edge proxy layer that sits in front of the server.

## Failure Modes

| Failure                          | What Actually Happens                                                                                                                           | Recovery                                                            |
| -------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------- |
| Thread panic                     | `panic = "abort"` — process terminates immediately; no unwinding                                                                                | External process supervisor restarts the process                    |
| Disk write error                 | `EngineError::Io` propagated; RESP client receives `ERR` response; connection stays open                                                        | Client retries; underlying disk issue must be resolved externally   |
| CRC mismatch on replay           | `EngineError::CrcMismatch` during recovery — active file truncates at the last good boundary, sealed-file footer falls back to scanning records | Automatic; the offending tail bytes are dropped                     |
| Bad record header                | `EngineError::BadRecord`; treated as the truncation point during replay                                                                         | Affected tail records are lost; older records survive               |
| RESP parse error                 | Connection closed; no response sent                                                                                                             | Client reconnects                                                   |
| HTTP malformed request           | JSON error body `{"error": "...", "message": "..."}` with 4xx status                                                                            | Client fixes request                                                |
| Expired key read                 | Tombstone appended, evicted from L1; `None` returned to caller                                                                                  | Transparent; client sees cache miss                                 |
| Crash during MSET (single shard) | Single fsynced write — either all records land or the partial tail is truncated by recovery's CRC check                                         | No partial state; client can safely retry                           |
| Crash during cross-shard MSET    | Each shard's subset is independent; some shards may have committed before the crash                                                             | Client retries; idempotent overwrites converge to the desired state |
| Crash mid-reclaim                | Old sealed files are still authoritative; tmp file from the partial reclaim is removed on next reclaim                                          | Automatic; no data loss (no rename happened)                        |
| L1 cache over capacity           | Eviction runs inline during insert; oldest Small-queue entries dropped first                                                                    | Automatic; no data loss (L2 is authoritative)                       |

## File Map

| File                               | What It Does                                                                                                                                                |
| ---------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `crates/proto/src/command.rs`      | Parses RESP arrays into `Command` enum; validates arity and option syntax                                                                                   |
| `crates/proto/src/response.rs`     | Builds RESP values (ok, nil, bulk, error, array, hello reply, scan reply)                                                                                   |
| `crates/proto/src/error.rs`        | Protocol-level error variants returned to clients                                                                                                           |
| `crates/engine/src/store.rs`       | `ShardStore`: all storage operations; coordinates L1 + L2; expiry logic; SCAN; bulk MGET                                                                    |
| `crates/engine/src/cache.rs`       | `MemCache`: S3-FIFO in-memory cache; eviction; ghost set; memory accounting                                                                                 |
| `crates/engine/src/types.rs`       | `Entry`, `SetOptions`, `TtlResult`, `ScanPage`                                                                                                              |
| `crates/engine/src/error.rs`       | Storage-level errors (I/O, CRC mismatch, bad record, invalid namespace, metadata JSON)                                                                      |
| `crates/engine/src/log/mod.rs`     | `NamespaceLog`: index + active + sealed files; put_full / put_many / tombstone / ttl_update / bulk_read / flush / reclaim                                   |
| `crates/engine/src/log/file.rs`    | `LogFile`: monoio io_uring file wrapper; append, read_at, write_footer, read_footer                                                                         |
| `crates/engine/src/log/record.rs`  | Record encoding/decoding; CRC-64/NVME via `crc-fast`; flag bits                                                                                             |
| `crates/engine/src/log/index.rs`   | `NsIndex`: hashmap + TTL sidecar + bucket-cursor SCAN                                                                                                       |
| `crates/engine/src/log/recover.rs` | Startup: parse sealed-file footers; clean-shutdown active file has a footer (fast path), crash falls back to CRC-truncating replay                          |
| `crates/engine/src/log/reclaim.rs` | Threshold-triggered merge of sealed files into a new sealed file; also exposed as `BGREWRITEAOF`                                                            |
| `crates/server/src/main.rs`        | Thread spawning; per-thread Monoio runtime + ShardStore initialization                                                                                      |
| `crates/server/src/config.rs`      | CLI arg + env var parsing into `Config`                                                                                                                     |
| `crates/server/src/dispatch.rs`    | Maps `Command` → `ShardStore` calls → RESP response; `ConnState`; cross-shard fan-out for MGET/MSET/DEL/EXISTS                                              |
| `crates/server/src/cross_shard.rs` | `CrossShardRequest` enum (MGet, MSet, Del, Set, Incr, DelRev, SetNx, SetXx, SetRev, GetDel, …) + per-shard receiver loop; `futures_channel::mpsc` transport |
| `crates/engine/src/watch.rs`       | `WatchEvent`, `KeyFilter`, `WatchRegistry` — per-shard subscription registry; dead-sender lazy pruning                                                      |
| `crates/server/src/resp.rs`        | TCP accept loop; RESP framing; connection state machine; `WATCH`/`PWATCH` streaming (RESP3 only)                                                            |
| `crates/server/src/http.rs`        | HTTP route handlers; header/query param extraction; JSON error responses; SSE watch endpoint; batch endpoint                                                |
| `crates/server/src/routing.rs`     | `peek_resp_key` / `peek_http_key` — peek first bytes of a new connection to extract routing key; `shard_for_key` (FxHash); percent-decode for HTTP paths    |
| `crates/server/src/metrics.rs`     | Prometheus metric definitions (`MetricsInner` / `Metrics`); `encode()` flushes atomic cache counters into registered `CounterVec` before gathering          |

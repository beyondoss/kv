# KV Architecture

A Redis-compatible key-value store that takes commands over RESP (TCP) or REST (HTTP), executes them against a two-level storage hierarchy (in-memory S3-FIFO cache + RocksDB), and returns results. Each OS thread runs a fully isolated shard — no cross-thread locking, no shared mutable state.

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
ShardStore::set()           ← store.rs
  ├─ postcard::encode(StoredValue { value, expires_at_ms, metadata })
  ├─ RocksDB::put(cf, key, encoded)         ← L2 write
  └─ MemCache::insert(key, value, ...)      ← L1 write
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
ShardStore::get()
  ├─ MemCache::get(key, now_ms)  ── hit? ──► check expiry ──► return Entry  (L1 fast path)
  │                                                │ expired
  │                                                ▼
  │                               remove from L1 + RocksDB, return None
  │
  └─ miss? ──► RocksDB::get(cf, key)
                 ├─ None ──────────────────────────────────────────► return None
                 └─ Some(bytes) ──► postcard::decode(StoredValue)
                                      ├─ expired? ──► delete RocksDB + skip L1 ──► None
                                      └─ live? ──► MemCache::insert ──► return Entry
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
  ├─ GET    /namespaces/{ns}/values/{key}     → ShardStore::get()
  ├─ PUT    /namespaces/{ns}/values/{key}     → ShardStore::set() / setnx()
  ├─ DELETE /namespaces/{ns}/values/{key}     → ShardStore::del()
  ├─ GET    /namespaces/{ns}/keys             → ShardStore::scan() (paginated)
  └─ GET    /healthz                          → 200 OK
  │
  ▼
HTTP Client
```

### TTL Expiry

```
Lazy (on access):
  ShardStore::get/ttl/del
    └─ expires_at_ms ≤ now_ms? ──► delete RocksDB + evict L1 ──► None

Background (every 30s per thread):
  ShardStore::sweep_cache()
    └─ MemCache::sweep_expired(now_ms)  ← removes L1 entries only, not RocksDB
```

### SCAN Pagination

```
SCAN 0 MATCH user:* COUNT 100
  │
  ▼
ShardStore::scan(cursor="0", pattern, count=100)
  ├─ "0" → RocksDB iterator from column family start
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

## Concepts & Terminology

| Term | What It Controls | NOT |
|------|-----------------|-----|
| Namespace (`ns`) | Which RocksDB column family receives reads/writes; set by `SELECT 0–15` (RESP) or `/namespaces/{ns}/` (HTTP) | Not an auth or tenant boundary |
| Shard / ShardStore | One independent storage unit per OS thread — its own RocksDB instance + L1 cache | Not a partition of data; all shards hold the full key space |
| L1 / MemCache | In-process S3-FIFO cache that short-circuits RocksDB reads | Not write-through durable storage |
| L2 / RocksDB | Persistent on-disk store; authoritative source of truth | Not the hot path for reads after first access |
| Column Family | One per database (0–15); `"default"` for db 0, `"db1"`…`"db15"` for the rest | Not a Redis slot or hash slot |
| Ghost Set | MemCache tracking of recently evicted keys; a ghost hit promotes the next insert directly to the Main queue | Not a tombstone or deletion marker |
| Cursor `"0"` | SCAN sentinel meaning "start from beginning" or "scan complete" — the same value signals both states | Not a literal zero integer |
| `\x01`-prefixed cursor | Continuation cursor: `b"\x01"` + last_key from the previous page | Not a user-visible value; internal to scan |

## Core Mechanism

### Threading Model

`main.rs` spawns one OS thread per CPU. Each thread:
1. Opens its own `ShardStore` (separate RocksDB path + 256 MB L1 cache by default)
2. Starts a Monoio async runtime (io-uring on Linux)
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

`ShardStore` is `!Sync` (via `Rc<>` wrapping). There is no shared mutable state between threads — each is fully autonomous. A routing layer (not in this codebase) is expected to hash client connections to a specific thread so that a given key always lands on the same shard.

### Two-Level Storage

Every read checks L1 first. L1 hits avoid all RocksDB I/O, deserialization, and system call overhead. On L1 miss the engine reads from RocksDB, decodes the `StoredValue`, populates L1, and returns the entry.

Writes go to both levels synchronously: RocksDB first (durable), then L1 (hot set).

### S3-FIFO Cache (`cache.rs`)

S3-FIFO partitions capacity into a Small queue (10%) and a Main queue (90%):

- **Insert:** New keys enter Small. If the key was recently evicted (ghost hit), it goes directly to Main.
- **Eviction:** Small is evicted FIFO. If the entry's `freq == 1` (accessed at least once since insertion), it's promoted to Main instead of discarded. Main is evicted FIFO, but entries with `freq == 1` get one reprieve (freq reset to 0, placed back in Main).
- **Ghost Set:** A bounded `HashSet` (≈10% of capacity) of recently evicted keys. Prevents one-hit wonders from polluting Main; ensures keys with real reuse skip the Small queue on re-insertion.

Memory accounting tracks `key.len() + value.len() + metadata.len()` per entry. Eviction runs until `current_bytes ≤ max_bytes`.

### RocksDB Storage Format

Values are serialized with `postcard` (compact binary, no schema) into:

```rust
struct StoredValue<'a> {
    value:          &'a [u8],
    expires_at_ms:  Option<u64>,   // Unix timestamp in milliseconds
    metadata:       Option<&'a [u8]>,
}
```

LZ4 block compression is enabled. One column family per database (16 total). Shards are separate RocksDB instances with paths like `{data_dir}/shard-{n}`.

### Command Parsing (`command.rs`)

RESP arrays are parsed into a `Command` enum with zero heap allocation for command name matching: command names are compared against 16-byte stack buffers. SET option tokens use 7-byte stack buffers. Arity is checked before any further parsing.

### Expiry

Expiry is stored as an absolute Unix timestamp in milliseconds. On every read, the current time is compared against `expires_at_ms`. If expired:
- The key is deleted from RocksDB and evicted from L1.
- The caller receives `None`.

RocksDB itself has no TTL mechanism in use here; expiry is entirely application-managed. This means expired keys that are never accessed remain on disk until RocksDB compaction or a future read deletes them.

### SCAN Glob Matching

Pattern matching uses a stack-based backtracking algorithm that handles `*` (any sequence) and `?` (single character). No heap allocation; runs inline during RocksDB iteration. See `store.rs:glob_match()`.

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

`ConnState` (`dispatch.rs`) holds `ns`, `resp_version`, and `quit`. The HELLO command is handled before the codec switches so the response uses the old version.

### Key Lifecycle

```
absent ──SET──► live
  live ──GET──► live  (freq bumped in L1)
  live ──DEL──► absent
  live ──expired────► absent  (lazy, on next access or L1 sweep)
  live ──PERSIST──► live (TTL cleared)
  live ──EXPIRE──► live (TTL replaced)
```

## Why It Behaves This Way

### Why each thread has its own RocksDB instance

Sharing one RocksDB across threads requires locking at the compaction and write-batch level even with `MultiThreaded` mode. Per-thread instances eliminate that coordination entirely and keep the hot path lock-free. The tradeoff is that a routing layer must pin each client connection to a thread — a key read on thread 0 won't see a write made on thread 1.

### Why expiry is lazy rather than proactive

Proactive expiry requires a background scan of all RocksDB keys, which competes with normal I/O and is expensive at scale. Lazy expiry costs nothing at write time and reclaims memory immediately on access. The background L1 sweep (every 30s) prevents L1 from filling with dead entries, but RocksDB may hold expired keys until they're accessed or until RocksDB's own compaction runs. Disk usage will be overstated for workloads with many short-lived keys that are never re-read.

### Why S3-FIFO instead of LRU

LRU requires updating a linked list on every cache hit (O(1) but with high cache-line contention). S3-FIFO uses FIFO queues (append/pop, no random access) and a single `freq` bit per entry. It performs comparably to LRU on typical access distributions while being significantly cheaper to update under high hit rates.

### Why postcard over JSON or bincode

postcard produces the most compact binary output of the common Rust serialization crates and is deterministic (no padding, no alignment). It decodes via borrowed slices — the `value` and `metadata` fields in `StoredValue` point directly into the RocksDB buffer without copying. JSON would double or triple storage size and require allocation.

### Why RESP cursor "0" means both start and done

Redis protocol defines SCAN to return "0" when iteration is complete. Reusing "0" as the start sentinel matches the Redis API contract exactly — clients loop `while cursor != "0"` after the first call, which naturally handles both starting and stopping. Internal continuation cursors are prefixed with `\x01` to ensure they can never collide with the literal "0" string.

### Why MSET is atomic

Redis MSET is documented as atomic. This implementation uses a single RocksDB `WriteBatch` — all key/value pairs are written in one `db.write(batch)` call. Either all keys land or none do. The L1 cache is populated after the batch write; in the narrow window between the two a cache miss will correctly fall back to RocksDB and see all keys.

## Configuration

| CLI Flag / Env Var | Default | What It Controls at Runtime |
|--------------------|---------|------------------------------|
| `--data-dir` / `KV_DATA_DIR` | `/var/lib/beyond-kv` | Root path for all RocksDB shard directories (`{data_dir}/shard-{n}`) |
| `--resp-port` / `KV_RESP_PORT` | `6379` | TCP port each thread's RESP listener binds to |
| `--http-port` / `KV_HTTP_PORT` | `4869` | TCP port each thread's HTTP listener binds to |
| `--threads` / `KV_THREADS` | `num_cpus::get()` | Number of OS threads (= number of shards) |
| `--memory-bytes` / `KV_MEMORY_BYTES` | `268435456` (256 MB) | Total L1 cache budget; divided evenly across threads |

## Failure Modes

| Failure | What Actually Happens | Recovery |
|---------|----------------------|----------|
| Thread panic | `panic = "abort"` — process terminates immediately; no unwinding | External process supervisor restarts the process |
| RocksDB write error | `EngineError::RocksDb` propagated; RESP client receives `ERR` response; connection stays open | Client retries; underlying disk issue must be resolved externally |
| Postcard decode error | `EngineError::Encode`; treated as a missing key in callers that swallow the error — a corrupted value becomes invisible | Affected key must be deleted and rewritten |
| RESP parse error | Connection closed; no response sent | Client reconnects |
| HTTP malformed request | JSON error body `{"error": "...", "message": "..."}` with 4xx status | Client fixes request |
| Expired key read | Deleted from RocksDB + L1; `None` returned to caller | Transparent; client sees cache miss |
| Crash during MSET | RocksDB WriteBatch is atomic — either all keys are written or none are | No partial state; client can safely retry |
| L1 cache over capacity | Eviction runs inline during insert; oldest Small-queue entries dropped first | Automatic; no data loss (L2 is authoritative) |

## File Map

| File | What It Does |
|------|-------------|
| `crates/proto/src/command.rs` | Parses RESP arrays into `Command` enum; validates arity and option syntax |
| `crates/proto/src/response.rs` | Builds RESP values (ok, nil, bulk, error, array, hello reply, scan reply) |
| `crates/proto/src/error.rs` | Protocol-level error variants returned to clients |
| `crates/engine/src/store.rs` | `ShardStore`: all storage operations; coordinates L1 + L2; expiry logic; SCAN |
| `crates/engine/src/cache.rs` | `MemCache`: S3-FIFO in-memory cache; eviction; ghost set; memory accounting |
| `crates/engine/src/types.rs` | `Entry`, `SetOptions`, `TtlResult`, `ScanPage` |
| `crates/engine/src/error.rs` | Storage-level errors (RocksDB, encode, I/O, invalid namespace) |
| `crates/server/src/main.rs` | Thread spawning; per-thread Monoio runtime + ShardStore initialization |
| `crates/server/src/config.rs` | CLI arg + env var parsing into `Config` |
| `crates/server/src/dispatch.rs` | Maps `Command` → `ShardStore` calls → RESP response; `ConnState` |
| `crates/server/src/resp.rs` | TCP accept loop; RESP framing; connection state machine |
| `crates/server/src/http.rs` | HTTP route handlers; header/query param extraction; JSON error responses |

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
Command::parse()            ← command.rs — stack-allocated parsing, arity check
  │ Command::Set { key, value, args }
  │   bad arity / unknown option ─────────────────────────────► ERR (connection stays open)
  ▼
dispatch()                  ← dispatch.rs — NX/XX condition, TTL conversion
  │ SetOptions { ttl, metadata }
  │   value > KV_MAX_VALUE_BYTES ─────────────────────────────► ERR 413 / RESP ERR
  ▼
ShardStore::set()           ← store.rs (async)
  │   frozen (handoff seal in progress) ──────────────────────► ERR Frozen
  ├─ NamespaceLog::put_full
  │    ├─ value ≥ 128 KiB → ValueStore::put(value)             ← blob write (io_uring, write-once)
  │    │    io error ──────────────────────────────────────────► ERR propagated to client
  │    ├─ record::encode(tstamp, flags, expires_at_ms, key, value-or-hash, metadata)
  │    └─ active_file.append(buf) → fsync                       ← L2 write (io_uring)
  │         io error → file poisoned ──────────────────────────► ERR; subsequent writes fail until restart
  └─ MemCache::insert(key, value, ...)                          ← L1 write (stores full value)
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
  ├─ MemCache::get(key, now_ms)  ── hit? ──► check expiry ──► return Entry  (L1 fast path; full value)
  │                                                │ expired
  │                                                ▼
  │                                  remove from L1, append tombstone, return None
  │
  └─ miss? ──► NsIndex::get(key)
                 ├─ None ──────────────────────────────────────────────────► return None
                 ├─ expired (TTL sidecar) ──► append tombstone ────────────► None
                 └─ live ──► file.read_at(record_offset, record_size)        (one io_uring SQE)
                                │ parse header → slice value field
                                ├─ VALUE_SEP flag clear: value field IS the value
                                │    └─ MemCache::insert(full value) ──────► return Entry
                                └─ VALUE_SEP flag set: value field is 16-byte hash
                                     └─ ValueStore::get(hash)               (one io_uring SQE)
                                          ├─ blob missing ─────────────────► ERR BadRecord
                                          └─ ok ──► MemCache::insert ──────► return Entry
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

### Startup / Recovery (per shard, per namespace)

```
ShardStore::open()
  └─ for each namespace dir found on disk:
       NamespaceLog::open()
         ├─ recover::open_namespace()
         │    ├─ for each sealed data-*.log (ascending file_id):
         │    │    ├─ read_footer()  ── magic matches? ──► apply_footer_entries()  (O(1), no body scan)
         │    │    │                                         └─ rebuilds index + TTL + valsep sidecars
         │    │    └─ magic mismatch / CRC fail ──► rebuild_from_records()  (full body scan, fallback)
         │    └─ highest file:
         │         ├─ footer present (clean shutdown) ──► treat as sealed, open new active
         │         └─ no footer (crash) ──► replay_active()
         │              └─ scan records; bad CRC → truncate at last good boundary
         ├─ for each live value-separated key: ValueStore::incr_ref(hash)
         └─ ValueStore::sweep_orphans()  ──► delete values/blob-* with no live key reference
```

### Background Durability (per shard, every 1 second)

```
ShardStore::sync_all()
  └─ for each open namespace:
       NamespaceLog::sync()
         └─ unsynced_bytes > 0? ──► active_file.fsync()  (io_uring)
              io error ──► kv_log_sync_failures_total++ ──► /readyz 503 after threshold
```

This IS the durability mechanism — `appendfsync everysec`. Individual writes call `write_all_at` (goes to the OS page cache; not yet on stable storage) and increment `unsynced_bytes`. The 1-second timer is the only thing that calls `fsync`. A crash before the next timer fires can lose up to ~1 second of writes. The meaningful secondary effect is on `/readyz`: fsync failures increment `readyz_sync_failure_count`; once it exceeds `KV_READYZ_SYNC_FAILURE_THRESHOLD` the shard reports degraded and `/readyz` returns 503.

**New-file directory durability.** Whenever a new `data-*.log` is created or renamed into place (fresh namespace, rotate, reclaim, FLUSHDB, clean-shutdown recovery), the engine fsyncs the _namespace directory_ (`file.rs:sync_dir`) so the file's directory entry is durable — not just its bytes. Without this a power loss could leave a created file's fsynced records unreachable (data present, name lost), violating the everysec contract for any file past the first. This runs only on those rare paths, never on the per-write hot path. (Residual assumption: that `fsync` is honest down through the filesystem/GlideFS/hardware stack — not verifiable in software.)

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

| Term                   | What It Controls                                                                                                                                                                                                   | NOT                                                                                                                               |
| ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------- |
| Namespace (`ns`)       | Which `NamespaceLog` (and therefore which on-disk directory) receives reads/writes; set by `SELECT <n>` (RESP, any non-negative integer) or `/namespaces/{ns}/` (HTTP); max 1024 open per shard                    | Not an auth or tenant boundary                                                                                                    |
| Shard / ShardStore     | One independent storage unit per OS thread — lazily-opened `NamespaceLog` per namespace + L1 cache                                                                                                                 | A partition of the keyspace: a key lives on exactly one shard, picked by `FxHash(key) % n_shards`                                 |
| L1 / MemCache          | In-process S3-FIFO cache that short-circuits disk reads                                                                                                                                                            | Not write-through durable storage                                                                                                 |
| L2 / NamespaceLog      | Persistent on-disk store; ordered in-RAM index (`BTreeMap`) over an append-only log file + a blob store; authoritative source of truth                                                                             | Not the hot path for reads after first access                                                                                     |
| Active file            | The currently-writable log file. Records are appended, fsynced, then made visible via the index                                                                                                                    | Not modified in place; only appended                                                                                              |
| Sealed file            | A previously-active file that has been merged through reclaim. Read-only, has a footer of live entries                                                                                                             | Not deleted until reclaim runs again                                                                                              |
| Run / Level            | A run is one sealed file; its level is its size-tier. Reclaim merges `fanout` runs at level L into one run at L+1 — bounds write amplification to O(log N)                                                         | Not persisted: a restart resets every run to level 0                                                                              |
| Write stripe (`wlock`) | One of 64 per-namespace async mutexes; a write locks `stripe[FxHash(key) % 64]` for check→append→commit. Serializes same-key writes; makes CAS/INCR atomic                                                         | Not cross-thread (shard is single-threaded); not taken by reads; not per-key (stripes are shared, collisions just over-serialize) |
| Value separation       | A value ≥ `value_sep_threshold` (128 KiB) is stored in the content-addressed blob store; the log record carries only its 16-byte hash, so compaction never re-uploads the value                                    | Not applied to small values (they stay inline); not a per-key dedup of small data                                                 |
| Blob                   | An immutable, content-addressed value file at `values/blob-{hash}`; refcounted, write-once, deduped across keys; at refcount 0 it is deleted by `collect_garbage` after the next fsync (deferred for crash-safety) | Not mutated in place; not moved by compaction; not deleted eagerly on unref                                                       |
| Ghost Set              | MemCache tracking of recently evicted keys; a ghost hit promotes the next insert directly to the Main queue                                                                                                        | Not a tombstone or deletion marker                                                                                                |
| Cursor `"0"`           | SCAN sentinel meaning "start from beginning" or "scan complete" — the same value signals both states                                                                                                               | Not a literal zero integer                                                                                                        |
| `\x01`-prefixed cursor | Single-shard continuation cursor: `b"\x01"` + last_key from the previous page                                                                                                                                      | Not a user-visible value; internal to scan                                                                                        |
| `\x02`-prefixed cursor | Multi-shard continuation cursor: `b"\x02"` + `[shard_idx: u8]` + per-shard inner cursor; only emitted when `n_shards > 1`                                                                                          | Never produced by single-shard deployments; not a user-visible value                                                              |

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

Every read checks L1 first. L1 hits avoid all disk I/O. On L1 miss the engine looks up the key in the in-RAM index (`BTreeMap`), then issues a single io_uring `read_at(record_offset, record_size)` against the file holding that record. The header carries `key_size`/`val_size`/`meta_size`, so the value and metadata are sliced out in-memory after the read completes. If the record's `VALUE_SEP` flag is set, the sliced "value" is a 16-byte hash and one additional blob read fetches the value — still O(1), since the hash came straight from the record. The blob is then re-hashed and checked against that content hash before being returned (parity with the CRC the inline path verifies on every read) — silent blob corruption or a blob/hash mismatch surfaces as an error instead of wrong data.

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

CRC-64/NVME via `crc-fast` covers everything after the CRC field. `flags` carries `TOMBSTONE` (0x01), `NO_EXPIRY` (0x02), `TTL_UPDATE` (0x04), `VALUE_SEP` (0x08). Tombstone and TTL-update records have `val_size = meta_size = 0`. A `VALUE_SEP` record's "value bytes" are a 16-byte content hash, not the value — the value lives in the blob store (see [Value Separation](#value-separation)).

**In-RAM index** (per namespace): `BTreeMap<Bytes, IndexEntry>` (ordered, so SCAN is a range walk). `IndexEntry` is 24 bytes:

```rust
struct IndexEntry {
    record_offset: u64,
    record_size: u32,
    file_id: u32, // u32 (not u16): file IDs are never reused, so a hot namespace
    // must not exhaust them — u32 ≈ unbounded; still packs to 24 B
    tstamp_ms: u64, // revision — enables O(1) CAS checks without a disk read
}
```

Two FxHashMap sidecars, each paid only by the keys that need it: a TTL sidecar `FxHashMap<Bytes, u64>` (TTL'd keys) and a value-separation sidecar `FxHashMap<Bytes, [u8;16]>` mapping a large-value key to its blob hash (used to unref the old blob on overwrite/delete and to rebuild blob refcounts on recovery).

**Sealed-file footer** (written when a file is sealed by reclaim): one entry per live key — `(key, record_offset, record_size, expires_at_ms, tstamp_ms, value_hash?)` — followed by a 24-byte trailer (body length + CRC + magic `0x4259_4F4E_445F_4B58`, "BYOND_KX" v3). The `value_hash` (present only for value-separated keys) is carried in the footer so recovery rebuilds both the index and the value-sep sidecar in O(1) without reading record bodies. On startup, recovery reads each sealed file's footer; if the magic doesn't match (older format or crash mid-seal), it falls back to a full record scan — which still repopulates the value-sep sidecar from each record's `VALUE_SEP` flag. The active file's tail is replayed record-by-record; first bad CRC truncates the active file at the last good boundary. After the index is rebuilt, blob refcounts are reconstructed by walking the value-sep sidecar (one `incr_ref` per live large-value key).

**Reclaim (compaction)** is **size-tiered** — one strategy, no flag (`reclaim_inner`). Triggered two ways: `BGREWRITEAOF` (current namespace, synchronous from the client's perspective) or the auto-reclaim background task (every `KV_RECLAIM_INTERVAL_SECS`, default 300s) which reclaims any namespace whose sealed file count exceeds `KV_RECLAIM_SEALED_THRESHOLD` (default 4, 0 = disabled).

A reclaim seals the active file as a fresh level-0 run, then repeatedly finds the lowest level holding ≥ `fanout` (`KV_COMPACTION_FANOUT`, default 8) runs and merges just those into one run at the next level, cascading upward (`reclaim_namespace` copies each live record's bytes verbatim into the merged file and unlinks its inputs). Each reclaim rewrites **one level, not the whole live set** — O(log N) amortized write amplification. On GlideFS this matters directly: a reclaim re-uploads one level's worth of bytes to S3, not the entire namespace.

**Reclaim does not error writes.** A write that arrives during a reclaim _waits_ (`await_reclaim`, before it takes the in-flight count) and proceeds when the reclaim finishes — it never returns `ReclamationBusy` to the client (only a second _concurrent reclaim_ gets that). Before sealing, reclaim **drains in-flight writes** (waits for `in_flight_writes == 0`) so the footer it writes is a consistent snapshot — a write that appended to the active file but hadn't yet updated the index can't be missed from the footer and silently lost on a later footer recovery. `FLUSHDB` uses the same gate + drain (so a write can't race the file replacement). Trade-off: writes _stall_ for the reclaim's duration (standard LSM write-stall, bounded by level size / tunable via `rotate_threshold`·`fanout`), but they always succeed.

`NamespaceLog::compaction_bytes` counts the bytes each reclaim rewrites, so write-amp is directly measurable. Level assignments live in an in-memory `RefCell<FxHashMap<u32,u8>>`; **a restart resets all runs to level 0** (levels are not yet persisted).

> **Why not full-merge** (rewrite the entire live set into one file per reclaim, the classic compacting-log design)? On GlideFS, full-merge re-uploads the whole namespace to S3 on _every_ reclaim — O(live-set) each time. Measured on the real engine over 12 reclaims of a churning ~200-key set, size-tiered rewrote **4.6× fewer bytes** than full-merge would. Point reads don't pay for the extra runs: the in-RAM index resolves each key straight to `(file_id, offset)`, so a GET is one read regardless of run count. Full-merge was removed, not flag-gated.

**Forks need no special handling.** A GlideFS fork is a copy-on-write volume: the child shares the parent's packs and only pays for what it writes. Because reclaim writes merged runs to _new_ offsets (never rewriting a parent's packs) and large values live in immutable blobs the child shares for free, a fork's amplification is bounded by its own divergence with zero fork-awareness in the engine — no "freeze the inherited base" step, no fork-vs-restart detection. (An earlier `freeze_inherited` design was removed: it required a fork hook that doesn't exist and pinned dead inherited data forever, defeating GC.)

**FLUSHDB** unlinks-and-recreates the namespace's data files (does NOT truncate in place) so CoW sharing with the parent fork's blocks is preserved, and drops the namespace's blob store (`values/`).

### Value Separation

A value whose length ≥ `LogConfig::value_sep_threshold` (default **128 KiB = one GlideFS block**) is written to a content-addressed blob store at `{ns}/values/blob-{hash}` instead of inline in the log; the log record carries only the 16-byte BLAKE3-128 hash with the `VALUE_SEP` flag set. Small values stay inline. `value_store.rs` is the store; the wiring lives in `log/mod.rs` (`put_full`/`put_full_cond`/`put_many` separate on write, `read_value`/`bulk_read` deref on read).

```
SET big (256 KiB) ─► blob store: values/blob-<hash>  (262,144 bytes, write-once)
                  └► log record: header + key + 16-byte hash  (≈100 bytes)
GET big ─► index → record (the hash) → blob store get(hash) → value   (still ONE disk read)
```

Behavior, observed on the running binary (256 KiB value):

| event                        | what actually happens                                                                                                                                              |
| ---------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| SET large value              | 262,144 bytes written to `values/`; **log grows ~100 bytes** (the pointer)                                                                                         |
| GET                          | index resolves the record → 16-byte hash → blob read; one read, value returned                                                                                     |
| identical content (any keys) | deduped to **one blob** (content-addressed, write-once)                                                                                                            |
| BGREWRITEAOF / reclaim       | copies the ~100-byte pointer record; the blob is **never touched** — log stays tiny                                                                                |
| overwrite / delete / expire  | the old blob is `unref`'d; at refcount 0 it is **queued**, then physically unlinked by `collect_garbage` after the next fsync (deferred — see durability ordering) |

**Why:** on GlideFS, the cost that matters is _bytes moved by compaction_ — relocating a record to a new offset re-uploads it to S3 (dedup is offset-keyed). Inline, every compaction re-moves the value; a value surviving N reclaims is uploaded N+1 times. Separated, compaction only ever moves the 16-byte pointer; the value is uploaded once and reclaimed by deletion (unlink → whole blocks freed → GlideFS dead-pack GC), never by rewrite. **Measured on the real engine** (60 keys × 32 KiB, 10 churning reclaims): inline moved **22.5 MiB** of compaction bytes, value-separated moved **0.01 MiB** — 3337× less. The threshold is one block because below it a blob-per-value wastes the rest of the block (space-amp explodes); at/above it write-amp collapses to ~1×.

Blob I/O is async on the shard's io_uring reactor — `monoio::fs` `read`/`write`/`remove_file`/`create_dir`/`sync_all` (the `mkdirat`/`unlinkat` features), never a blocking syscall on the hot path. Reads re-hash the blob and verify it against the content hash (integrity, see above). Blob refcounts are in-memory, rebuilt on open from the value-sep sidecar (which the footer/scan recovery repopulates); immediately after, `ValueStore::sweep_orphans` deletes any blob on disk that no live key references. The create and delete of a given content hash are serialized by a per-content **file-op lock** (16 stripes), so `collect_garbage` can never unlink a blob a same-content `put` is concurrently recreating (a by-construction guard against io_uring completion reordering).

**Durability ordering** — the pointer (in the log) and the value (in a blob file) live in _different_ files, so both edges of a blob's lifetime are ordered against the log's fsync:

- **Create before reference.** `put` makes the blob crash-durable _before_ it returns, before the caller writes the pointer record: `write_all_at` → `sync_all` (blob bytes) → fsync the `values/` directory (blob's name). Write-ahead ordering: a durable pointer can never reference a non-durable blob.
- **Delete after the superseding record is durable.** `unref` only drops the refcount and _queues_ the blob; `collect_garbage` (run after each `sync`) physically deletes it. So the old blob of an overwrite/delete survives until the record that superseded it is durable. Were it deleted eagerly, a power loss that lost the superseding record would revert the key to its old value — whose blob would be gone (a dangling pointer). Deferring makes the revert safe.

The log itself is `appendfsync everysec` (≤1 s of writes lost on power loss), so the worst a crash does to a value-separated write is leave an **orphan blob** (durable blob, pointer lost, or queued-but-uncollected) — which `sweep_orphans` reclaims on the next open. **There is no dangling-pointer (durable pointer, missing blob) window.** This is verified exhaustively by the `crash_consistency` test module: `exhaustive_tail_truncation_is_consistent` truncates the un-fsynced tail at **every byte offset** (and includes a value-sep overwrite in the crash zone — the case the deferred-delete fix protects); `corruption_truncates_at_bad_record_keeping_prefix` does the same for single-byte bit-rot of durable records; `torn_footer_falls_back_to_scan_across_files` reclaims to a sealed+active multi-file layout, then cuts the sealed file's footer at every offset to exercise the `read_footer`→record-scan fallback (which rebuilds value-sep state from the `VALUE_SEP` flag). Each asserts a valid recovered prefix with zero dangling pointers and zero blob leaks. The harness has teeth: reintroducing the synchronous-delete bug makes it fail at the exact offset where the overwrite is lost.

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

Pattern matching uses a stack-based backtracking algorithm that handles `*` (any sequence), `?` (single character), and `[abc]` / `[a-z]` / `[^abc]` character classes. No heap allocation; runs inline during the `BTreeMap` range walk — each key is tested as the cursor advances. See `store.rs:glob_match()`.

### Watch / Subscribe

Clients can subscribe to mutations on a key or a key prefix and receive a live stream of events. The mechanism is the same for both transports; only the framing differs.

**Revision** — every log record's `tstamp_ms` field doubles as a revision ID. No separate counter. Revisions are monotonically increasing per-shard (a hybrid logical clock: `max(wall_clock_ms, last_revision + 1)`), so they advance even if two writes land in the same millisecond or the wall clock steps backward mid-run. On open, the clock is **seeded from the highest tstamp recovered**, so revisions stay monotonic across a restart too (a post-restart write can never be assigned a revision ≤ existing data, which would otherwise corrupt `scan_since` watch resumption). Revisions are included in every `WatchEvent`, enabling resumable subscriptions.

**WatchRegistry** (`engine/src/watch.rs`) — one per `ShardStore`, owned behind `RefCell` (no locking needed; single-threaded per shard). Holds two tables:

- `keys: FxHashMap<(ns, key), Vec<UnboundedSender<WatchEvent>>>` — exact-key watchers
- `prefixes: Vec<((ns, prefix), UnboundedSender<WatchEvent>)>` — prefix watchers scanned linearly on each write

After each successful `set`, `mset`, or `del`, the store calls `WatchRegistry::notify`. Dead senders (disconnected clients) are pruned lazily on the next notify.

**Initial state delivery** (`watch_subscribe`):

- `since == 0` → call `NamespaceLog::current_entries` — reads the live index + fetches values from disk for matching keys. Delivers the current state snapshot immediately.
- `since > 0` → call `NamespaceLog::scan_since` — scans all log files in `file_id` order to replay mutations with `tstamp_ms > since`. Used by clients that reconnect after a brief disconnection to catch up without missing writes. Value-separated records are deref'd (and integrity-checked) during the scan, so replayed events carry the real value, not the blob-hash pointer.

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

CAS enables optimistic concurrency control: a write succeeds only if the current revision of the key matches the caller's expected value. The check and the write are atomic — `put_full_cond` holds the key's write stripe across check→append→commit, so no concurrent same-key write can interleave (even at the disk-I/O `.await`). A failed condition writes nothing: it checks before appending, so there is no record on disk for a CAS that returned "no" (this is what makes CAS crash-safe — a failed CAS can never resurrect after a crash).

**RESP** — `SET key value REV <n>`:

- If the key exists and its `IndexEntry.tstamp_ms == n` → write proceeds, new revision returned as a RESP integer.
- Otherwise (missing key, expired, or stale revision) → `nil` returned; key unchanged.

**HTTP** — `PUT /namespaces/{ns}/values/{key}` with `If-Match: <n>` request header:

- Match → `204 No Content` + `X-KV-Revision: <new_rev>` response header.
- Mismatch → `409 Conflict` + `{"error":"conflict","message":"revision mismatch"}`.
- `GET` always returns `X-KV-Revision: <n>` so the caller can capture the revision before a CAS write.

**Implementation** — `ShardStore::setrev()` → `NamespaceLog::put_full_cond(key, …, WriteCondition::Revision(n))`:

1. Acquire the key's write stripe (`wlock`) — held across the whole operation.
2. Check `IndexEntry.tstamp_ms == n` (O(1), no disk read; expired keys count as absent → mismatch).
3. On mismatch: return `Ok(None)` immediately — **no append, no disk I/O, no record**.
4. On match: encode + append + commit + notify watchers, return `Ok(Some(new_rev))`.

Because the stripe is held from the check through the commit, no concurrent same-key write can land in between — the check is authoritative and the failed path leaves nothing on disk.

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
  live ──GET──► live  (freq bumped in L1; revision in X-KV-Revision / Entry.revision)
  live ──DEL──► absent
  live ──expired────► absent  (lazy, on next access or L1 sweep)
  live ──PERSIST──► live (TTL cleared)
  live ──EXPIRE──► live (TTL replaced)
  live ──CAS (rev matches)────► live (new value; revision advances)
  live ──CAS (rev mismatch)───► live (unchanged; 409 / nil returned)
absent ──CAS──────────────────► absent (mismatch; 409 / nil returned)
```

| From   | Event              | To     | Guard                          | What Actually Happens                                                                                                                                           |
| ------ | ------------------ | ------ | ------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| absent | SET                | live   | —                              | Record appended + fsynced; index entry inserted; L1 populated. Large value written to blob store first; record carries 16-byte hash.                            |
| live   | SET (overwrite)    | live   | —                              | New record appended; index entry replaced; L1 updated. Old blob `unref`'d if value-separated; new blob written (dedup: no write if identical content).          |
| live   | SET NX             | live   | key present                    | No write, no disk I/O. Returns nil / 0.                                                                                                                         |
| absent | SET NX             | live   | key absent                     | Same as SET.                                                                                                                                                    |
| live   | SET XX             | live   | key present                    | Same as SET (overwrite).                                                                                                                                        |
| absent | SET XX             | absent | key absent                     | No write. Returns nil / 0.                                                                                                                                      |
| live   | DEL                | absent | —                              | Tombstone appended; index entry removed; L1 evicted. Blob `unref`'d if value-separated → unlinked at refcount 0.                                                |
| live   | EXPIRE             | live   | —                              | TTL_UPDATE record appended; TTL sidecar updated. No value rewrite.                                                                                              |
| live   | PERSIST            | live   | —                              | TTL_UPDATE record (NO_EXPIRY flag) appended; TTL sidecar entry removed.                                                                                         |
| live   | GET (TTL elapsed)  | absent | `now_ms ≥ expires_at_ms`       | Tombstone appended; index + TTL sidecar cleared; L1 evicted. Blob `unref`'d. Caller receives nil.                                                               |
| live   | CAS (rev matches)  | live   | `tstamp_ms == expected`        | Same as SET overwrite. New revision returned.                                                                                                                   |
| live   | CAS (rev mismatch) | live   | `tstamp_ms != expected`        | No write, no disk I/O. 409 / nil returned.                                                                                                                      |
| absent | CAS                | absent | key absent = revision mismatch | No write. 409 / nil returned.                                                                                                                                   |
| live   | FLUSHDB            | absent | —                              | All data files unlinked and recreated; blob store directory removed; index and sidecars cleared. CoW sharing with parent fork preserved (unlink, not truncate). |

## Why It Behaves This Way

### Why each thread has its own engine instance

Sharing storage across threads would require cross-thread locking on the index and the active-file write offset. Per-thread instances eliminate that coordination entirely: **reads are lock-free**, and there is no cross-thread synchronization anywhere. The tradeoff is that the routing layer must pin each client connection to a thread — a key read on thread 0 won't see a write made on thread 1.

Within a shard, writes take one **per-key stripe lock** (64 stripes per namespace, `wlock(key)`) for their check→append→commit. This is _not_ cross-thread (the shard is single-threaded; it's an async mutex serializing the cooperative tasks that interleave at `.await` points). Writes to different keys hash to different stripes and proceed fully concurrently; only same-key writes serialize. It exists so conditional writes (CAS/NX/XX) and read-modify-write (INCR) are atomic on disk — the holder checks before appending, so a failed condition or lost race writes nothing (no orphan record). Reads never take it.

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

Redis MSET is documented as atomic. Within a single shard this implementation builds one buffer containing every record and calls `write_all_at(buf, base_offset)` — a single OS write — then bulk-updates the index atomically. All keys are visible together or not at all: a crash before the next 1-second fsync loses all of them; a crash after leaves all of them. The L1 cache is populated after the write; a cache miss in the narrow window correctly falls back to disk and sees all keys.

Across shards (when MSET keys span shard boundaries), atomicity is **not** preserved: each shard's subset commits independently, matching Redis Cluster's semantics.

## Configuration

| CLI Flag / Env Var                                                     | Default              | What It Controls at Runtime                                                                                                                                                |
| ---------------------------------------------------------------------- | -------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `--data-dir` / `KV_DATA_DIR`                                           | `/var/lib/beyond/kv` | Root path for all shard directories (`{data_dir}/shard-{n}`)                                                                                                               |
| `--resp-port` / `KV_RESP_PORT`                                         | `6379`               | TCP port each thread's RESP listener binds to                                                                                                                              |
| `--http-address` / `KV_ADDRESS`                                        | `0.0.0.0:4869`       | Socket address each thread's HTTP listener binds to (full `ip:port`)                                                                                                       |
| `--threads` / `KV_THREADS`                                             | `num_cpus::get()`    | Number of OS threads (= number of shards)                                                                                                                                  |
| `--memory-bytes` / `KV_MEMORY_BYTES`                                   | `268435456` (256 MB) | Total L1 cache budget; divided evenly across threads                                                                                                                       |
| `--max-conns-per-shard` / `KV_MAX_CONNS_PER_SHARD`                     | `10000`              | Per-shard connection cap; connections beyond this are dropped immediately with a busy response                                                                             |
| `--idle-timeout-secs` / `KV_IDLE_TIMEOUT_SECS`                         | `60`                 | Seconds of inactivity before a connection is closed                                                                                                                        |
| `--max-value-bytes` / `KV_MAX_VALUE_BYTES`                             | `67108864` (64 MB)   | Maximum accepted value size; larger bodies are rejected with HTTP 413 or RESP `ERR`                                                                                        |
| `--reclaim-sealed-threshold` / `KV_RECLAIM_SEALED_THRESHOLD`           | `4`                  | Auto-reclaim a namespace when its sealed file count exceeds this value; `0` disables auto-reclaim                                                                          |
| `--reclaim-interval-secs` / `KV_RECLAIM_INTERVAL_SECS`                 | `300`                | Seconds between auto-reclaim scans (ignored when threshold is 0)                                                                                                           |
| `KV_COMPACTION_FANOUT`                                                 | `8`                  | Size-tiered compaction: a level merges into the next once it holds this many runs (higher = less write-amp, more space-amp); values < 2 ignored                            |
| `KV_VALUE_SEP_THRESHOLD`                                               | `131072` (128 KiB)   | Values ≥ this go to the content-addressed blob store instead of inline; one GlideFS block — below it a blob-per-value wastes space, at/above it write-amp collapses to ~1× |
| `--readyz-sync-failure-threshold` / `KV_READYZ_SYNC_FAILURE_THRESHOLD` | `3`                  | Consecutive log-sync failures on any shard before `/readyz` returns 503                                                                                                    |
| `--log-level` / `LOG_LEVEL`                                            | `info`               | `tracing` filter level; set `ENVIRONMENT=development` for pretty-printed logs                                                                                              |

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

| Failure                                                                     | What Actually Happens                                                                                                                                                                                                                                                                                                   | Recovery                                                                                                                                                                                                                           |
| --------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Thread panic                                                                | `panic = "abort"` — process terminates immediately; no unwinding                                                                                                                                                                                                                                                        | External process supervisor restarts the process                                                                                                                                                                                   |
| Process crash between writes and fsync                                      | Writes in the last ≤1 second (since the previous timer-fsync) are lost — they went to the OS page cache but not stable storage                                                                                                                                                                                          | Up to ~1 second of writes lost; recovery truncates the active file at the last fsynced CRC boundary                                                                                                                                |
| Disk write error                                                            | `EngineError::Io` propagated; RESP client receives `ERR` response; connection stays open                                                                                                                                                                                                                                | Client retries; underlying disk issue must be resolved externally                                                                                                                                                                  |
| CRC mismatch on replay                                                      | `EngineError::CrcMismatch` during recovery — active file truncates at the last good boundary, sealed-file footer falls back to scanning records                                                                                                                                                                         | Automatic; the offending tail bytes are dropped                                                                                                                                                                                    |
| Bad record header                                                           | `EngineError::BadRecord`; treated as the truncation point during replay                                                                                                                                                                                                                                                 | Affected tail records are lost; older records survive                                                                                                                                                                              |
| Value-separated blob corrupted (bit-rot, mismatch)                          | Read re-hashes the blob; on content-hash mismatch returns `EngineError::BadRecord` instead of the wrong bytes (parity with inline CRC)                                                                                                                                                                                  | Detected, not silent; the key reads as an error until the blob is restored/overwritten                                                                                                                                             |
| RESP parse error                                                            | Connection closed; no response sent                                                                                                                                                                                                                                                                                     | Client reconnects                                                                                                                                                                                                                  |
| HTTP malformed request                                                      | JSON error body `{"error": "...", "message": "..."}` with 4xx status                                                                                                                                                                                                                                                    | Client fixes request                                                                                                                                                                                                               |
| Expired key read                                                            | Tombstone appended, evicted from L1; `None` returned to caller                                                                                                                                                                                                                                                          | Transparent; client sees cache miss                                                                                                                                                                                                |
| Crash during MSET (single shard)                                            | All records are built into one buffer and written with a single `write_all_at` — they're atomically visible or not from the OS perspective, but are only on stable storage after the next 1s fsync. A crash before that fsync loses the whole MSET. Recovery truncates the active file at the last fsynced CRC boundary | The MSET either fully lands or is fully absent after recovery — no partial MSET state                                                                                                                                              |
| Crash during cross-shard MSET                                               | Each shard's subset is independent; some shards may have committed before the crash                                                                                                                                                                                                                                     | Client retries; idempotent overwrites converge to the desired state                                                                                                                                                                |
| Crash mid-reclaim                                                           | Old sealed files are still authoritative; tmp file from the partial reclaim is removed on next reclaim                                                                                                                                                                                                                  | Automatic; no data loss (no rename happened)                                                                                                                                                                                       |
| Crash between blob write and log append                                     | The blob is written but no record references it — an **orphan blob** (wasted disk only, never data loss). Recovery doesn't index it (no footer/record points at it)                                                                                                                                                     | `ValueStore::sweep_orphans` at the next open deletes every `values/blob-*` not referenced by a live key. Proven on the binary across a SIGKILL restart                                                                             |
| Power loss after a value-sep overwrite/delete, before its record is durable | The key reverts to its previous value (everysec: the un-fsynced overwrite is lost). The old blob is **still present** — its deletion was deferred until the superseding record's fsync, which didn't happen                                                                                                             | Reads return the old value correctly (no dangling pointer). If the superseding record _was_ durable, the old blob is instead a true orphan → reclaimed by `sweep_orphans`. Exhaustively verified by the crash-consistency tests    |
| Concurrent same-key write races a conditional write (CAS/NX/XX), then crash | **Closed.** Conditional writes hold the key's write stripe and check the condition _before_ appending, so a failed condition writes **no record at all** — there is no optimistic orphan to resurrect. (Previously: an aborted optimistic CAS left a valid orphan record a crash could resurrect.)                      | N/A — the orphan-producing code path was removed, not guarded. Verified by `concurrency_tests::concurrent_mixed_writes_recover_to_runtime_state` (recovery reproduces runtime exactly under heavy same-key CAS/SET/DEL contention) |
| L1 cache over capacity                                                      | Eviction runs inline during insert; oldest Small-queue entries dropped first                                                                                                                                                                                                                                            | Automatic; no data loss (L2 is authoritative)                                                                                                                                                                                      |

## File Map

| File                               | What It Does                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| ---------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `crates/proto/src/command.rs`      | Parses RESP arrays into `Command` enum; validates arity and option syntax                                                                                                                                                                                                                                                                                                                                                                                         |
| `crates/proto/src/response.rs`     | Builds RESP values (ok, nil, bulk, error, array, hello reply, scan reply)                                                                                                                                                                                                                                                                                                                                                                                         |
| `crates/proto/src/error.rs`        | Protocol-level error variants returned to clients                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `crates/engine/src/store.rs`       | `ShardStore`: all storage operations; coordinates L1 + L2; expiry logic; SCAN; bulk MGET                                                                                                                                                                                                                                                                                                                                                                          |
| `crates/engine/src/cache.rs`       | `MemCache`: S3-FIFO in-memory cache; eviction; ghost set; memory accounting                                                                                                                                                                                                                                                                                                                                                                                       |
| `crates/engine/src/types.rs`       | `Entry`, `SetOptions`, `TtlResult`, `ScanPage`                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `crates/engine/src/error.rs`       | Storage-level errors (I/O, CRC mismatch, bad record, invalid namespace, metadata JSON)                                                                                                                                                                                                                                                                                                                                                                            |
| `crates/engine/src/log/mod.rs`     | `NamespaceLog`: index + active + sealed files + blob store; put_full / put_many / tombstone / ttl_update / bulk_read / flush; `reclaim` → `reclaim_inner` (size-tiered); value separation on write (`maybe_separate`/`apply_valsep_insert`) and deref on read; `compaction_bytes`                                                                                                                                                                                 |
| `crates/engine/src/value_store.rs` | `ValueStore`: content-addressed blob store (`values/blob-{hash}`), all I/O async via `monoio::fs` (io_uring); `put` (write-once + dedup, fsync data+dir before returning), `get`, `unref` (refcount-- + queue), `collect_garbage` (delete queued blobs after fsync), `incr_ref` (recovery), `sweep_orphans` (reclaim crash-orphaned blobs at open), `clear` (FLUSHDB); per-content `flock` stripes serialize create/delete; callers re-hash on read for integrity |
| `crates/engine/src/log/config.rs`  | `LogConfig`: `rotate_threshold`, `fanout` (KV_COMPACTION_FANOUT), `value_sep_threshold`                                                                                                                                                                                                                                                                                                                                                                           |
| `crates/engine/src/log/file.rs`    | `LogFile`: monoio io_uring file wrapper; append, read_at; `FooterEntry` (+ `value_hash`) encode/decode + footer magic v3                                                                                                                                                                                                                                                                                                                                          |
| `crates/engine/src/log/record.rs`  | Record encoding/decoding; CRC-64/NVME via `crc-fast`; flag bits                                                                                                                                                                                                                                                                                                                                                                                                   |
| `crates/engine/src/log/index.rs`   | `NsIndex`: `BTreeMap` + TTL sidecar + value-sep hash sidecar + range-cursor SCAN                                                                                                                                                                                                                                                                                                                                                                                  |
| `crates/engine/src/log/recover.rs` | Startup: parse sealed-file footers (incl. `value_hash`); clean-shutdown active file has a footer (fast path), crash falls back to CRC-truncating replay; repopulates the value-sep sidecar                                                                                                                                                                                                                                                                        |
| `crates/engine/src/log/reclaim.rs` | `reclaim_namespace`: merge a set of sealed files into one new sealed file, unlink inputs (called once per level by size-tiered reclaim); also exposed as `BGREWRITEAOF`                                                                                                                                                                                                                                                                                           |
| `crates/server/src/main.rs`        | Thread spawning; per-thread Monoio runtime + ShardStore initialization                                                                                                                                                                                                                                                                                                                                                                                            |
| `crates/server/src/config.rs`      | CLI arg + env var parsing into `Config`                                                                                                                                                                                                                                                                                                                                                                                                                           |
| `crates/server/src/dispatch.rs`    | Maps `Command` → `ShardStore` calls → RESP response; `ConnState`; cross-shard fan-out for MGET/MSET/DEL/EXISTS                                                                                                                                                                                                                                                                                                                                                    |
| `crates/server/src/cross_shard.rs` | `CrossShardRequest` enum (MGet, MSet, Del, Set, Incr, DelRev, SetNx, SetXx, SetRev, GetDel, …) + per-shard receiver loop; `futures_channel::mpsc` transport                                                                                                                                                                                                                                                                                                       |
| `crates/engine/src/watch.rs`       | `WatchEvent`, `KeyFilter`, `WatchRegistry` — per-shard subscription registry; dead-sender lazy pruning                                                                                                                                                                                                                                                                                                                                                            |
| `crates/server/src/resp.rs`        | TCP accept loop; RESP framing; connection state machine; `WATCH`/`PWATCH` streaming (RESP3 only)                                                                                                                                                                                                                                                                                                                                                                  |
| `crates/server/src/http.rs`        | HTTP route handlers; header/query param extraction; JSON error responses; SSE watch endpoint; batch endpoint                                                                                                                                                                                                                                                                                                                                                      |
| `crates/server/src/routing.rs`     | `peek_resp_key` / `peek_http_key` — peek first bytes of a new connection to extract routing key; `shard_for_key` (FxHash); percent-decode for HTTP paths                                                                                                                                                                                                                                                                                                          |
| `crates/server/src/metrics.rs`     | Prometheus metric definitions (`MetricsInner` / `Metrics`); `encode()` flushes atomic cache counters into registered `CounterVec` before gathering                                                                                                                                                                                                                                                                                                                |

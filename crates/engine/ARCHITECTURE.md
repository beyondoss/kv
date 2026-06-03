# Engine Architecture

Takes key-value operations (GET/SET/DEL/SCAN/WATCH/CAS/INCR and their bulk variants) against named namespaces, routes them through an S3-FIFO in-memory cache and a per-namespace append-only log on disk (via io_uring), and pushes change events to registered watch subscribers — all on a single `monoio` thread per shard.

## Data Flow

### Read (GET / MGET)

```
get(ns, key)
  │
  ├─► L1 cache lookup (ns\x00key)
  │     hit  ──► bump freq → return Entry          (no I/O)
  │     miss ──► cache_miss_count++
  │
  ├─► NsIndex::get(key)  [in-RAM BTreeMap]
  │     None   ──► return None
  │     expired ──► tombstone(key) [async, io_uring] → remove from cache → None
  │
  ├─► NamespaceLog::read_value(entry)  [io_uring positioned read]
  │     VALUE_SEP flag? → ValueStore::get(hash) [blob read]
  │     inline?         → slice record bytes
  │
  └─► cache.insert(ns\x00key, value, …) → return Entry

mget: same path; cold reads dispatched concurrently via join_all → parallel io_uring
      expired keys tombstoned in one join_all batch
```

### Write (SET / MSET)

```
set(ns, key, value, opts)
  │
  ├─► ensure_ns(ns)  [open NamespaceLog lazily if first access]
  │
  ├─► KEEPTTL? → index.ttl(key)   else opts.ttl → absolute ms
  │
  ├─► value >= value_sep_threshold?
  │     yes → ValueStore::put(value)  [BLAKE3-128, write-once, fsync blob + dir]
  │             returns content hash (16 bytes) → stored as val in record
  │     no  → value bytes stored inline
  │
  ├─► NamespaceLog::put_full(key, val, meta, expires_at_ms)
  │     RecordHeader::encode() → LogFile::append() → io_uring write_at
  │     NsIndex::insert() ← new offset + tstamp_ms (revision)
  │
  ├─► cache.try_update(ns\x00key) || cache.insert(…)
  │
  └─► WatchRegistry::notify(ns, key, WatchEvent::Set{…})

mset: put_many() batches N records; single fsync for the whole batch
```

### Delete (DEL)

```
del(ns, keys[])
  │
  ├─► snapshot is_expired per key [index borrow, sync]
  │
  ├─► join_all(tombstone(k) for k in keys)  [parallel io_uring]
  │
  ├─► cache.remove(ns\x00k) for each key
  │
  └─► for each key where tombstone returned Some(revision) && !was_expired:
        count++; WatchRegistry::notify(Del{…})
      return count
```

### CAS Write (SETREV / SETNX / SETXX)

```
setrev(ns, key, value, expected_rev)
  │
  ├─► put_full_cond(key, value, meta, ttl, WriteCondition::Revision(expected_rev))
  │     index borrow → check tstamp_ms == expected_rev
  │     mismatch → return None (no write, no append)
  │     match    → append record → update index → return Some(new_revision)
  │
  ├─► hit: cache.try_update + WatchRegistry::notify
  └─► miss: return None (caller sees CAS failure)

setnx  → WriteCondition::KeyAbsent
setxx  → WriteCondition::KeyPresent
delrev → tombstone_cond(key, expected_rev)
```

### INCR (optimistic CAS loop)

```
incr(ns, key, delta)  — up to 64 retries
  │
  ├─► try {
  │     read current value from cache or disk (+ revision + ttl)
  │     parse as i64; add delta; check overflow
  │     put_full_cond(WriteCondition::Revision(rev) or KeyAbsent)
  │       None → CAS lost → retry
  │       Some(t) → update cache; notify watchers; return new_val
  │   }
  └─► 64 failures → EngineError::Conflict
```

### Watch Subscribe

```
watch_subscribe(ns, filter, since)
  │
  ├─► WatchRegistry::subscribe_key/prefix → mpsc::Receiver (cap 512)
  │     (subscribe FIRST — live events start queuing immediately)
  │
  ├─► since == 0 → current_entries(&filter)  [index snapshot of live keys]
  │   since > 0  → scan_since(&filter, since) [replay log records with tstamp > since]
  │
  └─► return (initial_events, receiver)
      caller deduplicates by revision (a write between subscribe + scan appears in both)
```

### Background / Periodic Paths

```
sync_logs()          — fsync all namespaces; appendfsync-everysec timer
sweep_cache()        — bulk-evict expired L1 entries; background timer
reclaim_if_needed()  — compaction: seal + merge when sealed_count > threshold
seal_all_for_shutdown() — freeze all namespaces, drain in-flight, write footers
```

## Concepts & Terminology

| Term             | What It Controls                                                                          | NOT                                                                                              |
| ---------------- | ----------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------ |
| `ShardStore`     | The entire shard: L1 cache + all namespaces + watch registry; public KV API               | Not thread-safe (`!Sync`); lives behind `Rc` on one monoio worker                                |
| Namespace        | An independent key-space with its own log, index, and value store                         | Not a security/tenant boundary — any call can name any namespace                                 |
| Cache key        | `{ns}\x00{key}` — the `\x00` byte separates namespace from key                            | Not the on-disk key; the separator prevents `ns="a", key="bc"` colliding with `ns="ab", key="c"` |
| `MemCache`       | S3-FIFO L1 cache; sized in bytes; evicts cold entries; not persisted                      | Not a write-through buffer — it caches reads and is updated on writes                            |
| Ghost set        | Remembers recently evicted keys; a re-insert goes to Main instead of Small                | Not a negative cache — does not cause misses; only affects queue placement                       |
| `NamespaceLog`   | All reads/writes for one namespace; owns the `NsIndex` and file set                       | See `src/log/ARCHITECTURE.md` for full log internals                                             |
| `ValueStore`     | Content-addressed blob store at `{ns_dir}/values/`; dedup + refcounted                    | Not for small values — only keys with values >= `value_sep_threshold`                            |
| `WatchRegistry`  | In-process pub/sub; holds senders per (ns, key) or (ns, prefix)                           | Not persistent — subscriptions are lost on restart                                               |
| Revision         | `tstamp_ms` of the write record; monotonically increasing; used for CAS and WATCH         | Not a sequence number — two writes on different shards may have the same ms                      |
| `WriteCondition` | Guards appends: `KeyAbsent`, `KeyPresent`, or `Revision(u64)` CAS check                   | The check happens against the in-memory index; serialized by the per-key stripe lock             |
| Frozen           | A per-namespace flag set by `freeze_and_drain()`; new writes return `EngineError::Frozen` | Cleared by `unfreeze()` on handoff abort; not set during normal operation                        |
| `flush()`        | Destroys all namespace files and recreates them (FLUSHDB)                                 | Not fsync — this erases all data in the namespace                                                |

## Core Mechanisms

### L1 Cache (S3-FIFO) — `cache.rs`

New entries enter the **Small** queue (10% of capacity). On eviction from Small:

- `freq == 1` (accessed since insertion) → promoted to **Main** (90%).
- `freq == 0` → evicted and recorded in the **ghost set** (bounded ~10% of est. capacity).

A new insert that is in the ghost set skips Small and goes directly to Main — it has demonstrated re-use and survives the next eviction pass. This prevents one-hit items from polluting Main while protecting hot entries that get evicted under burst pressure.

Cache key is `{ns}\x00{key}`. Keys ≤ 128 bytes total are built on the stack (`with_cache_key` uses a `[u8; 128]` buf) — no heap allocation on the hot read path.

`try_update` updates an existing entry in-place using a borrowed key slice (no alloc). `insert` falls back to an owned `Bytes` key only on cache misses.

### Write Path — no locks, no mutex

`ShardStore` lives on a single `monoio` thread. There are no cross-thread locks on the write path. Concurrency within one thread is cooperative (`.await` yield points only), so index reads and writes between yields are safe.

Per-key CAS serialization (`WriteCondition`) is handled inside `NamespaceLog::put_full_cond`: it takes a short per-key stripe lock (16 stripes), re-checks the condition against the current index entry, and only appends if the condition holds. This makes INCR's optimistic CAS loop correct: a lost race writes nothing (the append is suppressed) and signals a retry via `None`.

### Watch Registry — `watch.rs`

`WatchRegistry` holds:

- `keys: FxHashMap<(ns, key), Vec<Sender>>` for exact-key subscriptions (multiple subscribers per key).
- `prefixes: Vec<((ns, prefix), Sender)>` for prefix subscriptions (scanned linearly on every notify).

`notify` tries to send on all matching senders; `try_send` failure (closed or full channel) prunes the sender lazily. Channel capacity is 512; a slow subscriber that fills its buffer is dropped rather than back-pressuring writes.

Hard cap: 65,536 total live subscriptions. Dead senders are pruned before enforcing the cap, so subscriber churn never produces false capacity errors.

`watch_subscribe` subscribes **before** scanning initial state to prevent a race where a write arrives between scan and subscribe and is missed. Writes that arrive between subscribe and scan appear in both initial events and the live channel; callers deduplicate by revision.

### Namespace Isolation — `store.rs:ensure_ns`

Namespaces are lazily opened on first access. Up to 1,024 namespaces per shard. A post-await dedup prevents concurrent first-opens of the same name from inserting twice. The cap is re-checked after the await for the same reason.

Namespace names: 1–64 bytes, ASCII alphanumeric, `_`, or `-`. Invalid names return `EngineError::InvalidNamespace` immediately without touching the filesystem.

DB index mapping: `db == 0` → `"default"`, `db == n` → `"dn"` (e.g. `db3` → `"db3"`). See `store.rs:ns_name`.

### Glob Matching — `store.rs:glob_match`

SCAN pattern matching supports `*`, `?`, `[abc]`, `[^abc]`, `[a-z]`. A fast path locates the first metacharacter with `memchr3` (SIMD-accelerated); bytes before it are compared as a literal prefix using `memcmp`. The common `"prefix*"` pattern short-circuits after the literal check.

## State Machine

```
      open()
        │
        ▼
    NORMAL ◄─────────────── unfreeze()
   (writable)                   │
        │                       │
freeze_and_drain()          resume_after_abort()
        │                       │
        ▼                       │
    FROZEN ─── seal_all() ──► SEALED
   (writes return                (footer written,
    Frozen error)                ready for handoff)
```

| State  | Writes        | `seal_all_for_shutdown` result | How to Exit                                              |
| ------ | ------------- | ------------------------------ | -------------------------------------------------------- |
| NORMAL | accepted      | n/a                            | `freeze_and_drain()`                                     |
| FROZEN | `Err(Frozen)` | blocks until in-flight drain   | `resume_after_abort()`                                   |
| SEALED | `Err(Frozen)` | footers written                | `resume_after_abort()` reopens active log + `unfreeze()` |

Freeze/seal is a shard-handoff mechanism, not a normal shutdown path. SIGTERM calls `seal_all_for_shutdown` directly (freeze + drain + seal in one call).

## Why It Behaves This Way

### Why the cache uses S3-FIFO instead of LRU

S3-FIFO evicts one-hit items (frequency 0 in Small) immediately without giving them space in Main. This matches the KV workload where a significant fraction of reads are one-shot (bulk imports, range scans, ephemeral keys). A ghost set promotes re-inserts directly to Main so keys that were evicted under burst pressure regain their slot without burning another Small→Main promotion cycle.

LRU would require touching a per-item node on every access (pointer chasing). S3-FIFO sets a single `freq` byte per access and does queue manipulation only on eviction. The `Cell<u8>` freq field is cache-line-friendly.

### Why subscribe happens before scan in `watch_subscribe`

A write arriving between "read current state" and "register receiver" would be invisible: the scan misses it (wrote after snapshot) and the receiver misses it (registered after write). Subscribing first queues all live writes immediately; the initial scan then covers everything written before the subscribe. Duplicates (writes between subscribe and scan) are filtered by revision at the call site.

### Why INCR uses a CAS loop instead of a dedicated lock

INCR needs read-modify-write atomicity. A dedicated per-key mutex would block the event loop thread during the `.await` between read and write. Instead, INCR reads the current revision, computes the new value, then calls `put_full_cond(WriteCondition::Revision(rev))`. If a concurrent INCR wins the stripe and bumps the revision, `put_full_cond` returns `None` and INCR retries with the new value. No lock held across the I/O. 64 retries caps pathological livelock.

### Why cache keys use `\x00` as the namespace separator

Namespaces are ASCII alphanumeric plus `_` and `-`. None of these include `\x00`. Using `\x00` as the separator makes `ns="a", key="bc"` and `ns="ab", key="c"` produce distinct cache keys (`a\x00bc` vs `ab\x00c`) without a separate length prefix. The validation in `is_valid_ns_name` enforces this invariant.

### Why `flush()` unlinks and recreates instead of truncating

A truncate leaves the file descriptor open with length 0; any in-flight read at the old offset would return zeros rather than an I/O error, silently corrupting a response. Unlinking the old file and creating a new one ensures that: (a) in-flight reads on the old fd complete correctly against the old data (the inode stays alive until the last fd closes), and (b) new reads see a clean empty file. It also gives a new inode number that CoW snapshot invalidation can observe.

## Package Structure

| File                 | What It Does                                                                             |
| -------------------- | ---------------------------------------------------------------------------------------- |
| `src/store.rs`       | `ShardStore`: public KV API; routes operations through cache → index → log               |
| `src/cache.rs`       | `MemCache`: S3-FIFO in-memory L1 cache; eviction, ghost set, prefix removal              |
| `src/watch.rs`       | `WatchRegistry`: exact-key and prefix pub/sub; live change events                        |
| `src/value_store.rs` | `ValueStore`: content-addressed blob store for large (value-separated) values            |
| `src/types.rs`       | Shared types: `Entry`, `SetOptions`, `TtlResult`, `GetExOp`, `ScanPage`                  |
| `src/error.rs`       | `EngineError` enum; all error variants used across the crate                             |
| `src/log/`           | Append-only log, index, reclaim, recovery, record format — see `src/log/ARCHITECTURE.md` |
| `benches/engine.rs`  | Divan benchmarks: cache hits/misses, set (append/overwrite/sync), mget warm/cold         |
| `tests/emfile.rs`    | Smoke test: EMFILE (too many open files) handling                                        |
| `tests/writeamp.rs`  | Write amplification test: EXPIRE on large value must not rewrite the value               |

## Configuration

`ShardStore::open(data_dir, memory_bytes)` reads env vars at startup:

| Variable                 | Default | What It Controls                                                                                       |
| ------------------------ | ------- | ------------------------------------------------------------------------------------------------------ |
| `memory_bytes` (arg)     | caller  | Hard byte cap on L1 cache (`MemCache::max_bytes`); excess entries evicted immediately on insert        |
| `KV_COMPACTION_FANOUT`   | 8       | Size-tiered fanout: a log level merges once it holds this many runs (clamped `>= 2`)                   |
| `KV_VALUE_SEP_THRESHOLD` | 131072  | Values `>=` this byte count go to `ValueStore` instead of inline; one GlideFS block = 128 KiB          |
| `KV_TEST_FAIL_ONCE_FILE` | unset   | If set to a path that exists at seal time, injects `TestSealFailure` once (unlinks file after trigger) |

`LogConfig` fields `rotate_threshold` (1 GiB) and `fanout` (8) are set from env vars at `ShardStore::open`; `value_sep_threshold` (128 KiB) likewise. See `src/log/config.rs`.

## Failure Modes

| Failure                                   | What Actually Happens                                                     | Recovery                                                                         |
| ----------------------------------------- | ------------------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| L1 cache eviction under memory pressure   | Cold entries dropped from Small/Main; next read goes to disk via io_uring | Transparent; cache reloads on next access                                        |
| Watch channel full (512 items)            | Sender pruned; subscriber receives no further events on that channel      | Subscriber reconnects; new subscription with `since=last_seen_revision`          |
| Namespace limit reached (1,024)           | `ensure_ns` returns `EngineError::CapacityExceeded`; op fails             | Drop unused namespaces or reduce shard count                                     |
| Watch subscription limit reached (65,536) | New subscribe returns `CapacityExceeded` after pruning dead senders       | Subscribers must reconnect after dead senders are reclaimed                      |
| INCR CAS loop exhausted (64 tries)        | Returns `EngineError::Conflict`                                           | Caller retries; only fires under pathological same-key contention                |
| `seal_all_for_shutdown` I/O error         | First error returned; other namespace seals may or may not have succeeded | Next startup falls back to full record replay for any namespace missing a footer |
| `freeze_and_drain` + crash before footer  | No footer written; active file treated as crashed on next open            | Recovery replays records up to first bad CRC; no data loss beyond last fsync     |
| FLUSHDB called accidentally               | All namespace files unlinked + recreated; data is gone                    | No recovery — FLUSHDB is destructive; `ValueStore::clear()` also runs            |
| Process crash mid-write                   | Partial record at tail of active file                                     | Recovery truncates at first bad CRC; see `src/log/ARCHITECTURE.md`               |
| io_uring not available                    | `monoio::FusionDriver` falls back to legacy epoll driver                  | Performance degrades; correctness preserved                                      |

# Log Module Architecture

Accepts key-value writes as append-only records on disk, maintains an in-memory hash index of all live keys, and returns values by seeking to their stored offset via io_uring.

## Data Flow

### Write (put / tombstone / ttl_update)

```
Caller
  │
  ▼
await_reclaim()  ← stalls at 500µs intervals while reclaim/flush holds the flag
  │
  ▼
begin_write() → WriteGuard  ← increments in_flight_writes; returns Err(Frozen) if frozen
  │
  ▼
wlock(key).lock()  ← per-key write stripe (64 stripes; FxHash & 63)
  │                   same key → same stripe → serialized
  │                   different keys → (usually) different stripes → concurrent
  │
  ├─► (put_full_cond only) cond.check(live_rev(index, key, now))
  │     None → return Ok(None)  (no write, no blob, no append)
  │
  ├─► next_tstamp()  ← wall clock; nudges +1 if clock didn't advance
  │
  ├─► value >= value_sep_threshold?
  │     yes → ValueStore::put(value)  [blake3-hash, write-once, fsync blob + dir]
  │             → content_hash (16 bytes) stored as val; VALUE_SEP flag set
  │             on append failure → values.unref(hash)  [rollback]
  │     no  → value bytes stored inline
  │
  ├─► pool_acquire_write(capacity)
  │     RecordHeader::encode_into(buf, tstamp, flags, exp, key, val, meta)
  │
  ├─► LogFile::append(buf)
  │     poisoned? → Err("log file poisoned")
  │     reserve write_offset (Cell read+set, no lock — single-threaded)
  │     monoio write_all_at(buf, offset)  [io_uring positioned write]
  │     I/O error? → poisoned = true; Err(io)
  │
  ├─► pool_release_write(buf)
  │
  ├─► NsIndex::insert(key, IndexEntry{file_id, offset, size, tstamp}, expires_at_ms)
  │     old valsep hash? → values.unref(old_hash)
  │
  ├─► write_offset >= rotate_threshold?
  │     yes → rotate_active()  [seal footer + open new active file]
  │
  └─► return Ok(tstamp)
```

### `put_many` (MSET) — single write + single fsync

```
put_many(pairs)
  │
  ├─► collect distinct stripe indices for all keys
  │   sort + dedup → acquire all stripes in sorted order  [deadlock prevention]
  │
  ├─► for each pair:
  │     next_tstamp(); maybe_separate(value); encode_into(buf)
  │
  ├─► LogFile::append(whole_buf)  — one io_uring write for all N records
  │
  ├─► NsIndex bulk insert (one index.borrow_mut(), N inserts)
  │
  └─► return Vec<tstamp>  (one per pair, in input order)
```

### Read (read_value / bulk_read)

```
Caller
  │
  ▼
NsIndex::get() — look up file_id + record_offset + record_size
  │
  ├─ miss / expired ──► None (lazy tombstone appended on expiry)
  │
  └─ hit
       │
       ▼
     LogFile::read_exact(record_offset, record_size)
       pool_acquire(size) — exact-capacity match (monoio passes capacity to io_uring)
       └─► monoio read_exact_at()  [io_uring positioned read]
             │
             ▼
           extract_value_meta() — parse header, verify CRC64, slice value + meta
             │
             ▼
           VALUE_SEP flag?
             inline → return value bytes
             sep    → values.get(hash) + re-hash to verify BLAKE3-128 content
```

### Batch read

```
bulk_read([(slot, IndexEntry), ...])
  │
  ├─► join_all([read_exact(), ...])  — concurrent io_uring SQEs
  │
  ├─► extract_value_meta() for each result
  │
  ├─► join_all([deref(value, flags), ...])  — concurrent blob fetches (VALUE_SEP)
  │
  └─► [(slot, value, metadata), ...]
```

### Reclaim (size-tiered compaction)

Reclaim is size-tiered, not full-merge: each merge rewrites only one level's
runs, so write amplification is ~O(log N) instead of O(reclaims × live-set), and
on GlideFS a reclaim re-uploads one level rather than the whole namespace.

```
NamespaceLog::reclaim()
  │
  ├─ 0. reclaim_in_progress.replace(true) — atomic gate; second caller gets ReclamationBusy
  │     drain in_flight_writes to 0  [existing writes finish; new ones stall in await_reclaim]
  │
  ├─ 1. Seal active file — write footer — insert it as a fresh level-0 run
  │
  ├─ 2. Cascade: while some level L holds >= `fanout` runs:
  │       │
  │       ├─ collect that level's live records (index entries with those file_ids)
  │       ├─ reclaim::reclaim_namespace(): read them concurrently, write one merged file
  │       │     to data-{next_id}.log.tmp, footer + fsync, rename .tmp → .log,
  │       │     fsync dir, unlink the input files (leak-logged, never errors)
  │       ├─ open_ro the merged file FIRST (only fallible step), THEN swap index
  │       │     + sealed map atomically — a failed open leaves state consistent
  │       └─ tag the merged run at level L+1
  │
  ├─ 3. Open a fresh active LogFile → sync_dir
  │
  └─ 4. reclaim_in_progress.set(false) → return ReclaimReport
```

`fanout` (default 8) is the per-level run count that triggers a merge. Levels
are in-memory only (`level: file_id → u8`); recovered runs start at level 0.

### Recovery (startup)

```
open_namespace(dir, config)
  │
  ├─ list data-*.log files; sort by file_id
  │   highest file_id → candidate,  rest → sealed
  │
  ├─ for each sealed file:
  │     try load footer (CRC-validated per-key metadata)
  │     on footer missing/corrupt → full sequential scan
  │
  ├─ candidate (highest file_id):
  │     footer present (clean shutdown) → treat as sealed, open fresh empty active
  │     footer absent  (crash)          → replay records from offset 0,
  │                                       truncate at first bad CRC
  │
  ├─ apply in order:
  │    full record  → NsIndex::insert() (+ value-sep sidecar if VALUE_SEP)
  │    tombstone    → NsIndex::remove()
  │    ttl_update   → NsIndex::set_ttl() (only if key still present)
  │
  └─ NamespaceLog::open() post-steps:
       rebuild blob refcounts (one incr_ref per live value-separated key)
       sweep_orphans() — unlink any blob no live key references (crash leftover)
       seed the revision clock from the highest recovered tstamp_ms
```

### Watch Replay (scan_since / current_entries)

```
watch_subscribe(filter, since=0)
  → current_entries(filter, now)
      index.iter() → filter live matching keys
      bulk_read(all matches)  [concurrent io_uring]
      return Vec<WatchEvent::Set{…, revision: entry.tstamp_ms}>

watch_subscribe(filter, since>0)
  → scan_since(filter, since_revision)
      for each file (sealed asc, then active):
        end = data_end_offset()  ← reads magic at EOF to exclude footer bytes
        scan_file_records(file, end, filter, since_revision, values, &mut events)
          header read → parse_header → verify_crc (stop on mismatch)
          tstamp_ms > since_revision && !TTL_UPDATE? → WatchEvent
          VALUE_SEP? → values.get(hash) + re-hash verify
      events.sort_by_key(revision)
      return events
```

## Concepts & Terminology

| Term           | What It Controls                                                                                          | NOT                                                                                                 |
| -------------- | --------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| `NamespaceLog` | All reads/writes for one key-space; owns the index, file set, write stripes, and value store              | Not a shard — multiple namespaces can live in one shard                                             |
| `LogFile`      | One `data-{id}.log` file; tracks write offset, exposes positioned I/O; poisons on write error             | Not a WAL segment; the log IS the store                                                             |
| `active` file  | The only writable file at any time; receives all new appends                                              | Not memory-mapped; accessed via io_uring                                                            |
| `sealed` files | Immutable; readable only; eligible for reclaim                                                            | Not deleted until reclaim completes the rename                                                      |
| Footer         | Per-key metadata block at the end of a file; enables fast recovery                                        | Written to the active file on clean shutdown or rotation; absence means crash or active-in-progress |
| Tombstone      | A record with the `TOMBSTONE` flag; marks a key as deleted in the log                                     | Not a physical delete — the old record remains until reclaim                                        |
| TTL-update     | A tiny record with the `TTL_UPDATE` flag; updates expiry with no value copy                               | Not authoritative until replayed against the index; skipped by `scan_since`                         |
| `NsIndex`      | In-memory key → `IndexEntry` map (`BTreeMap`, ordered for SCAN) + TTL sidecar + value-sep sidecar         | Not persisted — rebuilt from log on every open                                                      |
| `IndexEntry`   | 24-byte struct: record_offset (u64) + record_size (u32) + file_id (u32) + tstamp_ms (u64)                 | Does not hold the value, the key, or flags; `tstamp_ms` doubles as the CAS revision                 |
| `ValueStore`   | Content-addressed blob store for large (value-separated) values; refcounted, deduped, deferred-GC         | Not in the log — compaction moves only the 16-byte pointer, never the blob                          |
| Write stripe   | One of 64 `Mutex<()>` buckets keyed by FxHash of the key; serializes same-key writes                      | Not a per-file or per-namespace lock; different keys usually hit different stripes                  |
| Reclaim        | GC: rewrites live keys into one new file; auto-triggered by sealed-file count threshold or `BGREWRITEAOF` | Writes stall (await_reclaim); they do not error while reclaim runs                                  |
| `flush()`      | Unlinks and recreates all files (CoW snapshot invalidation)                                               | Not fsync — this destroys all data in the namespace                                                 |
| `poisoned`     | `Cell<bool>` on `LogFile`; set on any append I/O error; all subsequent appends return `Err` immediately   | Not set on read errors; only write failures trigger poisoning                                       |
| `frozen`       | `Cell<bool>` on `NamespaceLog`; set by `freeze_and_drain`; `begin_write` returns `Err(Frozen)`            | Cleared by `unfreeze()` on handoff abort; not set during normal operation                           |

## Core Mechanisms

### Append and offset reservation (`file.rs:LogFile::append`)

`LogFile` serializes concurrent appends without a mutex by using a `Cell<u64>` as a reservation counter. Each caller atomically reads-and-increments the offset before issuing its `write_at`. Because io_uring writes are positioned, two concurrent appends to non-overlapping offsets are safe. This means `put_many()` can issue all its record writes before calling a single `sync()`, collapsing N fsyncs into 1.

If an append I/O fails, `append` sets `poisoned = true` and all subsequent append calls return an error immediately — without trying to write. This prevents a later write from landing past the torn slot, which would survive recovery while records between it and the truncation point are silently dropped. `file.rs:enospc_tests` verifies this property.

### Write stripe locking (`mod.rs:NamespaceLog::wlock`)

Every mutating method (`put_full`, `put_full_cond`, `tombstone`, `tombstone_cond`, `ttl_update`) holds the per-key write stripe across the full check-and-append cycle. With 64 stripes, distinct keys hash to distinct buckets in the common case — writes to different keys stay concurrent. Same-key writes serialize: `put_full_cond` checks the condition **after** acquiring the stripe, so no concurrent write to the same key can interleave between the check and the append. A failed condition produces no record — there is no optimistic-orphan that a crash could resurrect.

`put_many` acquires all stripes it touches in sorted-distinct order to prevent deadlocks: two batches that share a subset of stripes always acquire them in the same order.

### Freeze and drain (`mod.rs:freeze_and_drain` / `WriteGuard`)

Every write method calls `begin_write()` before appending. `begin_write` checks `frozen` and increments `in_flight_writes`, returning a `WriteGuard` that decrements on drop. The check-and-increment is synchronous (no `.await`), so it is serialized under monoio's single-threaded scheduler with `freeze_and_drain`'s flag-set + counter-poll.

`freeze_and_drain` sets `frozen = true` (new writes fail immediately with `Frozen`) then polls `in_flight_writes` to zero at 1ms intervals. Once drained, `seal_active_for_shutdown` builds a footer from a consistent snapshot of on-disk state.

### Record format (`record.rs`)

Every record on disk is self-describing:

```
Byte range   Field           Notes
0..8         crc64-nvme      covers bytes 8..end of record (not the CRC field itself)
8..16        tstamp_ms       monotonic; used for tie-breaking on recovery
16           flags           TOMBSTONE=0x01 | NO_EXPIRY=0x02 | TTL_UPDATE=0x04 | VALUE_SEP=0x08
17..25       expires_at_ms   0 when NO_EXPIRY flag set
25..29       key_size        u32
29..33       val_size        u32
33..37       meta_size       u32
37..         key || val || meta
```

`HEADER_LEN = 37`. When `VALUE_SEP` is set the `val` field is not the value but
a 16-byte BLAKE3-128 content hash pointing into the blob store (see Value
Separation below); `val_size == 16` in that case.

The CRC covers bytes 8..end (tstamp_ms through the end of meta). It does NOT cover the CRC field itself (bytes 0–7). Any byte-level corruption causes the record to be skipped: on recovery the active file is truncated to the last clean record, and on the watch catch-up path (`scan_since` / `scan_file_records`) the scan of that file stops at the first bad CRC rather than streaming a corrupt event.

### Sealed file footer (`file.rs`)

When a file is sealed (by reclaim, rotation, or clean-shutdown seal), a footer is appended:

```
[ FooterEntry × N ][ footer_body_len: u64 ][ crc64: u64 ][ magic: u64 = 0x4259_4F4E_445F_4B58 ]
```

Each `FooterEntry` wire format:

```
key_size(u32) + record_offset(u64) + record_size(u32) +
expires_at_ms(u64) + has_expiry(u8) + tstamp_ms(u64) +
has_valsep(u8) [+ value_hash(16 bytes if has_valsep)] + key_bytes
```

This carries enough to rebuild the index, the TTL sidecar, and the blob refcounts without reading record bodies. The 24-byte trailer is `footer_body_len`, the body CRC, and the magic.

The magic value (`BYOND_KX` in ASCII — the `X` marks the v3 format that added
the per-entry tstamp and value-sep hash) lets recovery distinguish a cleanly
sealed file from a crashed active file. If the footer is present and CRC-valid,
recovery uses it to populate the index without scanning the full file body.

`data_end_offset()` reads the magic from the last 8 bytes: if present, it returns `total_size - FOOTER_TRAILER_LEN - footer_body_len`, bounding `scan_since` so footer bytes are never misread as records.

### In-memory index and TTL sidecar (`index.rs`)

`NsIndex` stores all live keys with 24-byte `IndexEntry` values in a `BTreeMap<Bytes, IndexEntry>` (ordered for SCAN cursor semantics). Keys with a TTL also appear in a secondary `FxHashMap<Bytes, u64>` (expires_at_ms). Keys with a value-separated large value appear in a third `FxHashMap<Bytes, ContentHash>` (valsep sidecar). Only keys that carry a TTL or large value pay the extra allocation.

Reads check expiry inline and return `None` for stale entries; the caller is responsible for appending the tombstone (lazy deletion).

`live_count` tracks live key count with saturating arithmetic — incremented on insert, decremented on remove. An overwrite does not increment it. Matches Redis `DBSIZE` semantics (may overcount by lazy-expired-but-not-yet-tombstoned keys).

`scan()` implements Redis SCAN semantics: a cursor encodes the current position in the key set as the last yielded key (exclusive lower bound via `BTreeMap::range`); each call returns up to `count` live, non-expired keys matching an optional filter. Cursor stability: a key-based cursor is stable across concurrent map mutations.

### Reclaim atomicity (`reclaim.rs`)

The compaction rename (`data-{id}.log.tmp` → `data-{id}.log`) is the only atomic step. If the process crashes before the rename, the `.tmp` file is abandoned and recovery ignores it. If the crash happens after the rename but before old files are unlinked, the old sealed files remain; the next reclaim will skip them because the index no longer references their entries. Dead files produce a log warning, not an error.

Opening the merged file happens before mutating in-memory state. If `open_ro` fails (EMFILE, hardware error), the function returns with the index and sealed map untouched — old file descriptors remain open and keep serving reads even though `reclaim_namespace` already unlinked the paths. Keys stay accessible until restart.

Writes stall during reclaim (not error): every write calls `await_reclaim()` before `begin_write()`, polling `reclaim_in_progress` at 500µs intervals. This keeps the write error surface clean while reclaim runs.

### File rotation (`mod.rs:rotate_active`)

After every write, if `active.write_offset() >= config.rotate_threshold`, the active file is rotated: footer written, active inserted into the sealed map as level 0, new `data-{next_id}.log` opened, `sync_dir` called. Rotation is guarded by `rotate_in_progress: Cell<bool>` to prevent two concurrent writers from both entering `rotate_active` after each observes the threshold crossed.

### Value separation (`value_store.rs`)

Values `>= config.value_sep_threshold` (default 128 KiB = one GlideFS block) are
written WiscKey-style to a content-addressed blob store at `{dir}/values/`
instead of inline in the log. The log record then carries only a 16-byte
BLAKE3-128 content hash (the `VALUE_SEP` flag marks this). Because the pointer is
tiny and immutable, compaction relocates pointers, never large values —
collapsing large-value write amplification.

Blobs are:

- **Deduped** — identical content across keys/forks/tenants maps to one blob.
- **Refcounted** — refcounts are in-memory, rebuilt from the live index on open.
- **Write-once + crash-durable** — the blob's data AND its directory entry are
  fsynced before the pointer record can become durable, so a crash can at worst
  leave an orphan blob (reclaimed by `sweep_orphans`), never a dangling pointer.
- **Deferred-GC** — when the last reference drops, the blob is queued and only
  physically unlinked after the next log fsync (`collect_garbage`), so a
  power-loss revert always finds its blob still present. A same-content `put`
  racing the unlink is serialized by a per-hash file stripe.

On read, the blob is fetched by hash and re-hashed to verify integrity — parity
with the CRC the inline path pays on every read.

### Buffer pools (`file.rs`)

Two thread-local pools recycle `Vec<u8>` buffers to avoid per-I/O heap allocation:

- **Read pool** (`BUF_POOL`): exact-capacity match only. monoio passes `capacity` (not `len`) to io_uring as the read size via `bytes_total()`; a buffer with `cap > size` would let the kernel read past the requested bytes and corrupt the `len` field, breaking CRC checks. `pool_acquire` finds an exact-capacity match or allocates a fresh buffer. `BufGuard` returns the buffer to the pool on drop.
- **Write pool** (`WRITE_BUF_POOL`): at-least-capacity match (write size is known upfront). Buffers up to 64 KiB are pooled; larger ones (e.g. value-sep compaction) are discarded.

Both pools hold at most 32 buffers. The pools are thread-local because `NamespaceLog` is `!Sync` and lives on a single monoio worker.

## State Machine

```
      open_namespace()
            │
            ▼
         OPEN ◄───────────────────────────┐
       (active)                           │
       /      \                           │
  put/del    reclaim()                    │
     │            │                       │
     ▼            ▼                       │
APPENDED       SEALING ──► COMPACTING ────┘
(new offset    (write footer,  (rename .tmp,
 in index)      sync)           drop old files)
```

| Phase               | Files on disk               | Index state     | Writable?                                        |
| ------------------- | --------------------------- | --------------- | ------------------------------------------------ |
| OPEN                | active + 0..N sealed        | fully populated | yes                                              |
| SEALING             | active being sealed         | unchanged       | stalled (await_reclaim) — not errored            |
| COMPACTING          | sealed + .tmp               | unchanged       | stalled (await_reclaim)                          |
| OPEN (post-reclaim) | 1 new sealed + fresh active | unchanged       | yes                                              |
| FROZEN              | active (unmodified)         | fully populated | no — Err(Frozen); freeze_and_drain drains writes |
| SEALED (shutdown)   | footer appended to active   | fully populated | no — Err(Frozen)                                 |

## Why It Behaves This Way

### Why reclaim is not continuous

Reclaim cannot run concurrently with writes — it seals the active file, which would race with in-flight appends. Instead, the server schedules it between async tasks: when the sealed-file count exceeds `--reclaim-sealed-threshold` (default: 4), a reclaim runs on the next `--reclaim-interval-secs` tick (default: 300s). `BGREWRITEAOF` triggers the same path immediately. The hot path stays uncontested; reclaim is a periodic stop-the-append, not a background thread.

### Why TTL-update is a separate record type

Updating a TTL naively requires rewriting the full value (key + value + metadata). For large values this is expensive. A `TTL_UPDATE` record is a fixed-size append that contains only the key and the new expiry. On recovery, TTL-update records are replayed as index patches: they update only the expiry in `NsIndex` and leave the value record untouched. This makes `EXPIRE` O(record-header + key) on the write path.

`scan_since` skips `TTL_UPDATE` records — TTL changes are not watch events; only value mutations and deletions are.

### Why the index is always in RAM

The index is not persisted separately — it is rebuilt from the log on startup. This eliminates index/log consistency bugs (there is only one source of truth) and avoids write amplification (index updates are free on the write path). The tradeoff is that startup time scales with the number of records if footer loading fails; the footer format was added to make the common case fast (load footer → populate index in one pass, no record parsing).

### Why `write_offset` uses `Cell` instead of `AtomicU64`

The engine runs on a single-threaded `monoio` runtime per shard. There is no cross-thread contention on `write_offset`. `Cell` is cheaper than an atomic (no memory barriers) and makes the single-threaded contract explicit. If the concurrency model changes, `Cell` will produce a compile error at the call sites.

### Why `put_many` acquires stripes in sorted order

`put_many` touches potentially many per-key stripes at once. If two concurrent `put_many` batches each acquire their stripes in arbitrary order, they can deadlock: batch A holds stripe 3 and waits for stripe 7; batch B holds stripe 7 and waits for stripe 3. Sorting and deduplicating the stripe indices before acquiring ensures a total order across all lock operations, breaking the cycle.

### Why a failed `open_ro` in reclaim does not corrupt in-memory state

The index is mutated only after the merged file is open and ready to serve reads. If `open_ro` fails (e.g., EMFILE), the function returns an error and the index still references the old file IDs — whose `Rc<LogFile>` handles remain open. On Linux, unlinked files stay alive as long as there is an open descriptor; reads against those keys continue to succeed until restart. Mutating the index before the `open_ro` would leave entries pointing at a `file_id` absent from `sealed`, causing "file_id not found" on every affected read.

### Why `append` poisons the file on write failure

When an I/O error occurs, the write offset has already been incremented. A later write at the now-advanced offset would succeed, landing its record past the torn slot. On recovery, the scan would reach the later record (valid CRC), trust it, and discard everything between the torn slot and it — silently losing committed records. Poisoning prevents any subsequent write from advancing past the known-bad gap.

## Failure Modes

| Failure                                       | What Actually Happens                         | Recovery                                                                                                        |
| --------------------------------------------- | --------------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| Crash mid-append (active file)                | Partial record at tail of active file         | Recovery replays records; stops and truncates at first bad CRC                                                  |
| Crash mid-reclaim before rename               | `.tmp` file left on disk                      | Ignored on next open (no `.log` suffix); old sealed files intact                                                |
| Crash mid-reclaim after rename                | Old sealed files not unlinked                 | Next reclaim drops them; logged as warnings                                                                     |
| Sealed file footer corrupt                    | Footer CRC check fails                        | Falls back to full sequential record scan                                                                       |
| Read from expired key                         | Returns `None`; tombstone appended lazily     | Tombstone write is best-effort; a crash before it completes means the key re-expires on next read               |
| `flush()` called accidentally                 | All namespace files unlinked and recreated    | Data is gone; no recovery — `flush()` is a destructive reset                                                    |
| Clean shutdown (SIGTERM/SIGINT)               | Footer written to active file before exit     | Next startup treats it as sealed; no record replay needed                                                       |
| Crash after blob write, before pointer record | Orphan blob on disk, no referencing key       | `sweep_orphans` unlinks it on next open (refcounts rebuilt from the live index first)                           |
| Corrupt record on watch replay                | CRC mismatch in `scan_file_records`           | Scan of that file stops at the bad record; no bogus event is streamed                                           |
| `open_ro` of merged file fails mid-reclaim    | Merged file on disk, in-memory swap aborted   | Index/sealed left untouched; old (unlinked-but-open) fds keep serving reads until restart finds the merged file |
| Append I/O error (disk full, hardware fault)  | LogFile poisoned; all subsequent appends fail | Process should restart; recovery truncates at last clean record on next open                                    |
| `freeze_and_drain` + crash before footer      | No footer; active file treated as crashed     | Recovery replays records up to first bad CRC                                                                    |

## Configuration

`LogConfig` (`config.rs`):

| Field                 | Default | What It Controls                                                                              |
| --------------------- | ------- | --------------------------------------------------------------------------------------------- |
| `rotate_threshold`    | 1 GiB   | Byte threshold at which the active file is sealed and a fresh active is opened                |
| `fanout`              | 8       | Size-tiered compaction fanout: a level merges into the next once it holds this many runs      |
| `value_sep_threshold` | 128 KiB | Values `>=` this go to the content-addressed blob store instead of inline (one GlideFS block) |

`KV_COMPACTION_FANOUT` and `KV_VALUE_SEP_THRESHOLD` env vars override `fanout`
and `value_sep_threshold` at `ShardStore::open` (fanout is clamped to `>= 2`).

## Package Structure

| File         | What It Does                                                                                                                      |
| ------------ | --------------------------------------------------------------------------------------------------------------------------------- |
| `mod.rs`     | `NamespaceLog`: write/read/tombstone/TTL-update API; write stripe locking; freeze/drain; rotation; reclaim dispatch; watch replay |
| `index.rs`   | `NsIndex`: BTreeMap + TTL sidecar + valsep sidecar; cursor-based SCAN; `IndexEntry` (24 bytes)                                    |
| `file.rs`    | `LogFile`: positioned I/O via io_uring; offset reservation; poisoning; footer read/write; buffer pools                            |
| `record.rs`  | On-disk record format: CRC64-NVME encode/decode; flag constants; header parsing                                                   |
| `reclaim.rs` | `reclaim_namespace`: reads live records concurrently, writes merged file, renames atomically                                      |
| `recover.rs` | `open_namespace`: footer-fast or record-scan recovery; determines active vs sealed                                                |
| `config.rs`  | `LogConfig`: `rotate_threshold`, `fanout`, `value_sep_threshold`                                                                  |

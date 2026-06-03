# Log Module Architecture

Accepts key-value writes as append-only records on disk, maintains an in-memory hash index of all live keys, and returns values by seeking to their stored offset via io_uring.

## Data Flow

### Write (put / tombstone / ttl_update)

```
Caller
  │
  ▼
NamespaceLog::put_full() / put_many()
  │
  ├─► RecordHeader::encode() — serialize header + body into buf
  │     [crc64 | tstamp_ms | flags | expires_at_ms | key_sz | val_sz | meta_sz | key | val | meta]
  │
  ├─► LogFile::append()
  │     reserve write_offset with Cell (atomic increment, no lock needed)
  │     └─► monoio write_at() — io_uring positioned write
  │
  ├─► (if put_many) LogFile::sync() — single fsync for the whole batch
  │
  └─► NsIndex::insert() — update in-memory index to point at new offset
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
       └─► monoio read_at() — io_uring positioned read
             │
             ▼
           RecordHeader::decode() — verify CRC64, parse flags, extract value slice
```

### Batch read

```
bulk_read(entries)
  │
  ▼
futures::join_all([read_exact(), read_exact(), ...])  — concurrent io_uring ops
  │
  ▼
[Option<Bytes>, ...]
```

### Reclaim (size-tiered compaction)

Reclaim is size-tiered, not full-merge: each merge rewrites only one level's
runs, so write amplification is ~O(log N) instead of O(reclaims × live-set), and
on GlideFS a reclaim re-uploads one level rather than the whole namespace.

```
NamespaceLog::reclaim()
  │
  ├─ 1. Seal active file — write footer — and insert it as a fresh level-0 run
  │
  ├─ 2. Cascade: while some level L holds >= `fanout` runs:
  │       │
  │       ├─ collect that level's live records (index entries with those file_ids)
  │       ├─ reclaim_namespace(): read them concurrently, write one merged file
  │       │     to data-{next_id}.log.tmp, footer + fsync, rename .tmp → .log,
  │       │     fsync dir, unlink the input files (leak-logged, never errors)
  │       ├─ open_ro the merged file FIRST (only fallible step), THEN swap index
  │       │     + sealed map atomically — a failed open leaves state consistent
  │       └─ tag the merged run at level L+1
  │
  └─ 3. Open a fresh active LogFile → return ReclaimReport
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

## Concepts & Terminology

| Term           | What It Controls                                                                                          | NOT                                                                                 |
| -------------- | --------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| `NamespaceLog` | All reads/writes for one key-space; owns the index and file set                                           | Not a shard — multiple namespaces can live in one shard                             |
| `LogFile`      | One `data-{id}.log` file; tracks write offset, exposes positioned I/O                                     | Not a WAL segment; the log IS the store                                             |
| `active` file  | The only writable file at any time; receives all new appends                                              | Not memory-mapped; accessed via io_uring                                            |
| `sealed` files | Immutable; readable only; eligible for reclaim                                                            | Not deleted until reclaim completes the rename                                      |
| Footer         | Per-key metadata block at the end of a file; enables fast recovery                                        | Written to the active file on clean shutdown; absence means crash or in-progress    |
| Tombstone      | A record with the `TOMBSTONE` flag; marks a key as deleted in the log                                     | Not a physical delete — the old record remains until reclaim                        |
| TTL-update     | A tiny record with the `TTL_UPDATE` flag; updates expiry with no value copy                               | Not authoritative until replayed against the index                                  |
| `NsIndex`      | In-memory key → `IndexEntry` map (`BTreeMap`, ordered for SCAN) + TTL sidecar + value-sep sidecar         | Not persisted — rebuilt from log on every open                                      |
| `IndexEntry`   | 24-byte struct: record_offset (u64) + record_size (u32) + file_id (u32) + tstamp_ms (u64)                 | Does not hold the value, the key, or flags; `tstamp_ms` doubles as the CAS revision |
| `ValueStore`   | Content-addressed blob store for large (value-separated) values; refcounted, deduped, deferred-GC         | Not in the log — compaction moves only the 16-byte pointer, never the blob          |
| Reclaim        | GC: rewrites live keys into one new file; auto-triggered by sealed-file count threshold or `BGREWRITEAOF` | Caller must serialize with writes; cannot run concurrently with appends             |
| `flush()`      | Unlinks and recreates all files (CoW snapshot invalidation)                                               | Not fsync — this destroys all data in the namespace                                 |

## Core Mechanisms

### Append and offset reservation (`file.rs:append`)

`LogFile` serializes concurrent appends without a mutex by using a `Cell<u64>` as a reservation counter. Each caller atomically reads-and-increments the offset before issuing its `write_at`. Because io_uring writes are positioned, two concurrent appends to non-overlapping offsets are safe. This means `put_many()` can issue all its record writes before calling a single `sync()`, collapsing N fsyncs into 1.

### Record format (`record.rs`)

Every record on disk is self-describing:

```
Byte range   Field           Notes
0..8         crc64-nvme      covers bytes 8..end of record
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

The CRC covers the entire record body. Any byte-level corruption causes the
record to be skipped: on recovery the active file is truncated to the last clean
record, and on the watch catch-up path (`scan_since` / `scan_file_records`) the
scan of that file stops at the first bad CRC rather than streaming a corrupt
event.

### Sealed file footer (`file.rs`)

When a file is sealed (by reclaim, rotation, or clean-shutdown seal), a footer is appended:

```
[ FooterEntry × N ][ footer_body_len: u64 ][ crc64: u64 ][ magic: u64 = 0x4259_4F4E_445F_4B58 ]
```

Each `FooterEntry` carries `key`, `record_offset`, `record_size`,
`expires_at_ms` (optional), `tstamp_ms`, and the optional 16-byte value-sep hash
— enough to rebuild the index, the TTL sidecar, and the blob refcounts without
reading record bodies. The 24-byte trailer is `footer_body_len`, the body CRC,
and the magic.

The magic value (`BYOND_KX` in ASCII — the `X` marks the v3 format that added
the per-entry tstamp and value-sep hash) lets recovery distinguish a cleanly
sealed file from a crashed active file. If the footer is present and CRC-valid,
recovery uses it to populate the index without scanning the full file body.

### In-memory index and TTL sidecar (`index.rs`)

`NsIndex` stores all live keys with 16-byte `IndexEntry` values. Keys with a TTL also appear in a secondary `FxHashMap<Bytes, u64>` (expires_at_ms). Only keys that carry a TTL pay the extra allocation. Reads check expiry inline and return `None` for stale entries; the caller is responsible for appending the tombstone (lazy deletion).

`scan()` implements Redis SCAN semantics: a cursor encodes the current position in the key set; each call returns up to `count` live, non-expired keys matching an optional glob pattern.

### Reclaim atomicity (`reclaim.rs`)

The compaction rename (`data-{id}.log.tmp` → `data-{id}.log`) is the only atomic step. If the process crashes before the rename, the `.tmp` file is abandoned and recovery ignores it. If the crash happens after the rename but before old files are unlinked, the old sealed files remain; the next reclaim will skip them because the index no longer references their entries. Dead files produce a log warning, not an error.

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

| Phase               | Files on disk               | Index state     | Writable?                  |
| ------------------- | --------------------------- | --------------- | -------------------------- |
| OPEN                | active + 0..N sealed        | fully populated | yes                        |
| SEALING             | active being sealed         | unchanged       | no — caller must serialize |
| COMPACTING          | sealed + .tmp               | unchanged       | no — caller must serialize |
| OPEN (post-reclaim) | 1 new sealed + fresh active | unchanged       | yes                        |

## Why It Behaves This Way

### Why reclaim is not continuous

Reclaim cannot run concurrently with writes — it seals the active file, which would race with in-flight appends. Instead, the server schedules it between async tasks: when the sealed-file count exceeds `--reclaim-sealed-threshold` (default: 4), a reclaim runs on the next `--reclaim-interval-secs` tick (default: 300s). `BGREWRITEAOF` triggers the same path immediately. The hot path stays uncontested; reclaim is a periodic stop-the-append, not a background thread.

### Why TTL-update is a separate record type

Updating a TTL naively requires rewriting the full value (key + value + metadata). For large values this is expensive. A `TTL_UPDATE` record is a fixed-size append that contains only the key and the new expiry. On recovery, TTL-update records are replayed as index patches: they update only the expiry in `NsIndex` and leave the value record untouched. This makes `EXPIRE` O(record-header + key) on the write path.

### Why the index is always in RAM

The index is not persisted separately — it is rebuilt from the log on startup. This eliminates index/log consistency bugs (there is only one source of truth) and avoids write amplification (index updates are free on the write path). The tradeoff is that startup time scales with the number of records if footer loading fails; the footer format was added to make the common case fast (load footer → populate index in one pass, no record parsing).

### Why `write_offset` uses `Cell` instead of `AtomicU64`

The engine runs on a single-threaded `monoio` runtime per shard. There is no cross-thread contention on `write_offset`. `Cell` is cheaper than an atomic (no memory barriers) and makes the single-threaded contract explicit. If the concurrency model changes, `Cell` will produce a compile error at the call sites.

## Failure Modes

| Failure                                       | What Actually Happens                       | Recovery                                                                                                        |
| --------------------------------------------- | ------------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| Crash mid-append (active file)                | Partial record at tail of active file       | Recovery replays records; stops and truncates at first bad CRC                                                  |
| Crash mid-reclaim before rename               | `.tmp` file left on disk                    | Ignored on next open (no `.log` suffix); old sealed files intact                                                |
| Crash mid-reclaim after rename                | Old sealed files not unlinked               | Next reclaim drops them; logged as warnings                                                                     |
| Sealed file footer corrupt                    | Footer CRC check fails                      | Falls back to full sequential record scan                                                                       |
| Read from expired key                         | Returns `None`; tombstone appended lazily   | Tombstone write is best-effort; a crash before it completes means the key re-expires on next read               |
| `flush()` called accidentally                 | All namespace files unlinked and recreated  | Data is gone; no recovery — `flush()` is a destructive reset                                                    |
| Clean shutdown (SIGTERM/SIGINT)               | Footer written to active file before exit   | Next startup treats it as sealed; no record replay needed                                                       |
| Crash after blob write, before pointer record | Orphan blob on disk, no referencing key     | `sweep_orphans` unlinks it on next open (refcounts rebuilt from the live index first)                           |
| Corrupt record on watch replay                | CRC mismatch in `scan_file_records`         | Scan of that file stops at the bad record; no bogus event is streamed                                           |
| `open_ro` of merged file fails mid-reclaim    | Merged file on disk, in-memory swap aborted | Index/sealed left untouched; old (unlinked-but-open) fds keep serving reads until restart finds the merged file |

## Configuration

`LogConfig` (`config.rs`):

| Field                 | Default | What It Controls                                                                              |
| --------------------- | ------- | --------------------------------------------------------------------------------------------- |
| `rotate_threshold`    | 1 GiB   | Byte threshold at which the active file is sealed and a fresh active is opened                |
| `fanout`              | 8       | Size-tiered compaction fanout: a level merges into the next once it holds this many runs      |
| `value_sep_threshold` | 128 KiB | Values `>=` this go to the content-addressed blob store instead of inline (one GlideFS block) |

`KV_COMPACTION_FANOUT` and `KV_VALUE_SEP_THRESHOLD` env vars override `fanout`
and `value_sep_threshold` at `ShardStore::open` (fanout is clamped to `>= 2`).

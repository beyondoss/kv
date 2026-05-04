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

### Reclaim (compaction)

```
reclaim_namespace()
  │
  ├─ 1. Seal active file — write footer (per-key metadata + CRC64 + magic)
  │
  ├─ 2. Read all live index entries → read records from sealed files
  │
  ├─ 3. Write live records to data-{next_id}.log.tmp
  │
  ├─ 4. rename() .tmp → .log   (atomic)
  │
  ├─ 5. Drop old sealed files  (unlink; logs failures but does not error)
  │
  └─ 6. Open fresh active LogFile → return ReclaimReport
```

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
  └─ apply in order:
       full record  → NsIndex::insert()
       tombstone    → NsIndex::remove()
       ttl_update   → NsIndex::update_ttl() (only if key still present)
```

## Concepts & Terminology

| Term           | What It Controls                                                            | NOT                                                                              |
| -------------- | --------------------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| `NamespaceLog` | All reads/writes for one key-space; owns the index and file set             | Not a shard — multiple namespaces can live in one shard                          |
| `LogFile`      | One `data-{id}.log` file; tracks write offset, exposes positioned I/O       | Not a WAL segment; the log IS the store                                          |
| `active` file  | The only writable file at any time; receives all new appends                | Not memory-mapped; accessed via io_uring                                         |
| `sealed` files | Immutable; readable only; eligible for reclaim                              | Not deleted until reclaim completes the rename                                   |
| Footer         | Per-key metadata block at the end of a file; enables fast recovery          | Written to the active file on clean shutdown; absence means crash or in-progress |
| Tombstone      | A record with the `TOMBSTONE` flag; marks a key as deleted in the log       | Not a physical delete — the old record remains until reclaim                     |
| TTL-update     | A tiny record with the `TTL_UPDATE` flag; updates expiry with no value copy | Not authoritative until replayed against the index                               |
| `NsIndex`      | In-memory `FxHashMap` from key → `IndexEntry`; the read path                | Not persisted — rebuilt from log on every open                                   |
| `IndexEntry`   | 16-byte struct: file_id + record_offset + record_size + flags               | Does not hold the value or the key                                               |
| Reclaim        | Operator-triggered GC: rewrites live keys into one new file                 | Never automatic; caller must serialize with writes                               |
| `flush()`      | Unlinks and recreates all files (CoW snapshot invalidation)                 | Not fsync — this destroys all data in the namespace                              |

## Core Mechanisms

### Append and offset reservation (`file.rs:append`)

`LogFile` serializes concurrent appends without a mutex by using a `Cell<u64>` as a reservation counter. Each caller atomically reads-and-increments the offset before issuing its `write_at`. Because io_uring writes are positioned, two concurrent appends to non-overlapping offsets are safe. This means `put_many()` can issue all its record writes before calling a single `sync()`, collapsing N fsyncs into 1.

### Record format (`record.rs`)

Every record on disk is self-describing:

```
Byte range   Field           Notes
0..8         crc64-nvme      covers bytes 8..end of record
8..16        tstamp_ms       monotonic; used for tie-breaking on recovery
16           flags           TOMBSTONE=0x01 | NO_EXPIRY=0x02 | TTL_UPDATE=0x04
17..25       expires_at_ms   0 when NO_EXPIRY flag set
25..29       key_size        u32
29..33       val_size        u32
33..37       meta_size       u32
37..         key || val || meta
```

The CRC covers the entire record body. Any byte-level corruption causes the record to be skipped on recovery (active file is truncated to the last clean record).

### Sealed file footer (`file.rs`)

When a file is sealed (by reclaim or a future rotation), a footer is appended:

```
[ IndexEntry × N ][ entry_count: u64 ][ crc64: u64 ][ magic: u64 = 0x4259_4F4E_445F_4B56 ]
```

The magic value (`BYOND_KV` in ASCII) lets recovery distinguish a cleanly sealed file from a crashed active file. If the footer is present and CRC-valid, recovery uses it to populate the index without scanning the full file body.

### In-memory index and TTL sidecar (`index.rs`)

`NsIndex` stores all live keys with 16-byte `IndexEntry` values. Keys with a TTL also appear in a secondary `FxHashMap<Bytes, u64>` (expires_at_ms). Only keys that carry a TTL pay the extra allocation. Reads check expiry inline and return `None` for stale entries; the caller is responsible for appending the tombstone (lazy deletion).

`scan()` implements Redis SCAN semantics: a cursor encodes the current position in the key set; each call returns up to `count` live, non-expired keys matching an optional glob pattern.

### Reclaim atomicity (`reclaim.rs`)

The compaction rename (`data-{id}.log.tmp` → `data-{id}.log`) is the only atomic step. If the process crashes before the rename, the `.tmp` file is abandoned and recovery ignores it. If the crash happens after the rename but before old files are unlinked, the old sealed files remain; the next reclaim will skip them because the index no longer references their entries. Dead files produce a log warning, not an error.

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

### Why reclaim is never automatic

Reclaim cannot run concurrently with writes — it seals the active file, which would race with in-flight appends. Making it automatic would require either a write lock (adding latency) or a more complex copy-on-write scheme. The current design delegates scheduling to the operator (or a higher-level controller), keeping the engine simple and the hot path uncontested.

### Why TTL-update is a separate record type

Updating a TTL naively requires rewriting the full value (key + value + metadata). For large values this is expensive. A `TTL_UPDATE` record is a fixed-size append that contains only the key and the new expiry. On recovery, TTL-update records are replayed as index patches: they update only the expiry in `NsIndex` and leave the value record untouched. This makes `EXPIRE` O(record-header + key) on the write path.

### Why the index is always in RAM

The index is not persisted separately — it is rebuilt from the log on startup. This eliminates index/log consistency bugs (there is only one source of truth) and avoids write amplification (index updates are free on the write path). The tradeoff is that startup time scales with the number of records if footer loading fails; the footer format was added to make the common case fast (load footer → populate index in one pass, no record parsing).

### Why `write_offset` uses `Cell` instead of `AtomicU64`

The engine runs on a single-threaded `monoio` runtime per shard. There is no cross-thread contention on `write_offset`. `Cell` is cheaper than an atomic (no memory barriers) and makes the single-threaded contract explicit. If the concurrency model changes, `Cell` will produce a compile error at the call sites.

## Failure Modes

| Failure                         | What Actually Happens                      | Recovery                                                                                          |
| ------------------------------- | ------------------------------------------ | ------------------------------------------------------------------------------------------------- |
| Crash mid-append (active file)  | Partial record at tail of active file      | Recovery replays records; stops and truncates at first bad CRC                                    |
| Crash mid-reclaim before rename | `.tmp` file left on disk                   | Ignored on next open (no `.log` suffix); old sealed files intact                                  |
| Crash mid-reclaim after rename  | Old sealed files not unlinked              | Next reclaim drops them; logged as warnings                                                       |
| Sealed file footer corrupt      | Footer CRC check fails                     | Falls back to full sequential record scan                                                         |
| Read from expired key           | Returns `None`; tombstone appended lazily  | Tombstone write is best-effort; a crash before it completes means the key re-expires on next read |
| `flush()` called accidentally   | All namespace files unlinked and recreated | Data is gone; no recovery — `flush()` is a destructive reset                                      |
| Clean shutdown (SIGTERM/SIGINT) | Footer written to active file before exit  | Next startup treats it as sealed; no record replay needed                                         |

## Configuration

`LogConfig` (`config.rs`):

| Field           | Default      | What It Controls                                                                        |
| --------------- | ------------ | --------------------------------------------------------------------------------------- |
| `max_file_size` | (caller-set) | Byte threshold at which the active file is rotated to sealed and a new active is opened |

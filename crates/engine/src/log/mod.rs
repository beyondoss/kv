//! Log-structured per-namespace storage engine.
//!
//! - In-RAM hash index over an append-only log file per namespace.
//! - Full keys in RAM; values on disk.
//! - Operator-controlled reclaim. Never runs on a timer.
//! - All disk I/O is async via `monoio::fs` (io_uring on Linux).
//!
//! Concurrency model (v0):
//!
//! - Reads and writes from concurrent tasks on the same shard are safe. Writes
//!   atomically reserve `write_offset` via `Cell`, then submit their `write_at`
//!   to io_uring; non-overlapping ranges on the same fd are processed in
//!   parallel by the kernel.
//! - `reclaim`, `flush`, and `rotate_active` are NOT concurrent-safe with other
//!   operations on the same namespace. Caller must serialize.

pub mod config;
pub mod file;
pub mod index;
pub mod reclaim;
pub mod record;
pub mod recover;

use std::cell::{Cell, RefCell};
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures_util::future::join_all;
use rustc_hash::FxHashMap;
use tracing::warn;

use crate::error::{EngineError, Result};
use crate::log::config::LogConfig;
use crate::log::file::{
    BufGuard, FooterEntry, LogFile, data_filename, pool_acquire_write, pool_release_write, sync_dir,
};
use crate::log::index::{IndexEntry, NsIndex};
use crate::log::record::{HEADER_LEN, flags as rflags, parse_header, verify_crc};
use crate::value_store::{ContentHash, ValueStore};

pub fn now_ms() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_millis() as u64,
        Err(_) => {
            warn!("system clock is before UNIX epoch; timestamps will be 0");
            0
        }
    }
}

/// Condition for a conditional write. Used by [`NamespaceLog::put_full_cond`].
pub enum WriteCondition {
    /// Write only if the key is absent or expired (SETNX semantics).
    KeyAbsent,
    /// Write only if the key is present and live (SETXX semantics).
    KeyPresent,
    /// Write only if the key's current revision matches exactly (CAS semantics).
    Revision(u64),
}

impl WriteCondition {
    fn check(&self, current_rev: Option<u64>) -> bool {
        match self {
            WriteCondition::KeyAbsent => current_rev.is_none(),
            WriteCondition::KeyPresent => current_rev.is_some(),
            WriteCondition::Revision(rev) => current_rev == Some(*rev),
        }
    }
}

pub struct NamespaceLog {
    pub dir: PathBuf,
    /// Content-addressed blob store for value-separated (large) values. Lives at
    /// `{dir}/values/`. Values >= `config.value_sep_threshold` are stored here
    /// (write-once, deduped, GC'd when the last referencing key drops) and the
    /// log holds only a 16-byte hash pointer — so compaction never moves them.
    pub values: ValueStore,
    pub index: RefCell<NsIndex>,
    /// Sealed files in file_id ascending order. `Rc<LogFile>` so readers can
    /// clone a handle and drop the `RefCell` borrow before awaiting I/O.
    pub sealed: RefCell<FxHashMap<u32, Rc<LogFile>>>,
    /// Size-tier level per sealed `file_id` (tiered compaction only). 0 = freshly
    /// sealed; merging `fanout` runs at level L produces one run at level L+1.
    level: RefCell<FxHashMap<u32, u8>>,
    /// Active (writable) file.
    pub active: RefCell<Rc<LogFile>>,
    pub config: LogConfig,
    /// Cumulative bytes rewritten by compaction (reclaim). Instrumentation for
    /// measuring write amplification: full-merge grows ~O(reclaims × live-set),
    /// tiered ~O(log N).
    pub compaction_bytes: Cell<u64>,
    unsynced_bytes: Cell<u64>,
    /// Monotonically increasing tstamp_ms — wall clock with a +1 nudge if the
    /// clock didn't advance, so duplicate-key replays always pick the latest.
    last_tstamp: Cell<u64>,
    /// Guards against concurrent reclaim/flush calls. Both are documented as
    /// requiring external serialization; this flag turns a would-be RefCell
    /// panic into a clean Err at the call site.
    reclaim_in_progress: Cell<bool>,
    /// Guards against two async tasks both trying to rotate the active file when
    /// the threshold is crossed simultaneously (monoio yields between the check
    /// and the rotate_active await).
    rotate_in_progress: Cell<bool>,
    /// When set, all write paths return `Frozen` immediately. Used by the
    /// handoff seal path to guarantee that the index snapshot the footer is
    /// built from matches what's actually on disk — without it, writes that
    /// happen while `write_footer` awaits will be in the WAL but absent from
    /// the footer, and thus invisible to the successor process.
    frozen: Cell<bool>,
    /// Count of write methods currently between their entry check and exit.
    /// `freeze_and_drain` polls this to 0 before allowing the seal to proceed.
    in_flight_writes: Cell<u32>,
    /// Per-key write serialization, striped. Every mutating method locks
    /// `wlock(key)` for its check→append→commit, so two writes to the SAME key
    /// never interleave — while writes to DIFFERENT keys hash to different
    /// stripes and stay fully concurrent (lock-free reads are untouched). This
    /// is what makes conditional writes (CAS/NX/XX) atomic: holding the stripe,
    /// they check BEFORE appending, so a failed condition writes no record at
    /// all — eliminating the optimistic-orphan that a crash could resurrect.
    /// Collisions (distinct keys, same stripe) only cause rare, harmless extra
    /// serialization. INCR no longer needs a dedicated lock: its optimistic
    /// retry now appends nothing on a lost race.
    write_stripes: Vec<futures_util::lock::Mutex<()>>,
}

/// Number of write-lock stripes per namespace. Powers of two keep `& (N-1)`
/// cheap. 64 keeps per-key false-collisions rare without much memory.
const WRITE_STRIPES: usize = 64;

impl NamespaceLog {
    pub async fn open(dir: PathBuf, config: LogConfig) -> Result<Self> {
        let opened = recover::open_namespace(dir.clone()).await?;
        let sealed: FxHashMap<u32, Rc<LogFile>> = opened
            .sealed
            .into_iter()
            .map(|f| (f.file_id, Rc::new(f)))
            .collect();
        let active = Rc::new(opened.active);
        // Recovered sealed files start at level 0 (tiered compaction will merge
        // them upward as new runs accumulate). Levels are in-memory only.
        let level: FxHashMap<u32, u8> = sealed.keys().map(|&id| (id, 0u8)).collect();
        // Rebuild blob refcounts: one per live value-separated key (the sidecar
        // was repopulated from sealed footers + active-file replay during open).
        let values = ValueStore::new(dir.join("values"));
        for (_, h) in opened.index.valsep_iter() {
            values.incr_ref(h);
        }
        // Reclaim any blob a crash left without a referencing record (now that
        // refcounts reflect the live index, anything else on disk is an orphan).
        values.sweep_orphans().await?;
        // Seed the revision clock from the highest tstamp recovered, so revisions
        // never regress across a restart even if the wall clock stepped back
        // (next_tstamp already nudges within a run). This keeps CAS revisions and
        // watch `scan_since` resumption monotonic. (A tombstone whose tstamp
        // exceeds every live key's is not reflected here — a narrow, transient
        // case: reclaim drops dead tombstones, and recovery resolves last-writer
        // by physical order, not tstamp.)
        let max_tstamp = opened
            .index
            .iter()
            .map(|(_, e)| e.tstamp_ms)
            .max()
            .unwrap_or(0);
        Ok(Self {
            dir,
            values,
            index: RefCell::new(opened.index),
            sealed: RefCell::new(sealed),
            level: RefCell::new(level),
            active: RefCell::new(active),
            config,
            compaction_bytes: Cell::new(0),
            unsynced_bytes: Cell::new(0),
            last_tstamp: Cell::new(max_tstamp),
            reclaim_in_progress: Cell::new(false),
            rotate_in_progress: Cell::new(false),
            frozen: Cell::new(false),
            in_flight_writes: Cell::new(0),
            write_stripes: (0..WRITE_STRIPES)
                .map(|_| futures_util::lock::Mutex::new(()))
                .collect(),
        })
    }

    /// Stripe index for `key` (FxHash & (N-1)).
    fn stripe_idx(key: &[u8]) -> usize {
        use std::hash::{Hash, Hasher};
        let mut h = rustc_hash::FxHasher::default();
        key.hash(&mut h);
        (h.finish() as usize) & (WRITE_STRIPES - 1)
    }

    /// The write-serialization stripe for `key`. Same key → same stripe →
    /// serialized; different keys → (usually) different stripes → concurrent.
    fn wlock(&self, key: &[u8]) -> &futures_util::lock::Mutex<()> {
        &self.write_stripes[Self::stripe_idx(key)]
    }

    /// Block all subsequent writes (they return [`EngineError::Frozen`]) and
    /// wait for any already-in-flight writes to complete. Used by the seal
    /// path so the footer it writes is a consistent snapshot of on-disk state.
    pub async fn freeze_and_drain(&self) {
        self.frozen.set(true);
        // Already-in-flight writes incremented `in_flight_writes` before the
        // freeze (the check+increment is sync-atomic in `WriteGuard::enter`).
        // Poll until they all finish.
        while self.in_flight_writes.get() > 0 {
            monoio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    }

    /// Clear the freeze flag, allowing writes to proceed again. Used by the
    /// resume-after-abort path.
    pub fn unfreeze(&self) {
        self.frozen.set(false);
    }

    /// Atomically check the freeze flag and increment the in-flight counter.
    /// Returns a guard that decrements on drop. The check + increment is
    /// synchronous (no `.await`) so it is serialized with `freeze_and_drain`'s
    /// flag-set + counter-poll under monoio's single-threaded scheduler.
    fn begin_write(&self) -> Result<WriteGuard<'_>> {
        if self.frozen.get() {
            return Err(EngineError::Frozen);
        }
        self.in_flight_writes.set(self.in_flight_writes.get() + 1);
        Ok(WriteGuard {
            counter: &self.in_flight_writes,
        })
    }

    fn next_tstamp(&self) -> u64 {
        let now = now_ms();
        let last = self.last_tstamp.get();
        let next = if now > last { now } else { last + 1 };
        self.last_tstamp.set(next);
        next
    }

    fn active(&self) -> Rc<LogFile> {
        self.active.borrow().clone()
    }

    pub fn len(&self) -> usize {
        self.index.borrow().len()
    }

    pub fn is_empty(&self) -> bool {
        self.index.borrow().is_empty()
    }

    pub fn last_revision(&self) -> u64 {
        self.last_tstamp.get()
    }

    pub fn sealed_file_count(&self) -> usize {
        self.sealed.borrow().len()
    }

    /// Returns the tstamp (revision) assigned to this write. Callers must use
    /// this — NOT [`last_revision`](Self::last_revision) — for cache updates
    /// and client-visible revisions, because concurrent writes increment
    /// `last_tstamp` before any one of them commits.
    pub async fn put_full(
        &self,
        key: Bytes,
        value: &[u8],
        metadata: &[u8],
        expires_at_ms: Option<u64>,
    ) -> Result<u64> {
        self.await_reclaim().await; // stall (don't error) while a reclaim/flush runs
        let _wg = self.begin_write()?;
        let _w = self.wlock(&key).lock().await; // serialize writes to this key
        let tstamp = self.next_tstamp();
        let mut flags = 0u8;
        let exp = match expires_at_ms {
            Some(ms) => ms,
            None => {
                flags |= rflags::NO_EXPIRY;
                0
            }
        };
        // Value separation: a value >= the threshold is written to the blob store
        // (write-once, deduped) and the record carries only its 16-byte hash, so
        // compaction never re-uploads the value.
        let sep_hash = self.maybe_separate(value, &mut flags).await?;
        let stored: &[u8] = sep_hash.as_ref().map_or(value, |h| &h[..]);
        let mut buf = pool_acquire_write(HEADER_LEN + key.len() + stored.len() + metadata.len());
        record::encode_into(&mut buf, tstamp, flags, exp, &key, stored, metadata)?;
        let record_size = buf.len() as u32;
        let active = self.active();
        let (offset, buf) = match active.append(buf).await {
            Ok(r) => r,
            Err(e) => {
                // Append failed: roll back the blob ref so we don't leave a phantom
                // blob (written + ref'd, but no record references it).
                if let Some(h) = sep_hash {
                    self.values.unref(&h);
                }
                return Err(e);
            }
        };
        pool_release_write(buf);
        self.unsynced_bytes
            .set(self.unsynced_bytes.get() + record_size as u64);
        let entry = IndexEntry::new(active.file_id, offset, record_size, tstamp);
        let old_hash = self.apply_valsep_insert(key.clone(), entry, expires_at_ms, sep_hash);
        if let Some(oh) = old_hash {
            self.values.unref(&oh);
        }
        if active.write_offset() >= self.config.rotate_threshold {
            self.rotate_active().await?;
        }
        Ok(tstamp)
    }

    /// If `value` is large enough to separate, write it to the blob store (the
    /// store dedups + refcounts) and set the `VALUE_SEP` flag; return its hash.
    /// Otherwise return `None` (value stays inline). The blob is written before
    /// the log record so the record's hash always points at durable bytes.
    async fn maybe_separate(&self, value: &[u8], flags: &mut u8) -> Result<Option<ContentHash>> {
        if value.len() >= self.config.value_sep_threshold {
            *flags |= rflags::VALUE_SEP;
            Ok(Some(self.values.put(value).await?))
        } else {
            Ok(None)
        }
    }

    /// Insert the index entry and update the value-sep sidecar. Returns the key's
    /// PREVIOUS blob hash (if it was value-separated) so the caller can unref it
    /// after the new write commits — covering overwrite, large→small, and
    /// same-content cases uniformly (the new blob was already ref'd by `put`).
    fn apply_valsep_insert(
        &self,
        key: Bytes,
        entry: IndexEntry,
        expires_at_ms: Option<u64>,
        sep_hash: Option<ContentHash>,
    ) -> Option<ContentHash> {
        let mut index = self.index.borrow_mut();
        let old = index.valsep(&key);
        index.insert(key.clone(), entry, expires_at_ms);
        index.set_valsep(&key, sep_hash);
        old
    }

    /// Conditional write: write only if the current live state of `key` satisfies `cond`.
    /// Atomic — the key's write stripe is held across check + append + commit, so no
    /// concurrent write to the same key can interleave. A failed condition writes
    /// nothing. Returns `Ok(Some(tstamp))` if written, `Ok(None)` if the condition
    /// was not met. The returned tstamp is THIS write's revision; callers use it
    /// instead of [`last_revision`](Self::last_revision) for caches/responses.
    pub async fn put_full_cond(
        &self,
        key: Bytes,
        value: &[u8],
        metadata: &[u8],
        expires_at_ms: Option<u64>,
        cond: WriteCondition,
        now: u64,
    ) -> Result<Option<u64>> {
        self.await_reclaim().await; // stall (don't error) while a reclaim/flush runs
        let _wg = self.begin_write()?;
        let _w = self.wlock(&key).lock().await; // serialize writes to this key
        // Holding the key's write stripe, no concurrent write to this key can run.
        // So the condition check is authoritative: we check BEFORE appending, and a
        // failed condition writes NOTHING (no record, no blob) — there is no
        // optimistic orphan that a crash could resurrect, and no post-check.
        if !cond.check(Self::live_rev(&self.index.borrow(), &key, now)) {
            return Ok(None);
        }
        let tstamp = self.next_tstamp();
        let mut flags = 0u8;
        let exp = match expires_at_ms {
            Some(ms) => ms,
            None => {
                flags |= rflags::NO_EXPIRY;
                0
            }
        };
        let sep_hash = self.maybe_separate(value, &mut flags).await?;
        let stored: &[u8] = sep_hash.as_ref().map_or(value, |h| &h[..]);
        let mut buf = pool_acquire_write(HEADER_LEN + key.len() + stored.len() + metadata.len());
        record::encode_into(&mut buf, tstamp, flags, exp, &key, stored, metadata)?;
        let record_size = buf.len() as u32;
        let active = self.active();
        let (offset, buf) = match active.append(buf).await {
            Ok(r) => r,
            Err(e) => {
                // Append failed: roll back the blob ref so we don't leave a phantom
                // blob (written + ref'd, but no record references it).
                if let Some(h) = sep_hash {
                    self.values.unref(&h);
                }
                return Err(e);
            }
        };
        pool_release_write(buf);
        self.unsynced_bytes
            .set(self.unsynced_bytes.get() + record_size as u64);
        let entry = IndexEntry::new(active.file_id, offset, record_size, tstamp);
        let old_hash = self.apply_valsep_insert(key.clone(), entry, expires_at_ms, sep_hash);
        if let Some(oh) = old_hash {
            self.values.unref(&oh);
        }
        if active.write_offset() >= self.config.rotate_threshold {
            self.rotate_active().await?;
        }
        Ok(Some(tstamp))
    }

    fn live_rev(idx: &NsIndex, key: &[u8], now: u64) -> Option<u64> {
        if idx.is_expired(key, now) {
            None
        } else {
            idx.get(key).map(|e| e.tstamp_ms)
        }
    }

    /// Coalesce many puts into a single `write_at` + single `fsync`. Returns
    /// the tstamps assigned to each pair, in input order. Use these instead
    /// of [`last_revision`](Self::last_revision) — concurrent writes can bump
    /// `last_tstamp` higher than any tstamp this batch produced.
    pub async fn put_many(&self, pairs: &[(Bytes, Bytes)]) -> Result<Vec<u64>> {
        self.await_reclaim().await; // stall (don't error) while a reclaim/flush runs
        let _wg = self.begin_write()?;
        if pairs.is_empty() {
            return Ok(Vec::new());
        }
        // Serialize against same-key single-key writes by holding every stripe this
        // batch touches. Acquired in sorted-distinct order so two batches (or a
        // batch and a single write) can never deadlock on a lock-ordering cycle.
        let mut idxs: Vec<usize> = pairs.iter().map(|(k, _)| Self::stripe_idx(k)).collect();
        idxs.sort_unstable();
        idxs.dedup();
        let mut _stripe_guards = Vec::with_capacity(idxs.len());
        for i in idxs {
            _stripe_guards.push(self.write_stripes[i].lock().await);
        }
        let estimated: usize = pairs
            .iter()
            .map(|(k, v)| HEADER_LEN + k.len() + v.len())
            .sum();
        let mut buf = pool_acquire_write(estimated);
        let mut layout: Vec<(usize, u32, u64)> = Vec::with_capacity(pairs.len());
        // Per-pair blob hash for value-separated values (None = inline).
        let mut sep_hashes: Vec<Option<ContentHash>> = Vec::with_capacity(pairs.len());
        for (k, v) in pairs {
            let tstamp = self.next_tstamp();
            let mut flags = rflags::NO_EXPIRY;
            let sh = self.maybe_separate(v, &mut flags).await?;
            let stored: &[u8] = sh.as_ref().map_or(&v[..], |h| &h[..]);
            let start = buf.len();
            record::encode_into(&mut buf, tstamp, flags, 0, k, stored, &[])?;
            let record_size = (buf.len() - start) as u32;
            layout.push((start, record_size, tstamp));
            sep_hashes.push(sh);
        }
        let active = self.active();
        let buf_len = buf.len() as u64;
        let (base_offset, buf) = match active.append(buf).await {
            Ok(r) => r,
            Err(e) => {
                // Append failed: roll back every blob ref this batch took so none
                // are left as phantom blobs (written + ref'd, no record).
                for h in sep_hashes.into_iter().flatten() {
                    self.values.unref(&h);
                }
                return Err(e);
            }
        };
        pool_release_write(buf);
        self.unsynced_bytes.set(self.unsynced_bytes.get() + buf_len);
        let mut old_hashes: Vec<ContentHash> = Vec::new();
        {
            let mut index = self.index.borrow_mut();
            for (((k, _v), (rel_start, size, tstamp)), sh) in
                pairs.iter().zip(layout.iter()).zip(sep_hashes.iter())
            {
                let entry = IndexEntry::new(
                    active.file_id,
                    base_offset + *rel_start as u64,
                    *size,
                    *tstamp,
                );
                if let Some(oh) = index.valsep(k) {
                    old_hashes.push(oh);
                }
                index.insert(k.clone(), entry, None);
                index.set_valsep(k, *sh);
            }
        }
        for oh in old_hashes {
            self.values.unref(&oh);
        }
        if active.write_offset() >= self.config.rotate_threshold {
            self.rotate_active().await?;
        }
        Ok(layout.into_iter().map(|(_, _, t)| t).collect())
    }

    /// Append a tombstone for `key`; drop it from the index.
    /// Returns `Some(tstamp)` if the key was present and tombstoned, `None`
    /// otherwise. Callers must use this tstamp (not [`last_revision`](Self::last_revision))
    /// for watch events and any client-visible revision — concurrent writes
    /// can bump `last_tstamp` beyond this specific tombstone's tstamp.
    pub async fn tombstone(&self, key: &[u8]) -> Result<Option<u64>> {
        self.await_reclaim().await; // stall (don't error) while a reclaim/flush runs
        let _wg = self.begin_write()?;
        let _w = self.wlock(key).lock().await; // serialize writes to this key
        let old_hash = {
            let mut index = self.index.borrow_mut();
            let h = index.valsep(key);
            if index.remove(key).is_none() {
                return Ok(None);
            }
            h
        };
        let tstamp = self.next_tstamp();
        let mut buf = pool_acquire_write(HEADER_LEN + key.len());
        record::encode_into(&mut buf, tstamp, rflags::TOMBSTONE, 0, key, &[], &[])?;
        let active = self.active();
        let buf_len = buf.len() as u64;
        let (_, buf) = active.append(buf).await?;
        pool_release_write(buf);
        self.unsynced_bytes.set(self.unsynced_bytes.get() + buf_len);
        if let Some(h) = old_hash {
            self.values.unref(&h);
        }
        Ok(Some(tstamp))
    }

    /// Conditional tombstone: delete only if the current revision matches `expected_rev`.
    /// Returns `Some(tstamp)` if tombstoned, `None` if revision did not match.
    /// Atomic: the index removal is synchronous (no yield between check and removal).
    /// Callers must use the returned tstamp, not [`last_revision`](Self::last_revision),
    /// for client-visible revisions and watch events.
    pub async fn tombstone_cond(
        &self,
        key: &[u8],
        expected_rev: u64,
        now: u64,
    ) -> Result<Option<u64>> {
        self.await_reclaim().await; // stall (don't error) while a reclaim/flush runs
        let _wg = self.begin_write()?;
        let _w = self.wlock(key).lock().await; // serialize writes to this key
        // Both check and removal happen without yielding — no interleaving possible.
        let current_rev = Self::live_rev(&self.index.borrow(), key, now);
        if current_rev != Some(expected_rev) {
            return Ok(None);
        }
        let old_hash = {
            let mut index = self.index.borrow_mut();
            let h = index.valsep(key);
            index.remove(key);
            h
        };
        // Disk write (yields, but index already updated)
        let tstamp = self.next_tstamp();
        let mut buf = pool_acquire_write(HEADER_LEN + key.len());
        record::encode_into(&mut buf, tstamp, rflags::TOMBSTONE, 0, key, &[], &[])?;
        let active = self.active();
        let buf_len = buf.len() as u64;
        let (_, buf) = active.append(buf).await?;
        pool_release_write(buf);
        self.unsynced_bytes.set(self.unsynced_bytes.get() + buf_len);
        if let Some(h) = old_hash {
            self.values.unref(&h);
        }
        Ok(Some(tstamp))
    }

    /// Append a TTL-update record; modify only the sidecar. Returns the
    /// tstamp assigned to this update — callers must use it (not
    /// [`last_revision`](Self::last_revision)) for watch events.
    pub async fn ttl_update(&self, key: &[u8], expires_at_ms: Option<u64>) -> Result<u64> {
        self.await_reclaim().await; // stall (don't error) while a reclaim/flush runs
        let _wg = self.begin_write()?;
        let _w = self.wlock(key).lock().await; // serialize writes to this key
        let tstamp = self.next_tstamp();
        let mut flags = rflags::TTL_UPDATE;
        let exp = match expires_at_ms {
            Some(ms) => ms,
            None => {
                flags |= rflags::NO_EXPIRY;
                0
            }
        };
        let mut buf = pool_acquire_write(HEADER_LEN + key.len());
        record::encode_into(&mut buf, tstamp, flags, exp, key, &[], &[])?;
        let active = self.active();
        let buf_len = buf.len() as u64;
        let (_, buf) = active.append(buf).await?;
        pool_release_write(buf);
        self.unsynced_bytes.set(self.unsynced_bytes.get() + buf_len);
        let key_bytes = Bytes::copy_from_slice(key);
        self.index.borrow_mut().set_ttl(&key_bytes, expires_at_ms);
        Ok(tstamp)
    }

    fn locate_file(&self, file_id: u32) -> Option<Rc<LogFile>> {
        let active = self.active.borrow().clone();
        if active.file_id == file_id {
            return Some(active);
        }
        self.sealed.borrow().get(&file_id).cloned()
    }

    /// Fsync the active file if any writes are pending. Called by the per-shard
    /// 1-second timer task to provide `appendfsync everysec` semantics.
    pub async fn sync(&self) -> Result<()> {
        if self.unsynced_bytes.get() > 0 {
            self.active().sync().await?;
            self.unsynced_bytes.set(0);
        }
        // Every log record is durable now (just fsynced, or already was), so the
        // blobs orphaned by overwrites/deletes can finally be physically removed.
        // Deferring to here is what makes a power-loss revert safe: until the
        // superseding record is durable, the old blob stays on disk.
        self.values.collect_garbage().await;
        Ok(())
    }

    async fn read_record(&self, entry: IndexEntry) -> Result<BufGuard> {
        let file = self
            .locate_file(entry.file_id)
            .ok_or(EngineError::BadRecord {
                offset: entry.record_offset,
                reason: "file_id not found",
            })?;
        file.read_exact(entry.record_offset, entry.record_size as usize)
            .await
    }

    /// Returns `(value_field, metadata, flags)`. For value-separated records the
    /// `value_field` is the 16-byte content hash, not the value — call `deref`.
    fn extract_value_meta(bytes: &[u8]) -> Result<(Bytes, Bytes, u8)> {
        let hdr = parse_header(&bytes[..HEADER_LEN.min(bytes.len())], 0)?;
        let key_end = HEADER_LEN + hdr.key_size as usize;
        let val_end = key_end + hdr.val_size as usize;
        let meta_end = val_end + hdr.meta_size as usize;
        if bytes.len() < meta_end {
            return Err(EngineError::BadRecord {
                offset: 0,
                reason: "record bytes shorter than declared sizes",
            });
        }
        verify_crc(&hdr, &bytes[..HEADER_LEN], &bytes[HEADER_LEN..meta_end], 0)?;
        let value = Bytes::copy_from_slice(&bytes[key_end..val_end]);
        let metadata = Bytes::copy_from_slice(&bytes[val_end..meta_end]);
        Ok((value, metadata, hdr.flags))
    }

    /// Resolve a record's value field to the real value: if value-separated,
    /// fetch the blob by its hash; otherwise the field IS the value.
    async fn deref(&self, value: Bytes, flags: u8) -> Result<Bytes> {
        if flags & rflags::VALUE_SEP == 0 {
            return Ok(value);
        }
        if value.len() != std::mem::size_of::<ContentHash>() {
            return Err(EngineError::BadRecord {
                offset: 0,
                reason: "value-separated record's value field is not a 16-byte hash",
            });
        }
        let mut h: ContentHash = [0u8; 16];
        h.copy_from_slice(&value);
        let bytes = self.values.get(&h).await?;
        // Integrity: re-hash the blob and confirm it matches the content hash the
        // record points at — parity with the CRC check inline values get on every
        // read. Catches silent blob corruption AND a blob/hash mismatch, instead
        // of returning wrong bytes. BLAKE3 is SIMD-fast; this mirrors the per-read
        // CRC the inline path already pays over the value.
        if crate::value_store::content_hash(&bytes) != h {
            return Err(EngineError::BadRecord {
                offset: 0,
                reason: "value-separated blob content hash mismatch (corruption)",
            });
        }
        Ok(Bytes::from(bytes))
    }

    /// Single-record read: one `read_at`, parse header in-memory.
    pub async fn read_value(&self, entry: IndexEntry) -> Result<(Bytes, Bytes)> {
        let bytes = self.read_record(entry).await?;
        let (value, metadata, flags) = Self::extract_value_meta(&bytes)?;
        Ok((self.deref(value, flags).await?, metadata))
    }

    /// Bulk-read: submits all `read_at` futures concurrently via `join_all` so
    /// io_uring sees them as a batch. Used by `mget` to break the
    /// serial-per-key read pattern that dominates batched-GET latency.
    ///
    /// Caller passes already-resolved (slot_index, IndexEntry) for the disk
    /// misses; returns parallel `(slot_index, value, metadata)` tuples.
    pub async fn bulk_read(
        &self,
        misses: Vec<(usize, IndexEntry)>,
    ) -> Result<Vec<(usize, Bytes, Bytes)>> {
        if misses.is_empty() {
            return Ok(Vec::new());
        }
        let futures: Vec<_> = misses.iter().map(|(_, e)| self.read_record(*e)).collect();
        let results: Vec<Result<BufGuard>> = join_all(futures).await;
        let mut out: Vec<(usize, Bytes, Bytes)> = Vec::with_capacity(misses.len());
        for ((slot, _entry), bytes_res) in misses.into_iter().zip(results.into_iter()) {
            let bytes = bytes_res?;
            let (value, metadata, flags) = Self::extract_value_meta(&bytes)?;
            out.push((slot, self.deref(value, flags).await?, metadata));
        }
        Ok(out)
    }

    /// Return all live keys matching `filter` as `WatchEvent::Set` for initial-subscribe
    /// (since = 0). Reads from the in-memory index then fetches values from disk.
    pub async fn current_entries(
        &self,
        filter: &crate::watch::KeyFilter<'_>,
        now: u64,
    ) -> Result<Vec<crate::watch::WatchEvent>> {
        use crate::watch::WatchEvent;

        let live: Vec<(Bytes, index::IndexEntry, Option<u64>)> = {
            let idx = self.index.borrow();
            idx.iter()
                .filter(|(k, _)| filter.matches(k) && !idx.is_expired(k, now))
                .map(|(k, e)| (k.clone(), *e, idx.ttl(k)))
                .collect()
        };

        if live.is_empty() {
            return Ok(Vec::new());
        }

        // Submit all value reads concurrently via io_uring (same pattern as bulk_read).
        let misses: Vec<(usize, index::IndexEntry)> = live
            .iter()
            .enumerate()
            .map(|(i, (_, e, _))| (i, *e))
            .collect();
        let read_results = self.bulk_read(misses).await?;

        let mut events = Vec::with_capacity(live.len());
        for (slot, value, meta_bytes) in read_results {
            let (key, _, expires_at_ms) = &live[slot];
            let metadata = if meta_bytes.is_empty() {
                None
            } else {
                match serde_json::from_slice::<serde_json::Value>(&meta_bytes) {
                    Ok(v) => Some(Arc::new(v)),
                    Err(e) => {
                        warn!(key = ?key, error = %e, "corrupt metadata during current_entries; dropping field");
                        None
                    }
                }
            };
            events.push(WatchEvent::Set {
                key: key.clone(),
                value,
                metadata,
                expires_at_ms: *expires_at_ms,
                revision: 0,
            });
        }
        Ok(events)
    }

    /// Scan all log files for mutations with `tstamp_ms > since_revision` that match
    /// `filter`. Returns events in chronological order. Used for catch-up replay on
    /// reconnect.
    pub async fn scan_since(
        &self,
        filter: &crate::watch::KeyFilter<'_>,
        since_revision: u64,
    ) -> Result<Vec<crate::watch::WatchEvent>> {
        let mut files: Vec<(u32, Rc<LogFile>)> = self
            .sealed
            .borrow()
            .iter()
            .map(|(&id, f)| (id, f.clone()))
            .collect();
        files.sort_by_key(|(id, _)| *id);
        files.push((self.active.borrow().file_id, self.active()));

        let mut events = Vec::new();
        for (_, file) in &files {
            let end = file.data_end_offset().await;
            scan_file_records(file, end, filter, since_revision, &self.values, &mut events).await?;
        }
        // Sort by revision so callers see a clean chronological stream.
        events.sort_by_key(|e| match e {
            crate::watch::WatchEvent::Set { revision, .. } => *revision,
            crate::watch::WatchEvent::Del { revision, .. } => *revision,
        });
        Ok(events)
    }

    /// Write a footer to the active file without rotating. Called on clean shutdown
    /// so the next startup can load this file as a sealed file (fast footer read)
    /// instead of replaying it record-by-record.
    pub async fn seal_active_for_shutdown(&self) -> Result<()> {
        // Test-only fail-once hook. Production never sets the env var. When
        // set and the named file exists, we unlink the file (consume the
        // signal) and return Err so the seal protocol path is exercised
        // end-to-end against the real engine. The next seal succeeds.
        if let Ok(p) = std::env::var("KV_TEST_FAIL_ONCE_FILE")
            && std::path::Path::new(&p).exists()
        {
            let _ = std::fs::remove_file(&p);
            return Err(EngineError::TestSealFailure);
        }
        let active = self.active.borrow().clone();
        // Fsync the records BEFORE building the footer. Without this, records
        // appended after the last `sync_logs` tick sit in the OS page cache;
        // the footer references them but a process crash between SealComplete
        // and exit would lose the records, leaving the successor with footer
        // entries that point at bytes that never made it to disk.
        if self.unsynced_bytes.get() != 0 {
            active.sync().await?;
            self.unsynced_bytes.set(0);
        }
        let footer: Vec<FooterEntry> = {
            let index = self.index.borrow();
            index
                .iter()
                .filter(|(_, e)| e.file_id == active.file_id)
                .map(|(k, e)| FooterEntry {
                    key: k.clone(),
                    record_offset: e.record_offset,
                    record_size: e.record_size,
                    expires_at_ms: index.ttl(k),
                    tstamp_ms: e.tstamp_ms,
                    value_hash: index.valsep(k),
                })
                .collect()
        };
        active.write_footer(&footer).await
    }

    /// Move the (already-footered) active file into the sealed map and open a
    /// fresh active file. Used by [`crate::store::ShardStore::resume_after_abort`]
    /// to recover from a post-seal handoff abort: the seal footer was written
    /// to the active file by `seal_active_for_shutdown`, but no new active file
    /// was opened. This method completes that transition without writing a
    /// second footer.
    pub async fn reopen_active_after_seal(&self) -> Result<()> {
        let old_active = self.active.borrow().clone();
        let next_id = {
            let sealed = self.sealed.borrow();
            let max_existing = sealed
                .keys()
                .copied()
                .max()
                .unwrap_or(0)
                .max(old_active.file_id);
            max_existing
                .checked_add(1)
                .ok_or(EngineError::CapacityExceeded {
                    reason: "file_id overflow: namespace has too many log files",
                })?
        };
        self.sealed
            .borrow_mut()
            .insert(old_active.file_id, old_active);
        let new_path = self.dir.join(data_filename(next_id));
        let new_active = Rc::new(LogFile::open_rw(new_path, next_id).await?);
        sync_dir(&self.dir).await; // make the new file's directory entry durable
        *self.active.borrow_mut() = new_active;
        self.unsynced_bytes.set(0);
        Ok(())
    }

    /// Seal the current active file (writing its footer) and open a new active file.
    /// Called automatically when `active.write_offset() >= config.rotate_threshold`.
    ///
    /// NOT concurrent-safe with other write operations on this namespace. The
    /// `rotate_in_progress` guard prevents a second monoio task from racing in
    /// while the first is awaiting the footer write.
    pub async fn rotate_active(&self) -> Result<()> {
        if self.rotate_in_progress.get() {
            return Ok(());
        }
        self.rotate_in_progress.set(true);
        let result = self.rotate_active_inner().await;
        self.rotate_in_progress.set(false);
        result
    }

    async fn rotate_active_inner(&self) -> Result<()> {
        let old_active = self.active.borrow().clone();
        let footer: Vec<FooterEntry> = {
            let index = self.index.borrow();
            index
                .iter()
                .filter(|(_, e)| e.file_id == old_active.file_id)
                .map(|(k, e)| FooterEntry {
                    key: k.clone(),
                    record_offset: e.record_offset,
                    record_size: e.record_size,
                    expires_at_ms: index.ttl(k),
                    tstamp_ms: e.tstamp_ms,
                    value_hash: index.valsep(k),
                })
                .collect()
        };
        old_active.write_footer(&footer).await?;
        let next_id = {
            let sealed = self.sealed.borrow();
            let max_existing = sealed
                .keys()
                .copied()
                .max()
                .unwrap_or(0)
                .max(old_active.file_id);
            max_existing
                .checked_add(1)
                .ok_or(EngineError::CapacityExceeded {
                    reason: "file_id overflow: namespace has too many log files",
                })?
        };
        self.sealed
            .borrow_mut()
            .insert(old_active.file_id, old_active);
        let new_path = self.dir.join(data_filename(next_id));
        let new_active = Rc::new(LogFile::open_rw(new_path, next_id).await?);
        sync_dir(&self.dir).await; // make the new file's directory entry durable
        *self.active.borrow_mut() = new_active;
        self.unsynced_bytes.set(0);
        Ok(())
    }

    /// Unlink-and-recreate all files for the namespace. Preserves CoW sharing
    /// with the parent fork (parent's inode still references the old blocks;
    /// the new active file's blocks are local).
    ///
    /// Returns the tstamp assigned to this flush — use it (not
    /// [`last_revision`](Self::last_revision)) when emitting per-key Del
    /// watch events for the wiped namespace, so all events share a single
    /// monotonic revision that's strictly newer than every prior write.
    ///
    /// NOT safe under concurrent reads/writes — caller must serialize.
    pub async fn flush(&self) -> Result<u64> {
        // Wait out any reclaim, then take the exclusive flag (shared with reclaim)
        // so neither a reclaim nor another flush can run concurrently; writes wait
        // on the same flag. `replace` is the atomic gate against a racing op.
        self.await_reclaim().await;
        if self.reclaim_in_progress.replace(true) {
            return Err(EngineError::ReclamationBusy);
        }
        // Drain in-flight writes before unlinking/recreating the data files, so a
        // write mid-append can't race the file replacement.
        while self.in_flight_writes.get() > 0 {
            monoio::time::sleep(std::time::Duration::from_micros(50)).await;
        }
        // Burn a tstamp for the flush event itself. Doing this BEFORE the
        // flush guarantees the revision exceeds anything previously committed
        // (or even speculatively assigned by concurrent failed writes).
        let revision = self.next_tstamp();
        let result = self.flush_inner().await.map(|()| revision);
        self.reclaim_in_progress.set(false);
        result
    }

    async fn flush_inner(&self) -> Result<()> {
        // Drop file handles inside their cells.
        self.sealed.borrow_mut().clear();
        self.index.borrow_mut().clear();

        // Unlink all data-* files (including current active — it's still held
        // through the Rc until we replace it; on Linux the inode's blocks stay
        // alive for the open handle and are freed when we drop the Rc).
        let to_unlink: Vec<PathBuf> = match std::fs::read_dir(&self.dir) {
            Ok(entries) => entries
                .flatten()
                .map(|e| e.path())
                .filter(|p| {
                    p.file_name()
                        .and_then(|n| n.to_str())
                        .is_some_and(|n| n.starts_with("data-"))
                })
                .collect(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(e.into()),
        };
        let unlink_futures: Vec<_> = to_unlink
            .iter()
            .map(|p| monoio::fs::remove_file(p.clone()))
            .collect();
        for (path, res) in to_unlink.iter().zip(join_all(unlink_futures).await) {
            if let Err(e) = res {
                warn!(path = %path.display(), error = %e, "failed to unlink data file during flush");
            }
        }

        let path = self.dir.join(data_filename(0));
        let new_active = Rc::new(LogFile::open_rw(path, 0).await?);
        sync_dir(&self.dir).await; // make the recreated file's directory entry durable
        *self.active.borrow_mut() = new_active;
        self.unsynced_bytes.set(0);
        Ok(())
    }

    /// Operator-triggered reclaim (size-tiered compaction). Seals the active
    /// file as a fresh level-0 run, then repeatedly merges the lowest level
    /// that has reached `fanout` runs into one run at the next level. Each
    /// merge rewrites only that level's live records (O(log N) total write
    /// amplification) — never the whole live set, so on GlideFS a reclaim
    /// re-uploads one level, not the entire namespace.
    ///
    /// NOT concurrent-safe with other ops on this namespace.
    pub async fn reclaim(&self) -> Result<reclaim::ReclaimReport> {
        // Atomic check-and-set: a second concurrent reclaim on this namespace is a
        // no-op error (only one reclaim at a time). Writes do NOT error — they wait
        // on `await_reclaim` and proceed once this finishes.
        if self.reclaim_in_progress.replace(true) {
            return Err(EngineError::ReclamationBusy);
        }
        // Drain writes that already passed the gate before we set the flag, so the
        // seal's footer is a consistent snapshot — no in-flight write (appended but
        // not yet indexed) is missed and silently lost on the next footer recovery.
        // New writes now wait in `await_reclaim` BEFORE `begin_write`, so they don't
        // hold `in_flight_writes` and this drain always terminates (no deadlock).
        while self.in_flight_writes.get() > 0 {
            monoio::time::sleep(std::time::Duration::from_micros(50)).await;
        }
        let result = self.reclaim_inner().await;
        self.reclaim_in_progress.set(false);
        result
    }

    /// Block until no reclaim is in progress on this namespace. Called at the very
    /// start of every write (before `begin_write`), so writes stall during a
    /// reclaim instead of erroring, and waiters never hold the in-flight count.
    async fn await_reclaim(&self) {
        while self.reclaim_in_progress.get() {
            monoio::time::sleep(std::time::Duration::from_micros(50)).await;
        }
    }

    async fn reclaim_inner(&self) -> Result<reclaim::ReclaimReport> {
        use std::collections::{BTreeMap, HashSet};

        // 1. Seal the active file as a fresh level-0 run.
        let old_active = self.active.borrow().clone();
        let footer: Vec<FooterEntry> = {
            let index = self.index.borrow();
            index
                .iter()
                .filter(|(_, e)| e.file_id == old_active.file_id)
                .map(|(k, e)| FooterEntry {
                    key: k.clone(),
                    record_offset: e.record_offset,
                    record_size: e.record_size,
                    expires_at_ms: index.ttl(k),
                    tstamp_ms: e.tstamp_ms,
                    value_hash: index.valsep(k),
                })
                .collect()
        };
        old_active.write_footer(&footer).await?;
        self.sealed
            .borrow_mut()
            .insert(old_active.file_id, old_active); // level 0 (absent from map)

        let mut total = reclaim::ReclaimReport {
            live_keys: 0,
            live_bytes: 0,
            dead_files_dropped: 0,
            dead_files_leaked: 0,
            new_file_id: 0,
        };

        // 2. Cascade: while some level holds >= fanout runs, merge that level
        //    into one run at the next level.
        loop {
            let by_level: BTreeMap<u8, Vec<u32>> = {
                let levels = self.level.borrow();
                let mut m: BTreeMap<u8, Vec<u32>> = BTreeMap::new();
                for &id in self.sealed.borrow().keys() {
                    m.entry(levels.get(&id).copied().unwrap_or(0))
                        .or_default()
                        .push(id);
                }
                m
            };
            let (lvl, ids) = match by_level
                .iter()
                .find(|(_, ids)| ids.len() >= self.config.fanout)
            {
                Some((&l, ids)) => (l, ids.clone()),
                None => break,
            };

            let files: Vec<Rc<LogFile>> = {
                let sealed = self.sealed.borrow();
                ids.iter()
                    .filter_map(|id| sealed.get(id).cloned())
                    .collect()
            };
            let id_set: HashSet<u32> = ids.iter().copied().collect();
            let live: Vec<(Bytes, IndexEntry, Option<u64>)> = {
                let index = self.index.borrow();
                index
                    .iter()
                    .filter(|(_, e)| id_set.contains(&e.file_id))
                    .map(|(k, e)| (k.clone(), *e, index.ttl(k)))
                    .collect()
            };
            let next_id = {
                let sealed = self.sealed.borrow();
                sealed
                    .keys()
                    .copied()
                    .max()
                    .unwrap_or(0)
                    .max(self.active.borrow().file_id)
                    .checked_add(1)
                    .ok_or(EngineError::CapacityExceeded {
                        reason: "file_id overflow: namespace has too many log files",
                    })?
            };

            // reclaim_namespace writes one merged file (next_id) and unlinks the
            // input `files`; index borrow is not held across the await.
            let (report, new_entries) =
                reclaim::reclaim_namespace(self.dir.clone(), &files, next_id, &live).await?;
            total.live_keys = report.live_keys;
            total.live_bytes = report.live_bytes;
            self.compaction_bytes
                .set(self.compaction_bytes.get() + report.live_bytes);
            total.dead_files_dropped += report.dead_files_dropped;
            total.dead_files_leaked += report.dead_files_leaked;

            {
                let mut index = self.index.borrow_mut();
                for (key, entry, ttl) in new_entries {
                    index.insert(key, entry, ttl);
                }
            }
            {
                let mut sealed = self.sealed.borrow_mut();
                let mut levels = self.level.borrow_mut();
                for id in &ids {
                    sealed.remove(id);
                    levels.remove(id);
                }
            }
            let new_file =
                Rc::new(LogFile::open_ro(self.dir.join(data_filename(next_id)), next_id).await?);
            self.sealed.borrow_mut().insert(next_id, new_file);
            self.level
                .borrow_mut()
                .insert(next_id, lvl.saturating_add(1));
        }

        // 3. Open a fresh active file.
        let new_active_id = {
            let sealed = self.sealed.borrow();
            sealed
                .keys()
                .copied()
                .max()
                .unwrap_or(0)
                .max(self.active.borrow().file_id)
                .checked_add(1)
                .ok_or(EngineError::CapacityExceeded {
                    reason: "file_id overflow: namespace has too many log files",
                })?
        };
        let new_active = Rc::new(
            LogFile::open_rw(self.dir.join(data_filename(new_active_id)), new_active_id).await?,
        );
        sync_dir(&self.dir).await; // make the new active file's directory entry durable
        *self.active.borrow_mut() = new_active;
        self.unsynced_bytes.set(0);
        total.new_file_id = new_active_id;
        Ok(total)
    }
}

/// RAII counter increment returned by [`NamespaceLog::begin_write`]. Each
/// write method holds one for its lifetime; [`NamespaceLog::freeze_and_drain`]
/// polls the counter to zero before allowing a seal to proceed.
struct WriteGuard<'a> {
    counter: &'a Cell<u32>,
}

impl Drop for WriteGuard<'_> {
    fn drop(&mut self) {
        self.counter.set(self.counter.get().saturating_sub(1));
    }
}

/// Scan one log file for mutation records newer than `since_revision` that
/// match `filter`, appending decoded `WatchEvent`s to `events`.
async fn scan_file_records(
    file: &LogFile,
    end_offset: u64,
    filter: &crate::watch::KeyFilter<'_>,
    since_revision: u64,
    values: &ValueStore,
    events: &mut Vec<crate::watch::WatchEvent>,
) -> Result<()> {
    use crate::watch::WatchEvent;

    let mut offset = 0u64;
    while offset + record::HEADER_LEN as u64 <= end_offset {
        let hdr_bytes = match file.read_exact(offset, record::HEADER_LEN).await {
            Ok(b) => b,
            Err(_) => break,
        };
        let hdr = match record::parse_header(&hdr_bytes, offset) {
            Ok(h) => h,
            Err(_) => break,
        };
        // Guard against garbage sizes that would produce an absurd record_len.
        if hdr.key_size == 0 {
            break;
        }
        let record_len = hdr.record_len() as u64;
        if offset + record_len > end_offset {
            break;
        }

        // Only decode records newer than the client's last-seen revision, skip TTL-only updates.
        let is_ttl = hdr.flags & record::flags::TTL_UPDATE != 0;
        if hdr.tstamp_ms > since_revision && !is_ttl {
            let body = match file
                .read_exact(offset + record::HEADER_LEN as u64, hdr.body_len())
                .await
            {
                Ok(b) => b,
                Err(_) => break,
            };
            let key = &body[..hdr.key_size as usize];
            if filter.matches(key) {
                let is_tombstone = hdr.flags & record::flags::TOMBSTONE != 0;
                let key_b = Bytes::copy_from_slice(key);
                if is_tombstone {
                    events.push(WatchEvent::Del {
                        key: key_b,
                        revision: hdr.tstamp_ms,
                    });
                } else {
                    let val_start = hdr.key_size as usize;
                    let val_end = val_start + hdr.val_size as usize;
                    let meta_end = val_end + hdr.meta_size as usize;
                    // Value-separated records carry the 16-byte blob hash, not the
                    // value — deref it (and verify) so watchers replaying via
                    // scan_since see the real value, not the pointer.
                    let value = if hdr.flags & record::flags::VALUE_SEP != 0 {
                        let field = &body[val_start..val_end];
                        if field.len() != 16 {
                            warn!(
                                offset,
                                "value-sep record without a 16-byte hash; skipping watch event"
                            );
                            offset += record_len;
                            continue;
                        }
                        let mut h: ContentHash = [0u8; 16];
                        h.copy_from_slice(field);
                        match values.get(&h).await {
                            Ok(b) if crate::value_store::content_hash(&b) == h => Bytes::from(b),
                            Ok(_) => {
                                warn!(
                                    offset,
                                    "blob hash mismatch during watch replay; skipping event"
                                );
                                offset += record_len;
                                continue;
                            }
                            Err(e) => {
                                warn!(offset, error = %e, "blob read failed during watch replay; skipping event");
                                offset += record_len;
                                continue;
                            }
                        }
                    } else {
                        Bytes::copy_from_slice(&body[val_start..val_end])
                    };
                    let meta_bytes = &body[val_end..meta_end];
                    let metadata = if meta_bytes.is_empty() {
                        None
                    } else {
                        match serde_json::from_slice::<serde_json::Value>(meta_bytes) {
                            Ok(v) => Some(Arc::new(v)),
                            Err(e) => {
                                warn!(offset, error = %e, "corrupt metadata in scan_file_records; dropping field");
                                None
                            }
                        }
                    };
                    let expires_at_ms = if hdr.flags & record::flags::NO_EXPIRY != 0 {
                        None
                    } else {
                        Some(hdr.expires_at_ms)
                    };
                    events.push(WatchEvent::Set {
                        key: key_b,
                        value,
                        metadata,
                        expires_at_ms,
                        revision: hdr.tstamp_ms,
                    });
                }
            }
        }

        offset += record_len;
    }
    Ok(())
}

#[cfg(test)]
mod compaction_tests {
    use super::*;
    use crate::log::config::LogConfig;
    use bytes::Bytes;
    use tempfile::TempDir;

    fn run<F: std::future::Future>(f: F) -> F::Output {
        monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_timer()
            .build()
            .expect("monoio runtime")
            .block_on(f)
    }

    async fn write_batch(log: &NamespaceLog, lo: usize, hi: usize) {
        let val = vec![b'a'; 1000];
        for i in lo..hi {
            log.put_full(Bytes::from(format!("k{i:05}")), &val, &[], None)
                .await
                .unwrap();
        }
    }

    fn sealed_ids(log: &NamespaceLog) -> std::collections::HashSet<u32> {
        log.sealed.borrow().keys().copied().collect()
    }

    /// The flood fix: reclaim must NOT rewrite the inherited base on every
    /// reclaim. After a first reclaim produces a level-1 base run, a second
    /// batch + reclaim should merge only the NEW level-0 runs into a second
    /// level-1 run — leaving the original base untouched (still on disk, not
    /// re-uploaded to S3).
    #[test]
    fn reclaim_does_not_rewrite_base_each_reclaim() {
        run(async {
            let dir = TempDir::new().unwrap();
            let cfg = LogConfig {
                rotate_threshold: 4096,
                fanout: 4,
                value_sep_threshold: 128 * 1024,
            };
            let log = NamespaceLog::open(dir.path().to_path_buf(), cfg)
                .await
                .unwrap();

            write_batch(&log, 0, 30).await;
            assert!(
                log.sealed_file_count() >= 4,
                "batch should seal >= fanout level-0 runs"
            );
            log.reclaim().await.unwrap();
            let base = sealed_ids(&log);
            assert_eq!(
                base.len(),
                1,
                "level-0 runs merge into one level-1 base run"
            );

            write_batch(&log, 30, 60).await;
            log.reclaim().await.unwrap();
            let after = sealed_ids(&log);

            assert!(
                base.is_subset(&after),
                "tiered must leave the base run untouched (not re-upload it): base={base:?} after={after:?}"
            );
            assert_eq!(
                after.len(),
                2,
                "two level-1 runs (< fanout) — no base re-merge"
            );
            assert_eq!(log.len(), 60, "all keys live through tiered merges");
        });
    }

    /// Quantitative flood check on the REAL engine: 12 reclaims over a churning
    /// ~200-key live set. Size-tiered rewrites far less than full-merge would.
    /// Full-merge's cost is analytical (it rewrote the whole live set on every
    /// reclaim — 12 × live-set), so we compare measured tiered bytes against
    /// that ceiling without keeping the dead full-merge path around.
    #[test]
    fn reclaim_write_amp_beats_full_merge() {
        run(async {
            let dir = TempDir::new().unwrap();
            let cfg = LogConfig {
                rotate_threshold: 4096,
                fanout: 4,
                value_sep_threshold: 128 * 1024,
            };
            let log = NamespaceLog::open(dir.path().to_path_buf(), cfg)
                .await
                .unwrap();
            write_batch(&log, 0, 200).await; // base live set
            log.reclaim().await.unwrap(); // fold base into a level-1 run
            let live_set_bytes = log.compaction_bytes.get(); // ~one full live-set rewrite
            log.compaction_bytes.set(0); // measure the churn phase only

            let reclaims = 12usize;
            for r in 0..reclaims {
                let lo = (r * 16) % 200;
                write_batch(&log, lo, lo + 16).await; // overwrite 16 existing keys
                log.reclaim().await.unwrap();
            }
            let tiered = log.compaction_bytes.get();
            // Full-merge would rewrite the entire live set on every reclaim.
            let full_merge = live_set_bytes * reclaims as u64;
            eprintln!(
                "\n  COMPACTION BYTES over {reclaims} reclaims (base ~200 KiB):\n    full-merge (analytical = {reclaims}× live set) = {:.2} MiB\n    size-tiered (measured)                       = {:.2} MiB\n    tiered rewrites {:.1}× LESS\n",
                full_merge as f64 / 1048576.0,
                tiered as f64 / 1048576.0,
                full_merge as f64 / tiered.max(1) as f64
            );
            assert!(
                tiered * 2 < full_merge,
                "tiered must rewrite far less: tiered={tiered} full-merge={full_merge}"
            );
        });
    }
}

#[cfg(test)]
mod value_sep_tests {
    use super::*;
    use crate::log::config::LogConfig;
    use crate::value_store::content_hash;
    use bytes::Bytes;
    use tempfile::TempDir;

    fn run<F: std::future::Future>(f: F) -> F::Output {
        monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_timer()
            .build()
            .expect("monoio runtime")
            .block_on(f)
    }

    fn key(i: usize) -> Bytes {
        Bytes::from(format!("k{i:05}"))
    }

    /// A large value is stored in the blob store, NOT inline: the log record is a
    /// tiny pointer (header + key + 16-byte hash), and GET still returns the value.
    #[test]
    fn large_value_is_separated_and_reads_back() {
        run(async {
            let dir = TempDir::new().unwrap();
            let cfg = LogConfig {
                rotate_threshold: 1 << 30,
                fanout: 8,
                value_sep_threshold: 4096,
            };
            let log = NamespaceLog::open(dir.path().to_path_buf(), cfg)
                .await
                .unwrap();

            let big = vec![0xABu8; 64 * 1024]; // 64 KiB > 4 KiB threshold
            log.put_full(key(0), &big, &[], None).await.unwrap();

            assert_eq!(log.values.blob_count(), 1, "value went to the blob store");
            let entry = *log.index.borrow().get(b"k00000").unwrap();
            assert!(
                (entry.record_size as usize) < 4096,
                "log record is a tiny pointer, not the 64 KiB value: {} bytes",
                entry.record_size
            );
            let (v, _m) = log.read_value(entry).await.unwrap();
            assert_eq!(
                v,
                Bytes::from(big),
                "GET derefs the blob and returns the value"
            );

            // A small value stays inline (no new blob).
            log.put_full(key(1), b"small", &[], None).await.unwrap();
            assert_eq!(log.values.blob_count(), 1, "small value stays inline");
        });
    }

    /// THE proof: compaction moves only pointers for separated values. Churn a set
    /// of large values across many reclaims and compare compaction bytes to inline.
    #[test]
    fn compaction_moves_only_pointers_not_values() {
        run(async {
            async fn churn(threshold: usize) -> (u64, usize) {
                let dir = TempDir::new().unwrap();
                let cfg = LogConfig {
                    rotate_threshold: 64 * 1024,
                    fanout: 4,
                    value_sep_threshold: threshold,
                };
                let log = NamespaceLog::open(dir.path().to_path_buf(), cfg)
                    .await
                    .unwrap();
                let n = 60usize;
                let v0 = vec![0xCDu8; 32 * 1024];
                for i in 0..n {
                    log.put_full(key(i), &v0, &[], None).await.unwrap();
                }
                log.reclaim().await.unwrap();
                log.compaction_bytes.set(0); // measure the churn phase only
                for r in 0..10u8 {
                    let vr = vec![r; 32 * 1024]; // new content each round
                    for i in 0..n {
                        log.put_full(key(i), &vr, &[], None).await.unwrap();
                    }
                    log.reclaim().await.unwrap();
                }
                // all n keys still readable through the blob deref
                for i in 0..n {
                    let e = *log
                        .index
                        .borrow()
                        .get(format!("k{i:05}").as_bytes())
                        .unwrap();
                    assert_eq!(log.read_value(e).await.unwrap().0.len(), 32 * 1024);
                }
                (log.compaction_bytes.get(), log.values.blob_count())
            }
            let (vs_bytes, vs_blobs) = churn(4096).await; // value-separated
            let (inline_bytes, _) = churn(usize::MAX).await; // everything inline
            eprintln!(
                "\n  COMPACTION BYTES over 10 reclaims (60 keys x 32 KiB, churned):\n    inline       = {:.2} MiB\n    value-sep    = {:.2} MiB  ({} live blobs — dedup across keys)\n    value-sep moves {:.0}x fewer bytes (only pointers)\n",
                inline_bytes as f64 / 1048576.0,
                vs_bytes as f64 / 1048576.0,
                vs_blobs,
                inline_bytes as f64 / vs_bytes.max(1) as f64
            );
            assert!(
                vs_bytes * 5 < inline_bytes,
                "value-sep must move far fewer compaction bytes: vs={vs_bytes} inline={inline_bytes}"
            );
            assert!(
                vs_blobs <= 2,
                "identical per-round values dedup to ~1 blob, got {vs_blobs}"
            );
        });
    }

    /// Overwriting or deleting a separated value reclaims the old blob (refcount→0).
    #[test]
    fn overwrite_and_delete_gc_the_blob() {
        run(async {
            let dir = TempDir::new().unwrap();
            let cfg = LogConfig {
                rotate_threshold: 1 << 30,
                fanout: 8,
                value_sep_threshold: 4096,
            };
            let log = NamespaceLog::open(dir.path().to_path_buf(), cfg)
                .await
                .unwrap();

            log.put_full(key(0), &vec![1u8; 8192], &[], None)
                .await
                .unwrap();
            assert_eq!(log.values.blob_count(), 1);
            // overwrite with different content -> old blob GC'd, one blob remains
            log.put_full(key(0), &vec![2u8; 8192], &[], None)
                .await
                .unwrap();
            assert_eq!(
                log.values.blob_count(),
                1,
                "old blob reclaimed on overwrite"
            );
            // delete -> blob GC'd
            log.tombstone(b"k00000").await.unwrap();
            assert_eq!(log.values.blob_count(), 0, "blob reclaimed on delete");
        });
    }

    /// After a clean restart, separated values still read back (footer carried the
    /// hash; refcounts rebuilt), and a subsequent overwrite still GCs correctly.
    #[test]
    fn separated_values_survive_reopen() {
        run(async {
            let dir = TempDir::new().unwrap();
            let path = dir.path().to_path_buf();
            let cfg = LogConfig {
                rotate_threshold: 1 << 30,
                fanout: 8,
                value_sep_threshold: 4096,
            };
            let big = vec![0x5Au8; 100 * 1024];
            {
                let log = NamespaceLog::open(path.clone(), cfg).await.unwrap();
                log.put_full(key(0), &big, &[], None).await.unwrap();
                log.reclaim().await.unwrap(); // seal -> footer carries the value hash
            }
            // Reopen from disk.
            let log = NamespaceLog::open(path.clone(), cfg).await.unwrap();
            let e = *log.index.borrow().get(b"k00000").unwrap();
            assert_eq!(
                log.read_value(e).await.unwrap().0,
                Bytes::from(big.clone()),
                "value reads back after reopen"
            );
            assert_eq!(
                log.values.refcount(&content_hash(&big)),
                1,
                "refcount rebuilt from footer"
            );
            // Overwrite -> the rebuilt refcount lets the inherited blob GC.
            log.put_full(key(0), &vec![9u8; 100 * 1024], &[], None)
                .await
                .unwrap();
            assert_eq!(
                log.values.refcount(&content_hash(&big)),
                0,
                "old blob unref'd after reopen+overwrite"
            );
        });
    }

    /// CRASH recovery (no clean footer): a value-separated key written to the
    /// active file and never sealed must, after reopen, rebuild the value-sep
    /// sidecar from the RECORD SCAN (`replay_active`) — not the footer. Proven by
    /// a post-reopen overwrite correctly GC'ing the inherited blob.
    #[test]
    fn separated_values_survive_crash_recovery() {
        run(async {
            let dir = TempDir::new().unwrap();
            let path = dir.path().to_path_buf();
            let cfg = LogConfig {
                rotate_threshold: 1 << 30,
                fanout: 8,
                value_sep_threshold: 4096,
            };
            let big = vec![0x33u8; 100 * 1024];
            {
                let log = NamespaceLog::open(path.clone(), cfg).await.unwrap();
                log.put_full(key(0), &big, &[], None).await.unwrap();
                // Drop WITHOUT sealing -> active file has no footer (a crash).
            }
            let log = NamespaceLog::open(path.clone(), cfg).await.unwrap();
            let e = *log.index.borrow().get(b"k00000").unwrap();
            assert_eq!(
                log.read_value(e).await.unwrap().0,
                Bytes::from(big.clone()),
                "reads back after crash recovery"
            );
            assert_eq!(
                log.values.refcount(&content_hash(&big)),
                1,
                "refcount rebuilt from the record scan, not a footer"
            );
            log.put_full(key(0), &vec![0x44u8; 100 * 1024], &[], None)
                .await
                .unwrap();
            assert_eq!(
                log.values.refcount(&content_hash(&big)),
                0,
                "sidecar from scan let the old blob GC on overwrite"
            );
        });
    }

    /// MSET (`put_many`) separates large values, derefs them on read, dedups
    /// identical content, and GCs the old blob when a key is rewritten in a
    /// later batch.
    #[test]
    fn mset_separates_large_values() {
        run(async {
            let dir = TempDir::new().unwrap();
            let cfg = LogConfig {
                rotate_threshold: 1 << 30,
                fanout: 8,
                value_sep_threshold: 4096,
            };
            let log = NamespaceLog::open(dir.path().to_path_buf(), cfg)
                .await
                .unwrap();
            let big1 = Bytes::from(vec![1u8; 8192]);
            let big2 = Bytes::from(vec![2u8; 8192]);
            let small = Bytes::from_static(b"inline");
            log.put_many(&[
                (key(0), big1.clone()),
                (key(1), big2.clone()),
                (key(2), small.clone()),
            ])
            .await
            .unwrap();
            assert_eq!(
                log.values.blob_count(),
                2,
                "two distinct large values separated; small stays inline"
            );
            for (k, want) in [(0usize, &big1), (1, &big2), (2, &small)] {
                let e = *log
                    .index
                    .borrow()
                    .get(format!("k{k:05}").as_bytes())
                    .unwrap();
                assert_eq!(
                    log.read_value(e).await.unwrap().0,
                    *want,
                    "MSET value {k} reads back"
                );
            }
            // Rewrite key0 in a later MSET with new content -> old blob GC'd.
            let big1b = Bytes::from(vec![9u8; 8192]);
            log.put_many(&[(key(0), big1b)]).await.unwrap();
            assert_eq!(
                log.values.refcount(&content_hash(&big1)),
                0,
                "old MSET blob reclaimed"
            );
            assert_eq!(log.values.blob_count(), 2, "key0's new blob + key1's blob");
        });
    }

    /// Cross-key dedup refcount: two keys with identical large content share ONE
    /// blob (refcount 2). Deleting one must NOT delete the blob — the other key
    /// still reads correctly. This is the premature-deletion / data-loss guard.
    #[test]
    fn shared_blob_not_deleted_while_referenced() {
        run(async {
            let dir = TempDir::new().unwrap();
            let cfg = LogConfig {
                rotate_threshold: 1 << 30,
                fanout: 8,
                value_sep_threshold: 4096,
            };
            let log = NamespaceLog::open(dir.path().to_path_buf(), cfg)
                .await
                .unwrap();
            let v = vec![7u8; 8192];
            log.put_full(key(0), &v, &[], None).await.unwrap();
            log.put_full(key(1), &v, &[], None).await.unwrap(); // identical content
            assert_eq!(
                log.values.blob_count(),
                1,
                "identical content dedups to one blob"
            );
            assert_eq!(log.values.refcount(&content_hash(&v)), 2);

            log.tombstone(b"k00000").await.unwrap(); // delete ONE referencing key
            assert_eq!(
                log.values.blob_count(),
                1,
                "blob survives — k1 still references it"
            );
            let e = *log.index.borrow().get(b"k00001").unwrap();
            assert_eq!(
                log.read_value(e).await.unwrap().0,
                Bytes::from(v.clone()),
                "surviving key still reads"
            );

            log.tombstone(b"k00001").await.unwrap(); // delete the last reference
            assert_eq!(
                log.values.blob_count(),
                0,
                "blob reclaimed only after last reference drops"
            );
        });
    }

    /// CAS / conditional writes with large values: a successful CAS GCs the old
    /// blob; a CAS that LOSES the post-check must unref the blob it wrote (no leak).
    #[test]
    fn cas_large_value_gc_and_abort_unref() {
        run(async {
            let dir = TempDir::new().unwrap();
            let cfg = LogConfig {
                rotate_threshold: 1 << 30,
                fanout: 8,
                value_sep_threshold: 4096,
            };
            let log = NamespaceLog::open(dir.path().to_path_buf(), cfg)
                .await
                .unwrap();
            let now = super::now_ms();
            let v1 = vec![1u8; 8192];
            // SETNX-style on an absent key: writes + separates.
            assert!(
                log.put_full_cond(key(0), &v1, &[], None, WriteCondition::KeyAbsent, now)
                    .await
                    .unwrap()
                    .is_some()
            );
            assert_eq!(log.values.blob_count(), 1);
            let rev = log.index.borrow().get(b"k00000").unwrap().tstamp_ms;

            // CAS with matching revision: overwrites, old blob GC'd.
            let v2 = vec![2u8; 8192];
            assert!(
                log.put_full_cond(key(0), &v2, &[], None, WriteCondition::Revision(rev), now)
                    .await
                    .unwrap()
                    .is_some()
            );
            assert_eq!(
                log.values.refcount(&content_hash(&v1)),
                0,
                "old blob GC'd on successful CAS"
            );
            assert_eq!(log.values.refcount(&content_hash(&v2)), 1);

            // CAS with a stale revision: aborts. The blob it wrote must be unref'd.
            let v3 = vec![3u8; 8192];
            assert!(
                log.put_full_cond(key(0), &v3, &[], None, WriteCondition::Revision(rev), now)
                    .await
                    .unwrap()
                    .is_none()
            );
            assert_eq!(
                log.values.refcount(&content_hash(&v3)),
                0,
                "aborted CAS unref'd its blob — no leak"
            );
            assert_eq!(log.values.blob_count(), 1, "only v2's blob remains");
        });
    }
}

#[cfg(test)]
mod crash_consistency {
    use super::*;
    use crate::log::config::LogConfig;
    use crate::value_store::{ContentHash, content_hash};
    use bytes::Bytes;
    use std::collections::{BTreeMap, HashSet};
    use tempfile::TempDir;

    fn run<F: std::future::Future>(f: F) -> F::Output {
        monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_timer()
            .build()
            .expect("monoio runtime")
            .block_on(f)
    }

    #[derive(Clone)]
    enum Op {
        Set { k: u8, val: Vec<u8>, large: bool },
        Del { k: u8 },
    }
    fn kb(k: u8) -> Bytes {
        Bytes::from(format!("k{k}"))
    }

    /// Exhaustive power-loss crash-consistency proof. Write a workload, fsync at a
    /// known point, then — modelling a power loss, which can only lose the
    /// UN-fsynced tail — truncate the active log at EVERY byte offset in that tail
    /// and recover. After each recovery the state must be a valid prefix of the
    /// write history: exactly the records that fully fit below the cut, last-writer
    /// -wins; every surviving key must read back its correct value (deref proves the
    /// blob is present = no dangling pointer); and the blob count must equal the
    /// live large-value set (sweep reclaimed orphans = no leak).
    ///
    /// The tail contains a value-separated OVERWRITE (k0: A→B). That is the case the
    /// deferred-blob-deletion fix protects: a cut that loses the overwrite reverts
    /// k0 to A, and A's blob must still exist. Without the fix this test fails at
    /// those offsets with a dangling-pointer read error.
    #[test]
    fn exhaustive_tail_truncation_is_consistent() {
        run(async {
            let work = TempDir::new().unwrap();
            let cfg = LogConfig {
                rotate_threshold: 1 << 40,
                fanout: 8,
                value_sep_threshold: 256,
            };
            let big = |b: u8| vec![b; 512]; // >= threshold -> value-separated
            let ops = [
                Op::Set {
                    k: 0,
                    val: big(0xA1),
                    large: true,
                },
                Op::Set {
                    k: 1,
                    val: b"s1".to_vec(),
                    large: false,
                },
                // ---- fsync here: everything above is durable ----
                Op::Set {
                    k: 0,
                    val: big(0xB2),
                    large: true,
                }, // overwrite k0 (old A blob deferred)
                Op::Set {
                    k: 2,
                    val: big(0xC3),
                    large: true,
                },
                Op::Del { k: 1 },
                Op::Set {
                    k: 3,
                    val: b"s3".to_vec(),
                    large: false,
                },
            ];
            let fsync_after = 2usize;

            let mut ends: Vec<u64> = Vec::with_capacity(ops.len());
            let mut fsync_offset = 0u64;
            {
                let log = NamespaceLog::open(work.path().to_path_buf(), cfg)
                    .await
                    .unwrap();
                for (i, op) in ops.iter().enumerate() {
                    match op {
                        Op::Set { k, val, .. } => {
                            log.put_full(kb(*k), val, &[], None).await.unwrap();
                        }
                        Op::Del { k } => {
                            log.tombstone(kb(*k).as_ref()).await.unwrap();
                        }
                    }
                    ends.push(log.active.borrow().write_offset());
                    if i + 1 == fsync_after {
                        log.sync().await.unwrap();
                        fsync_offset = log.active.borrow().write_offset();
                    }
                }
                // Deliberately NO final sync: ops after `fsync_after` are the
                // crash-vulnerable un-fsynced tail.
            }

            // Capture the on-disk image (page cache reflects all written bytes).
            let data_bytes = std::fs::read(work.path().join(data_filename(0))).unwrap();
            let values_dir = work.path().join("values");
            let blob_snapshot: Vec<(std::ffi::OsString, Vec<u8>)> = std::fs::read_dir(&values_dir)
                .map(|rd| {
                    rd.flatten()
                        .map(|e| (e.file_name(), std::fs::read(e.path()).unwrap()))
                        .collect()
                })
                .unwrap_or_default();

            let crash = TempDir::new().unwrap();
            let crash_data = crash.path().join(data_filename(0));
            let crash_values = crash.path().join("values");

            for t in (fsync_offset as usize)..=data_bytes.len() {
                // Rebuild the crashed image: log truncated to t, FULL blob set
                // restored (sweep_orphans mutates it, so restore every iteration).
                std::fs::write(&crash_data, &data_bytes[..t]).unwrap();
                let _ = std::fs::remove_dir_all(&crash_values);
                if !blob_snapshot.is_empty() {
                    std::fs::create_dir_all(&crash_values).unwrap();
                    for (name, bytes) in &blob_snapshot {
                        std::fs::write(crash_values.join(name), bytes).unwrap();
                    }
                }

                // Oracle: the prefix of ops whose record fully fits below the cut.
                let mut state: BTreeMap<u8, (Vec<u8>, bool)> = BTreeMap::new();
                for (op, end) in ops.iter().zip(ends.iter()) {
                    if *end > t as u64 {
                        break;
                    }
                    match op {
                        Op::Set { k, val, large } => {
                            state.insert(*k, (val.clone(), *large));
                        }
                        Op::Del { k } => {
                            state.remove(k);
                        }
                    }
                }

                let log = NamespaceLog::open(crash.path().to_path_buf(), cfg)
                    .await
                    .unwrap();

                // (1) recovered key set == expected prefix key set
                let recovered: HashSet<Vec<u8>> =
                    log.index.borrow().iter().map(|(k, _)| k.to_vec()).collect();
                let expected: HashSet<Vec<u8>> = state.keys().map(|k| kb(*k).to_vec()).collect();
                assert_eq!(recovered, expected, "key set mismatch at truncation t={t}");

                // (2) every surviving key reads its correct value (deref => blob
                //     present => no dangling pointer)
                for (k, (val, _large)) in &state {
                    let e = *log.index.borrow().get(kb(*k).as_ref()).unwrap();
                    let got = log.read_value(e).await.unwrap_or_else(|err| {
                        panic!("DANGLING/corrupt read for k{k} at t={t}: {err:?}")
                    });
                    assert_eq!(
                        got.0.as_ref(),
                        val.as_slice(),
                        "value mismatch k{k} at t={t}"
                    );
                }

                // (3) blob count == distinct live large values (orphans swept => no leak)
                let want: HashSet<ContentHash> = state
                    .values()
                    .filter(|(_, large)| *large)
                    .map(|(v, _)| content_hash(v))
                    .collect();
                assert_eq!(
                    log.values.blob_count(),
                    want.len(),
                    "blob leak/missing at t={t}: have {} want {}",
                    log.values.blob_count(),
                    want.len()
                );
            }

            eprintln!(
                "\n  CRASH-CONSISTENCY: {} tail-truncation offsets ({}..={}) all recovered to a\n  valid prefix — zero dangling pointers, zero blob leaks.\n",
                data_bytes.len() - fsync_offset as usize + 1,
                fsync_offset,
                data_bytes.len()
            );
        });
    }

    /// Bit-rot of a DURABLE record: corrupt one byte inside record `i`, and
    /// recovery must detect the CRC mismatch, truncate at the start of record `i`
    /// (dropping it and everything after it), and leave the prefix [0, i) fully
    /// intact and readable — with the now-unreferenced blobs of the dropped tail
    /// reclaimed by `sweep_orphans`. Workload uses only distinct-key appends (no
    /// value-sep overwrites) so the recovered prefix never reverts to a value
    /// whose blob was legitimately GC'd.
    #[test]
    fn corruption_truncates_at_bad_record_keeping_prefix() {
        run(async {
            let work = TempDir::new().unwrap();
            let cfg = LogConfig {
                rotate_threshold: 1 << 40,
                fanout: 8,
                value_sep_threshold: 256,
            };
            let big = |b: u8| vec![b; 512];
            let ops = vec![
                Op::Set {
                    k: 0,
                    val: big(0xA1),
                    large: true,
                },
                Op::Set {
                    k: 1,
                    val: big(0xB2),
                    large: true,
                },
                Op::Set {
                    k: 2,
                    val: b"s2".to_vec(),
                    large: false,
                },
                Op::Set {
                    k: 3,
                    val: big(0xC3),
                    large: true,
                },
            ];

            let mut ends: Vec<u64> = Vec::with_capacity(ops.len());
            {
                let log = NamespaceLog::open(work.path().to_path_buf(), cfg)
                    .await
                    .unwrap();
                for op in &ops {
                    if let Op::Set { k, val, .. } = op {
                        log.put_full(kb(*k), val, &[], None).await.unwrap();
                    }
                    ends.push(log.active.borrow().write_offset());
                }
                log.sync().await.unwrap(); // everything durable
            }
            let data_bytes = std::fs::read(work.path().join(data_filename(0))).unwrap();
            let blob_snapshot: Vec<(std::ffi::OsString, Vec<u8>)> =
                std::fs::read_dir(work.path().join("values"))
                    .map(|rd| {
                        rd.flatten()
                            .map(|e| (e.file_name(), std::fs::read(e.path()).unwrap()))
                            .collect()
                    })
                    .unwrap_or_default();

            let crash = TempDir::new().unwrap();
            let crash_data = crash.path().join(data_filename(0));
            let crash_values = crash.path().join("values");

            for i in 0..ops.len() {
                let start = if i == 0 { 0 } else { ends[i - 1] as usize };
                let pos = (start + ends[i] as usize) / 2; // a byte inside record i
                let mut corrupt = data_bytes.clone();
                corrupt[pos] ^= 0xFF;
                std::fs::write(&crash_data, &corrupt).unwrap();
                let _ = std::fs::remove_dir_all(&crash_values);
                std::fs::create_dir_all(&crash_values).unwrap();
                for (name, bytes) in &blob_snapshot {
                    std::fs::write(crash_values.join(name), bytes).unwrap();
                }

                // Expected: only records strictly before the corrupted one survive.
                let mut state: BTreeMap<u8, (Vec<u8>, bool)> = BTreeMap::new();
                for op in &ops[..i] {
                    if let Op::Set { k, val, large } = op {
                        state.insert(*k, (val.clone(), *large));
                    }
                }

                let log = NamespaceLog::open(crash.path().to_path_buf(), cfg)
                    .await
                    .unwrap();
                let recovered: HashSet<Vec<u8>> =
                    log.index.borrow().iter().map(|(k, _)| k.to_vec()).collect();
                let expected: HashSet<Vec<u8>> = state.keys().map(|k| kb(*k).to_vec()).collect();
                assert_eq!(
                    recovered, expected,
                    "corruption at record {i} should keep exactly the prefix"
                );
                for (k, (val, _)) in &state {
                    let e = *log.index.borrow().get(kb(*k).as_ref()).unwrap();
                    let got = log.read_value(e).await.unwrap_or_else(|err| {
                        panic!("prefix key k{k} unreadable after corrupting record {i}: {err:?}")
                    });
                    assert_eq!(
                        got.0.as_ref(),
                        val.as_slice(),
                        "prefix value k{k} wrong after corrupting record {i}"
                    );
                }
                let want: HashSet<ContentHash> = state
                    .values()
                    .filter(|(_, l)| *l)
                    .map(|(v, _)| content_hash(v))
                    .collect();
                assert_eq!(
                    log.values.blob_count(),
                    want.len(),
                    "dropped-tail blobs not reclaimed after corrupting record {i}"
                );
            }
            eprintln!(
                "\n  CRASH-CONSISTENCY: single-byte corruption at every record truncates cleanly\n  at the bad record; the prefix stays intact and the dropped tail's blobs are swept.\n"
            );
        });
    }

    /// Torn footer + multi-file recovery: after a reclaim seals records (with
    /// value-separated keys) into a footered sealed file and opens a new active,
    /// truncating the SEALED file's footer must make `read_footer` reject the
    /// (now-invalid) magic and fall back to `rebuild_from_records` — a full scan
    /// that re-derives the value-sep sidecar from each record's VALUE_SEP flag.
    /// Across every footer-region cut (and into the records), every key must still
    /// recover and read back through the multi-file (sealed + active) layout.
    #[test]
    fn torn_footer_falls_back_to_scan_across_files() {
        run(async {
            let work = TempDir::new().unwrap();
            let cfg = LogConfig {
                rotate_threshold: 1 << 40,
                fanout: 8,
                value_sep_threshold: 256,
            };
            let big = |b: u8| vec![b; 512];
            let ops = vec![
                Op::Set {
                    k: 0,
                    val: big(0xD1),
                    large: true,
                },
                Op::Set {
                    k: 1,
                    val: b"s1".to_vec(),
                    large: false,
                },
                Op::Set {
                    k: 2,
                    val: big(0xE2),
                    large: true,
                },
                Op::Set {
                    k: 3,
                    val: big(0xF3),
                    large: true,
                },
            ];
            let mut ends: Vec<u64> = Vec::with_capacity(ops.len());
            let records_end;
            {
                let log = NamespaceLog::open(work.path().to_path_buf(), cfg)
                    .await
                    .unwrap();
                for op in &ops {
                    if let Op::Set { k, val, .. } = op {
                        log.put_full(kb(*k), val, &[], None).await.unwrap();
                    }
                    ends.push(log.active.borrow().write_offset());
                }
                records_end = log.active.borrow().write_offset();
                log.reclaim().await.unwrap(); // seal file 0 (records + footer), open active file 1
            }
            // The reclaim footered file 0 and created an empty active file 1.
            let sealed_bytes = std::fs::read(work.path().join(data_filename(0))).unwrap();
            assert!(
                sealed_bytes.len() as u64 > records_end,
                "footer was appended past the records"
            );
            let active1 = work.path().join(data_filename(1));
            assert!(
                active1.exists(),
                "reclaim opened a new active file (multi-file layout)"
            );
            let blob_snapshot: Vec<(std::ffi::OsString, Vec<u8>)> =
                std::fs::read_dir(work.path().join("values"))
                    .map(|rd| {
                        rd.flatten()
                            .map(|e| (e.file_name(), std::fs::read(e.path()).unwrap()))
                            .collect()
                    })
                    .unwrap_or_default();

            let crash = TempDir::new().unwrap();
            let f0 = crash.path().join(data_filename(0));
            let f1 = crash.path().join(data_filename(1));
            let cvals = crash.path().join("values");

            // Cut from late in the last record through the entire footer region.
            let lo = (records_end as usize).saturating_sub(40);
            for t in lo..=sealed_bytes.len() {
                std::fs::write(&f0, &sealed_bytes[..t]).unwrap();
                std::fs::write(&f1, b"").unwrap(); // empty active (highest id)
                let _ = std::fs::remove_dir_all(&cvals);
                std::fs::create_dir_all(&cvals).unwrap();
                for (name, bytes) in &blob_snapshot {
                    std::fs::write(cvals.join(name), bytes).unwrap();
                }

                // Records fully below the cut survive the scan; a cut in the footer
                // region (t >= records_end) keeps all records.
                let mut state: BTreeMap<u8, (Vec<u8>, bool)> = BTreeMap::new();
                for (op, end) in ops.iter().zip(ends.iter()) {
                    if *end > t as u64 {
                        break;
                    }
                    if let Op::Set { k, val, large } = op {
                        state.insert(*k, (val.clone(), *large));
                    }
                }

                let log = NamespaceLog::open(crash.path().to_path_buf(), cfg)
                    .await
                    .unwrap();
                let recovered: HashSet<Vec<u8>> =
                    log.index.borrow().iter().map(|(k, _)| k.to_vec()).collect();
                let expected: HashSet<Vec<u8>> = state.keys().map(|k| kb(*k).to_vec()).collect();
                assert_eq!(
                    recovered, expected,
                    "torn-footer scan recovered wrong key set at t={t}"
                );
                for (k, (val, _)) in &state {
                    let e = *log.index.borrow().get(kb(*k).as_ref()).unwrap();
                    let got = log.read_value(e).await.unwrap_or_else(|err| {
                        panic!("k{k} unreadable via torn-footer scan at t={t}: {err:?}")
                    });
                    assert_eq!(
                        got.0.as_ref(),
                        val.as_slice(),
                        "value mismatch k{k} at t={t}"
                    );
                }
                let want: HashSet<ContentHash> = state
                    .values()
                    .filter(|(_, l)| *l)
                    .map(|(v, _)| content_hash(v))
                    .collect();
                assert_eq!(
                    log.values.blob_count(),
                    want.len(),
                    "blob leak/missing at t={t}"
                );
            }
            eprintln!(
                "\n  CRASH-CONSISTENCY: torn footer over {} cuts → scan fallback rebuilt value-sep\n  state from records across the sealed+active multi-file layout. No dangling, no leaks.\n",
                sealed_bytes.len() - lo + 1
            );
        });
    }
}

#[cfg(test)]
mod concurrency_tests {
    use super::*;
    use crate::log::config::LogConfig;
    use bytes::Bytes;
    use std::collections::BTreeMap;
    use std::rc::Rc;
    use tempfile::TempDir;

    fn run<F: std::future::Future>(f: F) -> F::Output {
        monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_timer()
            .build()
            .expect("monoio runtime")
            .block_on(f)
    }

    async fn live_state(log: &NamespaceLog) -> BTreeMap<Vec<u8>, Vec<u8>> {
        let entries: Vec<(Vec<u8>, IndexEntry)> = log
            .index
            .borrow()
            .iter()
            .map(|(k, e)| (k.to_vec(), *e))
            .collect();
        let mut out = BTreeMap::new();
        for (k, e) in entries {
            let (v, _m) = log.read_value(e).await.unwrap();
            out.insert(k, v.to_vec());
        }
        out
    }

    /// Stress the new per-key write striping under real concurrency: many spawned
    /// tasks hammer a small shared keyspace with interleaved SET / CAS / DEL (so
    /// same-key writes actually contend on stripes). This must (a) never deadlock
    /// — single-key writes each hold exactly one stripe — and (b) leave on-disk
    /// state that, after a full fsync, recovery reproduces EXACTLY. A conditional
    /// write that loses a race writes nothing (no orphan), so the durable log can
    /// never replay to anything other than the live runtime state.
    #[test]
    fn concurrent_mixed_writes_recover_to_runtime_state() {
        run(async {
            let dir = TempDir::new().unwrap();
            let cfg = LogConfig {
                rotate_threshold: 1 << 40,
                fanout: 8,
                value_sep_threshold: 4096,
            };
            let log = Rc::new(
                NamespaceLog::open(dir.path().to_path_buf(), cfg)
                    .await
                    .unwrap(),
            );

            let mut handles = Vec::new();
            for t in 0..6u64 {
                let log = log.clone();
                handles.push(monoio::spawn(async move {
                    for i in 0..120u64 {
                        let k = Bytes::from(format!("k{}", (t + i) % 5)); // 5 hot keys, heavy same-key contention
                        let big = i % 7 == 0; // mix in value-separated (>4 KiB) writes
                        let val = if big {
                            vec![(t as u8).wrapping_add(i as u8); 8192]
                        } else {
                            vec![t as u8; 24]
                        };
                        match (t + i) % 3 {
                            0 => {
                                log.put_full(k, &val, &[], None).await.unwrap();
                            }
                            1 => {
                                // CAS against whatever revision we just observed.
                                let now = now_ms();
                                let cond = match log.index.borrow().get(k.as_ref()) {
                                    Some(e) => WriteCondition::Revision(e.tstamp_ms),
                                    None => WriteCondition::KeyAbsent,
                                };
                                let _ = log
                                    .put_full_cond(k, &val, &[], None, cond, now)
                                    .await
                                    .unwrap();
                            }
                            _ => {
                                let _ = log.tombstone(k.as_ref()).await.unwrap();
                            }
                        }
                    }
                }));
            }
            for h in handles {
                h.await;
            }

            // Full durability, then snapshot the live runtime state.
            log.sync().await.unwrap();
            let runtime = live_state(&log).await;
            drop(log);

            // Recover from disk; it must reproduce the exact runtime state — no
            // resurrected "failed" CAS, no lost update, no dangling value-sep blob.
            let log2 = NamespaceLog::open(dir.path().to_path_buf(), cfg)
                .await
                .unwrap();
            let recovered = live_state(&log2).await;
            assert_eq!(
                recovered, runtime,
                "recovery diverged from the concurrent runtime state"
            );
        });
    }
}

#[cfg(test)]
mod perf_overhead {
    use super::*;
    use crate::log::config::LogConfig;
    use bytes::Bytes;
    use std::time::Instant;
    use tempfile::TempDir;

    fn run<F: std::future::Future>(f: F) -> F::Output {
        monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_timer()
            .build()
            .expect("rt")
            .block_on(f)
    }

    /// Quantify the cost added to the WRITE path by the per-key stripe lock, and
    /// confirm the READ path is untouched. Reported, not asserted. Ignored by
    /// default (a perf probe, not a regression test): `cargo test -- --ignored`.
    #[test]
    #[ignore = "perf probe; run with --ignored --nocapture"]
    fn write_path_overhead() {
        run(async {
            let dir = TempDir::new().unwrap();
            let cfg = LogConfig {
                rotate_threshold: 1 << 40,
                fanout: 8,
                value_sep_threshold: 1 << 20,
            };
            let log = NamespaceLog::open(dir.path().to_path_buf(), cfg)
                .await
                .unwrap();
            let val = vec![0u8; 64]; // small inline value (the common case)

            // 1) bare added cost: FxHash(key) + uncontended stripe lock/unlock.
            let n = 500_000;
            let key = b"some-typical-key";
            let t = Instant::now();
            for _ in 0..n {
                let g = log.wlock(std::hint::black_box(key)).lock().await;
                std::hint::black_box(&g);
            }
            let lock_ns = t.elapsed().as_nanos() as f64 / n as f64;

            // 2) full small-value write (encode + append + index + stripe lock).
            let nw = 50_000;
            let t = Instant::now();
            for i in 0..nw {
                log.put_full(Bytes::from(format!("k{i:08}")), &val, &[], None)
                    .await
                    .unwrap();
            }
            let put_ns = t.elapsed().as_nanos() as f64 / nw as f64;

            // 3) warm read (lock-free path; the stripe lock is never taken).
            let e = *log.index.borrow().get(b"k00000000").unwrap();
            let nr = 200_000;
            let t = Instant::now();
            for _ in 0..nr {
                std::hint::black_box(log.read_value(e).await.unwrap());
            }
            let read_ns = t.elapsed().as_nanos() as f64 / nr as f64;

            eprintln!(
                "\n  PERF (single shard, sequential):\n    stripe lock acquire+release (uncontended) = {lock_ns:.0} ns   <- the per-write add\n    full small-value put_full                 = {:.0} ns/op  ({:.2}% is the lock)\n    warm read_value (lock-free, unchanged)    = {read_ns:.0} ns/op\n",
                put_ns,
                lock_ns / put_ns * 100.0
            );
        });
    }
}

#[cfg(test)]
mod integrity_tests {
    use super::*;
    use crate::log::config::LogConfig;
    use bytes::Bytes;
    use tempfile::TempDir;

    fn run<F: std::future::Future>(f: F) -> F::Output {
        monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_timer()
            .build()
            .expect("rt")
            .block_on(f)
    }

    /// #2: a value-separated blob corrupted on disk is DETECTED on read (content
    /// hash mismatch), not returned as wrong data — parity with the inline CRC
    /// check. (Drop the re-hash in `deref` and this returns corrupted bytes → the
    /// final assert fails.)
    #[test]
    fn corrupted_blob_is_detected_on_read() {
        run(async {
            let dir = TempDir::new().unwrap();
            let cfg = LogConfig {
                rotate_threshold: 1 << 30,
                fanout: 8,
                value_sep_threshold: 4096,
            };
            let log = NamespaceLog::open(dir.path().to_path_buf(), cfg)
                .await
                .unwrap();
            let big = vec![0x11u8; 8192];
            log.put_full(Bytes::from_static(b"k"), &big, &[], None)
                .await
                .unwrap();
            let e = *log.index.borrow().get(b"k").unwrap();
            assert_eq!(
                log.read_value(e).await.unwrap().0,
                Bytes::from(big.clone()),
                "sanity: reads back"
            );

            // Flip a byte in the blob file on disk.
            let blob = std::fs::read_dir(dir.path().join("values"))
                .unwrap()
                .flatten()
                .map(|d| d.path())
                .find(|p| {
                    p.file_name()
                        .unwrap()
                        .to_string_lossy()
                        .starts_with("blob-")
                })
                .expect("blob file");
            let mut bytes = std::fs::read(&blob).unwrap();
            bytes[0] ^= 0xFF;
            std::fs::write(&blob, bytes).unwrap();

            assert!(
                log.read_value(e).await.is_err(),
                "corrupted blob must be detected, not returned as data"
            );
        });
    }

    /// #3: revisions stay monotonic across a restart even when recovered data
    /// carries a tstamp ahead of the wall clock (clock skew / future-dated write).
    /// The revision clock seeds from the max recovered tstamp. (Seed from 0 — the
    /// old behavior — and the post-restart write gets a smaller revision: fails.)
    #[test]
    fn revisions_monotonic_across_restart() {
        run(async {
            let dir = TempDir::new().unwrap();
            let cfg = LogConfig {
                rotate_threshold: 1 << 30,
                fanout: 8,
                value_sep_threshold: 1 << 20,
            };
            let path = dir.path().to_path_buf();
            {
                let log = NamespaceLog::open(path.clone(), cfg).await.unwrap();
                log.put_full(Bytes::from_static(b"k0"), b"v0", &[], None)
                    .await
                    .unwrap();
            }
            // Append a record with a far-future tstamp directly to the active file.
            let future = now_ms() + 10_000_000;
            let rec = crate::log::record::encode(
                future,
                crate::log::record::flags::NO_EXPIRY,
                0,
                b"k1",
                b"v1",
                &[],
            )
            .unwrap();
            {
                use std::io::Write;
                let p = path.join(crate::log::file::data_filename(0));
                std::fs::OpenOptions::new()
                    .append(true)
                    .open(&p)
                    .unwrap()
                    .write_all(&rec)
                    .unwrap();
            }
            // Reopen → recovery sees `future`; the revision clock must seed from it.
            let log2 = NamespaceLog::open(path.clone(), cfg).await.unwrap();
            let rev = log2
                .put_full(Bytes::from_static(b"k2"), b"v2", &[], None)
                .await
                .unwrap();
            assert!(
                rev > future,
                "post-restart revision {rev} must exceed recovered max {future}"
            );
        });
    }
}

#[cfg(test)]
mod watch_valuesep_tests {
    use super::*;
    use crate::log::config::LogConfig;
    use crate::watch::{KeyFilter, WatchEvent};
    use bytes::Bytes;
    use tempfile::TempDir;

    fn run<F: std::future::Future>(f: F) -> F::Output {
        monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_timer()
            .build()
            .expect("rt")
            .block_on(f)
    }

    /// Regression: watch resumption (`scan_since`) must deref a value-separated
    /// record to its real value, not emit the 16-byte content-hash pointer. (Skip
    /// the deref and the event value is 16 bytes, not the 8 KiB value → fails.)
    #[test]
    fn scan_since_emits_real_value_for_separated_record() {
        run(async {
            let dir = TempDir::new().unwrap();
            let cfg = LogConfig {
                rotate_threshold: 1 << 30,
                fanout: 8,
                value_sep_threshold: 4096,
            };
            let log = NamespaceLog::open(dir.path().to_path_buf(), cfg)
                .await
                .unwrap();
            let big = vec![0x7Cu8; 8192]; // > threshold → separated
            log.put_full(Bytes::from_static(b"wk"), &big, &[], None)
                .await
                .unwrap();

            let events = log.scan_since(&KeyFilter::Exact(b"wk"), 0).await.unwrap();
            assert_eq!(events.len(), 1, "exactly one Set event");
            match &events[0] {
                WatchEvent::Set { value, .. } => {
                    assert_eq!(
                        value,
                        &Bytes::from(big.clone()),
                        "watch replay must emit the real value, not the hash"
                    );
                }
                other => panic!("expected Set, got {other:?}"),
            }
        });
    }
}

#[cfg(test)]
mod reclaim_concurrency_tests {
    use super::*;
    use crate::log::config::LogConfig;
    use bytes::Bytes;
    use std::rc::Rc;
    use tempfile::TempDir;

    fn run<F: std::future::Future>(f: F) -> F::Output {
        monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_timer()
            .build()
            .expect("rt")
            .block_on(f)
    }

    /// Regression: a write issued while a reclaim is running must WAIT for it and
    /// then succeed — it must NOT return `ReclamationBusy`. (Before: writes errored
    /// during reclaim.) A small rotate threshold makes the reclaim do real merge
    /// work so the concurrent write actually overlaps it.
    #[test]
    fn writes_wait_for_reclaim_then_succeed() {
        run(async {
            let dir = TempDir::new().unwrap();
            let cfg = LogConfig {
                rotate_threshold: 256,
                fanout: 4,
                value_sep_threshold: 1 << 20,
            };
            let log = Rc::new(
                NamespaceLog::open(dir.path().to_path_buf(), cfg)
                    .await
                    .unwrap(),
            );

            // Fill enough that many runs seal → reclaim has a multi-level merge to do.
            let val = vec![0xACu8; 80];
            for i in 0..80u32 {
                log.put_full(Bytes::from(format!("k{i:04}")), &val, &[], None)
                    .await
                    .unwrap();
            }

            // Run reclaim and a write concurrently; the write must wait, not error.
            let a = log.clone();
            let b = log.clone();
            let t_reclaim = monoio::spawn(async move { a.reclaim().await });
            let t_write = monoio::spawn(async move {
                b.put_full(
                    Bytes::from_static(b"during-reclaim"),
                    &[1u8; 80],
                    &[],
                    None,
                )
                .await
            });
            let wr = t_write.await;
            let rr = t_reclaim.await;
            assert!(rr.is_ok(), "reclaim failed: {rr:?}");
            assert!(
                wr.is_ok(),
                "write during reclaim must wait+succeed, not error: {wr:?}"
            );

            // The waited write is durable and reads back.
            let e = *log.index.borrow().get(b"during-reclaim").unwrap();
            assert_eq!(log.read_value(e).await.unwrap().0.len(), 80);
            assert_eq!(log.len(), 81, "all keys present");
        });
    }
}

#[cfg(test)]
mod reclaim_durability_tests {
    use super::*;
    use crate::log::config::LogConfig;
    use bytes::Bytes;
    use std::cell::Cell;
    use std::rc::Rc;
    use std::time::Duration;
    use tempfile::TempDir;

    fn run<F: std::future::Future>(f: F) -> F::Output {
        monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_timer()
            .build()
            .expect("rt")
            .block_on(f)
    }

    /// Teeth-verified regression for the footer-consistency drain.
    ///
    /// The bug: `reclaim()` built the sealed file's footer from the index WITHOUT
    /// first draining in-flight writes. A write that had passed the gate and
    /// reserved an offset in the active file but had not yet `index.insert`ed
    /// would be missing from that footer and silently lost on the next footer
    /// (fast-path) recovery — and could even append AFTER the footer trailer.
    ///
    /// The fix: `reclaim()` spins `while in_flight_writes > 0` before sealing.
    /// This asserts that contract directly: a held `WriteGuard` (exactly the
    /// "appended but not yet indexed" state, since the guard spans append→insert)
    /// pins `in_flight_writes == 1`, and reclaim must NOT seal until it is
    /// released. Remove the drain loop in `reclaim()` and this fails: reclaim
    /// seals (opens a new active, sets `done`) while the guard is still held.
    #[test]
    fn reclaim_does_not_seal_while_a_write_is_in_flight() {
        run(async {
            let dir = TempDir::new().unwrap();
            let cfg = LogConfig {
                rotate_threshold: 512,
                fanout: 4,
                value_sep_threshold: 1 << 20,
            };
            let log = Rc::new(
                NamespaceLog::open(dir.path().to_path_buf(), cfg)
                    .await
                    .unwrap(),
            );
            for i in 0..5u32 {
                log.put_full(Bytes::from(format!("seed{i}")), b"v", &[], None)
                    .await
                    .unwrap();
            }
            let active_before = log.active.borrow().file_id;

            // Pin in_flight_writes==1: the exact "in the seal window" state a real
            // write occupies between reserving its offset and inserting its index entry.
            let guard = log.begin_write().unwrap();
            assert_eq!(log.in_flight_writes.get(), 1);

            let done = Rc::new(Cell::new(false));
            let (a, d) = (log.clone(), done.clone());
            let h = monoio::spawn(async move {
                let r = a.reclaim().await;
                d.set(true);
                r
            });

            // Give reclaim ample scheduling to reach — and block in — its drain.
            for _ in 0..30 {
                monoio::time::sleep(Duration::from_micros(100)).await;
            }
            assert!(
                !done.get(),
                "reclaim completed while a write was in-flight — drain missing"
            );
            assert!(
                log.reclaim_in_progress.get(),
                "reclaim should be mid-drain, holding the gate"
            );
            assert_eq!(
                log.active.borrow().file_id,
                active_before,
                "reclaim sealed (opened a new active) while a write was still in-flight"
            );

            // Release the in-flight write → drain observes 0 → reclaim seals.
            drop(guard);
            let _report = h.await.unwrap(); // unwraps the reclaim Result — succeeds once drained
            assert!(done.get());
            assert_ne!(
                log.active.borrow().file_id,
                active_before,
                "reclaim should have sealed and opened a new active after draining"
            );
        });
    }

    /// End-to-end companion: writes issued concurrently with a reclaim all survive
    /// a subsequent FOOTER (fast-path) recovery. This exercises the full
    /// reclaim+write+reopen path and passes deterministically with the drain.
    /// (Note: its *teeth* are timing-dependent — in a quiet test the small-write
    /// io_uring appends rarely suspend long enough to interleave reclaim's seal,
    /// so the deterministic contract test above is what actually guards the fix.)
    #[test]
    fn acked_writes_during_reclaim_survive_footer_recovery() {
        run(async {
            for _round in 0..10u32 {
                let dir = TempDir::new().unwrap();
                let cfg = LogConfig {
                    rotate_threshold: 512,
                    fanout: 4,
                    value_sep_threshold: 1 << 20,
                };
                let path = dir.path().to_path_buf();
                let val = vec![0xBEu8; 64];

                let acked: Vec<String> = {
                    let log = Rc::new(NamespaceLog::open(path.clone(), cfg).await.unwrap());
                    for i in 0..60u32 {
                        log.put_full(Bytes::from(format!("base{i:04}")), &val, &[], None)
                            .await
                            .unwrap();
                    }
                    let a = log.clone();
                    let t_reclaim = monoio::spawn(async move {
                        let _ = a.reclaim().await;
                    });
                    let mut acked = Vec::new();
                    for j in 0..40u32 {
                        let k = format!("hot{j:04}");
                        if log
                            .put_full(Bytes::from(k.clone()), &val, &[], None)
                            .await
                            .is_ok()
                        {
                            acked.push(k);
                        }
                    }
                    t_reclaim.await;
                    log.seal_active_for_shutdown().await.ok();
                    acked
                };

                let log2 = NamespaceLog::open(path.clone(), cfg).await.unwrap();
                let idx = log2.index.borrow();
                for k in &acked {
                    assert!(
                        idx.get(k.as_bytes()).is_some(),
                        "acked write {k} lost after footer recovery (reclaim seal didn't drain it)"
                    );
                }
            }
        });
    }
}

#[cfg(test)]
mod enospc_recovery_tests {
    use super::*;
    use crate::log::config::LogConfig;
    use bytes::Bytes;
    use tempfile::TempDir;

    fn run<F: std::future::Future>(f: F) -> F::Output {
        monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_timer()
            .build()
            .expect("monoio runtime")
            .block_on(f)
    }

    /// A disk-full (ENOSPC) write fails cleanly end-to-end: the failing key is
    /// never indexed (insert happens only after a successful append), prior
    /// committed writes are untouched, the active file is poisoned so no later
    /// write shadows the torn slot, and after reopen the committed prefix survives
    /// intact with no corruption.
    #[test]
    fn disk_full_write_preserves_committed_prefix_across_recovery() {
        run(async {
            let dir = TempDir::new().unwrap();
            let path = dir.path().to_path_buf();
            let cfg = LogConfig {
                rotate_threshold: 1 << 30,
                fanout: 8,
                value_sep_threshold: 1 << 20,
            };
            {
                let log = NamespaceLog::open(path.clone(), cfg).await.unwrap();
                log.put_full(Bytes::from_static(b"k1"), b"v1", &[], None)
                    .await
                    .unwrap();
                log.put_full(Bytes::from_static(b"k2"), b"v2", &[], None)
                    .await
                    .unwrap();

                // Disk fills on the next record's append.
                log.active.borrow().force_next_write_failure();
                let r = log
                    .put_full(Bytes::from_static(b"k3"), b"v3", &[], None)
                    .await;
                assert!(
                    r.is_err(),
                    "disk-full write must surface an error to the caller"
                );

                assert!(
                    log.index.borrow().get(b"k3").is_none(),
                    "failed write must not be indexed"
                );
                assert!(log.index.borrow().get(b"k1").is_some());
                assert!(log.index.borrow().get(b"k2").is_some());
                assert!(
                    log.put_full(Bytes::from_static(b"k4"), b"v4", &[], None)
                        .await
                        .is_err(),
                    "writes after a disk-full poison must fail, not silently land past the gap"
                );
            }

            // Reopen: committed prefix survives, failed/blocked writes absent, clean replay.
            let log = NamespaceLog::open(path.clone(), cfg).await.unwrap();
            let e1 = *log.index.borrow().get(b"k1").unwrap();
            let e2 = *log.index.borrow().get(b"k2").unwrap();
            assert_eq!(
                log.read_value(e1).await.unwrap().0,
                Bytes::from_static(b"v1")
            );
            assert_eq!(
                log.read_value(e2).await.unwrap().0,
                Bytes::from_static(b"v2")
            );
            assert!(
                log.index.borrow().get(b"k3").is_none(),
                "failed write absent after recovery"
            );
            assert!(
                log.index.borrow().get(b"k4").is_none(),
                "blocked write absent after recovery"
            );
        });
    }
}

#[cfg(test)]
mod fd_footprint_tests {
    use super::*;
    use crate::log::config::LogConfig;
    use bytes::Bytes;
    use tempfile::TempDir;

    fn run<F: std::future::Future>(f: F) -> F::Output {
        monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_timer()
            .build()
            .expect("monoio runtime")
            .block_on(f)
    }

    fn open_fds() -> usize {
        std::fs::read_dir("/proc/self/fd")
            .map(|d| d.count())
            .unwrap_or(0)
    }

    /// A `NamespaceLog`'s open-fd footprint is one fd for the active file PLUS one
    /// per sealed file it holds open for reads. So `MAX_NAMESPACES` bounds the
    /// namespace *count* but NOT the descriptor count — fds scale with sealed
    /// files per namespace, which is what actually binds before the namespace cap
    /// in a many-namespaces deployment. This pins that relationship so a future
    /// change that, say, stops holding sealed fds (or starts leaking them) is
    /// visible. `fanout` is set huge so runs accumulate without compaction merges.
    #[test]
    fn open_fds_scale_with_sealed_file_count() {
        run(async {
            let dir = TempDir::new().unwrap();
            let cfg = LogConfig {
                rotate_threshold: 256,
                fanout: 1 << 20,
                value_sep_threshold: 1 << 20,
            };
            let log = NamespaceLog::open(dir.path().to_path_buf(), cfg)
                .await
                .unwrap();

            let fds_before = open_fds();
            let sealed_before = log.sealed_file_count();

            // Each ~300-byte record exceeds rotate_threshold (256) → seals the
            // active and opens a fresh one, accumulating sealed files.
            for i in 0..20u32 {
                log.put_full(Bytes::from(format!("k{i:04}")), &[0xAB; 300], &[], None)
                    .await
                    .unwrap();
            }

            let sealed_after = log.sealed_file_count();
            let fds_after = open_fds();
            let new_sealed = sealed_after - sealed_before;

            assert!(
                new_sealed >= 10,
                "expected sealed files to accumulate, got {new_sealed}"
            );
            // Each retained sealed file holds an fd: descriptor growth tracks it.
            assert!(
                fds_after >= fds_before + new_sealed,
                "open fds ({fds_after}) did not grow with sealed files ({fds_before} + {new_sealed}) \
                 — fd footprint is per-sealed-file and not bounded by the namespace cap"
            );
        });
    }
}

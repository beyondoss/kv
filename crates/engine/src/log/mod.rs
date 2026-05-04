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
pub mod record;
pub mod recover;
pub mod reclaim;

use std::cell::{Cell, RefCell};
use std::path::PathBuf;
use std::rc::Rc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures_util::future::join_all;
use rustc_hash::FxHashMap;
use tracing::warn;

use crate::error::{EngineError, Result};
use crate::log::config::LogConfig;
use crate::log::file::{data_filename, FooterEntry, LogFile};
use crate::log::index::{IndexEntry, NsIndex};
use crate::log::record::{flags as rflags, parse_header, HEADER_LEN};

pub fn now_ms() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_millis() as u64,
        Err(_) => {
            warn!("system clock is before UNIX epoch; timestamps will be 0");
            0
        }
    }
}

pub struct NamespaceLog {
    pub dir: PathBuf,
    pub index: RefCell<NsIndex>,
    /// Sealed files in file_id ascending order. `Rc<LogFile>` so readers can
    /// clone a handle and drop the `RefCell` borrow before awaiting I/O.
    pub sealed: RefCell<FxHashMap<u16, Rc<LogFile>>>,
    /// Active (writable) file.
    pub active: RefCell<Rc<LogFile>>,
    pub config: LogConfig,
    unsynced_bytes: Cell<u64>,
    /// Monotonically increasing tstamp_ms — wall clock with a +1 nudge if the
    /// clock didn't advance, so duplicate-key replays always pick the latest.
    last_tstamp: Cell<u64>,
    /// Guards against concurrent reclaim/flush calls. Both are documented as
    /// requiring external serialization; this flag turns a would-be RefCell
    /// panic into a clean Err at the call site.
    reclaim_in_progress: Cell<bool>,
}

impl NamespaceLog {
    pub async fn open(dir: PathBuf, config: LogConfig) -> Result<Self> {
        let opened = recover::open_namespace(dir.clone()).await?;
        let sealed: FxHashMap<u16, Rc<LogFile>> = opened.sealed
            .into_iter()
            .map(|f| (f.file_id, Rc::new(f)))
            .collect();
        let active = Rc::new(opened.active);
        Ok(Self {
            dir,
            index: RefCell::new(opened.index),
            sealed: RefCell::new(sealed),
            active: RefCell::new(active),
            config,
            unsynced_bytes: Cell::new(0),
            last_tstamp: Cell::new(0),
            reclaim_in_progress: Cell::new(false),
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

    pub fn sealed_file_count(&self) -> usize {
        self.sealed.borrow().len()
    }

    pub async fn put_full(
        &self,
        key: Bytes,
        value: &[u8],
        metadata: &[u8],
        expires_at_ms: Option<u64>,
    ) -> Result<()> {
        if self.reclaim_in_progress.get() {
            return Err(EngineError::ReclamationBusy);
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
        let buf = record::encode(tstamp, flags, exp, &key, value, metadata)?;
        let record_size = buf.len() as u32;

        let active = self.active();
        let offset = active.append(buf).await?;
        self.unsynced_bytes.set(self.unsynced_bytes.get() + record_size as u64);
        let entry = IndexEntry::new(active.file_id, offset, record_size);
        self.index.borrow_mut().insert(key, entry, expires_at_ms);
        Ok(())
    }

    /// Coalesce many puts into a single `write_at` + single `fsync`.
    pub async fn put_many(&self, pairs: &[(Bytes, Bytes)]) -> Result<()> {
        if self.reclaim_in_progress.get() {
            return Err(EngineError::ReclamationBusy);
        }
        if pairs.is_empty() {
            return Ok(());
        }
        let mut buf: Vec<u8> = Vec::new();
        let mut layout: Vec<(usize, u32)> = Vec::with_capacity(pairs.len());
        for (k, v) in pairs {
            let tstamp = self.next_tstamp();
            let start = buf.len();
            record::encode_into(&mut buf, tstamp, rflags::NO_EXPIRY, 0, k, v, &[])?;
            let record_size = (buf.len() - start) as u32;
            layout.push((start, record_size));
        }
        let active = self.active();
        let buf_len = buf.len() as u64;
        let base_offset = active.append(buf).await?;
        self.unsynced_bytes.set(self.unsynced_bytes.get() + buf_len);
        let mut index = self.index.borrow_mut();
        for ((k, _v), (rel_start, size)) in pairs.iter().zip(layout.iter()) {
            let entry = IndexEntry::new(active.file_id, base_offset + *rel_start as u64, *size);
            index.insert(k.clone(), entry, None);
        }
        Ok(())
    }

    /// Append a tombstone for `key`; drop it from the index.
    /// Returns true iff the key was present.
    pub async fn tombstone(&self, key: &[u8]) -> Result<bool> {
        if self.reclaim_in_progress.get() {
            return Err(EngineError::ReclamationBusy);
        }
        let was_present = self.index.borrow_mut().remove(key).is_some();
        if !was_present {
            return Ok(false);
        }
        let tstamp = self.next_tstamp();
        let buf = record::encode(tstamp, rflags::TOMBSTONE, 0, key, &[], &[])?;
        let active = self.active();
        let buf_len = buf.len() as u64;
        let _ = active.append(buf).await?;
        self.unsynced_bytes.set(self.unsynced_bytes.get() + buf_len);
        Ok(true)
    }

    /// Append a TTL-update record; modify only the sidecar.
    pub async fn ttl_update(&self, key: &[u8], expires_at_ms: Option<u64>) -> Result<()> {
        if self.reclaim_in_progress.get() {
            return Err(EngineError::ReclamationBusy);
        }
        let tstamp = self.next_tstamp();
        let mut flags = rflags::TTL_UPDATE;
        let exp = match expires_at_ms {
            Some(ms) => ms,
            None => {
                flags |= rflags::NO_EXPIRY;
                0
            }
        };
        let buf = record::encode(tstamp, flags, exp, key, &[], &[])?;
        let active = self.active();
        let buf_len = buf.len() as u64;
        let _ = active.append(buf).await?;
        self.unsynced_bytes.set(self.unsynced_bytes.get() + buf_len);
        let key_bytes = Bytes::copy_from_slice(key);
        self.index.borrow_mut().set_ttl(&key_bytes, expires_at_ms);
        Ok(())
    }

    fn locate_file(&self, file_id: u16) -> Option<Rc<LogFile>> {
        let active = self.active.borrow().clone();
        if active.file_id == file_id {
            return Some(active);
        }
        self.sealed.borrow().get(&file_id).cloned()
    }

    /// Fsync the active file if any writes are pending. Called by the per-shard
    /// 1-second timer task to provide `appendfsync everysec` semantics.
    pub async fn sync(&self) -> Result<()> {
        if self.unsynced_bytes.get() == 0 {
            return Ok(());
        }
        self.active().sync().await?;
        self.unsynced_bytes.set(0);
        Ok(())
    }

    async fn read_record(&self, entry: IndexEntry) -> Result<Vec<u8>> {
        let file = self.locate_file(entry.file_id).ok_or(EngineError::BadRecord {
            offset: entry.record_offset,
            reason: "file_id not found",
        })?;
        file.read_exact(entry.record_offset, entry.record_size as usize).await
    }

    fn extract_value_meta(bytes: &[u8]) -> Result<(Bytes, Bytes)> {
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
        let value = Bytes::copy_from_slice(&bytes[key_end..val_end]);
        let metadata = Bytes::copy_from_slice(&bytes[val_end..meta_end]);
        Ok((value, metadata))
    }

    /// Single-record read: one `read_at`, parse header in-memory.
    pub async fn read_value(&self, entry: IndexEntry) -> Result<(Bytes, Bytes)> {
        let bytes = self.read_record(entry).await?;
        Self::extract_value_meta(&bytes)
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
        let results: Vec<Result<Vec<u8>>> = join_all(futures).await;
        let mut out: Vec<(usize, Bytes, Bytes)> = Vec::with_capacity(misses.len());
        for ((slot, _entry), bytes_res) in misses.into_iter().zip(results.into_iter()) {
            let bytes = bytes_res?;
            let (value, metadata) = Self::extract_value_meta(&bytes)?;
            out.push((slot, value, metadata));
        }
        Ok(out)
    }

    /// Seal the current active file (writing its footer) and open a new active file.
    /// Call when `active.write_offset() >= config.rotate_threshold` to bound file size.
    ///
    /// NOT concurrent-safe with other write operations on this namespace. Caller must serialize.
    pub async fn rotate_active(&self) -> Result<()> {
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
                })
                .collect()
        };
        old_active.write_footer(&footer).await?;
        let next_id = {
            let sealed = self.sealed.borrow();
            let max_existing = sealed.keys().copied().max().unwrap_or(0).max(old_active.file_id);
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
        *self.active.borrow_mut() = new_active;
        self.unsynced_bytes.set(0);
        Ok(())
    }

    /// Unlink-and-recreate all files for the namespace. Preserves CoW sharing
    /// with the parent fork (parent's inode still references the old blocks;
    /// the new active file's blocks are local).
    ///
    /// NOT safe under concurrent reads/writes — caller must serialize.
    pub async fn flush(&self) -> Result<()> {
        if self.reclaim_in_progress.get() {
            return Err(EngineError::ReclamationBusy);
        }
        self.reclaim_in_progress.set(true);
        let result = self.flush_inner().await;
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
        if self.dir.exists() {
            for entry in std::fs::read_dir(&self.dir)? {
                let entry = entry?;
                let path = entry.path();
                let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                    continue;
                };
                if name.starts_with("data-") {
                    if let Err(e) = std::fs::remove_file(&path) {
                        warn!(path = %path.display(), error = %e, "failed to unlink data file during flush");
                    }
                }
            }
        }

        let path = self.dir.join(data_filename(0));
        let new_active = Rc::new(LogFile::open_rw(path, 0).await?);
        *self.active.borrow_mut() = new_active;
        self.unsynced_bytes.set(0);
        Ok(())
    }

    /// Operator-triggered reclaim. Seals the current active file with a
    /// footer, then merges all live records (across the just-sealed file plus
    /// previously-sealed files) into a single new sealed file. Old files are
    /// unlinked. A fresh active file is opened.
    ///
    /// NOT concurrent-safe with other ops on this namespace.
    pub async fn reclaim(&self) -> Result<reclaim::ReclaimReport> {
        if self.reclaim_in_progress.get() {
            return Err(EngineError::ReclamationBusy);
        }
        self.reclaim_in_progress.set(true);
        let result = self.reclaim_inner().await;
        self.reclaim_in_progress.set(false);
        result
    }

    async fn reclaim_inner(&self) -> Result<reclaim::ReclaimReport> {
        // Seal the current active.
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
                })
                .collect()
        };
        old_active.write_footer(&footer).await?;
        self.sealed.borrow_mut().insert(old_active.file_id, old_active.clone());

        // Pick the next file_id as max(existing) + 1.
        let next_id = {
            let sealed = self.sealed.borrow();
            sealed.keys().copied().max().unwrap_or(0)
                .checked_add(1)
                .ok_or(EngineError::CapacityExceeded {
                    reason: "file_id overflow: namespace has too many log files",
                })?
        };
        let new_active_id = next_id
            .checked_add(1)
            .ok_or(EngineError::CapacityExceeded {
                reason: "file_id overflow: namespace has too many log files",
            })?;

        let sealed_snapshot: Vec<Rc<LogFile>> = self.sealed.borrow().values().cloned().collect();

        // Snapshot live entries outside the await so the reclaim doesn't hold an index borrow.
        let live: Vec<(Bytes, IndexEntry, Option<u64>)> = {
            let index = self.index.borrow();
            index.iter().map(|(k, e)| (k.clone(), *e, index.ttl(k))).collect()
        };

        let (report, new_entries) =
            reclaim::reclaim_namespace(self.dir.clone(), &sealed_snapshot, next_id, &live).await?;

        // Apply new index entries.
        {
            let mut index = self.index.borrow_mut();
            for (key, entry, ttl) in new_entries {
                index.insert(key, entry, ttl);
            }
        }

        // Drop old sealed handles & swap in the single new sealed file.
        self.sealed.borrow_mut().clear();
        let new_sealed_path = self.dir.join(data_filename(next_id));
        let new_sealed = Rc::new(LogFile::open_ro(new_sealed_path, next_id).await?);
        self.sealed.borrow_mut().insert(next_id, new_sealed);

        // Open a fresh active file.
        let new_active_path = self.dir.join(data_filename(new_active_id));
        let new_active = Rc::new(LogFile::open_rw(new_active_path, new_active_id).await?);
        *self.active.borrow_mut() = new_active;
        self.unsynced_bytes.set(0);

        Ok(report)
    }
}


//! Content-addressed value store (value separation, WiscKey-style) for large
//! values — the GlideFS-friendly large-value path.
//!
//! A value is keyed by its BLAKE3-128 content hash and stored once; identical
//! values across keys/forks/tenants dedup to a single blob. The main log holds
//! only the small `(key -> hash)` pointer record, so compaction moves pointers,
//! never large values — collapsing large-value write amplification. Blobs are
//! immutable and refcounted; a blob is unlinked when its last reference drops.
//!
//! Blob I/O is async via `monoio::fs` (io_uring) — it runs on the same reactor
//! as the log engine and never blocks the shard's event loop. Refcounts are
//! in-memory, rebuilt from the live index on open; `sweep_orphans` reclaims any
//! blob a crash left without a referencing record.

use std::cell::RefCell;
use std::path::PathBuf;

use rustc_hash::FxHashMap;

use crate::error::Result;

/// BLAKE3-128 content hash (matches GlideFS's block addressing width).
pub type ContentHash = [u8; 16];

pub fn content_hash(value: &[u8]) -> ContentHash {
    let mut out = [0u8; 16];
    out.copy_from_slice(&blake3::hash(value).as_bytes()[..16]);
    out
}

fn hex16(h: &ContentHash) -> String {
    let mut s = String::with_capacity(32);
    for b in h {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// Parse a `blob-<32 hex>` filename back into its content hash.
fn parse_blob_name(name: &str) -> Option<ContentHash> {
    let hex = name.strip_prefix("blob-")?;
    if hex.len() != 32 {
        return None;
    }
    let mut h = [0u8; 16];
    for (i, b) in h.iter_mut().enumerate() {
        *b = u8::from_str_radix(hex.get(i * 2..i * 2 + 2)?, 16).ok()?;
    }
    Some(h)
}

/// Content-addressed, refcounted blob store. Refcounts are in-memory (rebuilt
/// from the index on open in the integrated engine).
pub struct ValueStore {
    dir: PathBuf,
    refs: RefCell<FxHashMap<ContentHash, u32>>,
    /// Blobs whose refcount has hit zero but whose deletion is deferred until the
    /// next fsync (see `collect_garbage`). Deleting a superseded blob before the
    /// superseding log record is durable would, on a power loss that loses that
    /// record, leave the reverted-to key pointing at a deleted blob (a dangling
    /// pointer). Deferring past the fsync makes the revert safe.
    pending_delete: RefCell<Vec<ContentHash>>,
    /// Striped locks serializing the file create/delete of a given blob. A blob's
    /// `put` (write) and `collect_garbage` (unlink) for the SAME content hash hold
    /// the same stripe, so they can never race — without it, an unlink in flight
    /// could delete a file a concurrent same-content `put` just recreated (a
    /// dangling pointer). Different content → different stripe → still concurrent.
    file_locks: Vec<futures_util::lock::Mutex<()>>,
}

/// Number of blob file-op stripes. Same-hash ops serialize; different hashes
/// stay concurrent. Blob writes are the rare large-value path, so this is small.
const FILE_LOCK_STRIPES: usize = 16;

impl ValueStore {
    pub fn new(dir: PathBuf) -> Self {
        // Dir is created lazily on the first blob write — an all-small-value
        // namespace never materializes a `values/` directory at all.
        Self {
            dir,
            refs: RefCell::new(FxHashMap::default()),
            pending_delete: RefCell::new(Vec::new()),
            file_locks: (0..FILE_LOCK_STRIPES)
                .map(|_| futures_util::lock::Mutex::new(()))
                .collect(),
        }
    }

    fn path(&self, h: &ContentHash) -> PathBuf {
        self.dir.join(format!("blob-{}", hex16(h)))
    }

    /// The file-op stripe for a content hash (first bytes of the hash → stripe).
    fn flock(&self, h: &ContentHash) -> &futures_util::lock::Mutex<()> {
        &self.file_locks[(h[0] as usize) & (FILE_LOCK_STRIPES - 1)]
    }

    /// Store `value`, deduplicated by content. Returns its content hash. Writes
    /// the blob only on first reference (immutable, write-once); subsequent puts
    /// of identical content just bump the refcount — no rewrite, no extra bytes.
    pub async fn put(&self, value: &[u8]) -> Result<ContentHash> {
        let h = content_hash(value);
        // Serialize this content's file create against a concurrent delete of the
        // same content in `collect_garbage` (held across the refcount bump + write
        // so the decision and the file op are atomic for this hash).
        let _fl = self.flock(&h).lock().await;
        let first = {
            let mut refs = self.refs.borrow_mut();
            let c = refs.entry(h).or_insert(0);
            *c += 1;
            *c == 1
        };
        if first {
            // Write the blob durably BEFORE the caller writes the pointer record
            // that references it. The log uses appendfsync-everysec, but the
            // pointer and the value live in different files — so we must fsync the
            // blob's data AND its directory entry here, or a power loss could
            // leave a durable pointer aimed at a non-durable blob (a dangling
            // pointer = corruption, worse than the everysec "lose the last 1s"
            // contract). With this ordering, the worst a crash can do is leave an
            // orphan blob (durable blob, lost pointer) — reclaimed by
            // `sweep_orphans` on the next open. All I/O is io_uring (no blocking).
            if let Err(e) = self.write_blob_durable(&h, value).await {
                self.dec(&h); // roll back the ref; no phantom reference to a missing blob
                return Err(e);
            }
        }
        Ok(h)
    }

    /// Write `value` to its blob path and make it crash-durable: fsync the file's
    /// data, then fsync the parent directory so the new directory entry survives a
    /// power loss. Returns only once the blob is durable on stable storage.
    async fn write_blob_durable(&self, h: &ContentHash, value: &[u8]) -> Result<()> {
        // Propagate a create failure rather than swallow it: if the directory
        // can't be made, the `open` below fails with a generic ENOENT that hides
        // the real cause (e.g. EACCES on the parent). idempotent: Ok if it exists.
        monoio::fs::create_dir_all(&self.dir).await?;
        let file = monoio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(self.path(h))
            .await?;
        let (res, _buf) = file.write_all_at(value.to_vec(), 0).await;
        res?;
        file.sync_all().await?; // blob bytes durable
        let _ = file.close().await;
        // fsync the directory so the blob's name is durable before any pointer
        // record referencing it can become durable. A failure here weakens the
        // crash-durability contract (the blob's directory entry may not survive a
        // power loss, leaving a durable pointer aimed at a nameless blob), so
        // surface it as an error rather than swallow it. The caller rolls back the
        // refcount on Err, so a failed durability step never leaves a phantom ref.
        let dir = monoio::fs::OpenOptions::new()
            .read(true)
            .open(&self.dir)
            .await?;
        let sync_res = dir.sync_all().await;
        let _ = dir.close().await;
        sync_res?;
        Ok(())
    }

    pub async fn get(&self, h: &ContentHash) -> Result<Vec<u8>> {
        Ok(monoio::fs::read(self.path(h)).await?)
    }

    /// Recovery: rebuild the in-memory refcount for a hash referenced by a live
    /// index entry, WITHOUT writing the blob (it already exists on disk from
    /// before the restart). Called once per live value-separated key at open.
    pub fn incr_ref(&self, h: &ContentHash) {
        *self.refs.borrow_mut().entry(*h).or_insert(0) += 1;
    }

    /// Decrement the in-memory refcount; return true if it hit zero (blob dead).
    fn dec(&self, h: &ContentHash) -> bool {
        let mut refs = self.refs.borrow_mut();
        match refs.get_mut(h) {
            Some(c) => {
                *c = c.saturating_sub(1);
                if *c == 0 {
                    refs.remove(h);
                    true
                } else {
                    false
                }
            }
            None => false,
        }
    }

    /// Drop one reference. When the last reference goes away the blob is NOT
    /// deleted immediately — it is queued for `collect_garbage`, which runs after
    /// the next fsync. This preserves the crash-consistency invariant: a blob is
    /// only physically removed once the log record that superseded it is durable,
    /// so a power loss that reverts the key always finds its blob still present.
    pub fn unref(&self, h: &ContentHash) {
        if self.dec(h) {
            self.pending_delete.borrow_mut().push(*h);
        }
    }

    /// Delete the blobs queued by `unref`, but only those still at refcount 0
    /// (a queued blob may have been re-referenced by an identical-content write
    /// in the meantime). MUST be called only after the log has been fsynced past
    /// the records that orphaned these blobs — i.e. right after `LogFile::sync`.
    /// Blobs whose deletion is skipped here for any reason are still reachable as
    /// orphans and reclaimed by `sweep_orphans` on the next open.
    pub async fn collect_garbage(&self) {
        let pending: Vec<ContentHash> = std::mem::take(&mut *self.pending_delete.borrow_mut());
        for h in pending {
            // Hold this content's file stripe so the refcount==0 check and the
            // unlink are atomic w.r.t. a concurrent same-content `put`: either the
            // put re-references it first (refcount>0 → we skip) or we delete first
            // (and the put then recreates it). The file can't be left missing while
            // a live key references it.
            let _fl = self.flock(&h).lock().await;
            if self.refcount(&h) == 0 {
                let _ = monoio::fs::remove_file(self.path(&h)).await;
            }
        }
    }

    /// Drop all blobs and refcounts (FLUSHDB). Nukes the whole `values/` tree —
    /// including any deferred-delete or orphan blobs — and resets all state.
    pub fn clear(&self) {
        self.refs.borrow_mut().clear();
        self.pending_delete.borrow_mut().clear();
        let _ = std::fs::remove_dir_all(&self.dir);
    }

    /// Reclaim orphan blobs: files on disk that no live key references. A crash
    /// between writing a blob and appending its log record (or between writing a
    /// new blob and unref'ing the old one) leaves such a file. Call once at open,
    /// AFTER refcounts have been rebuilt from the live index — then any blob not
    /// in `refs` is unreachable and safe to delete. Returns the count removed.
    ///
    /// Directory listing uses `std::fs` because this runs at open, before the
    /// shard serves traffic (same place `recover` already lists data files);
    /// the deletions go through io_uring.
    pub async fn sweep_orphans(&self) -> Result<usize> {
        let entries = match std::fs::read_dir(&self.dir) {
            Ok(e) => e,
            Err(_) => return Ok(0), // no values dir => nothing to sweep
        };
        let mut orphans: Vec<PathBuf> = Vec::new();
        for ent in entries.flatten() {
            let name = ent.file_name();
            let name = name.to_string_lossy();
            match parse_blob_name(&name) {
                Some(h) if !self.refs.borrow().contains_key(&h) => orphans.push(ent.path()),
                Some(_) => {} // referenced — keep
                None => {}    // not a blob file — ignore
            }
        }
        let removed = orphans.len();
        for p in orphans {
            let _ = monoio::fs::remove_file(p).await;
        }
        Ok(removed)
    }

    pub fn blob_count(&self) -> usize {
        self.refs.borrow().len()
    }

    pub fn refcount(&self, h: &ContentHash) -> u32 {
        self.refs.borrow().get(h).copied().unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn run<F: std::future::Future>(f: F) -> F::Output {
        monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_timer()
            .build()
            .expect("monoio runtime")
            .block_on(f)
    }

    /// Identical large values DEDUP to one blob (write-once), and refcounted GC
    /// reclaims it — on real files, through the async io_uring path.
    #[test]
    fn dedup_write_once_and_gc() {
        run(async {
            let dir = TempDir::new().unwrap();
            let vs = ValueStore::new(dir.path().join("values"));

            let big = vec![7u8; 1_000_000]; // 1 MiB
            let h1 = vs.put(&big).await.unwrap();
            let h2 = vs.put(&big).await.unwrap(); // identical content
            assert_eq!(h1, h2, "same content → same hash");
            assert_eq!(vs.blob_count(), 1, "identical values dedup to ONE blob");
            assert_eq!(vs.refcount(&h1), 2);
            assert_eq!(vs.get(&h1).await.unwrap(), big, "roundtrip");

            let other = vec![9u8; 1_000_000];
            vs.put(&other).await.unwrap();
            assert_eq!(vs.blob_count(), 2, "distinct content → distinct blob");

            // Drop both refs to the first blob → refcount 0, queued for deletion.
            vs.unref(&h1);
            vs.unref(&h1);
            assert_eq!(vs.refcount(&h1), 0);
            assert!(
                vs.get(&h1).await.is_ok(),
                "blob still on disk before collect (deferred delete)"
            );
            // collect_garbage runs after an fsync → now the blob is physically gone.
            vs.collect_garbage().await;
            assert!(
                vs.get(&h1).await.is_err(),
                "blob GC'd after collect_garbage"
            );
            assert_eq!(vs.blob_count(), 1, "only the live blob remains");
        });
    }

    /// A crash can leave a blob on disk with no referencing key. After refcounts
    /// are rebuilt from the live index, `sweep_orphans` reclaims exactly those.
    #[test]
    fn sweep_reclaims_orphans_only() {
        run(async {
            let dir = TempDir::new().unwrap();
            let vs = ValueStore::new(dir.path().join("values"));
            let live = vs.put(&vec![1u8; 4096]).await.unwrap();
            let orphan = vs.put(&vec![2u8; 4096]).await.unwrap();
            // Simulate a crash that wrote the orphan blob but never recorded its
            // reference: forget it from the in-memory refs (as a fresh open would,
            // since no live key points at it).
            vs.refs.borrow_mut().remove(&orphan);

            let removed = vs.sweep_orphans().await.unwrap();
            assert_eq!(removed, 1, "exactly the unreferenced blob is reclaimed");
            assert!(vs.get(&orphan).await.is_err(), "orphan blob deleted");
            assert_eq!(
                vs.get(&live).await.unwrap().len(),
                4096,
                "live blob untouched"
            );
        });
    }
}

#[cfg(test)]
mod gc_race_tests {
    use super::*;
    use std::rc::Rc;
    use tempfile::TempDir;

    fn run<F: std::future::Future>(f: F) -> F::Output {
        monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_timer()
            .build()
            .expect("monoio runtime")
            .block_on(f)
    }

    /// Deterministic regression (real teeth): a blob queued for deletion that is
    /// re-referenced BEFORE GC runs must survive — `collect_garbage` re-checks the
    /// live refcount instead of deleting everything it queued. (Remove the
    /// `refcount == 0` guard in `collect_garbage` and this fails: the blob is gone.)
    #[test]
    fn collect_garbage_skips_a_requeued_then_rereferenced_blob() {
        run(async {
            let dir = TempDir::new().unwrap();
            let vs = ValueStore::new(dir.path().join("values"));
            let v = vec![0x5Au8; 4096];
            let h = vs.put(&v).await.unwrap(); // refcount 1, file written
            vs.unref(&h); // refcount 0, queued for delete, file still present
            assert_eq!(vs.refcount(&h), 0);
            vs.put(&v).await.unwrap(); // re-reference BEFORE gc → refcount 1
            assert_eq!(vs.refcount(&h), 1);
            vs.collect_garbage().await; // must SKIP h (live again)
            assert_eq!(
                vs.get(&h).await.unwrap(),
                v,
                "re-referenced blob must survive GC"
            );
        });
    }

    /// Stress: `collect_garbage` racing a same-content `put`. The per-content file
    /// lock makes create/delete of one hash mutually exclusive, so a re-referenced
    /// blob is never left deleted — a by-construction guarantee against io_uring
    /// completion reordering. (The bad reorder is hard to force on a given kernel,
    /// so this passes with or without the lock; the deterministic test above has
    /// the teeth, the lock provides correctness under any ordering.)
    #[test]
    fn gc_does_not_delete_a_concurrently_recreated_blob() {
        run(async {
            let dir = TempDir::new().unwrap();
            let vs = Rc::new(ValueStore::new(dir.path().join("values")));
            let v = vec![0xABu8; 8192];
            let h = content_hash(&v);

            for _ in 0..300 {
                // Make h a queued (refcount 0) deletion with its file still on disk.
                vs.put(&v).await.unwrap();
                vs.unref(&h);
                assert_eq!(vs.refcount(&h), 0);

                // Race GC (wants to delete h) against a put re-referencing the same content.
                let a = vs.clone();
                let b = vs.clone();
                let vb = v.clone();
                let t_gc = monoio::spawn(async move { a.collect_garbage().await });
                let t_put = monoio::spawn(async move { b.put(&vb).await.unwrap() });
                t_gc.await;
                t_put.await;

                // The put re-referenced it → refcount 1 → the blob MUST still exist.
                assert_eq!(vs.refcount(&h), 1, "put should have re-referenced the blob");
                assert_eq!(
                    vs.get(&h).await.expect("live blob deleted by GC/put race"),
                    v,
                    "blob content intact after concurrent GC + recreate"
                );

                // Reset for the next round.
                vs.unref(&h);
                vs.collect_garbage().await;
            }
        });
    }
}

use std::cell::RefCell;
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Per-shard L1 cache hit/miss counters. Both fields are `Arc<AtomicU64>` so the
/// server-side metrics layer can read them without holding a reference to the store.
pub struct CacheCounters {
    pub hits: Arc<AtomicU64>,
    pub misses: Arc<AtomicU64>,
}

/// Aggregate result from a background reclaim pass across all namespaces.
#[derive(Debug, Default, Clone, Copy)]
pub struct ReclaimSummary {
    pub namespaces_reclaimed: u64,
    pub files_freed: u64,
    /// Bytes remaining in the live data after compaction.
    pub live_bytes: u64,
}

use bytes::Bytes;
use futures_channel::mpsc::Receiver;
use futures_util::future::join_all;
use memchr::memchr3;
use rustc_hash::FxHashMap;
use tracing::{info, warn};

use crate::cache::MemCache;
use crate::error::{EngineError, Result};
use crate::log::config::LogConfig;
use crate::log::index::IndexEntry;
use crate::log::{NamespaceLog, WriteCondition, now_ms};
use crate::types::{Entry, GetExOp, ScanPage, SetOptions, TtlResult};
use crate::watch::{KeyFilter, WatchEvent, WatchRegistry};

pub const DEFAULT_NS: &str = "default";

/// Maximum number of distinct namespaces a single shard will open. Prevents
/// unbounded NamespaceLog + file-descriptor growth from adversarial namespace names.
const MAX_NAMESPACES: usize = 1024;

/// Map database index to a namespace name: 0 → "default", n → "db{n}".
pub fn ns_name(db: u64) -> String {
    if db == 0 {
        DEFAULT_NS.to_string()
    } else {
        format!("db{db}")
    }
}

fn is_valid_ns_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

/// Per-shard KV store: one `NamespaceLog` per namespace (lazily opened) + one S3-FIFO L1 cache.
///
/// Lives behind `Rc<>` on a single monoio worker thread (`!Sync` via the cache's `RefCell`).
/// All public methods are `async` because cold reads dispatch to io_uring via
/// `monoio::fs`; L1 hits short-circuit without awaiting any I/O.
pub struct ShardStore {
    data_dir: std::path::PathBuf,
    config: LogConfig,
    namespaces: RefCell<FxHashMap<String, Rc<NamespaceLog>>>,
    cache: MemCache,
    watchers: RefCell<WatchRegistry>,
    cache_hit_count: Arc<AtomicU64>,
    cache_miss_count: Arc<AtomicU64>,
}

impl ShardStore {
    /// Open or create the shard store at `data_dir`.
    ///
    /// `std::fs::create_dir_all` and `std::fs::read_dir` are blocking syscalls.
    /// Acceptable at startup before any traffic begins, but should not be called
    /// from a hot async path after the runtime is handling requests.
    pub async fn open(data_dir: &Path, memory_bytes: usize) -> Result<Self> {
        std::fs::create_dir_all(data_dir)?;
        // Compaction is size-tiered (GlideFS-friendly). `KV_COMPACTION_FANOUT`
        // tunes the per-level fanout; `KV_VALUE_SEP_THRESHOLD` the
        // value-separation cutoff.
        let config = {
            let mut c = LogConfig::default();
            if let Ok(n) = std::env::var("KV_COMPACTION_FANOUT")
                .unwrap_or_default()
                .parse::<usize>()
            {
                if n >= 2 {
                    c.fanout = n;
                }
            }
            if let Ok(n) = std::env::var("KV_VALUE_SEP_THRESHOLD")
                .unwrap_or_default()
                .parse::<usize>()
            {
                c.value_sep_threshold = n;
            }
            c
        };
        let mut namespaces: FxHashMap<String, Rc<NamespaceLog>> = FxHashMap::default();

        // Collect valid namespace subdirectories, then open them concurrently.
        match std::fs::read_dir(data_dir) {
            Ok(entries) => {
                let dirs: Vec<(String, std::path::PathBuf)> = entries
                    .flatten()
                    .filter(|e| e.file_type().is_ok_and(|t| t.is_dir()))
                    .filter_map(|e| {
                        let name = e.file_name().to_string_lossy().into_owned();
                        if is_valid_ns_name(&name) {
                            Some((name, e.path()))
                        } else {
                            None
                        }
                    })
                    .collect();
                let futures: Vec<_> = dirs
                    .iter()
                    .map(|(_, path)| NamespaceLog::open(path.clone(), config))
                    .collect();
                let opened = join_all(futures).await;
                for ((name, _), nslog) in dirs.into_iter().zip(opened) {
                    namespaces.insert(name, Rc::new(nslog?));
                }
            }
            Err(e) => {
                warn!(path = %data_dir.display(), error = %e, "failed to read data directory; existing namespaces will not be loaded");
            }
        }

        // Always ensure the default namespace is open.
        if !namespaces.contains_key(DEFAULT_NS) {
            let nslog = NamespaceLog::open(data_dir.join(DEFAULT_NS), config).await?;
            namespaces.insert(DEFAULT_NS.to_string(), Rc::new(nslog));
        }

        Ok(Self {
            data_dir: data_dir.to_path_buf(),
            config,
            namespaces: RefCell::new(namespaces),
            cache: MemCache::new(memory_bytes),
            watchers: RefCell::new(WatchRegistry::new()),
            cache_hit_count: Arc::new(AtomicU64::new(0)),
            cache_miss_count: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Snapshot of this shard's cache hit/miss counters. Cheap clone — both
    /// counters live behind `Arc` so observers see live updates.
    pub fn cache_counters(&self) -> CacheCounters {
        CacheCounters {
            hits: self.cache_hit_count.clone(),
            misses: self.cache_miss_count.clone(),
        }
    }

    /// Open the namespace if not already open, then return a cloned handle.
    async fn ensure_ns(&self, ns: &str) -> Result<Rc<NamespaceLog>> {
        if !is_valid_ns_name(ns) {
            return Err(EngineError::InvalidNamespace {
                name: ns.to_owned(),
            });
        }
        {
            let ns_map = self.namespaces.borrow();
            if let Some(existing) = ns_map.get(ns).cloned() {
                return Ok(existing);
            }
            if ns_map.len() >= MAX_NAMESPACES {
                return Err(EngineError::CapacityExceeded {
                    reason: "namespace limit reached",
                });
            }
        }
        let dir = self.data_dir.join(ns);
        let nslog = Rc::new(NamespaceLog::open(dir, self.config).await?);
        // Re-check after the await: a concurrent task may have inserted this same
        // namespace (dedup — return theirs, drop ours), OR filled the map to the
        // cap while we were opening. Without the cap re-check, N concurrent opens
        // of distinct new namespaces could all pass the pre-await gate at
        // `len == MAX-1` and each insert, overshooting the cap.
        let mut ns_map = self.namespaces.borrow_mut();
        if let Some(existing) = ns_map.get(ns) {
            return Ok(existing.clone());
        }
        if ns_map.len() >= MAX_NAMESPACES {
            // Our freshly-created (empty) namespace dir is left behind; it is
            // harmless and reused idempotently if this namespace is opened later.
            return Err(EngineError::CapacityExceeded {
                reason: "namespace limit reached",
            });
        }
        Ok(ns_map.entry(ns.to_string()).or_insert(nslog).clone())
    }

    /// Test-only accessor that bypasses `ensure_ns` validation. Do not use in production code.
    #[cfg(test)]
    pub(crate) fn get_ns(&self, ns: &str) -> Result<Rc<NamespaceLog>> {
        self.namespaces
            .borrow()
            .get(ns)
            .cloned()
            .ok_or_else(|| EngineError::InvalidNamespace {
                name: ns.to_owned(),
            })
    }

    /// Build the composite cache key `ns\x00key` into a stack buffer for lookups,
    /// avoiding heap allocation for the common case (total ≤ 128 bytes).
    fn with_cache_key<R>(ns: &str, key: &[u8], f: impl FnOnce(&[u8]) -> R) -> R {
        let total = ns.len() + 1 + key.len();
        if total <= 128 {
            let mut buf = [0u8; 128];
            let nb = ns.as_bytes();
            buf[..nb.len()].copy_from_slice(nb);
            buf[nb.len()] = b'\x00';
            buf[nb.len() + 1..total].copy_from_slice(key);
            f(&buf[..total])
        } else {
            let mut v = Vec::with_capacity(total);
            v.extend_from_slice(ns.as_bytes());
            v.push(b'\x00');
            v.extend_from_slice(key);
            f(&v)
        }
    }

    /// Build an owned composite cache key for insertions.
    fn cache_key(ns: &str, key: &[u8]) -> Bytes {
        let mut ck = Vec::with_capacity(ns.len() + 1 + key.len());
        ck.extend_from_slice(ns.as_bytes());
        ck.push(b'\x00');
        ck.extend_from_slice(key);
        Bytes::from(ck)
    }

    #[allow(clippy::too_many_arguments)]
    fn upsert_cache(
        &self,
        ns: &str,
        key: &[u8],
        value: Bytes,
        expires_at_ms: Option<u64>,
        metadata: Option<Arc<serde_json::Value>>,
        meta_len: usize,
        revision: u64,
    ) {
        let updated = Self::with_cache_key(ns, key, |ck| {
            self.cache.try_update(
                ck,
                value.clone(),
                expires_at_ms,
                metadata.clone(),
                meta_len,
                revision,
            )
        });
        if !updated {
            self.cache.insert(
                Self::cache_key(ns, key),
                value,
                expires_at_ms,
                metadata,
                meta_len,
                revision,
            );
        }
    }

    fn instant_from_ms(
        expires_at_ms: Option<u64>,
        now_ms: u64,
        now_instant: Instant,
    ) -> Option<Instant> {
        expires_at_ms.map(|ms| now_instant + Duration::from_millis(ms.saturating_sub(now_ms)))
    }

    fn metadata_from_bytes(meta: &[u8]) -> Result<Option<Arc<serde_json::Value>>> {
        if meta.is_empty() {
            return Ok(None);
        }
        serde_json::from_slice::<serde_json::Value>(meta)
            .map(|v| Some(Arc::new(v)))
            .map_err(Into::into)
    }

    fn validate_ttl(d: Duration, now: u64) -> Result<u64> {
        let ms = u64::try_from(d.as_millis()).map_err(|_| EngineError::CapacityExceeded {
            reason: "ttl exceeds u64::MAX milliseconds",
        })?;
        now.checked_add(ms).ok_or(EngineError::CapacityExceeded {
            reason: "ttl would overflow absolute expiry timestamp",
        })
    }

    /// Inline get used by `getset` and `getdel`: check L1 cache, then index, then disk.
    /// Tombstones and evicts expired keys. Does NOT populate the cache on disk reads
    /// because the caller is about to overwrite or delete the key anyway.
    async fn get_inline(
        &self,
        nslog: &NamespaceLog,
        ns: &str,
        key: &[u8],
        now: u64,
        now_instant: Instant,
    ) -> Result<Option<Entry>> {
        if let Some((value, expires_at_ms, metadata, revision)) =
            Self::with_cache_key(ns, key, |ck| self.cache.get(ck, now))
        {
            return Ok(Some(Entry {
                value,
                expires_at: Self::instant_from_ms(expires_at_ms, now, now_instant),
                metadata,
                revision,
            }));
        }
        let (entry, expires_at_ms) = {
            let idx = nslog.index.borrow();
            match idx.get(key) {
                None => return Ok(None),
                Some(e) => (*e, idx.ttl(key)),
            }
        };
        if expires_at_ms.is_some_and(|ms| ms <= now) {
            nslog.tombstone(key).await?;
            Self::with_cache_key(ns, key, |ck| self.cache.remove(ck));
            return Ok(None);
        }
        let (value, meta_bytes) = nslog.read_value(entry).await?;
        let metadata = Self::metadata_from_bytes(&meta_bytes)?;
        Ok(Some(Entry {
            value,
            expires_at: Self::instant_from_ms(expires_at_ms, now, now_instant),
            metadata,
            revision: entry.tstamp_ms,
        }))
    }

    /// Inner TTL read returning `Remaining` in **milliseconds**.
    /// `ttl` divides by 1000; `pttl` returns the raw millisecond value.
    async fn ttl_raw(&self, ns: &str, key: &[u8]) -> Result<TtlResult> {
        let nslog = self.ensure_ns(ns).await?;
        let now = now_ms();
        let (result, should_tombstone) = {
            let idx = nslog.index.borrow();
            match idx.get(key) {
                None => (TtlResult::NotFound, false),
                Some(_) => match idx.ttl(key) {
                    None => (TtlResult::NoExpiry, false),
                    Some(ms) if ms <= now => (TtlResult::NotFound, true),
                    Some(ms) => (TtlResult::Remaining(ms.saturating_sub(now)), false),
                },
            }
        };
        if should_tombstone {
            nslog.tombstone(key).await?;
            Self::with_cache_key(ns, key, |ck| self.cache.remove(ck));
        }
        Ok(result)
    }

    /// Common body for `setnx` and `setxx`.
    /// `require_live = false` → write only if key does NOT exist (SETNX).
    /// `require_live = true`  → write only if key already exists (SETXX).
    async fn set_conditional(
        &self,
        ns: &str,
        key: &[u8],
        value: Bytes,
        opts: SetOptions,
        require_live: bool,
    ) -> Result<bool> {
        let nslog = self.ensure_ns(ns).await?;
        let now = now_ms();
        let cond = if require_live {
            WriteCondition::KeyPresent
        } else {
            WriteCondition::KeyAbsent
        };
        let expires_at_ms = opts.ttl.map(|d| Self::validate_ttl(d, now)).transpose()?;
        let meta_bytes: Vec<u8> = opts
            .metadata
            .as_ref()
            .map(|m| serde_json::to_vec(m.as_ref()))
            .transpose()?
            .unwrap_or_default();
        let key_bytes = Bytes::copy_from_slice(key);
        let revision = match nslog
            .put_full_cond(
                key_bytes.clone(),
                &value,
                &meta_bytes,
                expires_at_ms,
                cond,
                now,
            )
            .await?
        {
            Some(t) => t,
            None => return Ok(false),
        };
        self.upsert_cache(
            ns,
            key,
            value.clone(),
            expires_at_ms,
            opts.metadata.clone(),
            meta_bytes.len(),
            revision,
        );
        self.watchers.borrow_mut().notify(
            ns,
            key,
            WatchEvent::Set {
                key: key_bytes,
                value,
                metadata: opts.metadata,
                expires_at_ms,
                revision,
            },
        );
        Ok(true)
    }

    pub async fn get(&self, ns: &str, key: &[u8]) -> Result<Option<Entry>> {
        let now = now_ms();
        let now_instant = Instant::now();
        let nslog = self.ensure_ns(ns).await?;

        if let Some((value, expires_at_ms, metadata, revision)) =
            Self::with_cache_key(ns, key, |ck| self.cache.get(ck, now))
        {
            self.cache_hit_count.fetch_add(1, Ordering::Relaxed);
            return Ok(Some(Entry {
                value,
                expires_at: Self::instant_from_ms(expires_at_ms, now, now_instant),
                metadata,
                revision,
            }));
        }
        self.cache_miss_count.fetch_add(1, Ordering::Relaxed);

        let (entry, expires_at_ms) = {
            let idx = nslog.index.borrow();
            match idx.get(key) {
                None => return Ok(None),
                Some(e) => (*e, idx.ttl(key)),
            }
        };
        if expires_at_ms.is_some_and(|ms| ms <= now) {
            // Lazy delete on read.
            nslog.tombstone(key).await?;
            Self::with_cache_key(ns, key, |ck| self.cache.remove(ck));
            return Ok(None);
        }

        let (value, meta_bytes) = nslog.read_value(entry).await?;
        let metadata = Self::metadata_from_bytes(&meta_bytes)?;
        self.cache.insert(
            Self::cache_key(ns, key),
            value.clone(),
            expires_at_ms,
            metadata.clone(),
            meta_bytes.len(),
            entry.tstamp_ms,
        );
        Ok(Some(Entry {
            value,
            expires_at: Self::instant_from_ms(expires_at_ms, now, now_instant),
            metadata,
            revision: entry.tstamp_ms,
        }))
    }

    /// Bulk get. Cold reads are batched through io_uring via `join_all` so a
    /// 100-key MGET dispatches all the disk reads concurrently rather than
    /// serially awaiting each one.
    pub async fn mget(&self, ns: &str, keys: &[&[u8]]) -> Result<Vec<Option<Entry>>> {
        let now = now_ms();
        let now_instant = Instant::now();
        let nslog = self.ensure_ns(ns).await?;

        let mut results: Vec<Option<Entry>> = vec![None; keys.len()];
        let mut misses: Vec<(usize, IndexEntry)> = Vec::new();
        // (ttl, revision) paired so bulk_read can consume `misses` without a separate pass.
        let mut miss_meta: Vec<(Option<u64>, u64)> = Vec::new();
        let mut expired_keys: Vec<&[u8]> = Vec::new();

        for (i, key) in keys.iter().enumerate() {
            // L1
            if let Some((value, expires_at_ms, metadata, revision)) =
                Self::with_cache_key(ns, key, |ck| self.cache.get(ck, now))
            {
                self.cache_hit_count.fetch_add(1, Ordering::Relaxed);
                results[i] = Some(Entry {
                    value,
                    expires_at: Self::instant_from_ms(expires_at_ms, now, now_instant),
                    metadata,
                    revision,
                });
                continue;
            }
            self.cache_miss_count.fetch_add(1, Ordering::Relaxed);
            // Index lookup (in-RAM)
            let (entry, ttl) = {
                let idx = nslog.index.borrow();
                match idx.get(key) {
                    None => continue,
                    Some(e) => (*e, idx.ttl(key)),
                }
            };
            if ttl.is_some_and(|ms| ms <= now) {
                expired_keys.push(key);
                continue;
            }
            miss_meta.push((ttl, entry.tstamp_ms));
            misses.push((i, entry));
        }

        // Batch-tombstone expired keys concurrently so their individual disk writes
        // don't serialize inside the per-key loop above.
        if !expired_keys.is_empty() {
            let tombstone_futs: Vec<_> = expired_keys.iter().map(|k| nslog.tombstone(k)).collect();
            for result in join_all(tombstone_futs).await {
                result?;
            }
        }

        if !misses.is_empty() {
            let read = nslog.bulk_read(misses).await?;
            for ((slot, value, meta_bytes), (ttl, revision)) in read.into_iter().zip(miss_meta) {
                let metadata = Self::metadata_from_bytes(&meta_bytes)?;
                self.cache.insert(
                    Self::cache_key(ns, keys[slot]),
                    value.clone(),
                    ttl,
                    metadata.clone(),
                    meta_bytes.len(),
                    revision,
                );
                results[slot] = Some(Entry {
                    value,
                    expires_at: Self::instant_from_ms(ttl, now, now_instant),
                    metadata,
                    revision,
                });
            }
        }
        Ok(results)
    }

    pub async fn set(&self, ns: &str, key: &[u8], value: Bytes, opts: SetOptions) -> Result<()> {
        let nslog = self.ensure_ns(ns).await?;
        let now = now_ms();
        let expires_at_ms = if opts.keep_ttl {
            nslog.index.borrow().ttl(key)
        } else {
            opts.ttl.map(|d| Self::validate_ttl(d, now)).transpose()?
        };
        let meta_bytes: Vec<u8> = opts
            .metadata
            .as_ref()
            .map(|m| serde_json::to_vec(m.as_ref()))
            .transpose()?
            .unwrap_or_default();
        let key_bytes = Bytes::copy_from_slice(key);
        let revision = nslog
            .put_full(key_bytes.clone(), &value, &meta_bytes, expires_at_ms)
            .await?;
        self.upsert_cache(
            ns,
            key,
            value.clone(),
            expires_at_ms,
            opts.metadata.clone(),
            meta_bytes.len(),
            revision,
        );
        self.watchers.borrow_mut().notify(
            ns,
            key,
            WatchEvent::Set {
                key: key_bytes,
                value,
                metadata: opts.metadata,
                expires_at_ms,
                revision,
            },
        );
        Ok(())
    }

    /// MSET: atomically set multiple keys. Per-key TTL and metadata are not
    /// supported; use `set` for keys that require them.
    pub async fn mset(&self, ns: &str, pairs: &[(Bytes, Bytes)]) -> Result<()> {
        let nslog = self.ensure_ns(ns).await?;
        let revisions = nslog.put_many(pairs).await?;
        for ((key, value), &revision) in pairs.iter().zip(revisions.iter()) {
            self.upsert_cache(ns, key, value.clone(), None, None, 0, revision);
        }
        let mut w = self.watchers.borrow_mut();
        for ((key, value), &revision) in pairs.iter().zip(revisions.iter()) {
            w.notify(
                ns,
                key,
                WatchEvent::Set {
                    key: key.clone(),
                    value: value.clone(),
                    metadata: None,
                    expires_at_ms: None,
                    revision,
                },
            );
        }
        Ok(())
    }

    pub async fn del(&self, ns: &str, keys: &[&[u8]]) -> Result<u64> {
        let nslog = self.ensure_ns(ns).await?;
        let now = now_ms();

        // Snapshot expiry state before any async work (index borrows are sync).
        let was_expired: Vec<bool> = {
            let idx = nslog.index.borrow();
            keys.iter().map(|k| idx.is_expired(k, now)).collect()
        };

        // Tombstone all keys concurrently — same pattern as mget's expired-key batch.
        let tombstone_results = join_all(keys.iter().map(|k| nslog.tombstone(k))).await;

        for key in keys {
            Self::with_cache_key(ns, key, |ck| self.cache.remove(ck));
        }

        let mut count = 0u64;
        for ((key, tomb_result), expired) in keys.iter().zip(tombstone_results).zip(was_expired) {
            // Mirror the previous Rocks-based semantics: an expired-but-not-yet-tombstoned
            // key counts as 0 (already semantically gone).
            if let Some(revision) = tomb_result?
                && !expired
            {
                count += 1;
                self.watchers.borrow_mut().notify(
                    ns,
                    key,
                    WatchEvent::Del {
                        key: Bytes::copy_from_slice(key),
                        revision,
                    },
                );
            }
        }
        Ok(count)
    }

    pub async fn exists(&self, ns: &str, keys: &[&[u8]]) -> Result<u64> {
        let nslog = self.ensure_ns(ns).await?;
        let now = now_ms();
        let idx = nslog.index.borrow();
        let mut count = 0u64;
        for &key in keys {
            if idx.get(key).is_some() && !idx.is_expired(key, now) {
                count += 1;
            }
        }
        Ok(count)
    }

    pub async fn expire(&self, ns: &str, key: &[u8], ttl: Duration) -> Result<bool> {
        let nslog = self.ensure_ns(ns).await?;
        let now = now_ms();
        let now_instant = Instant::now();
        let present_and_live = {
            let idx = nslog.index.borrow();
            idx.get(key).is_some() && !idx.is_expired(key, now)
        };
        if !present_and_live {
            // Drop the cached entry if it shows up expired.
            Self::with_cache_key(ns, key, |ck| self.cache.remove(ck));
            return Ok(false);
        }
        let new_ms = Self::validate_ttl(ttl, now)?;
        // Read value before evicting cache — needed for watch notification.
        let entry = self.get_inline(&nslog, ns, key, now, now_instant).await?;
        let revision = nslog.ttl_update(key, Some(new_ms)).await?;
        // L1 carries its own copy of expires_at_ms; refresh on next get.
        Self::with_cache_key(ns, key, |ck| self.cache.remove(ck));
        if let Some(e) = entry {
            self.watchers.borrow_mut().notify(
                ns,
                key,
                WatchEvent::Set {
                    key: Bytes::copy_from_slice(key),
                    value: e.value,
                    metadata: e.metadata,
                    expires_at_ms: Some(new_ms),
                    revision,
                },
            );
        }
        Ok(true)
    }

    pub async fn persist(&self, ns: &str, key: &[u8]) -> Result<bool> {
        let nslog = self.ensure_ns(ns).await?;
        let now = now_ms();
        let now_instant = Instant::now();
        let (present, has_ttl, expired) = {
            let idx = nslog.index.borrow();
            (
                idx.get(key).is_some(),
                idx.ttl(key).is_some(),
                idx.is_expired(key, now),
            )
        };
        if !present || !has_ttl || expired {
            return Ok(false);
        }
        // Read value before evicting cache — needed for watch notification.
        let entry = self.get_inline(&nslog, ns, key, now, now_instant).await?;
        let revision = nslog.ttl_update(key, None).await?;
        Self::with_cache_key(ns, key, |ck| self.cache.remove(ck));
        if let Some(e) = entry {
            self.watchers.borrow_mut().notify(
                ns,
                key,
                WatchEvent::Set {
                    key: Bytes::copy_from_slice(key),
                    value: e.value,
                    metadata: e.metadata,
                    expires_at_ms: None,
                    revision,
                },
            );
        }
        Ok(true)
    }

    pub async fn getex(&self, ns: &str, key: &[u8], op: Option<GetExOp>) -> Result<Option<Entry>> {
        if op.is_none() {
            return self.get(ns, key).await;
        }
        let now = now_ms();
        let now_instant = Instant::now();
        let nslog = self.ensure_ns(ns).await?;

        // Inline get — same nslog reference ensures the TTL update shares the
        // same ensure_ns context with no intervening yield between read and write.
        let found = if let Some((cv, ce, cm, revision)) =
            Self::with_cache_key(ns, key, |ck| self.cache.get(ck, now))
        {
            Some(Entry {
                value: cv,
                expires_at: Self::instant_from_ms(ce, now, now_instant),
                metadata: cm,
                revision,
            })
        } else {
            let lookup = {
                let idx = nslog.index.borrow();
                idx.get(key).map(|e| (*e, idx.ttl(key)))
            };
            match lookup {
                None => return Ok(None),
                Some((entry, expires_at_ms)) => {
                    if expires_at_ms.is_some_and(|ms| ms <= now) {
                        nslog.tombstone(key).await?;
                        return Ok(None);
                    }
                    let (val, meta_bytes) = nslog.read_value(entry).await?;
                    let metadata = Self::metadata_from_bytes(&meta_bytes)?;
                    self.cache.insert(
                        Self::cache_key(ns, key),
                        val.clone(),
                        expires_at_ms,
                        metadata.clone(),
                        meta_bytes.len(),
                        entry.tstamp_ms,
                    );
                    Some(Entry {
                        value: val,
                        expires_at: Self::instant_from_ms(expires_at_ms, now, now_instant),
                        metadata,
                        revision: entry.tstamp_ms,
                    })
                }
            }
        };

        let Some(found) = found else {
            return Ok(None);
        };

        // op.is_some() is guaranteed by the early return at the top of this function.
        match op.expect("op is Some; checked at function entry") {
            GetExOp::SetTtl(ttl) => {
                let new_ms = Self::validate_ttl(ttl, now)?;
                nslog.ttl_update(key, Some(new_ms)).await?;
                Self::with_cache_key(ns, key, |ck| self.cache.remove(ck));
            }
            GetExOp::Persist => {
                nslog.ttl_update(key, None).await?;
                Self::with_cache_key(ns, key, |ck| self.cache.remove(ck));
            }
        }

        Ok(Some(found))
    }

    pub async fn ttl(&self, ns: &str, key: &[u8]) -> Result<TtlResult> {
        Ok(match self.ttl_raw(ns, key).await? {
            TtlResult::Remaining(ms) => TtlResult::Remaining(ms / 1000),
            other => other,
        })
    }

    pub async fn pttl(&self, ns: &str, key: &[u8]) -> Result<TtlResult> {
        self.ttl_raw(ns, key).await
    }

    pub async fn getset(&self, ns: &str, key: &[u8], value: Bytes) -> Result<Option<Entry>> {
        let now = now_ms();
        let now_instant = Instant::now();
        let nslog = self.ensure_ns(ns).await?;
        let old = self.get_inline(&nslog, ns, key, now, now_instant).await?;
        // Inline set — same nslog, no second ensure_ns call.
        let key_bytes = Bytes::copy_from_slice(key);
        let revision = nslog.put_full(key_bytes.clone(), &value, &[], None).await?;
        self.upsert_cache(ns, key, value.clone(), None, None, 0, revision);
        self.watchers.borrow_mut().notify(
            ns,
            key,
            WatchEvent::Set {
                key: key_bytes,
                value,
                metadata: None,
                expires_at_ms: None,
                revision,
            },
        );
        Ok(old)
    }

    pub async fn getdel(&self, ns: &str, key: &[u8]) -> Result<Option<Entry>> {
        let now = now_ms();
        let now_instant = Instant::now();
        let nslog = self.ensure_ns(ns).await?;
        let old = self.get_inline(&nslog, ns, key, now, now_instant).await?;
        let tomb_rev = nslog.tombstone(key).await?;
        Self::with_cache_key(ns, key, |ck| self.cache.remove(ck));
        if let (Some(_), Some(revision)) = (&old, tomb_rev) {
            self.watchers.borrow_mut().notify(
                ns,
                key,
                WatchEvent::Del {
                    key: Bytes::copy_from_slice(key),
                    revision,
                },
            );
        }
        Ok(old)
    }

    pub async fn setnx(
        &self,
        ns: &str,
        key: &[u8],
        value: Bytes,
        opts: SetOptions,
    ) -> Result<bool> {
        self.set_conditional(ns, key, value, opts, false).await
    }

    pub async fn setxx(
        &self,
        ns: &str,
        key: &[u8],
        value: Bytes,
        opts: SetOptions,
    ) -> Result<bool> {
        self.set_conditional(ns, key, value, opts, true).await
    }

    /// Compare-and-swap: write only if the current revision matches `expected_rev`.
    /// Returns `Some(new_revision)` on success, `None` if the revision didn't match.
    pub async fn setrev(
        &self,
        ns: &str,
        key: &[u8],
        value: Bytes,
        opts: SetOptions,
        expected_rev: u64,
    ) -> Result<Option<u64>> {
        let nslog = self.ensure_ns(ns).await?;
        let now = now_ms();
        let expires_at_ms = opts.ttl.map(|d| Self::validate_ttl(d, now)).transpose()?;
        let meta_bytes: Vec<u8> = opts
            .metadata
            .as_ref()
            .map(|m| serde_json::to_vec(m.as_ref()))
            .transpose()?
            .unwrap_or_default();
        let key_bytes = Bytes::copy_from_slice(key);
        let revision = match nslog
            .put_full_cond(
                key_bytes.clone(),
                &value,
                &meta_bytes,
                expires_at_ms,
                WriteCondition::Revision(expected_rev),
                now,
            )
            .await?
        {
            Some(t) => t,
            None => return Ok(None),
        };
        self.upsert_cache(
            ns,
            key,
            value.clone(),
            expires_at_ms,
            opts.metadata.clone(),
            meta_bytes.len(),
            revision,
        );
        self.watchers.borrow_mut().notify(
            ns,
            key,
            WatchEvent::Set {
                key: key_bytes,
                value,
                metadata: opts.metadata,
                expires_at_ms,
                revision,
            },
        );
        Ok(Some(revision))
    }

    /// Compare-and-delete: delete only if the current revision matches `expected_rev`.
    /// Returns `Some(())` if deleted, `None` if revision mismatch, `Err` on I/O failure.
    pub async fn delrev(&self, ns: &str, key: &[u8], expected_rev: u64) -> Result<Option<()>> {
        let now = now_ms();
        let nslog = self.ensure_ns(ns).await?;
        let revision = match nslog.tombstone_cond(key, expected_rev, now).await? {
            Some(t) => t,
            None => return Ok(None),
        };
        Self::with_cache_key(ns, key, |ck| self.cache.remove(ck));
        self.watchers.borrow_mut().notify(
            ns,
            key,
            WatchEvent::Del {
                key: Bytes::copy_from_slice(key),
                revision,
            },
        );
        Ok(Some(()))
    }

    /// Atomically increment or decrement an integer counter stored at `key` by `delta`.
    ///
    /// Missing keys are treated as 0. Returns the new value.
    /// Returns `EngineError::InvalidInput` if the stored value is not a valid i64 decimal
    /// string, or if the operation would overflow.
    /// The key's existing TTL is preserved.
    pub async fn incr(&self, ns: &str, key: &[u8], delta: i64) -> Result<i64> {
        let nslog = self.ensure_ns(ns).await?;

        // Optimistic read-modify-write. `put_full_cond` now holds the key's write
        // stripe and checks BEFORE appending, so a lost race writes nothing (no
        // futile append) and simply returns `None` — the retry below re-reads and
        // tries again. No dedicated INCR lock is needed any more.
        for _ in 0..8u8 {
            let now = now_ms();

            // Read current value + TTL + revision for the CAS condition.
            let (current, expires_at_ms, cond) = if let Some((value, ttl_ms, _meta, rev)) =
                Self::with_cache_key(ns, key, |ck| self.cache.get(ck, now))
            {
                (Some(value), ttl_ms, WriteCondition::Revision(rev))
            } else {
                let (idx_entry, ttl_ms) = {
                    let idx = nslog.index.borrow();
                    match idx.get(key) {
                        None => (None, None),
                        Some(e) => {
                            let entry = *e;
                            (Some((entry, entry.tstamp_ms)), idx.ttl(key))
                        }
                    }
                };
                match idx_entry {
                    None => (None, None, WriteCondition::KeyAbsent),
                    Some((e, rev)) => {
                        if ttl_ms.is_some_and(|ms| ms <= now) {
                            nslog.tombstone(key).await?;
                            Self::with_cache_key(ns, key, |ck| self.cache.remove(ck));
                            (None, None, WriteCondition::KeyAbsent)
                        } else {
                            let (value, _) = nslog.read_value(e).await?;
                            (Some(value), ttl_ms, WriteCondition::Revision(rev))
                        }
                    }
                }
            };

            let current_int = match current {
                None => 0i64,
                Some(ref v) => std::str::from_utf8(v)
                    .ok()
                    .and_then(|s| s.parse::<i64>().ok())
                    .ok_or(EngineError::InvalidInput {
                        reason: "value is not an integer or out of range",
                    })?,
            };

            let new_val = current_int
                .checked_add(delta)
                .ok_or(EngineError::InvalidInput {
                    reason: "increment or decrement would overflow",
                })?;

            let new_bytes = Bytes::from(new_val.to_string());
            let key_bytes = Bytes::copy_from_slice(key);

            let revision = match nslog
                .put_full_cond(key_bytes.clone(), &new_bytes, &[], expires_at_ms, cond, now)
                .await?
            {
                Some(t) => t,
                // CAS lost to a concurrent write on this key; re-read and retry.
                None => continue,
            };

            self.upsert_cache(ns, key, new_bytes.clone(), expires_at_ms, None, 0, revision);
            self.watchers.borrow_mut().notify(
                ns,
                key,
                WatchEvent::Set {
                    key: key_bytes,
                    value: new_bytes,
                    metadata: None,
                    expires_at_ms,
                    revision,
                },
            );
            return Ok(new_val);
        }

        Err(EngineError::Conflict {
            reason: "incr: too many concurrent modifications on the same key",
        })
    }

    /// Subscribe to key or prefix mutations. Returns initial events (current state for
    /// since=0, catch-up log scan for since>0) plus a live receiver. Subscribe BEFORE
    /// scanning to avoid missing live events produced between scan and subscribe.
    ///
    /// Note: a write that arrives between the subscribe and the initial scan will appear
    /// in both `initial` and the live receiver. Callers should deduplicate by revision.
    pub async fn watch_subscribe(
        &self,
        ns: &str,
        filter: KeyFilter<'_>,
        since: u64,
    ) -> Result<(Vec<WatchEvent>, Receiver<WatchEvent>)> {
        let ns_b = Bytes::copy_from_slice(ns.as_bytes());

        let rx = match &filter {
            KeyFilter::Exact(k) => self
                .watchers
                .borrow_mut()
                .subscribe_key(ns_b, Bytes::copy_from_slice(k)),
            KeyFilter::Prefix(p) => self
                .watchers
                .borrow_mut()
                .subscribe_prefix(ns_b, Bytes::copy_from_slice(p)),
        };

        let nslog = self.ensure_ns(ns).await?;
        let initial = if since == 0 {
            nslog.current_entries(&filter, now_ms()).await?
        } else {
            nslog.scan_since(&filter, since).await?
        };

        Ok((initial, rx))
    }

    pub fn sweep_cache(&self) -> u64 {
        self.cache.sweep_expired(now_ms())
    }

    pub fn cache_len(&self) -> usize {
        self.cache.len()
    }

    pub fn cache_bytes_used(&self) -> usize {
        self.cache.bytes_used()
    }

    pub fn namespace_count(&self) -> usize {
        self.namespaces.borrow().len()
    }

    /// Total sealed (compactable) log segments across all open namespaces.
    pub fn sealed_segment_count(&self) -> usize {
        self.namespaces
            .borrow()
            .values()
            .map(|ns| ns.sealed_file_count())
            .sum()
    }

    /// Fsync any unsynced writes across all namespaces. Called by the per-shard
    /// 1-second timer to provide `appendfsync everysec` durability semantics.
    pub async fn sync_logs(&self) -> crate::error::Result<()> {
        let ns_list: Vec<Rc<NamespaceLog>> = self.namespaces.borrow().values().cloned().collect();
        let results = join_all(ns_list.iter().map(|ns| ns.sync())).await;
        for result in results {
            result?;
        }
        Ok(())
    }

    /// Write a footer to every active namespace file. Call on clean shutdown after
    /// `sync_logs` so the next startup loads each file via fast footer read instead
    /// of replaying records.
    ///
    /// Each namespace is frozen (new writes return `EngineError::Frozen`) and any
    /// in-flight writes are drained before the footer is built. Without this,
    /// writes whose `.await` overlaps with `write_footer().await` would be
    /// appended to the file but absent from the footer entries — and thus
    /// invisible to the successor process that reads only the footer.
    pub async fn seal_all_for_shutdown(&self) -> crate::error::Result<()> {
        let ns_list: Vec<Rc<NamespaceLog>> = self.namespaces.borrow().values().cloned().collect();
        // Freeze writes and wait for in-flight writes to complete on every
        // namespace in parallel.
        join_all(ns_list.iter().map(|ns| ns.freeze_and_drain())).await;
        let results = join_all(ns_list.iter().map(|ns| ns.seal_active_for_shutdown())).await;
        // Propagate the first error. Earlier behavior was to log-and-continue,
        // but the handoff seal path needs to surface failures so the protocol
        // can send `SealFailed` and keep O as the writer. The SIGTERM path's
        // caller already handles `Err` here gracefully.
        let mut first_err: Option<crate::error::EngineError> = None;
        for result in results {
            if let Err(e) = result {
                tracing::warn!(error = %e, "failed to seal namespace on shutdown");
                first_err.get_or_insert(e);
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Reopen a fresh active log segment for every namespace that was previously
    /// sealed via [`seal_all_for_shutdown`]. Used by the handoff abort path
    /// (`Drainable::resume_after_abort`) to put the store back into a
    /// writable state. Also clears the per-namespace freeze flag so writes
    /// can proceed again.
    pub async fn resume_after_abort(&self) -> crate::error::Result<()> {
        let ns_list: Vec<Rc<NamespaceLog>> = self.namespaces.borrow().values().cloned().collect();
        let results = join_all(ns_list.iter().map(|ns| ns.reopen_active_after_seal())).await;
        for result in results {
            if let Err(e) = result {
                tracing::warn!(error = %e, "failed to reopen namespace active log on resume");
                return Err(e);
            }
        }
        for ns in &ns_list {
            ns.unfreeze();
        }
        Ok(())
    }

    /// One revision per open namespace, the highest `tstamp_ms` known for each.
    /// Used by the handoff protocol to report per-shard high-water marks.
    pub fn last_revision_per_namespace(&self) -> Vec<u64> {
        self.namespaces
            .borrow()
            .values()
            .map(|ns| ns.last_revision())
            .collect()
    }

    /// Trigger reclaim on one namespace. Used by `BGREWRITEAOF`.
    pub async fn reclaim(&self, ns: &str) -> Result<()> {
        let nslog = self.ensure_ns(ns).await?;
        let report = nslog.reclaim().await?;
        info!(?report, ns, "reclaim complete");
        Ok(())
    }

    /// Run reclaim on any namespace that has more than `threshold` sealed files.
    /// Called by the background reclaim timer. `threshold == 0` disables auto-reclaim.
    pub async fn reclaim_if_needed(&self, threshold: usize) -> Result<ReclaimSummary> {
        if threshold == 0 {
            return Ok(ReclaimSummary::default());
        }
        let ns_list: Vec<Rc<NamespaceLog>> = self.namespaces.borrow().values().cloned().collect();
        let to_reclaim: Vec<Rc<NamespaceLog>> = ns_list
            .into_iter()
            .filter(|ns| ns.sealed_file_count() > threshold)
            .collect();
        let results = join_all(to_reclaim.iter().map(|ns| ns.reclaim())).await;
        let mut summary = ReclaimSummary::default();
        for (ns, result) in to_reclaim.iter().zip(results) {
            let report = result?;
            info!(?report, dir = %ns.dir.display(), "auto-reclaim complete");
            summary.namespaces_reclaimed += 1;
            summary.files_freed += report.dead_files_dropped as u64;
            summary.live_bytes = report.live_bytes;
        }
        Ok(summary)
    }

    /// SCAN with key-cursor semantics:
    ///   cursor `b"0"` = start from the beginning of the keyspace.
    ///   continuation cursor = `b"\x01"` + last yielded key bytes (exclusive lower bound).
    ///   `ScanPage::next_cursor == b"0"` signals scan complete.
    ///
    /// Keys present for the full duration of a scan appear exactly once.
    /// Keys inserted or deleted between pages are handled gracefully: a key-based
    /// cursor is stable regardless of concurrent map mutations.
    pub async fn scan(
        &self,
        ns: &str,
        cursor: &[u8],
        pattern: Option<&[u8]>,
        count: u64,
    ) -> Result<ScanPage> {
        let nslog = self.ensure_ns(ns).await?;
        let now = now_ms();

        let resume_after: Option<&[u8]> = if cursor == b"0" {
            None
        } else if let Some(key) = cursor.strip_prefix(b"\x01") {
            Some(key)
        } else {
            None // invalid cursor: restart
        };

        let pat = pattern;
        let (keys, last_key) = nslog
            .index
            .borrow()
            .scan(resume_after, count as usize, now, |k| {
                pat.is_none_or(|p| glob_match(p, k))
            });

        let next_cursor = match last_key {
            None => Bytes::from_static(b"0"),
            Some(k) => {
                let mut v = Vec::with_capacity(1 + k.len());
                v.push(0x01u8);
                v.extend_from_slice(&k);
                Bytes::from(v)
            }
        };
        Ok(ScanPage { next_cursor, keys })
    }

    /// Returns the number of live keys in the namespace. O(1) via the index's
    /// maintained live_count — may overcount by the number of logically-expired
    /// but not-yet-tombstoned keys, matching Redis DBSIZE semantics.
    pub async fn db_size(&self, ns: &str) -> Result<u64> {
        let nslog = self.ensure_ns(ns).await?;
        let count = nslog.index.borrow().live_len();
        Ok(count as u64)
    }

    pub async fn flush_db(&self, ns: &str) -> Result<()> {
        let nslog = self.ensure_ns(ns).await?;
        // Evict all L1 entries for this namespace in one prefix sweep, done
        // before flush so the index is still intact for prefix derivation.
        let mut prefix = Vec::with_capacity(ns.len() + 1);
        prefix.extend_from_slice(ns.as_bytes());
        prefix.push(b'\x00');
        self.cache.remove_by_prefix(&prefix);

        // Snapshot live keys before flush clears the index, so watch subscribers
        // receive a Del event for every key that just disappeared.
        let now = now_ms();
        let live_keys: Vec<Bytes> = {
            let idx = nslog.index.borrow();
            idx.iter()
                .filter_map(|(k, _)| {
                    if !idx.is_expired(k, now) {
                        Some(k.clone())
                    } else {
                        None
                    }
                })
                .collect()
        };

        // `flush()` returns a fresh tstamp that is strictly newer than every
        // prior write — see its doc for why this beats reading last_revision()
        // here under concurrent writers.
        let revision = nslog.flush().await?;
        // Drop all value-separated blobs for this namespace (FLUSHDB clears everything).
        nslog.values.clear();

        let mut w = self.watchers.borrow_mut();
        for key in live_keys {
            w.notify(
                ns,
                &key,
                WatchEvent::Del {
                    key: key.clone(),
                    revision,
                },
            );
        }
        Ok(())
    }
}

/// Glob matching for KEYS/SCAN patterns: `*` (any sequence), `?` (any single char),
/// `[abc]` (character class), `[^abc]`/`[!abc]` (negated class), `[a-z]` (range).
/// A `[` with no closing `]` is treated as a literal byte.
pub(crate) fn glob_match(pattern: &[u8], s: &[u8]) -> bool {
    // Fast path: locate the first glob metacharacter with a SIMD byte scan.
    // Everything before it is a literal prefix that must match s exactly.
    let first_meta = memchr3(b'*', b'?', b'[', pattern);

    let (mut pi, mut si) = match first_meta {
        None => {
            // No metacharacters: plain equality check (memcmp, SIMD on most targets).
            return pattern == s;
        }
        Some(0) => (0, 0),
        Some(n) => {
            // Literal prefix of length n — reject quickly on mismatch.
            if s.len() < n || s[..n] != pattern[..n] {
                return false;
            }
            // Common case: "prefix*" — prefix matched, * accepts any suffix.
            if n + 1 == pattern.len() && pattern[n] == b'*' {
                return true;
            }
            (n, n)
        }
    };

    let mut star_pi = usize::MAX;
    let mut star_si = si;

    while si < s.len() {
        if pi < pattern.len() && pattern[pi] == b'[' {
            let (ok, consumed) = match_bracket(&pattern[pi..], s[si]);
            if ok {
                pi += consumed;
                si += 1;
            } else if star_pi != usize::MAX {
                pi = star_pi + 1;
                star_si += 1;
                si = star_si;
            } else {
                return false;
            }
        } else if pi < pattern.len() && (pattern[pi] == b'?' || pattern[pi] == s[si]) {
            pi += 1;
            si += 1;
        } else if pi < pattern.len() && pattern[pi] == b'*' {
            star_pi = pi;
            star_si = si;
            pi += 1;
        } else if star_pi != usize::MAX {
            pi = star_pi + 1;
            star_si += 1;
            si = star_si;
        } else {
            return false;
        }
    }
    while pi < pattern.len() && pattern[pi] == b'*' {
        pi += 1;
    }
    pi == pattern.len()
}

/// Match one character against a `[...]` bracket expression at `bracket[0] == b'['`.
/// Returns `(matched, bytes_consumed_from_pattern)`.
/// If there is no closing `]`, treats `[` as a literal byte and returns `(ch == b'[', 1)`.
fn match_bracket(bracket: &[u8], ch: u8) -> (bool, usize) {
    debug_assert_eq!(bracket[0], b'[');
    let mut i = 1;
    let negate = i < bracket.len() && (bracket[i] == b'^' || bracket[i] == b'!');
    if negate {
        i += 1;
    }
    let class_start = i;
    let mut matched = false;

    loop {
        if i >= bracket.len() {
            // No closing ']' — treat '[' as a literal byte.
            return (ch == b'[', 1);
        }
        // ']' closes the class unless it's the very first character (allows `[]abc]`
        // to include a literal `]` in the class).
        if bracket[i] == b']' && i > class_start {
            i += 1;
            break;
        }
        // Range: e.g. `a-z`. The `-` must not be followed immediately by `]`.
        if i + 2 < bracket.len() && bracket[i + 1] == b'-' && bracket[i + 2] != b']' {
            if ch >= bracket[i] && ch <= bracket[i + 2] {
                matched = true;
            }
            i += 3;
        } else {
            if ch == bracket[i] {
                matched = true;
            }
            i += 1;
        }
    }

    (if negate { !matched } else { matched }, i)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::io::Write as _;
    use std::path::Path;
    use tempfile::TempDir;

    fn run<F: Future>(f: F) -> F::Output {
        monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_timer()
            .build()
            .expect("monoio runtime")
            .block_on(f)
    }

    async fn open_store(path: &Path) -> ShardStore {
        ShardStore::open(path, 4 << 20).await.unwrap()
    }

    async fn set(s: &ShardStore, key: &[u8], value: &[u8]) {
        s.set(
            "default",
            key,
            Bytes::copy_from_slice(value),
            SetOptions::default(),
        )
        .await
        .unwrap();
    }

    async fn set_ttl(s: &ShardStore, key: &[u8], value: &[u8], ttl: Duration) {
        s.set(
            "default",
            key,
            Bytes::copy_from_slice(value),
            SetOptions {
                ttl: Some(ttl),
                metadata: None,
                keep_ttl: false,
            },
        )
        .await
        .unwrap();
    }

    async fn get_value(s: &ShardStore, key: &[u8]) -> Option<Bytes> {
        s.get("default", key).await.unwrap().map(|e| e.value)
    }

    // ── Basic CRUD ────────────────────────────────────────────────────────────

    #[test]
    fn set_get_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();
        run(async move {
            let s = open_store(&path).await;
            set(&s, b"k", b"hello").await;
            assert_eq!(get_value(&s, b"k").await.unwrap().as_ref(), b"hello");
        });
    }

    #[test]
    fn get_missing_returns_none() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();
        run(async move {
            let s = open_store(&path).await;
            assert!(get_value(&s, b"nope").await.is_none());
        });
    }

    #[test]
    fn set_overwrites_value() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();
        run(async move {
            let s = open_store(&path).await;
            set(&s, b"k", b"first").await;
            set(&s, b"k", b"second").await;
            assert_eq!(get_value(&s, b"k").await.unwrap().as_ref(), b"second");
        });
    }

    #[test]
    fn del_existing_returns_count_1() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();
        run(async move {
            let s = open_store(&path).await;
            set(&s, b"del-me", b"v").await;
            assert_eq!(s.del("default", &[b"del-me".as_ref()]).await.unwrap(), 1);
            assert!(get_value(&s, b"del-me").await.is_none());
        });
    }

    #[test]
    fn del_missing_returns_count_0() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();
        run(async move {
            let s = open_store(&path).await;
            assert_eq!(s.del("default", &[b"ghost".as_ref()]).await.unwrap(), 0);
        });
    }

    #[test]
    fn exists_live_key() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();
        run(async move {
            let s = open_store(&path).await;
            set(&s, b"ex-k", b"v").await;
            assert_eq!(s.exists("default", &[b"ex-k".as_ref()]).await.unwrap(), 1);
        });
    }

    #[test]
    fn exists_missing_key() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();
        run(async move {
            let s = open_store(&path).await;
            assert_eq!(
                s.exists("default", &[b"no-such".as_ref()]).await.unwrap(),
                0
            );
        });
    }

    #[test]
    fn expired_key_is_invisible() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();
        run(async move {
            let s = open_store(&path).await;
            set_ttl(&s, b"exp", b"v", Duration::from_millis(50)).await;
            std::thread::sleep(Duration::from_millis(100));
            assert!(get_value(&s, b"exp").await.is_none());
        });
    }

    #[test]
    fn setnx_on_missing_inserts() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();
        run(async move {
            let s = open_store(&path).await;
            assert!(
                s.setnx(
                    "default",
                    b"snx",
                    Bytes::from_static(b"v"),
                    SetOptions::default()
                )
                .await
                .unwrap()
            );
            assert_eq!(get_value(&s, b"snx").await.unwrap().as_ref(), b"v");
        });
    }

    #[test]
    fn setnx_on_live_key_is_no_op() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();
        run(async move {
            let s = open_store(&path).await;
            set(&s, b"snx-dup", b"original").await;
            assert!(
                !s.setnx(
                    "default",
                    b"snx-dup",
                    Bytes::from_static(b"clobber"),
                    SetOptions::default()
                )
                .await
                .unwrap()
            );
            assert_eq!(
                get_value(&s, b"snx-dup").await.unwrap().as_ref(),
                b"original"
            );
        });
    }

    #[test]
    fn expire_on_live_key_returns_true() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();
        run(async move {
            let s = open_store(&path).await;
            set(&s, b"k", b"v").await;
            assert!(
                s.expire("default", b"k", Duration::from_secs(60))
                    .await
                    .unwrap()
            );
        });
    }

    #[test]
    fn expire_on_missing_key_returns_false() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();
        run(async move {
            let s = open_store(&path).await;
            assert!(
                !s.expire("default", b"miss", Duration::from_secs(60))
                    .await
                    .unwrap()
            );
        });
    }

    #[test]
    fn ttl_on_persistent_returns_no_expiry() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();
        run(async move {
            let s = open_store(&path).await;
            set(&s, b"k", b"v").await;
            assert_eq!(s.ttl("default", b"k").await.unwrap(), TtlResult::NoExpiry);
        });
    }

    #[test]
    fn persist_removes_ttl() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();
        run(async move {
            let s = open_store(&path).await;
            set_ttl(&s, b"k", b"v", Duration::from_secs(60)).await;
            assert!(s.persist("default", b"k").await.unwrap());
            assert_eq!(s.ttl("default", b"k").await.unwrap(), TtlResult::NoExpiry);
        });
    }

    #[test]
    fn mget_returns_values_in_order() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();
        run(async move {
            let s = open_store(&path).await;
            set(&s, b"a", b"va").await;
            set(&s, b"b", b"vb").await;
            let res = s
                .mget(
                    "default",
                    &[b"a".as_ref(), b"missing".as_ref(), b"b".as_ref()],
                )
                .await
                .unwrap();
            assert_eq!(res.len(), 3);
            assert_eq!(res[0].as_ref().unwrap().value.as_ref(), b"va");
            assert!(res[1].is_none());
            assert_eq!(res[2].as_ref().unwrap().value.as_ref(), b"vb");
        });
    }

    #[test]
    fn mset_then_mget() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();
        run(async move {
            let s = open_store(&path).await;
            s.mset(
                "default",
                &[
                    (Bytes::from_static(b"k1"), Bytes::from_static(b"v1")),
                    (Bytes::from_static(b"k2"), Bytes::from_static(b"v2")),
                ],
            )
            .await
            .unwrap();
            let res = s
                .mget("default", &[b"k1".as_ref(), b"k2".as_ref()])
                .await
                .unwrap();
            assert_eq!(res[0].as_ref().unwrap().value.as_ref(), b"v1");
            assert_eq!(res[1].as_ref().unwrap().value.as_ref(), b"v2");
        });
    }

    #[test]
    fn flush_db_clears_keys() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();
        run(async move {
            let s = open_store(&path).await;
            set(&s, b"f1", b"v").await;
            set(&s, b"f2", b"v").await;
            s.flush_db("default").await.unwrap();
            let page = s.scan("default", b"0", None, 100).await.unwrap();
            assert!(page.keys.is_empty());
        });
    }

    #[test]
    fn namespace_isolation() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();
        run(async move {
            let s = open_store(&path).await;
            set(&s, b"k", b"in-default").await;
            s.set(
                "db3",
                b"k",
                Bytes::from_static(b"in-db3"),
                SetOptions::default(),
            )
            .await
            .unwrap();
            assert_eq!(get_value(&s, b"k").await.unwrap().as_ref(), b"in-default");
            assert_eq!(
                s.get("db3", b"k").await.unwrap().unwrap().value.as_ref(),
                b"in-db3"
            );
        });
    }

    #[test]
    fn scan_returns_all_keys() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();
        run(async move {
            let s = open_store(&path).await;
            for k in [b"sa".as_ref(), b"sb", b"sc"] {
                set(&s, k, b"v").await;
            }
            let mut all: Vec<Bytes> = Vec::new();
            let mut cursor = Bytes::from_static(b"0");
            loop {
                let page = s.scan("default", &cursor, None, 4).await.unwrap();
                all.extend(page.keys);
                cursor = page.next_cursor.clone();
                if cursor.as_ref() == b"0" {
                    break;
                }
            }
            let strs: Vec<&[u8]> = all.iter().map(|k| k.as_ref()).collect();
            assert!(strs.contains(&b"sa".as_ref()));
            assert!(strs.contains(&b"sb".as_ref()));
            assert!(strs.contains(&b"sc".as_ref()));
        });
    }

    #[test]
    fn scan_pattern_filters() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();
        run(async move {
            let s = open_store(&path).await;
            set(&s, b"user:1", b"v").await;
            set(&s, b"user:2", b"v").await;
            set(&s, b"session:1", b"v").await;
            let mut all: Vec<Bytes> = Vec::new();
            let mut cursor = Bytes::from_static(b"0");
            loop {
                let page = s
                    .scan("default", &cursor, Some(b"user:*".as_ref()), 100)
                    .await
                    .unwrap();
                all.extend(page.keys);
                cursor = page.next_cursor.clone();
                if cursor.as_ref() == b"0" {
                    break;
                }
            }
            assert_eq!(all.len(), 2);
        });
    }

    // ── Recovery / restart ────────────────────────────────────────────────────

    #[test]
    fn recovery_loads_persistent_keys() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();
        let path1 = path.clone();
        run(async move {
            let s = open_store(&path1).await;
            set(&s, b"k", b"v").await;
        });
        run(async move {
            let s = open_store(&path).await;
            let got = s.get("default", b"k").await.unwrap();
            assert_eq!(got.unwrap().value.as_ref(), b"v");
        });
    }

    #[test]
    fn recovery_drops_tombstoned_keys() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();
        let path1 = path.clone();
        run(async move {
            let s = open_store(&path1).await;
            set(&s, b"k", b"v").await;
            s.del("default", &[b"k".as_ref()]).await.unwrap();
        });
        run(async move {
            let s = open_store(&path).await;
            assert!(s.get("default", b"k").await.unwrap().is_none());
        });
    }

    #[test]
    fn recovery_preserves_ttl() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();
        let path1 = path.clone();
        run(async move {
            let s = open_store(&path1).await;
            s.set(
                "default",
                b"k",
                Bytes::from_static(b"v"),
                SetOptions {
                    ttl: Some(Duration::from_secs(3600)),
                    metadata: None,
                    keep_ttl: false,
                },
            )
            .await
            .unwrap();
        });
        run(async move {
            let s = open_store(&path).await;
            let res = s.ttl("default", b"k").await.unwrap();
            assert!(matches!(res, TtlResult::Remaining(secs) if secs > 0));
        });
    }

    #[test]
    fn ttl_update_does_not_rewrite_value() {
        // EXPIRE on a 1 MiB value should append a tiny TTL-update record, not a full rewrite.
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();
        run(async move {
            let s = open_store(&path).await;
            let big = Bytes::from(vec![b'x'; 1_000_000]);
            s.set("default", b"big", big, SetOptions::default())
                .await
                .unwrap();
            let pre = active_size(&s, "default").await;
            s.expire("default", b"big", Duration::from_secs(60))
                .await
                .unwrap();
            let post = active_size(&s, "default").await;
            let delta = post - pre;
            assert!(
                delta < 100,
                "EXPIRE on a 1 MiB value should append <100 bytes (TTL-update record), got {delta}"
            );
        });
    }

    async fn active_size(s: &ShardStore, ns: &str) -> u64 {
        let nslog = s.get_ns(ns).unwrap();
        let active = nslog.active.borrow().clone();
        active.size().await.unwrap()
    }

    #[test]
    fn flushdb_unlinks_data_files() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();
        let dir = path.join("default");
        run(async move {
            let s = open_store(&path).await;
            set(&s, b"k", b"v").await;
            let pre_files: Vec<_> = std::fs::read_dir(&dir).unwrap().collect();
            let pre_inode = std::os::unix::fs::MetadataExt::ino(
                &std::fs::metadata(pre_files[0].as_ref().unwrap().path()).unwrap(),
            );
            s.flush_db("default").await.unwrap();
            let post_files: Vec<_> = std::fs::read_dir(&dir).unwrap().collect();
            assert_eq!(post_files.len(), 1);
            let post_inode = std::os::unix::fs::MetadataExt::ino(
                &std::fs::metadata(post_files[0].as_ref().unwrap().path()).unwrap(),
            );
            assert_ne!(
                pre_inode, post_inode,
                "FLUSHDB must unlink + recreate (different inode), not truncate in place"
            );
        });
    }

    // ── watch / watch_subscribe ───────────────────────────────────────────────

    #[test]
    fn watch_subscribe_delivers_set_event() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();
        run(async move {
            let s = open_store(&path).await;
            let (initial, mut rx) = s
                .watch_subscribe("default", KeyFilter::Exact(b"wk"), 0)
                .await
                .unwrap();
            assert!(initial.is_empty(), "key does not exist yet");

            set(&s, b"wk", b"v1").await;

            let event = rx.try_recv().unwrap();
            match event {
                WatchEvent::Set {
                    key,
                    value,
                    revision,
                    ..
                } => {
                    assert_eq!(key.as_ref(), b"wk");
                    assert_eq!(value.as_ref(), b"v1");
                    assert!(revision > 0);
                }
                other => panic!("unexpected event: {other:?}"),
            }
        });
    }

    #[test]
    fn watch_subscribe_delivers_del_event() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();
        run(async move {
            let s = open_store(&path).await;
            set(&s, b"wdel", b"v").await;
            let (_, mut rx) = s
                .watch_subscribe("default", KeyFilter::Exact(b"wdel"), 0)
                .await
                .unwrap();

            s.del("default", &[b"wdel".as_ref()]).await.unwrap();

            let event = rx.try_recv().unwrap();
            assert!(matches!(event, WatchEvent::Del { .. }));
        });
    }

    #[test]
    fn watch_subscribe_initial_returns_current_value() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();
        run(async move {
            let s = open_store(&path).await;
            set(&s, b"wexist", b"hello").await;

            let (initial, _rx) = s
                .watch_subscribe("default", KeyFilter::Exact(b"wexist"), 0)
                .await
                .unwrap();
            assert_eq!(initial.len(), 1);
            match &initial[0] {
                WatchEvent::Set { key, value, .. } => {
                    assert_eq!(key.as_ref(), b"wexist");
                    assert_eq!(value.as_ref(), b"hello");
                }
                other => panic!("unexpected event: {other:?}"),
            }
        });
    }

    #[test]
    fn watch_subscribe_prefix_receives_matching_keys() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();
        run(async move {
            use futures_channel::mpsc::TryRecvError;
            let s = open_store(&path).await;
            let (_, mut rx) = s
                .watch_subscribe("default", KeyFilter::Prefix(b"cfg/"), 0)
                .await
                .unwrap();

            set(&s, b"cfg/a", b"1").await;
            set(&s, b"other/b", b"2").await; // should not arrive
            set(&s, b"cfg/b", b"3").await;

            let e1 = rx.try_recv().unwrap();
            let e2 = rx.try_recv().unwrap();
            // third try should be empty (other/b filtered out)
            assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Empty);

            let keys: Vec<_> = [e1, e2]
                .iter()
                .map(|e| match e {
                    WatchEvent::Set { key, .. } => key.clone(),
                    _ => panic!("expected Set"),
                })
                .collect();
            assert!(keys.iter().any(|k| k.as_ref() == b"cfg/a"));
            assert!(keys.iter().any(|k| k.as_ref() == b"cfg/b"));
        });
    }

    #[test]
    fn watch_subscribe_scan_since_replays_missed_writes() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();
        run(async move {
            let s = open_store(&path).await;
            // Write before subscribing so we can replay via since.
            set(&s, b"repl", b"v1").await;
            let revision_after_v1 = s.ensure_ns("default").await.unwrap().last_revision();
            set(&s, b"repl", b"v2").await;
            s.del("default", &[b"repl".as_ref()]).await.unwrap();

            // Subscribe with since = revision_after_v1 — should replay v2 set + del.
            let (initial, _rx) = s
                .watch_subscribe("default", KeyFilter::Exact(b"repl"), revision_after_v1)
                .await
                .unwrap();

            assert_eq!(initial.len(), 2, "expected set(v2) + del");
            assert!(matches!(initial[0], WatchEvent::Set { .. }));
            assert!(matches!(initial[1], WatchEvent::Del { .. }));
        });
    }

    #[test]
    fn watch_dead_sender_cleaned_up() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();
        run(async move {
            let s = open_store(&path).await;
            let (_initial, rx) = s
                .watch_subscribe("default", KeyFilter::Exact(b"cleanup"), 0)
                .await
                .unwrap();
            // Drop the receiver — sender is now dead.
            drop(rx);
            // Notify should not panic; dead sender gets pruned.
            set(&s, b"cleanup", b"v").await;
            // A second set also works (prune already happened).
            set(&s, b"cleanup", b"v2").await;
        });
    }

    #[test]
    fn getset_delivers_watch_event() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();
        run(async move {
            let s = open_store(&path).await;
            let (_initial, mut rx) = s
                .watch_subscribe("default", KeyFilter::Exact(b"gsk"), 0)
                .await
                .unwrap();

            s.getset("default", b"gsk", Bytes::from_static(b"new"))
                .await
                .unwrap();

            let event = rx.try_recv().unwrap();
            match event {
                WatchEvent::Set { key, value, .. } => {
                    assert_eq!(key.as_ref(), b"gsk");
                    assert_eq!(value.as_ref(), b"new");
                }
                other => panic!("expected Set, got {other:?}"),
            }
        });
    }

    #[test]
    fn getdel_delivers_watch_event() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();
        run(async move {
            let s = open_store(&path).await;
            set(&s, b"gdk", b"v").await;
            let (_initial, mut rx) = s
                .watch_subscribe("default", KeyFilter::Exact(b"gdk"), 0)
                .await
                .unwrap();

            s.getdel("default", b"gdk").await.unwrap();

            let event = rx.try_recv().unwrap();
            assert!(
                matches!(event, WatchEvent::Del { .. }),
                "expected Del, got {event:?}"
            );
        });
    }

    // ── glob_match ────────────────────────────────────────────────────────────

    // --- setrev / delrev ---

    #[test]
    fn setrev_correct_revision_succeeds() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();
        run(async move {
            let s = open_store(&path).await;
            set(&s, b"k", b"v1").await;
            let rev = s.get("default", b"k").await.unwrap().unwrap().revision;
            let new_rev = s
                .setrev(
                    "default",
                    b"k",
                    Bytes::from("v2"),
                    SetOptions::default(),
                    rev,
                )
                .await
                .unwrap();
            assert!(new_rev.is_some());
            assert_eq!(get_value(&s, b"k").await.unwrap().as_ref(), b"v2");
        });
    }

    #[test]
    fn setrev_wrong_revision_returns_none() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();
        run(async move {
            let s = open_store(&path).await;
            set(&s, b"k", b"v").await;
            let result = s
                .setrev(
                    "default",
                    b"k",
                    Bytes::from("new"),
                    SetOptions::default(),
                    0,
                )
                .await
                .unwrap();
            assert!(result.is_none(), "wrong rev must return None");
            assert_eq!(get_value(&s, b"k").await.unwrap().as_ref(), b"v");
        });
    }

    #[test]
    fn setrev_missing_key_returns_none() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();
        run(async move {
            let s = open_store(&path).await;
            let result = s
                .setrev(
                    "default",
                    b"missing",
                    Bytes::from("v"),
                    SetOptions::default(),
                    1,
                )
                .await
                .unwrap();
            assert!(result.is_none());
        });
    }

    #[test]
    fn delrev_correct_revision_deletes() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();
        run(async move {
            let s = open_store(&path).await;
            set(&s, b"k", b"v").await;
            let rev = s.get("default", b"k").await.unwrap().unwrap().revision;
            let result = s.delrev("default", b"k", rev).await.unwrap();
            assert!(result.is_some());
            assert!(get_value(&s, b"k").await.is_none());
        });
    }

    #[test]
    fn delrev_wrong_revision_preserves_key() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();
        run(async move {
            let s = open_store(&path).await;
            set(&s, b"k", b"v").await;
            let result = s.delrev("default", b"k", 0).await.unwrap();
            assert!(result.is_none());
            assert!(get_value(&s, b"k").await.is_some());
        });
    }

    /// Concurrent CAS on the same key: among N writers contending against the
    /// same starting revision, exactly one wins. The revision the winner
    /// returns must match the revision a subsequent GET reports, and a
    /// follow-up setrev with that revision must succeed.
    ///
    /// Regression test for a cache/revision skew bug where successful
    /// conditional writes were upserting the cache with `last_revision()`
    /// (the GLOBAL tstamp counter) instead of THIS write's tstamp — failed
    /// concurrent writers still bumped `next_tstamp` before their post-check
    /// failed, so the cache ended up holding a tstamp higher than the one
    /// actually committed to the index. Subsequent CAS using the cached
    /// revision then failed pre-check ("revision mismatch") forever.
    #[test]
    fn concurrent_setrev_one_winner_consistent_revision() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();
        run(async move {
            let s = open_store(&path).await;
            set(&s, b"k", b"v0").await;
            let start_rev = s.get("default", b"k").await.unwrap().unwrap().revision;

            // Fire N concurrent setrev calls all expecting `start_rev`.
            const N: usize = 8;
            let futures = (0..N).map(|i| {
                let s = &s;
                async move {
                    s.setrev(
                        "default",
                        b"k",
                        Bytes::from(format!("v{}", i + 1)),
                        SetOptions::default(),
                        start_rev,
                    )
                    .await
                    .unwrap()
                }
            });
            let results = futures_util::future::join_all(futures).await;

            // Exactly one must report Some(new_rev); the rest must be None.
            let winners: Vec<u64> = results.iter().filter_map(|r| *r).collect();
            assert_eq!(
                winners.len(),
                1,
                "exactly one concurrent setrev must win, got {} winners",
                winners.len()
            );
            let winner_rev = winners[0];

            // The cache (and index) must agree with the winner's reported rev.
            // If they don't, this is the bug we're guarding against: cached
            // rev > committed rev, breaking every subsequent CAS.
            let after = s.get("default", b"k").await.unwrap().unwrap();
            assert_eq!(
                after.revision, winner_rev,
                "GET after concurrent setrev returned rev={} but the winner said it wrote rev={}",
                after.revision, winner_rev
            );

            // And a follow-up CAS using that revision must succeed — this is
            // the operation the cache-skew bug would have made impossible.
            let follow_up = s
                .setrev(
                    "default",
                    b"k",
                    Bytes::from("vN"),
                    SetOptions::default(),
                    winner_rev,
                )
                .await
                .unwrap();
            assert!(
                follow_up.is_some(),
                "setrev with the revision GET just reported must succeed"
            );
        });
    }

    // --- namespace cap ---

    #[test]
    fn namespace_cap_at_1024() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();
        run(async move {
            let s = open_store(&path).await;
            // "default" is created by open_store; add 1023 more to fill the cap.
            for i in 0..1023usize {
                let ns = format!("ns{i}");
                s.set(&ns, b"k", Bytes::from("v"), SetOptions::default())
                    .await
                    .unwrap();
            }
            let err = s
                .set("ns_overflow", b"k", Bytes::from("v"), SetOptions::default())
                .await
                .unwrap_err();
            assert!(
                matches!(err, EngineError::CapacityExceeded { .. }),
                "expected CapacityExceeded, got {err:?}"
            );
        });
    }

    // --- FLUSHDB + restart ---

    #[test]
    fn flushdb_then_restart_yields_empty_namespace() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();
        run(async move {
            {
                let s = open_store(&path).await;
                set(&s, b"key", b"value").await;
                s.flush_db("default").await.unwrap();
            }
            let s2 = open_store(&path).await;
            assert!(
                get_value(&s2, b"key").await.is_none(),
                "flushed data must not survive restart"
            );
        });
    }

    // --- dirty shutdown / CRC truncation ---

    #[test]
    fn dirty_shutdown_truncates_at_last_good_crc() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();
        run(async move {
            let active_path = {
                let s = open_store(&path).await;
                set(&s, b"survive", b"yes").await;
                // Drop without seal — simulates crash (no footer written).
                let nslog = s.get_ns("default").unwrap();
                nslog.active.borrow().path.clone()
            };
            // Append garbage shorter than HEADER_LEN so replay breaks cleanly.
            let garbage = vec![0xdeu8; 20];
            std::fs::OpenOptions::new()
                .append(true)
                .open(&active_path)
                .unwrap()
                .write_all(&garbage)
                .unwrap();

            // Reopen — recover must truncate the garbage and preserve the good record.
            let s2 = open_store(&path).await;
            assert_eq!(
                get_value(&s2, b"survive").await.unwrap().as_ref(),
                b"yes",
                "clean record before truncation point must survive recovery"
            );
        });
    }

    #[test]
    fn glob_basics() {
        assert!(glob_match(b"", b""));
        assert!(!glob_match(b"", b"a"));
        assert!(glob_match(b"*", b""));
        assert!(glob_match(b"*", b"abc"));
        assert!(glob_match(b"a*b", b"axxb"));
        assert!(glob_match(b"a?c", b"abc"));
        assert!(!glob_match(b"a?c", b"ac"));
    }

    #[test]
    fn glob_bracket_class() {
        assert!(glob_match(b"[abc]", b"a"));
        assert!(glob_match(b"[abc]", b"b"));
        assert!(glob_match(b"[abc]", b"c"));
        assert!(!glob_match(b"[abc]", b"d"));
        // negated class
        assert!(!glob_match(b"[^abc]", b"a"));
        assert!(glob_match(b"[^abc]", b"d"));
        assert!(!glob_match(b"[!abc]", b"b"));
        assert!(glob_match(b"[!abc]", b"z"));
        // range
        assert!(glob_match(b"[a-z]", b"m"));
        assert!(!glob_match(b"[a-z]", b"A"));
        assert!(glob_match(b"[0-9]", b"5"));
        assert!(!glob_match(b"[0-9]", b"x"));
        // combined with wildcards — Redis-style pattern
        assert!(glob_match(b"user:[12]*", b"user:1abc"));
        assert!(glob_match(b"user:[12]*", b"user:2abc"));
        assert!(!glob_match(b"user:[12]*", b"user:3abc"));
        // unclosed '[' treated as literal
        assert!(glob_match(b"[abc", b"[abc"));
        assert!(!glob_match(b"[abc", b"a"));
    }

    // ── Concurrent INCR ───────────────────────────────────────────────────────

    #[test]
    fn concurrent_incr_under_contention_produces_correct_sum() {
        // Exercises the CAS retry loop in `incr`. The real server spawns each
        // client connection on its own monoio task, so the contenders here are
        // independent `monoio::spawn`-ed tasks (not sub-futures of a single
        // `join_all`, which polls round-robin within one task and gives the
        // first-polled sub-future a deterministic scheduling advantage that
        // doesn't reflect production behavior). The final counter must equal
        // CONTENDERS × PER_CONTENDER regardless of CAS interleaving.
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();
        run(async move {
            let s = Rc::new(open_store(&path).await);
            const CONTENDERS: usize = 4;
            const PER_CONTENDER: i64 = 25;
            let handles: Vec<_> = (0..CONTENDERS)
                .map(|_| {
                    let s = s.clone();
                    monoio::spawn(async move {
                        for _ in 0..PER_CONTENDER {
                            s.incr("default", b"ctr", 1).await.unwrap();
                        }
                    })
                })
                .collect();
            for h in handles {
                h.await;
            }
            let final_val: i64 = std::str::from_utf8(&get_value(&s, b"ctr").await.unwrap())
                .unwrap()
                .parse()
                .unwrap();
            assert_eq!(final_val, CONTENDERS as i64 * PER_CONTENDER);
        });
    }
}

#[cfg(test)]
mod namespace_cap_tests {
    use super::*;
    use crate::error::EngineError;
    use crate::types::SetOptions;
    use bytes::Bytes;
    use std::future::Future;
    use std::rc::Rc;
    use tempfile::TempDir;

    fn run<F: Future>(f: F) -> F::Output {
        monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_timer()
            .build()
            .expect("monoio runtime")
            .block_on(f)
    }

    /// Concurrent opens of distinct new namespaces cannot overshoot the cap.
    /// With one slot free, two tasks both pass the synchronous pre-await gate
    /// (`len == MAX-1`), both open concurrently, then resume: the first inserts
    /// (filling the slot) and the second's post-await re-check must reject it.
    /// Without the cap re-check after the await, both insert → `MAX+1`: teeth.
    #[test]
    fn concurrent_boundary_opens_do_not_overshoot_cap() {
        run(async {
            let dir = TempDir::new().unwrap();
            let s = Rc::new(ShardStore::open(dir.path(), 4 << 20).await.unwrap());

            // Fill to exactly one slot below the cap.
            let mut i = 0usize;
            while s.namespace_count() < MAX_NAMESPACES - 1 {
                s.set(
                    &format!("db{i}"),
                    b"k",
                    Bytes::from_static(b"v"),
                    SetOptions::default(),
                )
                .await
                .unwrap();
                i += 1;
            }
            assert_eq!(s.namespace_count(), MAX_NAMESPACES - 1);

            // Two concurrent opens of distinct new namespaces racing for the last slot.
            let (s1, s2) = (s.clone(), s.clone());
            let t1 = monoio::spawn(async move {
                s1.set(
                    "race_a",
                    b"k",
                    Bytes::from_static(b"v"),
                    SetOptions::default(),
                )
                .await
            });
            let t2 = monoio::spawn(async move {
                s2.set(
                    "race_b",
                    b"k",
                    Bytes::from_static(b"v"),
                    SetOptions::default(),
                )
                .await
            });
            let (r1, r2) = (t1.await, t2.await);

            assert_eq!(
                s.namespace_count(),
                MAX_NAMESPACES,
                "cap must hold exactly under concurrent boundary opens (no overshoot)"
            );
            let wins = [r1.is_ok(), r2.is_ok()].into_iter().filter(|b| *b).count();
            assert_eq!(wins, 1, "exactly one concurrent open wins the last slot");
            let rejected = [&r1, &r2]
                .into_iter()
                .filter(|r| matches!(r, Err(EngineError::CapacityExceeded { .. })))
                .count();
            assert_eq!(rejected, 1, "the loser must get a clean CapacityExceeded");
        });
    }

    /// The per-shard namespace cap degrades gracefully: opening distinct
    /// namespaces succeeds up to `MAX_NAMESPACES`, the next one is rejected with a
    /// clean `CapacityExceeded` (no panic, the map does not grow), and existing
    /// namespaces — including `default` — keep serving reads and writes at the cap.
    /// This is the bound that keeps NamespaceLog + file-descriptor growth finite.
    #[test]
    fn namespace_cap_is_enforced_and_existing_namespaces_keep_working() {
        run(async {
            let dir = TempDir::new().unwrap();
            let s = ShardStore::open(dir.path(), 4 << 20).await.unwrap();

            // "default" pre-exists; open distinct namespaces until the map is full.
            let mut i = 0usize;
            while s.namespace_count() < MAX_NAMESPACES {
                let ns = format!("db{i}");
                s.set(&ns, b"k", Bytes::from_static(b"v"), SetOptions::default())
                    .await
                    .unwrap();
                i += 1;
            }
            assert_eq!(s.namespace_count(), MAX_NAMESPACES);

            // One namespace over the cap: clean rejection, map unchanged.
            let over = s
                .set(
                    &format!("db{i}"),
                    b"k",
                    Bytes::from_static(b"v"),
                    SetOptions::default(),
                )
                .await;
            assert!(
                matches!(over, Err(EngineError::CapacityExceeded { .. })),
                "expected CapacityExceeded at the cap, got {over:?}"
            );
            assert_eq!(
                s.namespace_count(),
                MAX_NAMESPACES,
                "rejected open must not grow the map"
            );

            // Existing namespaces are unaffected by the cap.
            s.set(
                "db0",
                b"k2",
                Bytes::from_static(b"v2"),
                SetOptions::default(),
            )
            .await
            .unwrap();
            assert!(
                s.get("db0", b"k2").await.unwrap().is_some(),
                "existing ns still writable at cap"
            );
            s.set(
                "default",
                b"dk",
                Bytes::from_static(b"dv"),
                SetOptions::default(),
            )
            .await
            .unwrap();
            assert!(
                s.get("default", b"dk").await.unwrap().is_some(),
                "default ns still works at cap"
            );
        });
    }
}

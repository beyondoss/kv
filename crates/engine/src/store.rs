use std::cell::RefCell;
use std::path::Path;
use std::rc::Rc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use rustc_hash::FxHashMap;
use tracing::warn;

use crate::cache::MemCache;
use crate::error::{EngineError, Result};
use crate::log::config::LogConfig;
use crate::log::index::IndexEntry;
use crate::log::{now_ms, NamespaceLog};
use crate::types::{Entry, ScanPage, SetOptions, TtlResult};

pub const DEFAULT_NS: &str = "default";

/// Map database index to a namespace name: 0 → "default", n → "db{n}".
pub fn ns_name(db: u64) -> String {
    if db == 0 { DEFAULT_NS.to_string() } else { format!("db{db}") }
}

fn is_valid_ns_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
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
}

impl ShardStore {
    pub async fn open(data_dir: &Path, memory_bytes: usize) -> Result<Self> {
        std::fs::create_dir_all(data_dir)?;
        let config = LogConfig::default();
        let mut namespaces: FxHashMap<String, Rc<NamespaceLog>> = FxHashMap::default();

        // Open all existing namespace subdirectories found on disk.
        if let Ok(entries) = std::fs::read_dir(data_dir) {
            for entry in entries.flatten() {
                if entry.file_type().map_or(false, |t| t.is_dir()) {
                    let name = entry.file_name().to_string_lossy().into_owned();
                    if is_valid_ns_name(&name) {
                        let nslog = NamespaceLog::open(entry.path(), config).await?;
                        namespaces.insert(name, Rc::new(nslog));
                    }
                }
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
        })
    }

    /// Open the namespace if not already open, then return a cloned handle.
    async fn ensure_ns(&self, ns: &str) -> Result<Rc<NamespaceLog>> {
        if !is_valid_ns_name(ns) {
            return Err(EngineError::InvalidNamespace { name: ns.to_owned() });
        }
        if let Some(existing) = self.namespaces.borrow().get(ns).cloned() {
            return Ok(existing);
        }
        let dir = self.data_dir.join(ns);
        let nslog = Rc::new(NamespaceLog::open(dir, self.config).await?);
        // Re-check after the await — another spawned task may have beaten us.
        Ok(self.namespaces.borrow_mut().entry(ns.to_string()).or_insert(nslog).clone())
    }

    #[allow(dead_code)]
    pub(crate) fn get_ns(&self, ns: &str) -> Result<Rc<NamespaceLog>> {
        self.namespaces.borrow().get(ns).cloned()
            .ok_or_else(|| EngineError::InvalidNamespace { name: ns.to_owned() })
    }

    fn cache_key(ns: &str, key: &[u8]) -> Bytes {
        let mut ck = Vec::with_capacity(ns.len() + 1 + key.len());
        ck.extend_from_slice(ns.as_bytes());
        ck.push(b'\x00');
        ck.extend_from_slice(key);
        Bytes::from(ck)
    }

    fn instant_from_ms(expires_at_ms: Option<u64>, now: u64) -> Option<Instant> {
        expires_at_ms.map(|ms| Instant::now() + Duration::from_millis(ms.saturating_sub(now)))
    }

    fn metadata_from_bytes(meta: &[u8]) -> Option<serde_json::Value> {
        if meta.is_empty() {
            return None;
        }
        serde_json::from_slice(meta)
            .map_err(|e| warn!(error = %e, "metadata decode failed"))
            .ok()
    }

    pub async fn get(&self, ns: &str, key: &[u8]) -> Result<Option<Entry>> {
        let now = now_ms();
        if let Some((value, expires_at_ms, metadata)) = self.cache.get(&Self::cache_key(ns, key), now) {
            return Ok(Some(Entry {
                value,
                expires_at: Self::instant_from_ms(expires_at_ms, now),
                metadata,
            }));
        }
        let nslog = self.ensure_ns(ns).await?;

        let (entry, expires_at_ms) = match nslog.index.borrow().get(key) {
            None => return Ok(None),
            Some(e) => (*e, nslog.index.borrow().ttl(key)),
        };
        if expires_at_ms.map_or(false, |ms| ms <= now) {
            // Lazy delete on read.
            nslog.tombstone(key).await?;
            return Ok(None);
        }

        let (value, meta_bytes) = nslog.read_value(entry).await?;
        let metadata = Self::metadata_from_bytes(&meta_bytes);
        self.cache.insert(
            Self::cache_key(ns, key),
            value.clone(),
            expires_at_ms,
            metadata.clone(),
        );
        Ok(Some(Entry {
            value,
            expires_at: Self::instant_from_ms(expires_at_ms, now),
            metadata,
        }))
    }

    /// Bulk get. Cold reads are batched through io_uring via `join_all` so a
    /// 100-key MGET dispatches all the disk reads concurrently rather than
    /// serially awaiting each one.
    pub async fn mget(&self, ns: &str, keys: &[&[u8]]) -> Result<Vec<Option<Entry>>> {
        let now = now_ms();
        let nslog = self.ensure_ns(ns).await?;

        let mut results: Vec<Option<Entry>> = vec![None; keys.len()];
        let mut misses: Vec<(usize, IndexEntry)> = Vec::new();
        let mut miss_ttls: Vec<Option<u64>> = Vec::new();

        for (i, key) in keys.iter().enumerate() {
            // L1
            if let Some((value, expires_at_ms, metadata)) =
                self.cache.get(&Self::cache_key(ns, key), now)
            {
                results[i] = Some(Entry {
                    value,
                    expires_at: Self::instant_from_ms(expires_at_ms, now),
                    metadata,
                });
                continue;
            }
            // Index lookup (in-RAM)
            let (entry, ttl) = {
                let idx = nslog.index.borrow();
                match idx.get(key) {
                    None => continue,
                    Some(e) => (*e, idx.ttl(key)),
                }
            };
            if ttl.map_or(false, |ms| ms <= now) {
                // Expired — lazy-delete and skip.
                nslog.tombstone(key).await?;
                continue;
            }
            misses.push((i, entry));
            miss_ttls.push(ttl);
        }

        if !misses.is_empty() {
            let read = nslog.bulk_read(misses).await?;
            for ((slot, value, meta_bytes), ttl) in read.into_iter().zip(miss_ttls.into_iter()) {
                let metadata = Self::metadata_from_bytes(&meta_bytes);
                self.cache.insert(
                    Self::cache_key(ns, keys[slot]),
                    value.clone(),
                    ttl,
                    metadata.clone(),
                );
                results[slot] = Some(Entry {
                    value,
                    expires_at: Self::instant_from_ms(ttl, now),
                    metadata,
                });
            }
        }
        Ok(results)
    }

    pub async fn set(&self, ns: &str, key: &[u8], value: Bytes, opts: SetOptions) -> Result<()> {
        let nslog = self.ensure_ns(ns).await?;
        let expires_at_ms = opts
            .ttl
            .map(|d| now_ms() + u64::try_from(d.as_millis()).unwrap_or(u64::MAX));
        let meta_bytes: Vec<u8> = opts
            .metadata
            .as_ref()
            .and_then(|m| serde_json::to_vec(m).ok())
            .unwrap_or_default();
        let key_bytes = Bytes::copy_from_slice(key);
        nslog
            .put_full(key_bytes.clone(), &value, &meta_bytes, expires_at_ms)
            .await?;
        self.cache.insert(
            Self::cache_key(ns, key),
            value,
            expires_at_ms,
            opts.metadata,
        );
        Ok(())
    }

    pub async fn mset(&self, ns: &str, pairs: &[(Bytes, Bytes)]) -> Result<()> {
        let nslog = self.ensure_ns(ns).await?;
        nslog.put_many(pairs).await?;
        for (key, value) in pairs {
            self.cache.insert(Self::cache_key(ns, key), value.clone(), None, None);
        }
        Ok(())
    }

    pub async fn del(&self, ns: &str, keys: &[&[u8]]) -> Result<u64> {
        let nslog = self.ensure_ns(ns).await?;
        let now = now_ms();
        let mut count = 0u64;
        for &key in keys {
            // Mirror the previous Rocks-based semantics: an expired-but-not-yet-tombstoned
            // key counts as 0 (already semantically gone).
            let was_expired = {
                let idx = nslog.index.borrow();
                idx.is_expired(key, now)
            };
            let was_present = nslog.tombstone(key).await?;

            self.cache.remove(&Self::cache_key(ns, key));
            if was_present && !was_expired {
                count += 1;
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
        let present_and_live = {
            let idx = nslog.index.borrow();
            idx.get(key).is_some() && !idx.is_expired(key, now)
        };
        if !present_and_live {
            // Drop the cached entry if it shows up expired.
            self.cache.remove(&Self::cache_key(ns, key));
            return Ok(false);
        }
        let new_ms = now + u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX);
        nslog.ttl_update(key, Some(new_ms)).await?;
        // L1 carries its own copy of expires_at_ms; refresh on next get.
        self.cache.remove(&Self::cache_key(ns, key));
        Ok(true)
    }

    pub async fn persist(&self, ns: &str, key: &[u8]) -> Result<bool> {
        let nslog = self.ensure_ns(ns).await?;
        let (present, has_ttl) = {
            let idx = nslog.index.borrow();
            let p = idx.get(key).is_some();
            let t = idx.ttl(key).is_some();
            (p, t)
        };
        if !present || !has_ttl {
            return Ok(false);
        }
        nslog.ttl_update(key, None).await?;
        self.cache.remove(&Self::cache_key(ns, key));
        Ok(true)
    }

    pub async fn ttl(&self, ns: &str, key: &[u8]) -> Result<TtlResult> {
        let nslog = self.ensure_ns(ns).await?;
        let now = now_ms();
        let idx = nslog.index.borrow();
        match idx.get(key) {
            None => Ok(TtlResult::NotFound),
            Some(_) => match idx.ttl(key) {
                None => Ok(TtlResult::NoExpiry),
                Some(ms) if ms <= now => Ok(TtlResult::NotFound),
                Some(ms) => Ok(TtlResult::Remaining((ms - now) / 1000)),
            },
        }
    }

    pub async fn pttl(&self, ns: &str, key: &[u8]) -> Result<TtlResult> {
        let nslog = self.ensure_ns(ns).await?;
        let now = now_ms();
        let idx = nslog.index.borrow();
        match idx.get(key) {
            None => Ok(TtlResult::NotFound),
            Some(_) => match idx.ttl(key) {
                None => Ok(TtlResult::NoExpiry),
                Some(ms) if ms <= now => Ok(TtlResult::NotFound),
                Some(ms) => Ok(TtlResult::Remaining(ms.saturating_sub(now))),
            },
        }
    }

    pub async fn getset(&self, ns: &str, key: &[u8], value: Bytes) -> Result<Option<Entry>> {
        let old = self.get(ns, key).await?;
        self.set(ns, key, value, SetOptions::default()).await?;
        Ok(old)
    }

    pub async fn getdel(&self, ns: &str, key: &[u8]) -> Result<Option<Entry>> {
        let old = self.get(ns, key).await?;
        let nslog = self.ensure_ns(ns).await?;
        nslog.tombstone(key).await?;
        self.cache.remove(&Self::cache_key(ns, key));
        Ok(old)
    }

    pub async fn setnx(&self, ns: &str, key: &[u8], value: Bytes, opts: SetOptions) -> Result<bool> {
        let nslog = self.ensure_ns(ns).await?;
        let now = now_ms();
        let live = {
            let idx = nslog.index.borrow();
            idx.get(key).is_some() && !idx.is_expired(key, now)
        };
        if live {
            return Ok(false);
        }
        // If expired, the index still holds it — treat as absent and proceed.
        self.set(ns, key, value, opts).await?;
        Ok(true)
    }

    pub async fn setxx(&self, ns: &str, key: &[u8], value: Bytes, opts: SetOptions) -> Result<bool> {
        let nslog = self.ensure_ns(ns).await?;
        let now = now_ms();
        let live = {
            let idx = nslog.index.borrow();
            idx.get(key).is_some() && !idx.is_expired(key, now)
        };
        if !live {
            return Ok(false);
        }
        self.set(ns, key, value, opts).await?;
        Ok(true)
    }

    pub fn sweep_cache(&self) {
        self.cache.sweep_expired(now_ms());
    }

    /// Fsync any unsynced writes across all namespaces. Called by the per-shard
    /// 1-second timer to provide `appendfsync everysec` durability semantics.
    pub async fn sync_logs(&self) -> crate::error::Result<()> {
        let ns_list: Vec<Rc<NamespaceLog>> = self.namespaces.borrow().values().cloned().collect();
        for ns in ns_list {
            ns.sync().await?;
        }
        Ok(())
    }

    /// Trigger reclaim on one namespace. Used by `BGREWRITEAOF`.
    pub async fn reclaim(&self, ns: &str) -> Result<()> {
        let nslog = self.ensure_ns(ns).await?;
        nslog.reclaim().await?;
        Ok(())
    }

    /// Run reclaim on any namespace that has more than `threshold` sealed files.
    /// Called by the background reclaim timer. `threshold == 0` disables auto-reclaim.
    pub async fn reclaim_if_needed(&self, threshold: usize) -> Result<()> {
        if threshold == 0 {
            return Ok(());
        }
        let ns_list: Vec<Rc<NamespaceLog>> = self.namespaces.borrow().values().cloned().collect();
        for nslog in ns_list {
            if nslog.sealed_file_count() > threshold {
                nslog.reclaim().await?;
            }
        }
        Ok(())
    }

    /// SCAN with bucket-cursor semantics:
    ///   cursor `b"0"` = start (and the same byte string signals scan complete).
    ///   continuation cursor = 8 LE bytes of the iteration position.
    /// Spec-compliant with Redis SCAN: may skip yielded-then-deleted keys, may
    /// see newly-inserted keys inconsistently. Yield-once is preserved within a
    /// single page; cross-page guarantees match Redis's documented contract.
    pub async fn scan(
        &self,
        ns: &str,
        cursor: &[u8],
        pattern: Option<&[u8]>,
        count: u64,
    ) -> Result<ScanPage> {
        let nslog = self.ensure_ns(ns).await?;
        let now = now_ms();

        let cursor_pos: u64 = if cursor == b"0" {
            0
        } else if cursor.len() == 8 {
            cursor.try_into().map(u64::from_le_bytes).unwrap_or(0)
        } else {
            // Backwards-compat for `\x01`-prefixed cursors from prior versions: just restart.
            0
        };

        let pat = pattern;
        let (keys, next_cursor_pos) =
            nslog.index.borrow().scan(cursor_pos, count as usize, now, |k| {
                pat.map_or(true, |p| glob_match(p, k))
            });

        let next_cursor = if next_cursor_pos == 0 {
            Bytes::from_static(b"0")
        } else {
            Bytes::copy_from_slice(&next_cursor_pos.to_le_bytes())
        };
        Ok(ScanPage { next_cursor, keys })
    }

    pub async fn db_size(&self, ns: &str) -> Result<u64> {
        let nslog = self.ensure_ns(ns).await?;
        Ok(nslog.len() as u64)
    }

    pub async fn flush_db(&self, ns: &str) -> Result<()> {
        let nslog = self.ensure_ns(ns).await?;
        // Drop matching L1 entries — the namespace prefix uniquely identifies them.
        // We can't iterate L1 by namespace cheaply, so we walk the index first to
        // know which keys to remove. After flush the index is empty, so do this
        // before the unlink-and-recreate.
        let cache_keys: Vec<Bytes> = nslog
            .index
            .borrow()
            .iter()
            .map(|(k, _)| Self::cache_key(ns, k))
            .collect();
        for ck in cache_keys {
            self.cache.remove(&ck);
        }
        nslog.flush().await?;
        Ok(())
    }
}

/// Minimal glob matching for KEYS/SCAN patterns: `*` (any sequence), `?` (any single char).
pub(crate) fn glob_match(pattern: &[u8], s: &[u8]) -> bool {
    let mut pi = 0usize;
    let mut si = 0usize;
    let mut star_pi = usize::MAX;
    let mut star_si = 0usize;

    while si < s.len() {
        if pi < pattern.len() && (pattern[pi] == b'?' || pattern[pi] == s[si]) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
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
        s.set("default", key, Bytes::copy_from_slice(value), SetOptions::default())
            .await
            .unwrap();
    }

    async fn set_ttl(s: &ShardStore, key: &[u8], value: &[u8], ttl: Duration) {
        s.set(
            "default",
            key,
            Bytes::copy_from_slice(value),
            SetOptions { ttl: Some(ttl), metadata: None },
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
            assert_eq!(s.exists("default", &[b"no-such".as_ref()]).await.unwrap(), 0);
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
            assert!(s
                .setnx("default", b"snx", Bytes::from_static(b"v"), SetOptions::default())
                .await
                .unwrap());
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
            assert!(!s
                .setnx("default", b"snx-dup", Bytes::from_static(b"clobber"), SetOptions::default())
                .await
                .unwrap());
            assert_eq!(get_value(&s, b"snx-dup").await.unwrap().as_ref(), b"original");
        });
    }

    #[test]
    fn expire_on_live_key_returns_true() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();
        run(async move {
            let s = open_store(&path).await;
            set(&s, b"k", b"v").await;
            assert!(s.expire("default", b"k", Duration::from_secs(60)).await.unwrap());
        });
    }

    #[test]
    fn expire_on_missing_key_returns_false() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();
        run(async move {
            let s = open_store(&path).await;
            assert!(!s.expire("default", b"miss", Duration::from_secs(60)).await.unwrap());
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
                .mget("default", &[b"a".as_ref(), b"missing".as_ref(), b"b".as_ref()])
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
            let res = s.mget("default", &[b"k1".as_ref(), b"k2".as_ref()]).await.unwrap();
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
            s.set("db3", b"k", Bytes::from_static(b"in-db3"), SetOptions::default())
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
                SetOptions { ttl: Some(Duration::from_secs(3600)), metadata: None },
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
            s.set("default", b"big", big, SetOptions::default()).await.unwrap();
            let pre = active_size(&s, "default").await;
            s.expire("default", b"big", Duration::from_secs(60)).await.unwrap();
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

    // ── glob_match ────────────────────────────────────────────────────────────

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
}

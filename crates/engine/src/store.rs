use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tracing::warn;

use bytes::Bytes;
use rocksdb::{ColumnFamilyDescriptor, DBWithThreadMode, Direction, IteratorMode, MultiThreaded, Options, WriteBatch, compaction_filter::Decision};

use crate::cache::MemCache;
use crate::error::{EngineError, Result};
use crate::types::{Entry, ScanPage, SetOptions, TtlResult};

// Postcard-encoded value stored in RocksDB.
// Uses borrowed slices for zero-copy encoding; postcard::from_bytes borrows from the input.
#[derive(serde::Serialize, serde::Deserialize)]
struct StoredValue<'a> {
    #[serde(borrow)]
    value: &'a [u8],
    expires_at_ms: Option<u64>,
    #[serde(borrow)]
    metadata: Option<&'a [u8]>,
}

pub const DEFAULT_NS: &str = "default";

pub fn ns_for_db(db: u64) -> &'static str {
    match db {
        0 => "default",
        1 => "db1",   2 => "db2",   3 => "db3",   4 => "db4",
        5 => "db5",   6 => "db6",   7 => "db7",   8 => "db8",
        9 => "db9",   10 => "db10", 11 => "db11", 12 => "db12",
        13 => "db13", 14 => "db14", 15 => "db15",
        _ => "default",
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn expiry_filter(_level: u32, _key: &[u8], value: &[u8]) -> Decision {
    if let Ok(sv) = postcard::from_bytes::<StoredValue<'_>>(value) {
        if let Some(ms) = sv.expires_at_ms {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            if ms <= now {
                return Decision::Remove;
            }
        }
    }
    Decision::Keep
}

fn encode(value: &[u8], expires_at_ms: Option<u64>, metadata: Option<&[u8]>) -> Result<Vec<u8>> {
    let sv = StoredValue { value, expires_at_ms, metadata };
    Ok(postcard::to_allocvec(&sv)?)
}

fn decode(raw: &[u8]) -> Result<StoredValue<'_>> {
    Ok(postcard::from_bytes(raw)?)
}

fn to_entry(sv: StoredValue<'_>) -> Option<Entry> {
    let now = now_ms();
    if sv.expires_at_ms.map_or(false, |ms| ms <= now) {
        return None;
    }
    let expires_at = sv.expires_at_ms.map(|ms| {
        Instant::now() + Duration::from_millis(ms - now)
    });
    let metadata = sv.metadata.and_then(|b| {
        serde_json::from_slice(b).map_err(|e| warn!(error = %e, "metadata decode failed")).ok()
    });
    Some(Entry {
        value: Bytes::copy_from_slice(sv.value),
        expires_at,
        metadata,
    })
}

/// Per-shard KV store backed by RocksDB.
///
/// One `ShardStore` per worker thread — never shared across threads (`!Send` via `Rc` in the
/// server layer, but `ShardStore` itself is `Send` to allow construction on the main thread
/// before being moved to the worker).
pub struct ShardStore {
    db: Arc<DBWithThreadMode<MultiThreaded>>,
    cache: MemCache,
}

impl ShardStore {
    pub fn open(data_dir: &Path, memory_bytes: usize) -> Result<Self> {
        std::fs::create_dir_all(data_dir)?;

        let known_ns = &[
            DEFAULT_NS,
            "db1", "db2", "db3", "db4", "db5", "db6", "db7",
            "db8", "db9", "db10", "db11", "db12", "db13", "db14", "db15",
        ];

        let cfs: Vec<ColumnFamilyDescriptor> = known_ns
            .iter()
            .map(|ns| {
                let mut opts = Options::default();
                opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
                opts.set_compaction_filter("expiry", expiry_filter);
                ColumnFamilyDescriptor::new(*ns, opts)
            })
            .collect();

        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);

        let db = DBWithThreadMode::<MultiThreaded>::open_cf_descriptors(
            &db_opts,
            data_dir,
            cfs,
        )?;

        Ok(Self { db: Arc::new(db), cache: MemCache::new(memory_bytes) })
    }

    fn cf<'a>(&'a self, ns: &str) -> Result<Arc<rocksdb::BoundColumnFamily<'a>>> {
        self.db.cf_handle(ns).ok_or_else(|| EngineError::InvalidNamespace { name: ns.to_owned() })
    }

    /// Build a namespace-qualified cache key: `{ns}\x00{key}`.
    ///
    /// The null byte cannot appear in a namespace name, so this is always unambiguous.
    fn cache_key(ns: &str, key: &[u8]) -> Bytes {
        let mut ck = Vec::with_capacity(ns.len() + 1 + key.len());
        ck.extend_from_slice(ns.as_bytes());
        ck.push(b'\x00');
        ck.extend_from_slice(key);
        Bytes::from(ck)
    }

    pub fn get(&self, ns: &str, key: &[u8]) -> Result<Option<Entry>> {
        let now = now_ms();

        // L1 hit
        if let Some((value, expires_at_ms, metadata)) = self.cache.get(&Self::cache_key(ns, key), now) {
            let expires_at = expires_at_ms.map(|ms| {
                Instant::now() + Duration::from_millis(ms.saturating_sub(now))
            });
            return Ok(Some(Entry { value, expires_at, metadata }));
        }

        // L2
        let cf = self.cf(ns)?;
        match self.db.get_cf(&cf, key)? {
            None => Ok(None),
            Some(raw) => {
                let sv = decode(&raw)?;
                if sv.expires_at_ms.map_or(false, |ms| ms <= now) {
                    let _ = self.db.delete_cf(&cf, key);
                    return Ok(None);
                }
                let value = Bytes::copy_from_slice(sv.value);
                let expires_at = sv.expires_at_ms.map(|ms| {
                    Instant::now() + Duration::from_millis(ms - now)
                });
                let metadata = sv.metadata.and_then(|b| {
                    serde_json::from_slice(b).map_err(|e| warn!(error = %e, "metadata decode failed")).ok()
                });
                // Populate L1
                self.cache.insert(
                    Self::cache_key(ns, key),
                    value.clone(),
                    sv.expires_at_ms,
                    metadata.clone(),
                );
                Ok(Some(Entry { value, expires_at, metadata }))
            }
        }
    }

    pub fn set(&self, ns: &str, key: &[u8], value: Bytes, opts: SetOptions) -> Result<()> {
        let cf = self.cf(ns)?;
        let expires_at_ms = opts.ttl.map(|d| now_ms() + u64::try_from(d.as_millis()).unwrap_or(u64::MAX));
        let meta_bytes = opts.metadata.as_ref().and_then(|m| serde_json::to_vec(m).ok());
        let encoded = encode(&value, expires_at_ms, meta_bytes.as_deref())?;
        self.db.put_cf(&cf, key, &encoded)?;
        self.cache.insert(
            Self::cache_key(ns, key),
            value,
            expires_at_ms,
            opts.metadata,
        );
        Ok(())
    }

    pub fn mset(&self, ns: &str, pairs: &[(Bytes, Bytes)]) -> Result<()> {
        let cf = self.cf(ns)?;
        let mut batch = WriteBatch::default();
        for (key, value) in pairs {
            let encoded = encode(value, None, None)?;
            batch.put_cf(&cf, key, &encoded);
        }
        self.db.write(batch)?;
        for (key, value) in pairs {
            self.cache.insert(Self::cache_key(ns, key), value.clone(), None, None);
        }
        Ok(())
    }

    pub fn del(&self, ns: &str, keys: &[&[u8]]) -> Result<u64> {
        let cf = self.cf(ns)?;
        let now = now_ms();
        let mut batch = WriteBatch::default();
        let mut count = 0u64;
        for &key in keys {
            if let Some(raw) = self.db.get_cf(&cf, key)? {
                let sv = decode(&raw)?;
                // Don't count expired keys as deleted — they're already semantically gone
                let live = !sv.expires_at_ms.map_or(false, |ms| ms <= now);
                batch.delete_cf(&cf, key);
                self.cache.remove(&Self::cache_key(ns, key));
                if live {
                    count += 1;
                }
            }
        }
        if !batch.is_empty() {
            self.db.write(batch)?;
        }
        Ok(count)
    }

    pub fn exists(&self, ns: &str, keys: &[&[u8]]) -> Result<u64> {
        let cf = self.cf(ns)?;
        let now = now_ms();
        let mut count = 0u64;
        for &key in keys {
            if let Some(raw) = self.db.get_cf(&cf, key)? {
                let sv = decode(&raw)?;
                if !sv.expires_at_ms.map_or(false, |ms| ms <= now) {
                    count += 1;
                }
            }
        }
        Ok(count)
    }

    pub fn expire(&self, ns: &str, key: &[u8], ttl: Duration) -> Result<bool> {
        let cf = self.cf(ns)?;
        match self.db.get_cf(&cf, key)? {
            None => Ok(false),
            Some(raw) => {
                let sv = decode(&raw)?;
                let now = now_ms();
                if sv.expires_at_ms.map_or(false, |ms| ms <= now) {
                    let _ = self.db.delete_cf(&cf, key);
                    self.cache.remove(&Self::cache_key(ns, key));
                    return Ok(false);
                }
                let new_expires = Some(now + u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX));
                let encoded = encode(sv.value, new_expires, sv.metadata)?;
                self.db.put_cf(&cf, key, &encoded)?;
                let meta_val = sv.metadata.and_then(|b| {
                    serde_json::from_slice(b).map_err(|e| warn!(error = %e, "metadata decode failed")).ok()
                });
                self.cache.insert(
                    Self::cache_key(ns, key),
                    Bytes::copy_from_slice(sv.value),
                    new_expires,
                    meta_val,
                );
                Ok(true)
            }
        }
    }

    pub fn persist(&self, ns: &str, key: &[u8]) -> Result<bool> {
        let cf = self.cf(ns)?;
        match self.db.get_cf(&cf, key)? {
            None => Ok(false),
            Some(raw) => {
                let sv = decode(&raw)?;
                if sv.expires_at_ms.is_none() {
                    return Ok(false);
                }
                let encoded = encode(sv.value, None, sv.metadata)?;
                self.db.put_cf(&cf, key, &encoded)?;
                let meta_val = sv.metadata.and_then(|b| {
                    serde_json::from_slice(b).map_err(|e| warn!(error = %e, "metadata decode failed")).ok()
                });
                self.cache.insert(
                    Self::cache_key(ns, key),
                    Bytes::copy_from_slice(sv.value),
                    None,
                    meta_val,
                );
                Ok(true)
            }
        }
    }

    pub fn ttl(&self, ns: &str, key: &[u8]) -> Result<TtlResult> {
        let cf = self.cf(ns)?;
        match self.db.get_cf(&cf, key)? {
            None => Ok(TtlResult::NotFound),
            Some(raw) => {
                let sv = decode(&raw)?;
                match sv.expires_at_ms {
                    None => Ok(TtlResult::NoExpiry),
                    Some(ms) => {
                        let now = now_ms();
                        if ms <= now {
                            let _ = self.db.delete_cf(&cf, key);
                            Ok(TtlResult::NotFound)
                        } else {
                            Ok(TtlResult::Remaining((ms - now) / 1000))
                        }
                    }
                }
            }
        }
    }

    pub fn pttl(&self, ns: &str, key: &[u8]) -> Result<TtlResult> {
        let cf = self.cf(ns)?;
        match self.db.get_cf(&cf, key)? {
            None => Ok(TtlResult::NotFound),
            Some(raw) => {
                let sv = decode(&raw)?;
                match sv.expires_at_ms {
                    None => Ok(TtlResult::NoExpiry),
                    Some(ms) => {
                        let now = now_ms();
                        if ms <= now {
                            let _ = self.db.delete_cf(&cf, key);
                            Ok(TtlResult::NotFound)
                        } else {
                            Ok(TtlResult::Remaining(ms - now))
                        }
                    }
                }
            }
        }
    }

    pub fn getset(&self, ns: &str, key: &[u8], value: Bytes) -> Result<Option<Entry>> {
        let cf = self.cf(ns)?;
        let old = match self.db.get_cf(&cf, key)? {
            None => None,
            Some(raw) => decode(&raw).ok().and_then(to_entry),
        };
        let encoded = encode(&value, None, None)?;
        self.db.put_cf(&cf, key, &encoded)?;
        // Update L1 — new value has no TTL
        self.cache.insert(Self::cache_key(ns, key), value, None, None);
        Ok(old)
    }

    pub fn getdel(&self, ns: &str, key: &[u8]) -> Result<Option<Entry>> {
        let cf = self.cf(ns)?;
        match self.db.get_cf(&cf, key)? {
            None => Ok(None),
            Some(raw) => {
                let sv = decode(&raw)?;
                self.db.delete_cf(&cf, key)?;
                self.cache.remove(&Self::cache_key(ns, key));
                if sv.expires_at_ms.map_or(false, |ms| ms <= now_ms()) {
                    return Ok(None);
                }
                Ok(to_entry(sv))
            }
        }
    }

    /// Set key only if it does not exist.
    ///
    /// The check-then-set is not a RocksDB atomic operation, but is safe because
    /// `ShardStore: !Sync` (via `MemCache`'s `RefCell` fields), so `&ShardStore`
    /// cannot be shared across threads and concurrent calls on the same shard are
    /// impossible.
    pub fn setnx(&self, ns: &str, key: &[u8], value: Bytes, opts: SetOptions) -> Result<bool> {
        let cf = self.cf(ns)?;
        if let Some(raw) = self.db.get_cf(&cf, key)? {
            let sv = decode(&raw)?;
            // An expired key is semantically absent — fall through to the set path
            if !sv.expires_at_ms.map_or(false, |ms| ms <= now_ms()) {
                return Ok(false);
            }
            // Clean up the expired entry before overwriting
            let _ = self.db.delete_cf(&cf, key);
            self.cache.remove(&Self::cache_key(ns, key));
        }
        let expires_at_ms = opts.ttl.map(|d| now_ms() + u64::try_from(d.as_millis()).unwrap_or(u64::MAX));
        let meta_bytes = opts.metadata.as_ref().and_then(|m| serde_json::to_vec(m).ok());
        let encoded = encode(&value, expires_at_ms, meta_bytes.as_deref())?;
        self.db.put_cf(&cf, key, &encoded)?;
        self.cache.insert(
            Self::cache_key(ns, key),
            value,
            expires_at_ms,
            opts.metadata,
        );
        Ok(true)
    }

    /// Sweep expired keys from the L1 cache. Called by the background sweeper task.
    pub fn sweep_cache(&self) {
        self.cache.sweep_expired(now_ms());
    }

    /// Write raw bytes directly into a column family, bypassing encode/L1.
    /// Used only in tests to inject corrupt data.
    #[cfg(test)]
    pub fn raw_put_db(&self, ns: &str, key: &[u8], raw: &[u8]) -> Result<()> {
        let cf = self.cf(ns)?;
        self.db.put_cf(&cf, key, raw)?;
        self.cache.remove(&Self::cache_key(ns, key));
        Ok(())
    }

    pub fn scan(&self, ns: &str, cursor: &[u8], pattern: Option<&[u8]>, count: u64) -> Result<ScanPage> {
        let cf = self.cf(ns)?;
        // Cursor "0" is the start-of-scan sentinel. Continuation cursors are tagged
        // with a leading \x01 byte followed by the last key seen, ensuring they never
        // collide with the sentinel even if a key is literally the byte string "0".
        const TAG: u8 = b'\x01';
        let (mode, skip_cursor) = if cursor == b"0" {
            (IteratorMode::Start, None)
        } else {
            let key = cursor.strip_prefix(&[TAG]).unwrap_or(cursor);
            (IteratorMode::From(key, Direction::Forward), Some(key))
        };

        let mut iter = self.db.iterator_cf(&cf, mode).peekable();

        // Skip the cursor key itself — it was the last key of the previous page
        if let Some(skip) = skip_cursor {
            if let Some(Ok((k, _))) = iter.peek() {
                if k.as_ref() == skip {
                    iter.next();
                }
            }
        }

        let now = now_ms();
        let count = count.max(1) as usize;
        let mut keys: Vec<Bytes> = Vec::with_capacity(count.min(4096));
        let mut last_key: Option<Bytes> = None;

        for item in iter {
            let (k, v) = item?;
            if let Ok(sv) = decode(&v) {
                if sv.expires_at_ms.map_or(false, |ms| ms <= now) {
                    continue;
                }
            }
            if let Some(pat) = pattern {
                if !glob_match(pat, &k) {
                    continue;
                }
            }
            last_key = Some(Bytes::copy_from_slice(&k));
            keys.push(Bytes::copy_from_slice(&k));
            if keys.len() >= count {
                break;
            }
        }

        let next_cursor = if keys.len() >= count {
            let last = last_key.unwrap();
            let mut cur = Vec::with_capacity(1 + last.len());
            cur.push(TAG);
            cur.extend_from_slice(&last);
            Bytes::from(cur)
        } else {
            Bytes::from_static(b"0")
        };

        Ok(ScanPage { next_cursor, keys })
    }

    pub fn db_size(&self, ns: &str) -> Result<u64> {
        let cf = self.cf(ns)?;
        // estimate-num-keys is O(1); may over-count expired keys not yet compacted
        Ok(self.db
            .property_int_value_cf(&cf, "rocksdb.estimate-num-keys")?
            .unwrap_or(0))
    }

    pub fn flush_db(&self, ns: &str) -> Result<()> {
        let cf = self.cf(ns)?;
        let mut batch = WriteBatch::default();
        let mut batch_count = 0usize;
        for item in self.db.iterator_cf(&cf, IteratorMode::Start) {
            let (k, _) = item?;
            self.cache.remove(&Self::cache_key(ns, &k));
            batch.delete_cf(&cf, &k);
            batch_count += 1;
            if batch_count >= 4096 {
                self.db.write(std::mem::take(&mut batch))?;
                batch_count = 0;
            }
        }
        if batch_count > 0 {
            self.db.write(batch)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn store() -> (ShardStore, TempDir) {
        let tmp = TempDir::new().unwrap();
        (ShardStore::open(tmp.path(), 4 << 20).unwrap(), tmp)
    }

    fn set(s: &ShardStore, key: &[u8], value: &[u8]) {
        s.set("default", key, Bytes::copy_from_slice(value), SetOptions::default()).unwrap();
    }

    fn set_ttl_ms(s: &ShardStore, key: &[u8], value: &[u8], ttl: Duration) {
        s.set("default", key, Bytes::copy_from_slice(value), SetOptions { ttl: Some(ttl), metadata: None }).unwrap();
    }

    fn get(s: &ShardStore, key: &[u8]) -> Option<Bytes> {
        s.get("default", key).unwrap().map(|e| e.value)
    }

    // ── Basic CRUD ────────────────────────────────────────────────────────────

    #[test]
    fn set_get_roundtrip() {
        let (s, _t) = store();
        set(&s, b"k", b"hello");
        assert_eq!(get(&s, b"k").unwrap().as_ref(), b"hello");
    }

    #[test]
    fn get_missing_returns_none() {
        let (s, _t) = store();
        assert!(get(&s, b"nope").is_none());
    }

    #[test]
    fn set_overwrites_value() {
        let (s, _t) = store();
        set(&s, b"k", b"first");
        set(&s, b"k", b"second");
        assert_eq!(get(&s, b"k").unwrap().as_ref(), b"second");
    }

    #[test]
    fn del_existing_returns_count_1() {
        let (s, _t) = store();
        set(&s, b"del-me", b"v");
        assert_eq!(s.del("default", &[b"del-me".as_ref()]).unwrap(), 1);
        assert!(get(&s, b"del-me").is_none());
    }

    #[test]
    fn del_missing_returns_count_0() {
        let (s, _t) = store();
        assert_eq!(s.del("default", &[b"ghost".as_ref()]).unwrap(), 0);
    }

    #[test]
    fn del_batch_returns_only_live_count() {
        let (s, _t) = store();
        set(&s, b"d1", b"v");
        set(&s, b"d2", b"v");
        assert_eq!(s.del("default", &[b"d1".as_ref(), b"d2".as_ref(), b"d3".as_ref()]).unwrap(), 2);
    }

    #[test]
    fn del_evicts_from_l1_cache() {
        let (s, _t) = store();
        set(&s, b"cache-key", b"v");
        let _ = get(&s, b"cache-key"); // warm L1
        s.del("default", &[b"cache-key".as_ref()]).unwrap();
        assert!(get(&s, b"cache-key").is_none(), "del must evict from L1 cache");
    }

    // ── EXISTS ────────────────────────────────────────────────────────────────

    #[test]
    fn exists_live_key() {
        let (s, _t) = store();
        set(&s, b"ex-k", b"v");
        assert_eq!(s.exists("default", &[b"ex-k".as_ref()]).unwrap(), 1);
    }

    #[test]
    fn exists_missing_key() {
        let (s, _t) = store();
        assert_eq!(s.exists("default", &[b"no-such".as_ref()]).unwrap(), 0);
    }

    // ── TTL expiry ────────────────────────────────────────────────────────────

    #[test]
    fn expired_key_is_invisible() {
        let (s, _t) = store();
        set_ttl_ms(&s, b"exp", b"v", Duration::from_millis(50));
        std::thread::sleep(Duration::from_millis(100));
        assert!(get(&s, b"exp").is_none());
    }

    #[test]
    fn del_on_expired_key_returns_0() {
        let (s, _t) = store();
        set_ttl_ms(&s, b"exp-del", b"v", Duration::from_millis(50));
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(s.del("default", &[b"exp-del".as_ref()]).unwrap(), 0);
    }

    #[test]
    fn exists_on_expired_key_returns_0() {
        let (s, _t) = store();
        set_ttl_ms(&s, b"exp-ex", b"v", Duration::from_millis(50));
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(s.exists("default", &[b"exp-ex".as_ref()]).unwrap(), 0);
    }

    // ── SETNX ─────────────────────────────────────────────────────────────────

    #[test]
    fn setnx_on_missing_inserts() {
        let (s, _t) = store();
        assert!(s.setnx("default", b"snx", Bytes::from_static(b"v"), SetOptions::default()).unwrap());
        assert_eq!(get(&s, b"snx").unwrap().as_ref(), b"v");
    }

    #[test]
    fn setnx_on_live_key_is_no_op() {
        let (s, _t) = store();
        set(&s, b"snx-dup", b"original");
        assert!(!s.setnx("default", b"snx-dup", Bytes::from_static(b"clobber"), SetOptions::default()).unwrap());
        assert_eq!(get(&s, b"snx-dup").unwrap().as_ref(), b"original");
    }

    #[test]
    fn setnx_on_expired_key_treats_it_as_absent() {
        let (s, _t) = store();
        set_ttl_ms(&s, b"snx-exp", b"old", Duration::from_millis(50));
        std::thread::sleep(Duration::from_millis(100));
        assert!(s.setnx("default", b"snx-exp", Bytes::from_static(b"new"), SetOptions::default()).unwrap());
        assert_eq!(get(&s, b"snx-exp").unwrap().as_ref(), b"new");
    }

    // ── EXPIRE / PERSIST / TTL / PTTL ────────────────────────────────────────

    #[test]
    fn expire_on_live_key_returns_true() {
        let (s, _t) = store();
        set(&s, b"exp-live", b"v");
        assert!(s.expire("default", b"exp-live", Duration::from_secs(60)).unwrap());
    }

    #[test]
    fn expire_on_missing_key_returns_false() {
        let (s, _t) = store();
        assert!(!s.expire("default", b"exp-miss", Duration::from_secs(60)).unwrap());
    }

    #[test]
    fn expire_on_already_expired_key_returns_false() {
        let (s, _t) = store();
        set_ttl_ms(&s, b"exp-dead", b"v", Duration::from_millis(50));
        std::thread::sleep(Duration::from_millis(100));
        assert!(!s.expire("default", b"exp-dead", Duration::from_secs(60)).unwrap());
    }

    #[test]
    fn ttl_on_persistent_key_returns_no_expiry() {
        let (s, _t) = store();
        set(&s, b"pst", b"v");
        assert_eq!(s.ttl("default", b"pst").unwrap(), TtlResult::NoExpiry);
    }

    #[test]
    fn ttl_on_missing_key_returns_not_found() {
        let (s, _t) = store();
        assert_eq!(s.ttl("default", b"miss").unwrap(), TtlResult::NotFound);
    }

    #[test]
    fn ttl_on_expiring_key_returns_remaining_secs() {
        let (s, _t) = store();
        set_ttl_ms(&s, b"ttl-k", b"v", Duration::from_secs(60));
        match s.ttl("default", b"ttl-k").unwrap() {
            TtlResult::Remaining(secs) => assert!(secs > 0 && secs <= 60),
            other => panic!("expected Remaining, got {other:?}"),
        }
    }

    #[test]
    fn pttl_on_expiring_key_returns_remaining_ms() {
        let (s, _t) = store();
        set_ttl_ms(&s, b"pttl-k", b"v", Duration::from_secs(10));
        match s.pttl("default", b"pttl-k").unwrap() {
            TtlResult::Remaining(ms) => assert!(ms > 0 && ms <= 10_000),
            other => panic!("expected Remaining, got {other:?}"),
        }
    }

    #[test]
    fn persist_removes_ttl_returns_true() {
        let (s, _t) = store();
        set_ttl_ms(&s, b"persist-k", b"v", Duration::from_secs(60));
        assert!(s.persist("default", b"persist-k").unwrap());
        assert_eq!(s.ttl("default", b"persist-k").unwrap(), TtlResult::NoExpiry);
    }

    #[test]
    fn persist_on_persistent_key_returns_false() {
        let (s, _t) = store();
        set(&s, b"no-ttl", b"v");
        assert!(!s.persist("default", b"no-ttl").unwrap());
    }

    // ── GETSET / GETDEL ───────────────────────────────────────────────────────

    #[test]
    fn getset_returns_old_value_and_stores_new() {
        let (s, _t) = store();
        set(&s, b"gs", b"old");
        let old = s.getset("default", b"gs", Bytes::from_static(b"new")).unwrap().unwrap();
        assert_eq!(old.value.as_ref(), b"old");
        assert_eq!(get(&s, b"gs").unwrap().as_ref(), b"new");
    }

    #[test]
    fn getset_clears_ttl() {
        let (s, _t) = store();
        set_ttl_ms(&s, b"gs-ttl", b"v", Duration::from_secs(60));
        let _ = s.getset("default", b"gs-ttl", Bytes::from_static(b"new")).unwrap();
        assert_eq!(s.ttl("default", b"gs-ttl").unwrap(), TtlResult::NoExpiry);
    }

    #[test]
    fn getdel_returns_value_and_removes_key() {
        let (s, _t) = store();
        set(&s, b"gd", b"bye");
        let val = s.getdel("default", b"gd").unwrap().unwrap();
        assert_eq!(val.value.as_ref(), b"bye");
        assert!(get(&s, b"gd").is_none());
    }

    #[test]
    fn getdel_on_missing_returns_none() {
        let (s, _t) = store();
        assert!(s.getdel("default", b"gd-miss").unwrap().is_none());
    }

    // ── SCAN ──────────────────────────────────────────────────────────────────

    #[test]
    fn scan_empty_store_returns_zero_cursor() {
        let (s, _t) = store();
        let page = s.scan("default", b"0", None, 100).unwrap();
        assert_eq!(page.next_cursor.as_ref(), b"0");
        assert!(page.keys.is_empty());
    }

    #[test]
    fn scan_returns_all_keys() {
        let (s, _t) = store();
        for k in [b"sa".as_ref(), b"sb", b"sc"] {
            set(&s, k, b"v");
        }
        let page = s.scan("default", b"0", None, 100).unwrap();
        let keys: Vec<&[u8]> = page.keys.iter().map(|k| k.as_ref()).collect();
        assert!(keys.contains(&b"sa".as_ref()) && keys.contains(&b"sb".as_ref()) && keys.contains(&b"sc".as_ref()));
    }

    #[test]
    fn scan_paginates_correctly() {
        let (s, _t) = store();
        for i in 0u8..15 {
            let key = format!("pg:{i:02}").into_bytes();
            s.set("default", &key, Bytes::from_static(b"v"), SetOptions::default()).unwrap();
        }
        let mut all: Vec<Bytes> = Vec::new();
        let mut cursor = Bytes::from_static(b"0");
        loop {
            let page = s.scan("default", &cursor, None, 4).unwrap();
            all.extend(page.keys);
            cursor = page.next_cursor.clone();
            if cursor.as_ref() == b"0" { break; }
        }
        assert_eq!(all.len(), 15);
    }

    #[test]
    fn scan_star_pattern_matches_all() {
        let (s, _t) = store();
        set(&s, b"foo:1", b"v");
        set(&s, b"bar:1", b"v");
        let page = s.scan("default", b"0", Some(b"*".as_ref()), 100).unwrap();
        assert_eq!(page.keys.len(), 2);
    }

    #[test]
    fn scan_prefix_pattern_filters() {
        let (s, _t) = store();
        set(&s, b"user:1", b"v");
        set(&s, b"user:2", b"v");
        set(&s, b"session:1", b"v");
        let page = s.scan("default", b"0", Some(b"user:*".as_ref()), 100).unwrap();
        assert_eq!(page.keys.len(), 2);
    }

    #[test]
    fn scan_question_mark_wildcard() {
        let (s, _t) = store();
        set(&s, b"fo1", b"v");
        set(&s, b"fo2", b"v");
        set(&s, b"foobar", b"v");
        let page = s.scan("default", b"0", Some(b"fo?".as_ref()), 100).unwrap();
        assert_eq!(page.keys.len(), 2, "fo? matches fo1, fo2 but not foobar");
    }

    #[test]
    fn scan_skips_expired_keys() {
        let (s, _t) = store();
        set(&s, b"live", b"v");
        set_ttl_ms(&s, b"dead", b"v", Duration::from_millis(50));
        std::thread::sleep(Duration::from_millis(100));
        let page = s.scan("default", b"0", None, 100).unwrap();
        let keys: Vec<&[u8]> = page.keys.iter().map(|k| k.as_ref()).collect();
        assert!(keys.contains(&b"live".as_ref()));
        assert!(!keys.contains(&b"dead".as_ref()));
    }

    // ── FLUSH_DB ──────────────────────────────────────────────────────────────

    #[test]
    fn flush_db_clears_all_keys() {
        let (s, _t) = store();
        set(&s, b"f1", b"v");
        set(&s, b"f2", b"v");
        s.flush_db("default").unwrap();
        let page = s.scan("default", b"0", None, 100).unwrap();
        assert!(page.keys.is_empty());
    }

    #[test]
    fn flush_db_twice_is_idempotent() {
        let (s, _t) = store();
        set(&s, b"f", b"v");
        s.flush_db("default").unwrap();
        s.flush_db("default").unwrap();
    }

    // ── Namespace isolation ───────────────────────────────────────────────────

    #[test]
    fn key_in_default_not_visible_in_db1() {
        let (s, _t) = store();
        set(&s, b"ns-k", b"default-val");
        assert!(s.get("db1", b"ns-k").unwrap().is_none());
    }

    #[test]
    fn independent_values_per_namespace() {
        let (s, _t) = store();
        set(&s, b"k", b"default-val");
        s.set("db3", b"k", Bytes::from_static(b"db3-val"), SetOptions::default()).unwrap();
        assert_eq!(get(&s, b"k").unwrap().as_ref(), b"default-val");
        assert_eq!(s.get("db3", b"k").unwrap().unwrap().value.as_ref(), b"db3-val");
    }

    #[test]
    fn flush_db_does_not_touch_other_namespaces() {
        let (s, _t) = store();
        set(&s, b"shared", b"in-default");
        s.set("db5", b"shared", Bytes::from_static(b"in-db5"), SetOptions::default()).unwrap();
        s.flush_db("db5").unwrap();
        assert_eq!(get(&s, b"shared").unwrap().as_ref(), b"in-default");
        assert!(s.get("db5", b"shared").unwrap().is_none());
    }

    // ── Postcard corruption ───────────────────────────────────────────────────

    #[test]
    fn corrupt_rocksdb_value_causes_get_to_return_err() {
        let (s, _t) = store();
        s.raw_put_db("default", b"corrupt", b"\xff\xfe\xfd\x00garbled").unwrap();
        let result = s.get("default", b"corrupt");
        assert!(result.is_err(), "corrupt RocksDB value must surface as Err, not None");
    }

    #[test]
    fn corrupt_rocksdb_value_in_getset_is_silently_treated_as_absent() {
        // getset() uses decode().ok() — the old value is swallowed on decode failure,
        // and the new value is unconditionally written.
        let (s, _t) = store();
        s.raw_put_db("default", b"corrupt-gs", b"\xff\xfe\xfd").unwrap();
        let old = s.getset("default", b"corrupt-gs", Bytes::from_static(b"new")).unwrap();
        assert!(old.is_none(), "corrupt old value must be treated as absent in getset");
        assert_eq!(get(&s, b"corrupt-gs").unwrap().as_ref(), b"new");
    }

    // ── glob_match edge cases ─────────────────────────────────────────────────

    #[test]
    fn glob_empty_pattern_matches_empty_string() {
        assert!(glob_match(b"", b""), "empty pattern must match empty string");
    }

    #[test]
    fn glob_empty_pattern_does_not_match_nonempty() {
        assert!(!glob_match(b"", b"a"));
    }

    #[test]
    fn glob_question_mark_does_not_match_empty() {
        assert!(!glob_match(b"?", b""), "? requires exactly one character");
    }

    #[test]
    fn glob_star_matches_empty() {
        assert!(glob_match(b"*", b""), "* must match empty string");
    }

    #[test]
    fn glob_consecutive_stars_behave_as_one() {
        assert!(glob_match(b"**", b"anything"));
        assert!(glob_match(b"**", b""));
        assert!(glob_match(b"a**b", b"aXXb"));
    }

    #[test]
    fn glob_star_between_literals() {
        assert!(glob_match(b"a*b", b"ab"));
        assert!(glob_match(b"a*b", b"aXb"));
        assert!(glob_match(b"a*b", b"aXXXb"));
        assert!(!glob_match(b"a*b", b"aX"));
        assert!(!glob_match(b"a*b", b"Xb"));
    }

    #[test]
    fn glob_question_mark_matches_exactly_one() {
        assert!(glob_match(b"a?b", b"aXb"));
        assert!(!glob_match(b"a?b", b"ab"), "? needs one char between a and b");
        assert!(!glob_match(b"a?b", b"aXXb"), "? matches exactly one, not two");
    }

    #[test]
    fn glob_pattern_longer_than_string_is_no_match() {
        assert!(!glob_match(b"abcde", b"abc"));
    }

    // ── SCAN cursor stability ─────────────────────────────────────────────────

    #[test]
    fn scan_cursor_stable_when_key_deleted_between_pages() {
        let (s, _t) = store();
        for k in [b"ka".as_ref(), b"kb", b"kc", b"kd"] {
            set(&s, k, b"v");
        }
        // Get first page (count=2 → "ka","kb", cursor points after "kb")
        let page1 = s.scan("default", b"0", None, 2).unwrap();
        assert_eq!(page1.keys.len(), 2);
        let cursor = page1.next_cursor.clone();
        assert_ne!(cursor.as_ref(), b"0", "should have a continuation cursor");

        // Delete "kc" between pages
        s.del("default", &[b"kc".as_ref()]).unwrap();

        // Second page should yield only "kd" — "kc" is gone, no duplicates
        let page2 = s.scan("default", &cursor, None, 100).unwrap();
        let keys: Vec<&[u8]> = page2.keys.iter().map(|k| k.as_ref()).collect();
        assert!(!keys.contains(&b"ka".as_ref()), "ka already seen on page1");
        assert!(!keys.contains(&b"kb".as_ref()), "kb already seen on page1");
        assert!(!keys.contains(&b"kc".as_ref()), "kc was deleted");
        assert!(keys.contains(&b"kd".as_ref()), "kd must appear on page2");
    }

    #[test]
    fn scan_cursor_key_deleted_does_not_cause_repeat_or_skip() {
        let (s, _t) = store();
        for k in [b"xa".as_ref(), b"xb", b"xc"] {
            set(&s, k, b"v");
        }
        // Page with count=1 yields "xa", cursor = b"\x01xa"
        let page1 = s.scan("default", b"0", None, 1).unwrap();
        assert_eq!(page1.keys[0].as_ref(), b"xa");
        let cursor = page1.next_cursor.clone();

        // Delete "xa" (the cursor key itself)
        s.del("default", &[b"xa".as_ref()]).unwrap();

        // Next page must still yield "xb" and "xc" — no repeat, no skip
        let page2 = s.scan("default", &cursor, None, 100).unwrap();
        let keys: Vec<&[u8]> = page2.keys.iter().map(|k| k.as_ref()).collect();
        assert!(keys.contains(&b"xb".as_ref()));
        assert!(keys.contains(&b"xc".as_ref()));
    }
}

/// Minimal glob matching for KEYS/SCAN patterns.
/// Supports `*` (any sequence) and `?` (any single char).
fn glob_match(pattern: &[u8], s: &[u8]) -> bool {
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

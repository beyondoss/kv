use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tracing::warn;

use bytes::Bytes;
use rocksdb::{ColumnFamilyDescriptor, DBWithThreadMode, Direction, IteratorMode, MultiThreaded, Options, WriteBatch};

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

        let mut cf_opts = Options::default();
        cf_opts.set_compression_type(rocksdb::DBCompressionType::Lz4);

        let cfs: Vec<ColumnFamilyDescriptor> = known_ns
            .iter()
            .map(|ns| ColumnFamilyDescriptor::new(*ns, cf_opts.clone()))
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

    pub fn get(&self, ns: &str, key: &[u8]) -> Result<Option<Entry>> {
        let now = now_ms();

        // L1 hit
        if let Some((value, expires_at_ms, metadata)) = self.cache.get(key, now) {
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
                    Bytes::copy_from_slice(key),
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
            Bytes::copy_from_slice(key),
            value,
            expires_at_ms,
            opts.metadata,
        );
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
                self.cache.remove(key);
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
                    self.cache.remove(key);
                    return Ok(false);
                }
                let new_expires = Some(now + u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX));
                let encoded = encode(sv.value, new_expires, sv.metadata)?;
                self.db.put_cf(&cf, key, &encoded)?;
                let meta_val = sv.metadata.and_then(|b| {
                    serde_json::from_slice(b).map_err(|e| warn!(error = %e, "metadata decode failed")).ok()
                });
                self.cache.insert(
                    Bytes::copy_from_slice(key),
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
                    Bytes::copy_from_slice(key),
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
        self.cache.insert(Bytes::copy_from_slice(key), value, None, None);
        Ok(old)
    }

    pub fn getdel(&self, ns: &str, key: &[u8]) -> Result<Option<Entry>> {
        let cf = self.cf(ns)?;
        match self.db.get_cf(&cf, key)? {
            None => Ok(None),
            Some(raw) => {
                let sv = decode(&raw)?;
                self.db.delete_cf(&cf, key)?;
                self.cache.remove(key);
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
            self.cache.remove(key);
        }
        let expires_at_ms = opts.ttl.map(|d| now_ms() + u64::try_from(d.as_millis()).unwrap_or(u64::MAX));
        let meta_bytes = opts.metadata.as_ref().and_then(|m| serde_json::to_vec(m).ok());
        let encoded = encode(&value, expires_at_ms, meta_bytes.as_deref())?;
        self.db.put_cf(&cf, key, &encoded)?;
        self.cache.insert(
            Bytes::copy_from_slice(key),
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
            self.cache.remove(&k);
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

use bytes::Bytes;
use rustc_hash::FxHashMap;

/// 24-byte packed entry pointing at a full record on disk.
///
/// Single-I/O GET: `read_at(record_offset, record_size)` returns the full record
/// (header + key + value + metadata). The header carries key/value/meta sizes so
/// we can slice the value out in-memory.
///
/// Layout: u64 + u32 + u16 + (2 pad) + u64 = 24 bytes.
///
/// 4 GiB single-record limit (well above Redis's 512 MiB string ceiling).
/// 65k files per namespace × `rotate_threshold` = comfortable disk ceiling.
///
/// `tstamp_ms` doubles as the per-key revision for CAS checks. It is the
/// monotonically-increasing timestamp written into the record header — O(1)
/// to compare without a disk read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexEntry {
    pub record_offset: u64,
    pub record_size: u32,
    pub file_id: u16,
    pub tstamp_ms: u64,
}

impl IndexEntry {
    pub fn new(file_id: u16, record_offset: u64, record_size: u32, tstamp_ms: u64) -> Self {
        Self {
            record_offset,
            record_size,
            file_id,
            tstamp_ms,
        }
    }
}

/// Per-namespace in-memory index.
pub struct NsIndex {
    map: FxHashMap<Bytes, IndexEntry>,
    /// TTL sidecar — only TTL'd keys pay extra memory.
    ttl: FxHashMap<Bytes, u64>,
    /// Best-effort live key count: incremented on insert, decremented on remove.
    /// Lazy-expired keys are included until tombstoned, matching Redis DBSIZE semantics.
    live_count: usize,
}

impl Default for NsIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl NsIndex {
    pub fn new() -> Self {
        Self {
            map: FxHashMap::default(),
            ttl: FxHashMap::default(),
            live_count: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn get(&self, key: &[u8]) -> Option<&IndexEntry> {
        self.map.get(key)
    }

    pub fn ttl(&self, key: &[u8]) -> Option<u64> {
        self.ttl.get(key).copied()
    }

    pub fn insert(&mut self, key: Bytes, entry: IndexEntry, expires_at_ms: Option<u64>) {
        match expires_at_ms {
            Some(ms) => {
                self.ttl.insert(key.clone(), ms);
            }
            None => {
                self.ttl.remove(&key);
            }
        }
        let is_new = !self.map.contains_key(key.as_ref());
        self.map.insert(key, entry);
        if is_new {
            self.live_count += 1;
        }
    }

    pub fn set_ttl(&mut self, key: &Bytes, expires_at_ms: Option<u64>) {
        match expires_at_ms {
            Some(ms) => {
                self.ttl.insert(key.clone(), ms);
            }
            None => {
                self.ttl.remove(key);
            }
        }
    }

    pub fn remove(&mut self, key: &[u8]) -> Option<IndexEntry> {
        self.ttl.remove(key);
        let removed = self.map.remove(key);
        if removed.is_some() {
            self.live_count = self.live_count.saturating_sub(1);
        }
        removed
    }

    pub fn clear(&mut self) {
        self.map.clear();
        self.ttl.clear();
        self.live_count = 0;
    }

    pub fn live_len(&self) -> usize {
        self.live_count
    }

    /// Returns true if `expires_at_ms` is set and is at or before `now_ms`.
    pub fn is_expired(&self, key: &[u8], now_ms: u64) -> bool {
        self.ttl.get(key).copied().map_or(false, |ms| ms <= now_ms)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&Bytes, &IndexEntry)> {
        self.map.iter()
    }

    /// Walk hash buckets starting at `cursor`, yielding up to `count` keys that
    /// pass `filter` and aren't expired at `now_ms`. Returns `(yielded, next_cursor)`.
    /// `next_cursor == 0` signals scan complete.
    ///
    /// Cursor is a position index into `FxHashMap::iter()`. The iteration order
    /// is NOT stable across any mutation (insert or delete can shift bucket
    /// positions regardless of rehash). Multi-batch scans are best-effort: keys
    /// written or deleted between batches may be skipped or returned twice.
    /// This matches Redis SCAN semantics: callers must tolerate duplicates and
    /// gaps when the keyspace mutates during iteration.
    pub fn scan<F>(
        &self,
        cursor: u64,
        count: usize,
        now_ms: u64,
        mut filter: F,
    ) -> (Vec<Bytes>, u64)
    where
        F: FnMut(&[u8]) -> bool,
    {
        let count = count.max(1);
        let mut yielded: Vec<Bytes> = Vec::with_capacity(count.min(4096));
        let mut next_cursor: u64 = 0;
        let mut last_idx: u64 = 0;
        for (i, (k, _v)) in self.map.iter().enumerate() {
            let i = i as u64;
            last_idx = i + 1;
            if i < cursor {
                continue;
            }
            if self.ttl.get(k).copied().map_or(false, |ms| ms <= now_ms) {
                continue;
            }
            if filter(k) {
                yielded.push(k.clone());
                if yielded.len() >= count {
                    next_cursor = last_idx;
                    break;
                }
            }
        }
        if (last_idx as usize) >= self.map.len() {
            next_cursor = 0;
        }
        (yielded, next_cursor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    #[test]
    fn entry_size_is_24() {
        assert_eq!(std::mem::size_of::<IndexEntry>(), 24);
    }

    #[test]
    fn insert_and_get() {
        let mut idx = NsIndex::new();
        idx.insert(b("k"), IndexEntry::new(0, 100, 50, 12345), None);
        let e = idx.get(b"k").unwrap();
        assert_eq!(e.record_offset, 100);
        assert_eq!(e.record_size, 50);
        assert_eq!(e.tstamp_ms, 12345);
    }

    #[test]
    fn remove_drops_ttl_too() {
        let mut idx = NsIndex::new();
        idx.insert(b("k"), IndexEntry::new(0, 0, 1, 0), Some(1000));
        assert!(idx.ttl(b"k").is_some());
        idx.remove(b"k");
        assert!(idx.ttl(b"k").is_none());
        assert!(idx.get(b"k").is_none());
    }

    #[test]
    fn ttl_only_paid_for_expiring_keys() {
        let mut idx = NsIndex::new();
        idx.insert(b("a"), IndexEntry::new(0, 0, 1, 0), None);
        idx.insert(b("b"), IndexEntry::new(0, 0, 1, 0), Some(1000));
        assert_eq!(idx.ttl.len(), 1);
        assert_eq!(idx.map.len(), 2);
    }

    #[test]
    fn is_expired() {
        let mut idx = NsIndex::new();
        idx.insert(b("k"), IndexEntry::new(0, 0, 1, 0), Some(100));
        assert!(idx.is_expired(b"k", 100));
        assert!(idx.is_expired(b"k", 200));
        assert!(!idx.is_expired(b"k", 99));
    }

    #[test]
    fn scan_yields_all_keys_eventually() {
        let mut idx = NsIndex::new();
        for i in 0..50u8 {
            idx.insert(
                Bytes::copy_from_slice(&[i]),
                IndexEntry::new(0, 0, 1, 0),
                None,
            );
        }
        let mut seen: Vec<Bytes> = Vec::new();
        let mut cursor = 0u64;
        loop {
            let (keys, next) = idx.scan(cursor, 7, 0, |_| true);
            seen.extend(keys);
            cursor = next;
            if cursor == 0 {
                break;
            }
        }
        assert_eq!(seen.len(), 50);
    }

    #[test]
    fn scan_filter_applied() {
        let mut idx = NsIndex::new();
        for i in 0..10u8 {
            idx.insert(
                Bytes::copy_from_slice(&[b'a' + i]),
                IndexEntry::new(0, 0, 1, 0),
                None,
            );
        }
        let mut seen: Vec<Bytes> = Vec::new();
        let mut cursor = 0u64;
        loop {
            let (keys, next) = idx.scan(cursor, 100, 0, |k| k[0] >= b'e');
            seen.extend(keys);
            cursor = next;
            if cursor == 0 {
                break;
            }
        }
        assert_eq!(seen.len(), 6);
    }

    #[test]
    fn scan_skips_expired() {
        let mut idx = NsIndex::new();
        idx.insert(b("live"), IndexEntry::new(0, 0, 1, 0), None);
        idx.insert(b("dead"), IndexEntry::new(0, 0, 1, 0), Some(50));
        let (keys, _next) = idx.scan(0, 100, 100, |_| true);
        let strs: Vec<&[u8]> = keys.iter().map(|k| k.as_ref()).collect();
        assert!(strs.contains(&b"live".as_ref()));
        assert!(!strs.contains(&b"dead".as_ref()));
    }
}

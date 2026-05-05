use std::collections::BTreeMap;

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
    map: BTreeMap<Bytes, IndexEntry>,
    /// TTL sidecar — only TTL'd keys pay extra memory. FxHashMap for O(1) point lookups.
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
            map: BTreeMap::new(),
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
        if self.map.insert(key, entry).is_none() {
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

    /// Walk keys starting after `cursor` (exclusive lower bound), yielding up to
    /// `count` live keys that pass `filter`. Returns `(yielded, next_cursor)`.
    ///
    /// `cursor = None` starts from the beginning of the keyspace.
    /// `cursor = Some(key)` resumes strictly after `key`.
    /// `next_cursor = None` signals scan complete; `Some(key)` is the exclusive
    /// lower bound for the next page call.
    ///
    /// Because the cursor is a key rather than a position index, it is stable
    /// across concurrent inserts and deletes: keys present for the full duration
    /// of a scan appear exactly once. Keys inserted after the cursor position may
    /// appear; keys deleted before their position is reached will not.
    pub fn scan<F>(
        &self,
        cursor: Option<&[u8]>,
        count: usize,
        now_ms: u64,
        mut filter: F,
    ) -> (Vec<Bytes>, Option<Bytes>)
    where
        F: FnMut(&[u8]) -> bool,
    {
        use std::collections::Bound;
        let count = count.max(1);
        let mut yielded: Vec<Bytes> = Vec::with_capacity(count.min(4096));

        let start = match cursor {
            None => Bound::Unbounded,
            Some(key) => Bound::Excluded(key),
        };

        let has_ttl = !self.ttl.is_empty();
        for (k, _v) in self.map.range::<[u8], _>((start, Bound::Unbounded)) {
            if has_ttl && self.ttl.get(k).copied().map_or(false, |ms| ms <= now_ms) {
                continue;
            }
            if !filter(k) {
                continue;
            }
            yielded.push(k.clone());
            if yielded.len() >= count {
                break;
            }
        }

        let next = if yielded.len() >= count {
            yielded.last().cloned()
        } else {
            None
        };

        (yielded, next)
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
        let mut cursor: Option<Bytes> = None;
        loop {
            let (keys, next) = idx.scan(cursor.as_deref(), 7, 0, |_| true);
            seen.extend(keys);
            cursor = next;
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(seen.len(), 50);
    }

    #[test]
    fn scan_no_duplicates_or_gaps_in_static_map() {
        // Key-based cursor must yield each key exactly once when the map is stable.
        let mut idx = NsIndex::new();
        for i in 0..100u8 {
            idx.insert(
                Bytes::copy_from_slice(format!("key-{i:03}").as_bytes()),
                IndexEntry::new(0, 0, 1, 0),
                None,
            );
        }
        let mut seen: Vec<Bytes> = Vec::new();
        let mut cursor: Option<Bytes> = None;
        loop {
            let (keys, next) = idx.scan(cursor.as_deref(), 11, 0, |_| true);
            seen.extend(keys);
            cursor = next;
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(seen.len(), 100, "no gaps");
        let mut deduped = seen.clone();
        deduped.dedup();
        assert_eq!(deduped.len(), seen.len(), "no duplicates");
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
        let mut cursor: Option<Bytes> = None;
        loop {
            let (keys, next) = idx.scan(cursor.as_deref(), 100, 0, |k| k[0] >= b'e');
            seen.extend(keys);
            cursor = next;
            if cursor.is_none() {
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
        let (keys, _next) = idx.scan(None, 100, 100, |_| true);
        let strs: Vec<&[u8]> = keys.iter().map(|k| k.as_ref()).collect();
        assert!(strs.contains(&b"live".as_ref()));
        assert!(!strs.contains(&b"dead".as_ref()));
    }

    #[test]
    fn live_count_tracks_insert_overwrite_remove() {
        let mut idx = NsIndex::new();
        assert_eq!(idx.live_len(), 0);
        idx.insert(b("a"), IndexEntry::new(0, 0, 1, 1), None);
        assert_eq!(idx.live_len(), 1);
        idx.insert(b("b"), IndexEntry::new(0, 1, 1, 2), None);
        assert_eq!(idx.live_len(), 2);
        // Overwrite: count must not increase.
        idx.insert(b("a"), IndexEntry::new(0, 2, 1, 3), None);
        assert_eq!(idx.live_len(), 2);
        idx.remove(b"a");
        assert_eq!(idx.live_len(), 1);
        idx.remove(b"b");
        assert_eq!(idx.live_len(), 0);
        // Removing a non-existent key must not underflow.
        idx.remove(b"missing");
        assert_eq!(idx.live_len(), 0);
    }

    #[test]
    fn scan_after_deletion_excludes_removed_key() {
        let mut idx = NsIndex::new();
        idx.insert(b("keep"), IndexEntry::new(0, 0, 1, 1), None);
        idx.insert(b("gone"), IndexEntry::new(0, 1, 1, 2), None);
        idx.remove(b"gone");
        let (keys, _) = idx.scan(None, 100, 0, |_| true);
        assert!(keys.contains(&b("keep")));
        assert!(!keys.contains(&b("gone")));
    }

    #[test]
    fn scan_count_zero_clamped_to_one() {
        let mut idx = NsIndex::new();
        idx.insert(b("k"), IndexEntry::new(0, 0, 1, 1), None);
        // count=0 must be clamped to 1 so scan always makes progress.
        let (keys, _) = idx.scan(None, 0, 0, |_| true);
        assert_eq!(keys.len(), 1);
    }
}

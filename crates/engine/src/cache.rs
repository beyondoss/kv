use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::sync::Arc;

use bytes::Bytes;
use rustc_hash::{FxHashMap, FxHashSet};

struct CacheEntry {
    value: Bytes,
    expires_at_ms: Option<u64>,
    metadata: Option<Arc<serde_json::Value>>,
    freq: Cell<u8>, // capped at 1
    size: usize,
    revision: u64,
}

struct Slot {
    key: Bytes,
}

/// S3-FIFO in-memory cache.
///
/// Not `Send` or `Sync` — designed to live entirely on one worker thread behind `Rc`.
/// Uses `Cell`/`RefCell` for interior mutability; no locks needed.
///
/// Eviction: Small queue (10%) + Main queue (90%) + ghost set.
/// New entries enter Small. Entries accessed while in Small are promoted to Main.
/// Entries evicted from Small are tracked in the ghost set; if re-inserted before
/// the ghost entry is displaced they go directly to Main, preventing churn.
pub struct MemCache {
    entries: RefCell<FxHashMap<Bytes, CacheEntry>>,
    small: RefCell<VecDeque<Slot>>,
    main: RefCell<VecDeque<Slot>>,
    /// Hash set for O(1) ghost membership checks. Stores full keys to avoid hash collision.
    ghost: RefCell<FxHashSet<Bytes>>,
    /// Insertion-ordered queue for bounded eviction of ghost entries.
    ghost_queue: RefCell<VecDeque<Bytes>>,
    /// Maximum number of ghost entries (~10% of estimated total capacity).
    ghost_max: usize,
    current_bytes: Cell<usize>,
    max_bytes: usize,
    /// Slots left in Small/Main by `remove()`. Compacted when they dominate queue length.
    stale_slots: Cell<usize>,
}

impl MemCache {
    pub fn new(max_bytes: usize) -> Self {
        // Bound ghost to ~10% of estimated entry count (assuming ≥64 bytes per entry).
        let ghost_max = (max_bytes / 640).max(64);
        Self {
            entries: RefCell::new(FxHashMap::default()),
            small: RefCell::new(VecDeque::new()),
            main: RefCell::new(VecDeque::new()),
            ghost: RefCell::new(FxHashSet::default()),
            ghost_queue: RefCell::new(VecDeque::new()),
            ghost_max,
            current_bytes: Cell::new(0),
            max_bytes,
            stale_slots: Cell::new(0),
        }
    }

    /// Returns `(value, expires_at_ms, metadata, revision)` if the key exists and is not expired.
    #[must_use]
    #[allow(clippy::type_complexity)]
    pub fn get(
        &self,
        key: &[u8],
        now_ms: u64,
    ) -> Option<(Bytes, Option<u64>, Option<Arc<serde_json::Value>>, u64)> {
        let entries = self.entries.borrow();
        let entry = entries.get(key)?;

        if entry.expires_at_ms.is_some_and(|ms| ms <= now_ms) {
            let size = entry.size;
            drop(entries);
            self.entries.borrow_mut().remove(key);
            self.current_bytes
                .set(self.current_bytes.get().saturating_sub(size));
            return None;
        }

        entry.freq.set(1);
        let value = entry.value.clone();
        let expires_at_ms = entry.expires_at_ms;
        let metadata = entry.metadata.clone(); // Arc clone: two atomic increments
        let revision = entry.revision;
        Some((value, expires_at_ms, metadata, revision))
    }

    /// Update an existing entry in-place using a borrowed composite key.
    /// Returns `true` if the key was found and updated; `false` if absent.
    /// Call `insert` with an owned key when this returns `false`.
    /// Avoids allocating the composite cache key on the common overwrite path.
    pub fn try_update(
        &self,
        key: &[u8],
        value: Bytes,
        expires_at_ms: Option<u64>,
        metadata: Option<Arc<serde_json::Value>>,
        meta_size: usize,
        revision: u64,
    ) -> bool {
        let size = (key.len() + value.len() + meta_size).max(1);
        let mut entries = self.entries.borrow_mut();
        let Some(e) = entries.get_mut(key) else {
            return false;
        };
        let old_size = e.size;
        e.freq.set(1);
        e.value = value;
        e.expires_at_ms = expires_at_ms;
        e.metadata = metadata;
        e.size = size;
        e.revision = revision;
        let cur = self.current_bytes.get();
        self.current_bytes.set(cur.saturating_sub(old_size) + size);
        drop(entries);
        self.evict_to_limit();
        true
    }

    pub fn insert(
        &self,
        key: Bytes,
        value: Bytes,
        expires_at_ms: Option<u64>,
        metadata: Option<Arc<serde_json::Value>>,
        meta_size: usize,
        revision: u64,
    ) {
        let size = (key.len() + value.len() + meta_size).max(1);

        // Update in-place if already present
        {
            let mut entries = self.entries.borrow_mut();
            if let Some(e) = entries.get_mut(key.as_ref()) {
                let old_size = e.size;
                e.freq.set(1);
                e.value = value;
                e.expires_at_ms = expires_at_ms;
                e.metadata = metadata;
                e.size = size;
                e.revision = revision;
                let cur = self.current_bytes.get();
                self.current_bytes.set(cur.saturating_sub(old_size) + size);
                drop(entries);
                self.evict_to_limit();
                return;
            }
        }

        let in_main = self.ghost.borrow().contains(key.as_ref());

        let entry = CacheEntry {
            value,
            expires_at_ms,
            metadata,
            freq: Cell::new(0),
            size,
            revision,
        };

        self.entries.borrow_mut().insert(key.clone(), entry);
        self.current_bytes.set(self.current_bytes.get() + size);

        let slot = Slot { key };
        if in_main {
            self.ghost_remove(&slot.key);
            self.main.borrow_mut().push_back(slot);
        } else {
            self.small.borrow_mut().push_back(slot);
        }

        self.evict_to_limit();
    }

    pub fn remove(&self, key: &[u8]) {
        // Bind the result before the if-let so the RefMut drops here, not inside the block.
        let removed = self.entries.borrow_mut().remove(key);
        if let Some(entry) = removed {
            self.current_bytes
                .set(self.current_bytes.get().saturating_sub(entry.size));
            let stale = self.stale_slots.get() + 1;
            self.stale_slots.set(stale);
            // Compact when stale slots dominate total queue length to bound memory growth.
            let queue_len = self.small.borrow().len() + self.main.borrow().len();
            if stale > queue_len / 2 {
                self.compact_queues();
            }
        }
    }

    /// Remove all entries whose key starts with `prefix`. More efficient than
    /// calling `remove` per-key for namespace flushes: one pass over the entry
    /// map, one compaction if needed.
    pub fn remove_by_prefix(&self, prefix: &[u8]) {
        let to_remove: Vec<Bytes> = self
            .entries
            .borrow()
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect();
        if to_remove.is_empty() {
            return;
        }
        let mut freed = 0usize;
        {
            let mut entries = self.entries.borrow_mut();
            for k in &to_remove {
                if let Some(e) = entries.remove(k) {
                    freed += e.size;
                }
            }
        }
        if freed > 0 {
            self.current_bytes
                .set(self.current_bytes.get().saturating_sub(freed));
        }
        let stale = self.stale_slots.get() + to_remove.len();
        self.stale_slots.set(stale);
        let queue_len = self.small.borrow().len() + self.main.borrow().len();
        if stale > queue_len / 2 {
            self.compact_queues();
        }
    }

    /// Remove stale slots (keys no longer in `entries`) from both queues.
    fn compact_queues(&self) {
        let entries = self.entries.borrow();
        self.small
            .borrow_mut()
            .retain(|s| entries.contains_key(s.key.as_ref()));
        self.main
            .borrow_mut()
            .retain(|s| entries.contains_key(s.key.as_ref()));
        self.stale_slots.set(0);
    }

    /// Remove all keys expiring at or before `now_ms`. Called by background sweeper.
    pub fn sweep_expired(&self, now_ms: u64) {
        let mut freed = 0usize;
        let mut freed_count = 0usize;
        self.entries.borrow_mut().retain(|_, v| {
            if v.expires_at_ms.is_some_and(|ms| ms <= now_ms) {
                freed += v.size;
                freed_count += 1;
                false
            } else {
                true
            }
        });
        if freed > 0 {
            self.current_bytes
                .set(self.current_bytes.get().saturating_sub(freed));
        }
        if freed_count > 0 {
            let stale = self.stale_slots.get() + freed_count;
            self.stale_slots.set(stale);
            let queue_len = self.small.borrow().len() + self.main.borrow().len();
            if stale > queue_len / 2 {
                self.compact_queues();
            }
        }
    }

    pub fn len(&self) -> usize {
        self.entries.borrow().len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.borrow().is_empty()
    }

    pub fn bytes_used(&self) -> usize {
        self.current_bytes.get()
    }

    /// Insert a key into the bounded ghost set, evicting the oldest entry if at capacity.
    fn ghost_insert(&self, key: Bytes) {
        let mut set = self.ghost.borrow_mut();
        let mut queue = self.ghost_queue.borrow_mut();
        // Drain stale queue entries (already removed from set) and enforce the cap.
        while set.len() >= self.ghost_max {
            match queue.pop_front() {
                Some(old) => {
                    set.remove(&old);
                }
                None => break,
            }
        }
        if set.insert(key.clone()) {
            queue.push_back(key);
        }
    }

    /// Remove a key from the ghost set. The corresponding queue entry becomes stale
    /// and will be cleaned up lazily during the next `ghost_insert`.
    fn ghost_remove(&self, key: &[u8]) {
        self.ghost.borrow_mut().remove(key);
    }

    fn evict_to_limit(&self) {
        while self.current_bytes.get() > self.max_bytes {
            if !self.evict_one() {
                break;
            }
        }
    }

    /// Evict one entry. Returns `true` if an entry was evicted.
    fn evict_one(&self) -> bool {
        // Phase 1: drain Small until we evict one or Small is empty
        loop {
            let slot = match self.small.borrow_mut().pop_front() {
                Some(s) => s,
                None => break,
            };

            let entries = self.entries.borrow();
            match entries.get(slot.key.as_ref()) {
                None => continue, // stale slot (key was deleted)
                Some(entry) => {
                    if entry.freq.get() > 0 {
                        // Accessed since insertion — promote to Main
                        entry.freq.set(0);
                        drop(entries);
                        self.main.borrow_mut().push_back(slot);
                        continue;
                    } else {
                        // Cold — evict; record in ghost
                        let size = entry.size;
                        let key = slot.key;
                        drop(entries);
                        self.entries.borrow_mut().remove(key.as_ref());
                        self.ghost_insert(key);
                        self.current_bytes
                            .set(self.current_bytes.get().saturating_sub(size));
                        return true;
                    }
                }
            }
        }

        // Phase 2: evict from Main
        loop {
            let slot = match self.main.borrow_mut().pop_front() {
                Some(s) => s,
                None => return false,
            };

            let entries = self.entries.borrow();
            match entries.get(slot.key.as_ref()) {
                None => continue, // stale
                Some(entry) => {
                    if entry.freq.get() > 0 {
                        // Give one more chance
                        entry.freq.set(0);
                        drop(entries);
                        self.main.borrow_mut().push_back(slot);
                        continue;
                    } else {
                        let size = entry.size;
                        drop(entries);
                        self.entries.borrow_mut().remove(slot.key.as_ref());
                        self.current_bytes
                            .set(self.current_bytes.get().saturating_sub(size));
                        return true;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(s: &str) -> Bytes {
        Bytes::from(s.to_owned())
    }

    #[test]
    fn insert_and_get() {
        let cache = MemCache::new(1024);
        cache.insert(b("k"), b("v"), None, None, 0, 0);
        let (val, exp, meta, _rev) = cache.get(b"k", 0).unwrap();
        assert_eq!(val, b("v"));
        assert_eq!(exp, None);
        assert_eq!(meta, None);
    }

    #[test]
    fn get_missing_returns_none() {
        let cache = MemCache::new(1024);
        assert!(cache.get(b"missing", 0).is_none());
    }

    #[test]
    fn remove() {
        let cache = MemCache::new(1024);
        cache.insert(b("k"), b("v"), None, None, 0, 0);
        cache.remove(b"k");
        assert!(cache.get(b"k", 0).is_none());
        assert!(cache.is_empty());
    }

    #[test]
    fn expiry_lazy_on_get() {
        let cache = MemCache::new(1024);
        cache.insert(b("k"), b("v"), Some(100), None, 0, 0);
        assert!(cache.get(b"k", 50).is_some()); // not yet expired
        assert!(cache.get(b"k", 100).is_none()); // expired (ms <= now)
        assert!(cache.is_empty());
    }

    #[test]
    fn sweep_expired() {
        let cache = MemCache::new(4096);
        cache.insert(b("a"), b("1"), Some(100), None, 0, 0);
        cache.insert(b("b"), b("2"), Some(200), None, 0, 0);
        cache.insert(b("c"), b("3"), None, None, 0, 0);
        cache.sweep_expired(150);
        assert!(cache.get(b"a", 0).is_none());
        assert!(cache.get(b"b", 0).is_some());
        assert!(cache.get(b"c", 0).is_some());
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn in_place_update() {
        let cache = MemCache::new(1024);
        cache.insert(b("key"), b("v1"), None, None, 0, 1);
        assert_eq!(cache.len(), 1);
        cache.insert(b("key"), b("v_new"), Some(999), None, 0, 2);
        let (val, exp, _, rev) = cache.get(b"key", 0).unwrap();
        assert_eq!(val, b("v_new"));
        assert_eq!(exp, Some(999));
        assert_eq!(rev, 2);
        assert_eq!(cache.len(), 1);
        // key(3) + value(5) = 8
        assert_eq!(cache.bytes_used(), 8);
    }

    #[test]
    fn bytes_used_includes_key_size() {
        let cache = MemCache::new(1_000_000);
        let key = Bytes::from(vec![b'k'; 100]);
        let val = Bytes::from(vec![b'v'; 50]);
        cache.insert(key, val, None, None, 0, 0);
        assert_eq!(cache.bytes_used(), 150);
    }

    #[test]
    fn bytes_used_includes_metadata_size() {
        let cache = MemCache::new(1_000_000);
        let meta = serde_json::json!({"x": 1});
        let meta_bytes = serde_json::to_vec(&meta).unwrap();
        cache.insert(
            b("k"),
            b("v"),
            None,
            Some(Arc::new(meta)),
            meta_bytes.len(),
            0,
        );
        // key(1) + value(1) + serialized_meta
        assert_eq!(cache.bytes_used(), 1 + 1 + meta_bytes.len());
    }

    #[test]
    fn capacity_is_respected() {
        let cache = MemCache::new(100);
        for i in 0u8..20 {
            let k = Bytes::from(vec![i; 5]);
            let v = Bytes::from(vec![i; 5]);
            cache.insert(k, v, None, None, 0, 0);
        }
        assert!(
            cache.bytes_used() <= 100,
            "bytes_used {} exceeded max_bytes 100",
            cache.bytes_used()
        );
    }

    #[test]
    fn hot_small_entry_promoted_to_main_on_eviction() {
        // 5 entries of 20 bytes each = exactly 100 bytes (at capacity).
        // Access key[0] so its freq=1 — it must be promoted to Main, not evicted.
        let cache = MemCache::new(100);
        for i in 0u8..5 {
            cache.insert(
                Bytes::from(vec![i; 10]),
                Bytes::from(vec![i; 10]),
                None,
                None,
                0,
                0,
            );
        }
        let _ = cache.get(&[0u8; 10], 0); // set freq=1
        // Inserting 5 more forces eviction; key[0] should survive via promotion
        for i in 5u8..10 {
            cache.insert(
                Bytes::from(vec![i; 10]),
                Bytes::from(vec![i; 10]),
                None,
                None,
                0,
                0,
            );
        }
        assert!(
            cache.get(&[0u8; 10], 0).is_some(),
            "hot key should survive eviction via Small→Main promotion"
        );
    }

    #[test]
    fn ghost_re_insertion_goes_directly_to_main() {
        // Fill to capacity (5 entries × 20 bytes = 100 bytes).
        // Key[0] is never accessed — it will be evicted cold into the ghost set.
        // Re-inserting key[0] should place it in Main (ghost hit), not Small.
        let cache = MemCache::new(100);
        for i in 0u8..5 {
            cache.insert(
                Bytes::from(vec![i; 10]),
                Bytes::from(vec![i; 10]),
                None,
                None,
                0,
                0,
            );
        }
        // Insert 5 more — key[0] is evicted cold and lands in ghost
        for i in 5u8..10 {
            cache.insert(
                Bytes::from(vec![i; 10]),
                Bytes::from(vec![i; 10]),
                None,
                None,
                0,
                0,
            );
        }
        assert!(
            cache.get(&[0u8; 10], 0).is_none(),
            "key[0] should have been evicted"
        );
        // Re-insert key[0] — ghost hit means it targets Main, surviving where Small entry would not
        let new_val = Bytes::from(vec![99u8; 10]);
        cache.insert(
            Bytes::from(vec![0u8; 10]),
            new_val.clone(),
            None,
            None,
            0,
            0,
        );
        let (val, _, _, _) = cache.get(&[0u8; 10], 0).unwrap();
        assert_eq!(val, new_val);
    }

    #[test]
    fn ghost_bounded_by_ghost_max() {
        // ghost_max = (50 / 640).max(64) = 64
        let cache = MemCache::new(50);
        for i in 0u16..500 {
            let k = i.to_le_bytes().to_vec();
            cache.insert(Bytes::from(k), Bytes::from(vec![1u8]), None, None, 0, 0);
        }
        let ghost_len = cache.ghost.borrow().len();
        assert!(
            ghost_len <= cache.ghost_max,
            "ghost set size {} exceeded ghost_max {}",
            ghost_len,
            cache.ghost_max
        );
    }

    #[test]
    fn is_empty() {
        let cache = MemCache::new(1024);
        assert!(cache.is_empty());
        cache.insert(b("k"), b("v"), None, None, 0, 0);
        assert!(!cache.is_empty());
        cache.remove(b"k");
        assert!(cache.is_empty());
    }

    #[test]
    fn metadata_round_trip() {
        let cache = MemCache::new(4096);
        let meta = serde_json::json!({"score": 42, "tags": ["a", "b"]});
        cache.insert(b("k"), b("v"), None, Some(Arc::new(meta.clone())), 0, 0);
        let (_, _, got_meta, _) = cache.get(b"k", 0).unwrap();
        assert_eq!(got_meta.as_deref(), Some(&meta));
    }

    // --- remove_by_prefix ---

    #[test]
    fn remove_by_prefix_removes_matching() {
        let cache = MemCache::new(4096);
        cache.insert(b("user:alice"), b("1"), None, None, 0, 0);
        cache.insert(b("user:bob"), b("2"), None, None, 0, 0);
        cache.insert(b("session:x"), b("3"), None, None, 0, 0);
        cache.remove_by_prefix(b"user:");
        assert!(cache.get(b"user:alice", 0).is_none());
        assert!(cache.get(b"user:bob", 0).is_none());
        assert!(cache.get(b"session:x", 0).is_some());
    }

    #[test]
    fn remove_by_prefix_empty_prefix_removes_all() {
        let cache = MemCache::new(4096);
        cache.insert(b("a"), b("1"), None, None, 0, 0);
        cache.insert(b("b"), b("2"), None, None, 0, 0);
        cache.remove_by_prefix(b"");
        assert!(cache.is_empty());
    }

    #[test]
    fn remove_by_prefix_no_match_is_noop() {
        let cache = MemCache::new(4096);
        cache.insert(b("foo"), b("bar"), None, None, 0, 0);
        cache.remove_by_prefix(b"zzz");
        assert!(cache.get(b"foo", 0).is_some());
    }

    #[test]
    fn remove_by_prefix_on_empty_cache_is_noop() {
        let cache = MemCache::new(4096);
        cache.remove_by_prefix(b"any:");
        assert!(cache.is_empty());
    }
}

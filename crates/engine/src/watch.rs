use bytes::Bytes;
use futures_channel::mpsc::{Receiver, Sender, channel};
use rustc_hash::FxHashMap;

/// Capacity for per-subscriber watch channels. A slow subscriber that fills
/// its buffer is pruned (same as a disconnected subscriber) rather than
/// allowed to grow without bound.
const WATCH_CHANNEL_CAPACITY: usize = 512;

#[derive(Debug, Clone)]
pub enum WatchEvent {
    Set {
        key: Bytes,
        value: Bytes,
        metadata: Option<serde_json::Value>,
        expires_at_ms: Option<u64>,
        revision: u64,
    },
    Del {
        key: Bytes,
        revision: u64,
    },
}

pub enum KeyFilter<'a> {
    Exact(&'a [u8]),
    Prefix(&'a [u8]),
}

impl<'a> KeyFilter<'a> {
    pub fn matches(&self, key: &[u8]) -> bool {
        match self {
            KeyFilter::Exact(k) => *k == key,
            KeyFilter::Prefix(p) => key.starts_with(p),
        }
    }
}

pub struct WatchRegistry {
    keys: FxHashMap<(Bytes, Bytes), Vec<Sender<WatchEvent>>>,
    prefixes: Vec<((Bytes, Bytes), Sender<WatchEvent>)>,
}

impl WatchRegistry {
    pub fn new() -> Self {
        Self {
            keys: FxHashMap::default(),
            prefixes: Vec::new(),
        }
    }

    pub fn subscribe_key(&mut self, ns: Bytes, key: Bytes) -> Receiver<WatchEvent> {
        // Prune dead senders for this key before inserting the new one.
        if let Some(senders) = self.keys.get_mut(&(ns.clone(), key.clone())) {
            senders.retain(|tx| !tx.is_closed());
        }
        let (tx, rx) = channel(WATCH_CHANNEL_CAPACITY);
        self.keys.entry((ns, key)).or_default().push(tx);
        rx
    }

    pub fn subscribe_prefix(&mut self, ns: Bytes, prefix: Bytes) -> Receiver<WatchEvent> {
        // Prune dead prefix senders before inserting the new one.
        self.prefixes.retain(|(_, tx)| !tx.is_closed());
        let (tx, rx) = channel(WATCH_CHANNEL_CAPACITY);
        self.prefixes.push(((ns, prefix), tx));
        rx
    }

    pub fn notify(&mut self, ns: &str, key: &[u8], event: WatchEvent) {
        if self.keys.is_empty() && self.prefixes.is_empty() {
            return;
        }
        let ns_b = Bytes::copy_from_slice(ns.as_bytes());
        let key_b = Bytes::copy_from_slice(key);

        if let Some(senders) = self.keys.get_mut(&(ns_b.clone(), key_b.clone())) {
            senders.retain_mut(|tx| tx.try_send(event.clone()).is_ok());
            if senders.is_empty() {
                self.keys.remove(&(ns_b, key_b));
            }
        }

        self.prefixes.retain_mut(|((wns, prefix), tx)| {
            if wns.as_ref() == ns.as_bytes() && key.starts_with(prefix.as_ref()) {
                tx.try_send(event.clone()).is_ok()
            } else {
                true
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set_event(key: &[u8]) -> WatchEvent {
        WatchEvent::Set {
            key: Bytes::copy_from_slice(key),
            value: Bytes::from_static(b"v"),
            metadata: None,
            expires_at_ms: None,
            revision: 1,
        }
    }

    fn del_event(key: &[u8]) -> WatchEvent {
        WatchEvent::Del {
            key: Bytes::copy_from_slice(key),
            revision: 2,
        }
    }

    #[test]
    fn exact_key_receives_event() {
        let mut reg = WatchRegistry::new();
        let mut rx = reg.subscribe_key(Bytes::from_static(b"ns"), Bytes::from_static(b"k"));
        reg.notify("ns", b"k", set_event(b"k"));
        assert!(matches!(rx.try_recv().unwrap(), WatchEvent::Set { .. }));
    }

    #[test]
    fn exact_key_ignores_other_keys() {
        let mut reg = WatchRegistry::new();
        let mut rx = reg.subscribe_key(Bytes::from_static(b"ns"), Bytes::from_static(b"k"));
        reg.notify("ns", b"other", set_event(b"other"));
        assert!(rx.try_recv().is_err(), "channel should be empty");
    }

    #[test]
    fn exact_key_ignores_other_namespaces() {
        let mut reg = WatchRegistry::new();
        let mut rx = reg.subscribe_key(Bytes::from_static(b"ns1"), Bytes::from_static(b"k"));
        reg.notify("ns2", b"k", set_event(b"k"));
        assert!(rx.try_recv().is_err(), "channel should be empty");
    }

    #[test]
    fn prefix_receives_matching_keys() {
        let mut reg = WatchRegistry::new();
        let mut rx = reg.subscribe_prefix(Bytes::from_static(b"ns"), Bytes::from_static(b"cfg/"));
        reg.notify("ns", b"cfg/a", set_event(b"cfg/a"));
        reg.notify("ns", b"cfg/b", del_event(b"cfg/b"));
        reg.notify("ns", b"other", set_event(b"other")); // no match

        assert!(rx.try_recv().is_ok()); // cfg/a
        assert!(rx.try_recv().is_ok()); // cfg/b
        assert!(rx.try_recv().is_err(), "other should be filtered"); // other filtered
    }

    #[test]
    fn dead_exact_sender_pruned() {
        let mut reg = WatchRegistry::new();
        let rx = reg.subscribe_key(Bytes::from_static(b"ns"), Bytes::from_static(b"k"));
        drop(rx);
        // First notify prunes the dead sender.
        reg.notify("ns", b"k", set_event(b"k"));
        // Registry should no longer hold an entry for this key.
        assert!(
            !reg.keys
                .contains_key(&(Bytes::from_static(b"ns"), Bytes::from_static(b"k")))
        );
    }

    #[test]
    fn dead_prefix_sender_pruned() {
        let mut reg = WatchRegistry::new();
        let rx = reg.subscribe_prefix(Bytes::from_static(b"ns"), Bytes::from_static(b"cfg/"));
        drop(rx);
        reg.notify("ns", b"cfg/x", set_event(b"cfg/x"));
        assert!(reg.prefixes.is_empty());
    }

    #[test]
    fn multiple_subscribers_same_key() {
        let mut reg = WatchRegistry::new();
        let mut rx1 = reg.subscribe_key(Bytes::from_static(b"ns"), Bytes::from_static(b"k"));
        let mut rx2 = reg.subscribe_key(Bytes::from_static(b"ns"), Bytes::from_static(b"k"));
        reg.notify("ns", b"k", set_event(b"k"));
        assert!(rx1.try_recv().is_ok());
        assert!(rx2.try_recv().is_ok());
    }
}

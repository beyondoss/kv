use std::sync::Arc;

use bytes::Bytes;
use futures_channel::mpsc::{Receiver, Sender, channel};
use rustc_hash::FxHashMap;

use crate::error::{EngineError, Result};

/// Capacity for per-subscriber watch channels. A slow subscriber that fills
/// its buffer is pruned (same as a disconnected subscriber) rather than
/// allowed to grow without bound.
const WATCH_CHANNEL_CAPACITY: usize = 512;

/// Hard cap on the number of live subscriptions (exact keys + prefixes) a
/// single registry will hold. Dead senders are pruned lazily on `notify` and
/// on each `subscribe_*`; this cap bounds the worst case where a client
/// registers many distinct keys faster than pruning reclaims them, preventing
/// unbounded growth of the `keys`/`prefixes` collections on the shard thread.
const MAX_TOTAL_SUBSCRIPTIONS: usize = 65_536;

#[derive(Debug, Clone)]
pub enum WatchEvent {
    Set {
        key: Bytes,
        value: Bytes,
        metadata: Option<Arc<serde_json::Value>>,
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

impl Default for WatchRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl WatchRegistry {
    pub fn new() -> Self {
        Self {
            keys: FxHashMap::default(),
            prefixes: Vec::new(),
        }
    }

    pub fn subscribe_key(&mut self, ns: Bytes, key: Bytes) -> Result<Receiver<WatchEvent>> {
        // Prune dead senders for this key before inserting the new one.
        if let Some(senders) = self.keys.get_mut(&(ns.clone(), key.clone())) {
            senders.retain(|tx| !tx.is_closed());
        }
        self.ensure_capacity()?;
        let (tx, rx) = channel(WATCH_CHANNEL_CAPACITY);
        self.keys.entry((ns, key)).or_default().push(tx);
        Ok(rx)
    }

    pub fn subscribe_prefix(&mut self, ns: Bytes, prefix: Bytes) -> Result<Receiver<WatchEvent>> {
        // Prune dead prefix senders before inserting the new one.
        self.prefixes.retain(|(_, tx)| !tx.is_closed());
        self.ensure_capacity()?;
        let (tx, rx) = channel(WATCH_CHANNEL_CAPACITY);
        self.prefixes.push(((ns, prefix), tx));
        Ok(rx)
    }

    /// Total live subscriptions across exact keys and prefixes.
    fn total_subscriptions(&self) -> usize {
        self.keys.values().map(Vec::len).sum::<usize>() + self.prefixes.len()
    }

    /// Reject a new subscription only if the registry is genuinely full. The
    /// cheap count runs first; only when it trips do we pay for a full prune of
    /// dead senders and re-count, so ordinary subscriber churn (disconnects that
    /// haven't been pruned yet) never produces a false capacity error.
    fn ensure_capacity(&mut self) -> Result<()> {
        if self.total_subscriptions() < MAX_TOTAL_SUBSCRIPTIONS {
            return Ok(());
        }
        self.keys.retain(|_, senders| {
            senders.retain(|tx| !tx.is_closed());
            !senders.is_empty()
        });
        self.prefixes.retain(|(_, tx)| !tx.is_closed());
        if self.total_subscriptions() >= MAX_TOTAL_SUBSCRIPTIONS {
            return Err(EngineError::CapacityExceeded {
                reason: "watch subscription limit reached",
            });
        }
        Ok(())
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
        let mut rx = reg
            .subscribe_key(Bytes::from_static(b"ns"), Bytes::from_static(b"k"))
            .unwrap();
        reg.notify("ns", b"k", set_event(b"k"));
        assert!(matches!(rx.try_recv().unwrap(), WatchEvent::Set { .. }));
    }

    #[test]
    fn exact_key_ignores_other_keys() {
        let mut reg = WatchRegistry::new();
        let mut rx = reg
            .subscribe_key(Bytes::from_static(b"ns"), Bytes::from_static(b"k"))
            .unwrap();
        reg.notify("ns", b"other", set_event(b"other"));
        assert!(rx.try_recv().is_err(), "channel should be empty");
    }

    #[test]
    fn exact_key_ignores_other_namespaces() {
        let mut reg = WatchRegistry::new();
        let mut rx = reg
            .subscribe_key(Bytes::from_static(b"ns1"), Bytes::from_static(b"k"))
            .unwrap();
        reg.notify("ns2", b"k", set_event(b"k"));
        assert!(rx.try_recv().is_err(), "channel should be empty");
    }

    #[test]
    fn prefix_receives_matching_keys() {
        let mut reg = WatchRegistry::new();
        let mut rx = reg
            .subscribe_prefix(Bytes::from_static(b"ns"), Bytes::from_static(b"cfg/"))
            .unwrap();
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
        let rx = reg
            .subscribe_key(Bytes::from_static(b"ns"), Bytes::from_static(b"k"))
            .unwrap();
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
        let rx = reg
            .subscribe_prefix(Bytes::from_static(b"ns"), Bytes::from_static(b"cfg/"))
            .unwrap();
        drop(rx);
        reg.notify("ns", b"cfg/x", set_event(b"cfg/x"));
        assert!(reg.prefixes.is_empty());
    }

    #[test]
    fn multiple_subscribers_same_key() {
        let mut reg = WatchRegistry::new();
        let mut rx1 = reg
            .subscribe_key(Bytes::from_static(b"ns"), Bytes::from_static(b"k"))
            .unwrap();
        let mut rx2 = reg
            .subscribe_key(Bytes::from_static(b"ns"), Bytes::from_static(b"k"))
            .unwrap();
        reg.notify("ns", b"k", set_event(b"k"));
        assert!(rx1.try_recv().is_ok());
        assert!(rx2.try_recv().is_ok());
    }

    #[test]
    fn subscription_cap_rejects_when_full_but_reclaims_dead_first() {
        let mut reg = WatchRegistry::new();
        // Fill the registry to the cap with distinct live keys.
        let mut live = Vec::with_capacity(MAX_TOTAL_SUBSCRIPTIONS);
        for i in 0..MAX_TOTAL_SUBSCRIPTIONS {
            let key = Bytes::from(format!("k{i}"));
            live.push(
                reg.subscribe_key(Bytes::from_static(b"ns"), key)
                    .expect("under cap"),
            );
        }
        assert_eq!(reg.total_subscriptions(), MAX_TOTAL_SUBSCRIPTIONS);

        // At the cap, one more is rejected.
        assert!(matches!(
            reg.subscribe_key(Bytes::from_static(b"ns"), Bytes::from_static(b"overflow")),
            Err(EngineError::CapacityExceeded { .. })
        ));

        // Drop one receiver: its sender is now dead. The next subscribe must
        // reclaim it instead of falsely rejecting.
        live.pop();
        let rx = reg.subscribe_key(Bytes::from_static(b"ns"), Bytes::from_static(b"after-drop"));
        assert!(rx.is_ok(), "dead sender should have been reclaimed");
        assert_eq!(reg.total_subscriptions(), MAX_TOTAL_SUBSCRIPTIONS);
    }
}

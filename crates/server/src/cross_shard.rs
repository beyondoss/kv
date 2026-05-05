//! Cross-shard request transport.
//!
//! Each shard owns one inbound `Receiver<CrossShardRequest>` and shares a
//! `Arc<[Sender<...>]>` with every other shard so multi-key commands whose keys
//! span shards can fan out, gather sub-results, and return a single response in
//! original key order.
//!
//! Errors crossing shards are converted to `String` because `EngineError`
//! carries `std::io::Error` which is not always cheaply cloneable across
//! threads. The originator wraps the message back into a RESP error.

use std::rc::Rc;
use std::sync::Arc;

use beyond_kv_engine::store::ShardStore;
use beyond_kv_engine::types::{Entry, ScanPage};
use beyond_kv_engine::watch::{KeyFilter, WatchEvent};
use bytes::Bytes;
use futures_channel::mpsc::{self, Receiver, Sender};
use futures_channel::oneshot;
use futures_util::StreamExt;

/// Channel capacity per shard inbox. Sized generously: backpressure on a hot
/// foreign shard manifests as a slow `await` on the originating connection,
/// which is the desired behavior — but a tight bound would amplify latency for
/// any client touching that shard.
pub const CROSS_SHARD_CHAN_BOUND: usize = 1024;

pub type ShardSenders = Arc<[Sender<CrossShardRequest>]>;

/// MGET sub-result: each entry pairs its original index with the looked-up value.
pub type MGetReply = Result<Vec<(usize, Option<Entry>)>, String>;

/// Owned version of `KeyFilter<'_>` that can be sent across thread boundaries.
pub enum OwnedKeyFilter {
    Exact(Bytes),
    Prefix(Bytes),
}

impl OwnedKeyFilter {
    pub fn as_filter(&self) -> KeyFilter<'_> {
        match self {
            Self::Exact(k) => KeyFilter::Exact(k),
            Self::Prefix(p) => KeyFilter::Prefix(p),
        }
    }
}

pub enum CrossShardRequest {
    MGet {
        ns: String,
        keys: Vec<(usize, Bytes)>,
        reply: oneshot::Sender<MGetReply>,
    },
    MSet {
        ns: String,
        pairs: Vec<(Bytes, Bytes)>,
        reply: oneshot::Sender<Result<(), String>>,
    },
    Del {
        ns: String,
        keys: Vec<Bytes>,
        reply: oneshot::Sender<Result<u64, String>>,
    },
    Exists {
        ns: String,
        keys: Vec<Bytes>,
        reply: oneshot::Sender<Result<u64, String>>,
    },
    DbSize {
        ns: String,
        reply: oneshot::Sender<Result<u64, String>>,
    },
    FlushDb {
        ns: String,
        reply: oneshot::Sender<Result<(), String>>,
    },
    /// Scan a single page on this shard. Used by the cross-shard SCAN fan-out.
    Scan {
        ns: String,
        cursor: Bytes,
        pattern: Option<Bytes>,
        count: u64,
        reply: oneshot::Sender<Result<ScanPage, String>>,
    },
    /// Return all matching keys from this shard. Used by cross-shard KEYS fan-out.
    AllKeys {
        ns: String,
        pattern: Option<Bytes>,
        reply: oneshot::Sender<Result<Vec<Bytes>, String>>,
    },
    /// Register a watch subscription on this shard and return the initial state
    /// plus a live event channel. Used for cross-shard WATCH/PWATCH fan-out.
    WatchSubscribe {
        ns: String,
        filter: OwnedKeyFilter,
        since: u64,
        reply: oneshot::Sender<Result<(Vec<WatchEvent>, Receiver<WatchEvent>), String>>,
    },
}

/// Build one channel per shard. `txs[i]` routes requests to shard `i`'s `rxs[i]`.
pub fn build_channels(
    n: usize,
) -> (
    Vec<Sender<CrossShardRequest>>,
    Vec<Receiver<CrossShardRequest>>,
) {
    let mut txs = Vec::with_capacity(n);
    let mut rxs = Vec::with_capacity(n);
    for _ in 0..n {
        let (tx, rx) = mpsc::channel(CROSS_SHARD_CHAN_BOUND);
        txs.push(tx);
        rxs.push(rx);
    }
    (txs, rxs)
}

/// Drain this shard's inbox, spawning each request as its own task so a slow
/// store op (e.g. cold MGET reads through io_uring) does not block the next
/// inbound request behind it.
pub async fn serve(store: Rc<ShardStore>, mut rx: Receiver<CrossShardRequest>) {
    while let Some(req) = rx.next().await {
        let store = store.clone();
        monoio::spawn(async move {
            handle(store, req).await;
        });
    }
}

async fn handle(store: Rc<ShardStore>, req: CrossShardRequest) {
    match req {
        CrossShardRequest::MGet { ns, keys, reply } => {
            let refs: Vec<&[u8]> = keys.iter().map(|(_, k)| k.as_ref()).collect();
            let res = store
                .mget(&ns, &refs)
                .await
                .map(|entries| {
                    keys.iter()
                        .map(|(idx, _)| *idx)
                        .zip(entries)
                        .collect::<Vec<_>>()
                })
                .map_err(|e| e.to_string());
            let _ = reply.send(res);
        }
        CrossShardRequest::MSet { ns, pairs, reply } => {
            let res = store.mset(&ns, &pairs).await.map_err(|e| e.to_string());
            let _ = reply.send(res);
        }
        CrossShardRequest::Del { ns, keys, reply } => {
            let refs: Vec<&[u8]> = keys.iter().map(|k| k.as_ref()).collect();
            let res = store.del(&ns, &refs).await.map_err(|e| e.to_string());
            let _ = reply.send(res);
        }
        CrossShardRequest::Exists { ns, keys, reply } => {
            let refs: Vec<&[u8]> = keys.iter().map(|k| k.as_ref()).collect();
            let res = store.exists(&ns, &refs).await.map_err(|e| e.to_string());
            let _ = reply.send(res);
        }
        CrossShardRequest::DbSize { ns, reply } => {
            let res = store.db_size(&ns).await.map_err(|e| e.to_string());
            let _ = reply.send(res);
        }
        CrossShardRequest::FlushDb { ns, reply } => {
            let res = store.flush_db(&ns).await.map_err(|e| e.to_string());
            let _ = reply.send(res);
        }
        CrossShardRequest::Scan {
            ns,
            cursor,
            pattern,
            count,
            reply,
        } => {
            let res = store
                .scan(&ns, &cursor, pattern.as_deref(), count)
                .await
                .map_err(|e| e.to_string());
            let _ = reply.send(res);
        }
        CrossShardRequest::AllKeys { ns, pattern, reply } => {
            let mut all: Vec<Bytes> = Vec::new();
            let mut cursor = Bytes::from_static(b"0");
            let res = loop {
                match store.scan(&ns, &cursor, pattern.as_deref(), 512).await {
                    Err(e) => break Err(e.to_string()),
                    Ok(page) => {
                        let done = page.next_cursor == b"0".as_ref();
                        all.extend(page.keys);
                        cursor = page.next_cursor;
                        if done {
                            break Ok(all);
                        }
                    }
                }
            };
            let _ = reply.send(res);
        }
        CrossShardRequest::WatchSubscribe {
            ns,
            filter,
            since,
            reply,
        } => {
            let res = store
                .watch_subscribe(&ns, filter.as_filter(), since)
                .await
                .map_err(|e| e.to_string());
            let _ = reply.send(res);
        }
    }
}

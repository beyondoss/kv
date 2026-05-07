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

use std::os::unix::net::UnixStream as StdUnixStream;
use std::rc::Rc;
use std::sync::Arc;
use std::task::Poll;

use beyond_kv_engine::store::ShardStore;
use beyond_kv_engine::types::{Entry, ScanPage, SetOptions};
use beyond_kv_engine::watch::{KeyFilter, WatchEvent};
use bytes::Bytes;
use futures_channel::mpsc::{self, Receiver, Sender};
use futures_channel::oneshot;
use futures_util::StreamExt;
use monoio::io::AsyncReadRent;

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
    /// Set a single key with full options on a foreign shard.
    Set {
        ns: String,
        key: Bytes,
        value: Bytes,
        opts: SetOptions,
        reply: oneshot::Sender<Result<(), String>>,
    },
    /// Atomically increment a counter on a foreign shard.
    Incr {
        ns: String,
        key: Bytes,
        delta: i64,
        reply: oneshot::Sender<Result<i64, String>>,
    },
    /// Conditionally delete a key by revision on a foreign shard.
    DelRev {
        ns: String,
        key: Bytes,
        revision: u64,
        reply: oneshot::Sender<Result<Option<()>, String>>,
    },
    /// Set a key only if it does not exist on a foreign shard.
    SetNx {
        ns: String,
        key: Bytes,
        value: Bytes,
        opts: SetOptions,
        reply: oneshot::Sender<Result<bool, String>>,
    },
    /// Set a key only if it already exists on a foreign shard.
    SetXx {
        ns: String,
        key: Bytes,
        value: Bytes,
        opts: SetOptions,
        reply: oneshot::Sender<Result<bool, String>>,
    },
    /// Compare-and-swap write on a foreign shard.
    SetRev {
        ns: String,
        key: Bytes,
        value: Bytes,
        opts: SetOptions,
        revision: u64,
        reply: oneshot::Sender<Result<Option<u64>, String>>,
    },
    /// Atomically get-then-delete a key on a foreign shard.
    GetDel {
        ns: String,
        key: Bytes,
        orig_idx: usize,
        reply: oneshot::Sender<Result<(usize, Option<Entry>), String>>,
    },
    /// Register a watch subscription on this shard and return the initial state
    /// plus a live event channel. Used for cross-shard WATCH/PWATCH fan-out.
    WatchSubscribe {
        ns: String,
        filter: OwnedKeyFilter,
        since: u64,
        #[allow(clippy::type_complexity)]
        reply: oneshot::Sender<Result<(Vec<WatchEvent>, Receiver<WatchEvent>), String>>,
    },
}

/// Build one channel + wakeup pipe per shard.
///
/// Returns `(txs, wakeup_writes, rxs, wakeup_reads)`:
/// - `txs[i]` and `wakeup_writes[i]` go into the shared `Arc<[_]>` so any
///   shard can send a request and interrupt shard `i`'s io_uring sleep.
/// - `rxs[i]` and `wakeup_reads[i]` go to worker `i`'s `serve()` call.
#[allow(clippy::type_complexity)]
pub fn build_channels(
    n: usize,
) -> (
    Vec<Sender<CrossShardRequest>>,
    Vec<StdUnixStream>,
    Vec<Receiver<CrossShardRequest>>,
    Vec<StdUnixStream>,
) {
    let mut txs = Vec::with_capacity(n);
    let mut wake_writes = Vec::with_capacity(n);
    let mut rxs = Vec::with_capacity(n);
    let mut wake_reads = Vec::with_capacity(n);
    for _ in 0..n {
        let (tx, rx) = mpsc::channel(CROSS_SHARD_CHAN_BOUND);
        let (wake_read, wake_write) =
            StdUnixStream::pair().expect("cross-shard wakeup unix socket pair");
        // Non-blocking so callers in async context never block on a full pipe.
        // A WouldBlock result means a wakeup is already queued — that's fine.
        wake_write
            .set_nonblocking(true)
            .expect("cross-shard wakeup set_nonblocking");
        txs.push(tx);
        wake_writes.push(wake_write);
        rxs.push(rx);
        wake_reads.push(wake_read);
    }
    (txs, wake_writes, rxs, wake_reads)
}

/// Drain this shard's cross-shard inbox using the same wakeup-pipe pattern as
/// the accept loop. The wakeup pipe read-end is registered with io_uring so
/// that a byte written by a remote shard actually interrupts `io_uring_enter`
/// — bare futures wakers do NOT wake a sleeping monoio thread.
pub async fn serve(
    store: Rc<ShardStore>,
    mut rx: Receiver<CrossShardRequest>,
    wakeup_read: StdUnixStream,
) {
    if let Err(e) = wakeup_read.set_nonblocking(true) {
        tracing::error!("cross-shard wakeup set_nonblocking failed: {e}");
        return;
    }
    let mut wakeup = match monoio::net::UnixStream::from_std(wakeup_read) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!("failed to register cross-shard wakeup stream: {e}");
            return;
        }
    };
    let waker = std::task::Waker::noop();
    let mut cx = std::task::Context::from_waker(waker);
    let mut buf = vec![0u8; 64];
    loop {
        let res;
        (res, buf) = wakeup.read(buf).await;
        match res {
            Ok(0) | Err(_) => break,
            Ok(_) => {}
        }
        loop {
            match rx.poll_next_unpin(&mut cx) {
                Poll::Ready(Some(req)) => {
                    let store = store.clone();
                    monoio::spawn(async move {
                        handle(store, req).await;
                    });
                }
                Poll::Ready(None) => return, // all senders dropped → shutdown
                Poll::Pending => break,      // inbox empty, wait for next wakeup
            }
        }
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
        CrossShardRequest::Set {
            ns,
            key,
            value,
            opts,
            reply,
        } => {
            let res = store
                .set(&ns, &key, value, opts)
                .await
                .map_err(|e| e.to_string());
            let _ = reply.send(res);
        }
        CrossShardRequest::Incr {
            ns,
            key,
            delta,
            reply,
        } => {
            let res = store
                .incr(&ns, &key, delta)
                .await
                .map_err(|e| e.to_string());
            let _ = reply.send(res);
        }
        CrossShardRequest::DelRev {
            ns,
            key,
            revision,
            reply,
        } => {
            let res = store
                .delrev(&ns, &key, revision)
                .await
                .map_err(|e| e.to_string());
            let _ = reply.send(res);
        }
        CrossShardRequest::SetNx {
            ns,
            key,
            value,
            opts,
            reply,
        } => {
            let res = store
                .setnx(&ns, &key, value, opts)
                .await
                .map_err(|e| e.to_string());
            let _ = reply.send(res);
        }
        CrossShardRequest::SetXx {
            ns,
            key,
            value,
            opts,
            reply,
        } => {
            let res = store
                .setxx(&ns, &key, value, opts)
                .await
                .map_err(|e| e.to_string());
            let _ = reply.send(res);
        }
        CrossShardRequest::SetRev {
            ns,
            key,
            value,
            opts,
            revision,
            reply,
        } => {
            let res = store
                .setrev(&ns, &key, value, opts, revision)
                .await
                .map_err(|e| e.to_string());
            let _ = reply.send(res);
        }
        CrossShardRequest::GetDel {
            ns,
            key,
            orig_idx,
            reply,
        } => {
            let res = store
                .getdel(&ns, &key)
                .await
                .map(|e| (orig_idx, e))
                .map_err(|e| e.to_string());
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

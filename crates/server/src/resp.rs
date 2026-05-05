use std::net::SocketAddr;
use std::os::unix::net::UnixStream as StdUnixStream;
use std::rc::Rc;
use std::sync::{Arc, mpsc};
use std::time::Duration;

use beyond_kv_engine::store::{DEFAULT_NS, ShardStore};
use beyond_kv_engine::watch::{KeyFilter, WatchEvent};
use beyond_kv_proto::command::Command;
use beyond_resp::{RespCodec, Value};
use bytes::Bytes;
use futures_channel::mpsc::Receiver;
use futures_util::StreamExt as FuturesStreamExt;
use futures_util::stream::SelectAll;
use monoio::net::TcpStream;
use monoio_codec::Framed;

use crate::cross_shard::{CrossShardRequest, OwnedKeyFilter, ShardSenders};
use crate::dispatch::dispatch;
use crate::routing::shard_for_key;

pub struct ConnState {
    pub ns: String,
    pub resp_version: u8,
    pub quit: bool,
    pub shard_idx: usize,
    pub n_shards: usize,
    pub cross_shard_txs: Option<ShardSenders>,
    /// One write-end per shard. Written after each cross-shard `try_send` to
    /// interrupt the target shard's `io_uring_enter` sleep.
    pub cross_shard_wakeups: Option<Arc<[StdUnixStream]>>,
}

impl Default for ConnState {
    fn default() -> Self {
        Self {
            ns: DEFAULT_NS.to_string(),
            resp_version: 2,
            quit: false,
            shard_idx: 0,
            n_shards: 1,
            cross_shard_txs: None,
            cross_shard_wakeups: None,
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn serve(
    store: Rc<ShardStore>,
    rx: mpsc::Receiver<(std::net::TcpStream, SocketAddr)>,
    wakeup_read: StdUnixStream,
    max_conns: usize,
    idle_timeout: Duration,
    shard_idx: usize,
    n_shards: usize,
    cross_shard_txs: ShardSenders,
    cross_shard_wakeups: Arc<[StdUnixStream]>,
) {
    crate::serve_loop(rx, wakeup_read, max_conns, "RESP", |s, _peer, guard| {
        let store = store.clone();
        let cross_shard_txs = cross_shard_txs.clone();
        let cross_shard_wakeups = cross_shard_wakeups.clone();
        monoio::spawn(async move {
            let _guard = guard;
            handle_conn(
                s,
                store,
                idle_timeout,
                shard_idx,
                n_shards,
                cross_shard_txs,
                cross_shard_wakeups,
            )
            .await;
        });
    })
    .await;
}

async fn handle_conn(
    stream: TcpStream,
    store: Rc<ShardStore>,
    idle_timeout: Duration,
    shard_idx: usize,
    n_shards: usize,
    cross_shard_txs: ShardSenders,
    cross_shard_wakeups: Arc<[StdUnixStream]>,
) {
    let mut framed = Framed::new(stream, RespCodec::resp2());
    let mut state = ConnState {
        shard_idx,
        n_shards,
        cross_shard_txs: Some(cross_shard_txs),
        cross_shard_wakeups: Some(cross_shard_wakeups),
        ..ConnState::default()
    };

    loop {
        use monoio::io::sink::Sink;
        use monoio::io::stream::Stream;

        let value = match monoio::time::timeout(idle_timeout, framed.next()).await {
            Ok(Some(Ok(v))) => v,
            Ok(Some(Err(e))) => {
                tracing::debug!("decode error: {e}");
                break;
            }
            Ok(None) => break,
            Err(_elapsed) => {
                tracing::debug!("RESP connection idle timeout");
                break;
            }
        };

        let is_hello = matches!(value, Value::Array(ref v) if v.first()
            .and_then(|v| if let Value::BulkString(b) = v { Some(b) } else { None })
            .map(|b| b.eq_ignore_ascii_case(b"HELLO"))
            .unwrap_or(false));

        let cmd = match Command::parse(value) {
            Ok(cmd) => cmd,
            Err(e) => {
                let resp = beyond_kv_proto::response::error("ERR", &e.to_string());
                let _ = framed.send(resp).await;
                let _ = <_ as Sink<Value>>::flush(&mut framed).await;
                continue;
            }
        };

        // WATCH/PWATCH take over the connection for streaming — intercept before dispatch.
        match &cmd {
            Command::Watch { keys, since } => {
                if state.resp_version < 3 {
                    let err = beyond_kv_proto::response::error(
                        "WRONGTYPE",
                        "WATCH requires RESP3 (send HELLO 3 first)",
                    );
                    let _ = framed.send(err).await;
                    let _ = <_ as Sink<Value>>::flush(&mut framed).await;
                    break;
                }
                run_key_watch_loop(&mut framed, &store, &state, keys.clone(), *since).await;
                break;
            }
            Command::PWatch { prefix, since } => {
                if state.resp_version < 3 {
                    let err = beyond_kv_proto::response::error(
                        "WRONGTYPE",
                        "PWATCH requires RESP3 (send HELLO 3 first)",
                    );
                    let _ = framed.send(err).await;
                    let _ = <_ as Sink<Value>>::flush(&mut framed).await;
                    break;
                }
                run_prefix_watch_loop(&mut framed, &store, &state, prefix.clone(), *since).await;
                break;
            }
            _ => {}
        }

        let response = {
            let resp = dispatch(cmd, &store, &mut state).await;
            if is_hello {
                framed.codec_mut().set_version(match state.resp_version {
                    3 => beyond_resp::Version::Resp3,
                    _ => beyond_resp::Version::Resp2,
                });
            }
            resp
        };

        if framed.send(response).await.is_err() {
            break;
        }
        if framed.read_buffer().is_empty() && <_ as Sink<Value>>::flush(&mut framed).await.is_err()
        {
            break;
        }

        if state.quit {
            break;
        }
    }
}

async fn watch_subscribe_remote(
    txs: &[futures_channel::mpsc::Sender<CrossShardRequest>],
    wakeups: Option<&[StdUnixStream]>,
    target: usize,
    ns: String,
    filter: OwnedKeyFilter,
    since: u64,
) -> Result<(Vec<WatchEvent>, Receiver<WatchEvent>), String> {
    use std::io::Write as _;
    let (reply_tx, reply_rx) = futures_channel::oneshot::channel();
    let req = CrossShardRequest::WatchSubscribe {
        ns,
        filter,
        since,
        reply: reply_tx,
    };
    txs[target]
        .clone()
        .try_send(req)
        .map_err(|e| e.to_string())?;
    if let Some(w) = wakeups.and_then(|ws| ws.get(target)) {
        let _ = (&*w).write_all(&[1u8]);
    }
    reply_rx.await.map_err(|e| e.to_string())?
}

async fn run_key_watch_loop(
    framed: &mut Framed<TcpStream, RespCodec>,
    store: &ShardStore,
    state: &ConnState,
    keys: Vec<Bytes>,
    since: Option<u64>,
) {
    use monoio::io::sink::Sink;

    let since_rev = since.unwrap_or(0);
    let mut merged: SelectAll<Receiver<WatchEvent>> = SelectAll::new();

    for key in &keys {
        let target = shard_for_key(key, state.n_shards);
        let result = if target == state.shard_idx || state.n_shards <= 1 {
            store
                .watch_subscribe(&state.ns, KeyFilter::Exact(key), since_rev)
                .await
                .map_err(|e| e.to_string())
        } else if let Some(txs) = state.cross_shard_txs.as_deref() {
            watch_subscribe_remote(
                txs,
                state.cross_shard_wakeups.as_deref(),
                target,
                state.ns.clone(),
                OwnedKeyFilter::Exact(key.clone()),
                since_rev,
            )
            .await
        } else {
            store
                .watch_subscribe(&state.ns, KeyFilter::Exact(key), since_rev)
                .await
                .map_err(|e| e.to_string())
        };

        match result {
            Ok((initial, rx)) => {
                for event in &initial {
                    if framed.send(event_to_push(event)).await.is_err() {
                        return;
                    }
                }
                merged.push(rx);
            }
            Err(e) => {
                let err = beyond_kv_proto::response::error("ERR", &e);
                let _ = framed.send(err).await;
                let _ = <_ as Sink<Value>>::flush(framed).await;
                return;
            }
        }
    }

    // Flush all initial events, then signal live stream start.
    if <_ as Sink<Value>>::flush(framed).await.is_err() {
        return;
    }
    let ready = Value::Push(vec![
        Value::BulkString(Bytes::from_static(b"watch")),
        Value::BulkString(Bytes::from_static(b"ready")),
    ]);
    if framed.send(ready).await.is_err() {
        return;
    }
    if <_ as Sink<Value>>::flush(framed).await.is_err() {
        return;
    }

    watch_stream_loop(framed, merged).await;
}

async fn run_prefix_watch_loop(
    framed: &mut Framed<TcpStream, RespCodec>,
    store: &ShardStore,
    state: &ConnState,
    prefix: Bytes,
    since: Option<u64>,
) {
    use monoio::io::sink::Sink;

    let since_rev = since.unwrap_or(0);
    let mut merged: SelectAll<Receiver<WatchEvent>> = SelectAll::new();
    let mut had_error = false;

    for shard in 0..state.n_shards {
        let result = if shard == state.shard_idx || state.n_shards <= 1 {
            store
                .watch_subscribe(&state.ns, KeyFilter::Prefix(&prefix), since_rev)
                .await
                .map_err(|e| e.to_string())
        } else if let Some(txs) = state.cross_shard_txs.as_deref() {
            watch_subscribe_remote(
                txs,
                state.cross_shard_wakeups.as_deref(),
                shard,
                state.ns.clone(),
                OwnedKeyFilter::Prefix(prefix.clone()),
                since_rev,
            )
            .await
        } else {
            break; // single-shard, already handled above
        };

        match result {
            Ok((initial, rx)) => {
                for event in &initial {
                    if framed.send(event_to_push(event)).await.is_err() {
                        had_error = true;
                        break;
                    }
                }
                if had_error {
                    return;
                }
                merged.push(rx);
            }
            Err(e) => {
                let err = beyond_kv_proto::response::error("ERR", &e);
                let _ = framed.send(err).await;
                let _ = <_ as Sink<Value>>::flush(framed).await;
                return;
            }
        }

        if state.n_shards <= 1 {
            break; // single-shard fast path: only one iteration needed
        }
    }

    if <_ as Sink<Value>>::flush(framed).await.is_err() {
        return;
    }
    let ready = Value::Push(vec![
        Value::BulkString(Bytes::from_static(b"watch")),
        Value::BulkString(Bytes::from_static(b"ready")),
    ]);
    if framed.send(ready).await.is_err() {
        return;
    }
    if <_ as Sink<Value>>::flush(framed).await.is_err() {
        return;
    }

    watch_stream_loop(framed, merged).await;
}

async fn watch_stream_loop(
    framed: &mut Framed<TcpStream, RespCodec>,
    mut rx: SelectAll<Receiver<WatchEvent>>,
) {
    use monoio::io::sink::Sink;
    use monoio::io::stream::Stream;

    loop {
        monoio::select! {
            frame = framed.next() => {
                match frame {
                    Some(Ok(v)) if is_unwatch(&v) => break,
                    Some(Ok(_)) => {} // ignore other frames in watch mode
                    _ => break,
                }
            }
            result = monoio::time::timeout(
                Duration::from_secs(30),
                FuturesStreamExt::next(&mut rx),
            ) => {
                match result {
                    Ok(Some(event)) => {
                        if send_watch_push(framed, &event).await.is_err() {
                            break;
                        }
                    }
                    Ok(None) => break, // all senders dropped (store shutdown)
                    Err(_timeout) => {
                        // Heartbeat so proxies and clients don't time out the connection.
                        let hb = Value::Push(vec![
                            Value::BulkString(Bytes::from_static(b"watch")),
                            Value::BulkString(Bytes::from_static(b"heartbeat")),
                        ]);
                        if framed.send(hb).await.is_err() {
                            break;
                        }
                        if <_ as Sink<Value>>::flush(framed).await.is_err() {
                            break;
                        }
                    }
                }
            }
        }
    }
}

fn is_unwatch(value: &Value) -> bool {
    let elems = match value {
        Value::Array(v) | Value::Push(v) => v,
        _ => return false,
    };
    elems
        .first()
        .and_then(|v| {
            if let Value::BulkString(b) = v {
                Some(b)
            } else {
                None
            }
        })
        .map(|b| b.eq_ignore_ascii_case(b"UNWATCH"))
        .unwrap_or(false)
}

async fn send_watch_push(
    framed: &mut Framed<TcpStream, RespCodec>,
    event: &WatchEvent,
) -> Result<(), ()> {
    use monoio::io::sink::Sink;

    framed.send(event_to_push(event)).await.map_err(|_| ())?;
    <_ as Sink<Value>>::flush(framed).await.map_err(|_| ())
}

fn event_to_push(event: &WatchEvent) -> Value {
    match event {
        WatchEvent::Set {
            key,
            value,
            revision,
            ..
        } => Value::Push(vec![
            Value::BulkString(Bytes::from_static(b"watch")),
            Value::BulkString(Bytes::from_static(b"set")),
            Value::BulkString(key.clone()),
            Value::BulkString(value.clone()),
            Value::BulkString(Bytes::from(revision.to_string())),
        ]),
        WatchEvent::Del { key, revision } => Value::Push(vec![
            Value::BulkString(Bytes::from_static(b"watch")),
            Value::BulkString(Bytes::from_static(b"del")),
            Value::BulkString(key.clone()),
            Value::BulkString(Bytes::new()),
            Value::BulkString(Bytes::from(revision.to_string())),
        ]),
    }
}

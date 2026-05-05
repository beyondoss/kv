use std::time::Duration;

use beyond_kv_engine::store::{DEFAULT_NS, ShardStore};
use beyond_kv_engine::types::{Entry, SetOptions};
use beyond_kv_proto::command::{Command, GetExTtl, SetArgs, SetCondition, SetTtl};
use beyond_kv_proto::response::{self as r};
use beyond_resp::Value;
use bytes::Bytes;
use futures_channel::oneshot;
use futures_util::future::join_all;
use futures_util::sink::SinkExt;

use crate::cross_shard::{CrossShardRequest, MGetReply};
use crate::resp::ConnState;
use crate::routing::shard_for_key;

/// Byte prefix that distinguishes a multi-shard SCAN continuation cursor from
/// a plain "0" (start/done) sentinel. Format: `\x02 + [shard: u8] + [per-shard cursor]`.
const SCAN_CURSOR_PREFIX: u8 = 0x02;

const KEYS_SCAN_LIMIT: usize = 1_000_000;

pub async fn dispatch(cmd: Command, store: &ShardStore, state: &mut ConnState) -> Value {
    tracing::debug!(cmd = cmd_name(&cmd), ns = %state.ns);
    match cmd {
        Command::Ping { message } => message
            .map(r::bulk)
            .unwrap_or_else(|| Value::SimpleString(bytes::Bytes::from_static(b"PONG"))),

        Command::Hello { version } => {
            let v = version.unwrap_or(2).clamp(2, 3);
            state.resp_version = v;
            r::hello_reply(v)
        }

        Command::Select { db } => {
            state.ns = beyond_kv_engine::store::ns_name(db);
            r::ok()
        }

        Command::BgRewriteAof => match store.reclaim(&state.ns).await {
            Ok(()) => Value::SimpleString(bytes::Bytes::from_static(
                b"Background append only file rewriting started",
            )),
            Err(e) => r::error("ERR", &e.to_string()),
        },

        Command::Quit => {
            state.quit = true;
            r::ok()
        }

        Command::Reset => {
            // RESP spec: RESET clears per-connection state and replies "+RESET",
            // but does NOT close the connection (unlike QUIT). Preserve routing
            // fields and cross-shard transport — the connection's shard pinning
            // and the shared sender array are not "per-session" state.
            state.ns = DEFAULT_NS.to_string();
            state.resp_version = 2;
            state.quit = false;
            Value::SimpleString(Bytes::from_static(b"RESET"))
        }

        Command::Get { key } => match store.get(&state.ns, &key).await {
            Ok(Some(entry)) => r::bulk(entry.value),
            Ok(None) => r::nil(),
            Err(e) => r::error("ERR", &e.to_string()),
        },

        Command::Set { key, value, args } => {
            let opts = set_opts_from_args(&args);

            // Handle NX / XX / REV conditions
            match args.condition {
                SetCondition::Nx => match store.setnx(&state.ns, &key, value, opts).await {
                    Ok(true) => r::ok(),
                    Ok(false) => r::nil(),
                    Err(e) => r::error("ERR", &e.to_string()),
                },
                SetCondition::Xx => match store.setxx(&state.ns, &key, value, opts).await {
                    Ok(true) => r::ok(),
                    Ok(false) => r::nil(),
                    Err(e) => r::error("ERR", &e.to_string()),
                },
                SetCondition::Rev(expected) => {
                    match store.setrev(&state.ns, &key, value, opts, expected).await {
                        Ok(Some(new_rev)) => r::integer(new_rev as i64),
                        Ok(None) => r::error("CONFLICT", "revision mismatch"),
                        Err(e) => r::error("ERR", &e.to_string()),
                    }
                }
                SetCondition::Always => {
                    if args.get {
                        match store.getset(&state.ns, &key, value).await {
                            Ok(Some(old)) => r::bulk(old.value),
                            Ok(None) => r::nil(),
                            Err(e) => r::error("ERR", &e.to_string()),
                        }
                    } else {
                        match store.set(&state.ns, &key, value, opts).await {
                            Ok(()) => r::ok(),
                            Err(e) => r::error("ERR", &e.to_string()),
                        }
                    }
                }
            }
        }

        Command::Del { keys } => match dispatch_del(keys, store, state).await {
            Ok(n) => r::integer(n as i64),
            Err(e) => r::error("ERR", &e),
        },

        Command::Exists { keys } => match dispatch_exists(keys, store, state).await {
            Ok(n) => r::integer(n as i64),
            Err(e) => r::error("ERR", &e),
        },

        Command::Expire { key, secs } => {
            match store
                .expire(&state.ns, &key, Duration::from_secs(secs))
                .await
            {
                Ok(true) => r::integer(1),
                Ok(false) => r::integer(0),
                Err(e) => r::error("ERR", &e.to_string()),
            }
        }

        Command::PExpire { key, millis } => {
            match store
                .expire(&state.ns, &key, Duration::from_millis(millis))
                .await
            {
                Ok(true) => r::integer(1),
                Ok(false) => r::integer(0),
                Err(e) => r::error("ERR", &e.to_string()),
            }
        }

        Command::ExpireAt { key, unix_secs } => {
            let now_secs = now_unix_secs();
            if unix_secs <= now_secs {
                match store.del(&state.ns, &[key.as_ref()]).await {
                    Ok(_) => r::integer(1),
                    Err(e) => r::error("ERR", &e.to_string()),
                }
            } else {
                let ttl = Duration::from_secs(unix_secs - now_secs);
                match store.expire(&state.ns, &key, ttl).await {
                    Ok(true) => r::integer(1),
                    Ok(false) => r::integer(0),
                    Err(e) => r::error("ERR", &e.to_string()),
                }
            }
        }

        Command::PExpireAt { key, unix_millis } => {
            let now_ms = now_unix_ms();
            if unix_millis <= now_ms {
                match store.del(&state.ns, &[key.as_ref()]).await {
                    Ok(_) => r::integer(1),
                    Err(e) => r::error("ERR", &e.to_string()),
                }
            } else {
                let ttl = Duration::from_millis(unix_millis - now_ms);
                match store.expire(&state.ns, &key, ttl).await {
                    Ok(true) => r::integer(1),
                    Ok(false) => r::integer(0),
                    Err(e) => r::error("ERR", &e.to_string()),
                }
            }
        }

        Command::Ttl { key } => match store.ttl(&state.ns, &key).await {
            Ok(beyond_kv_engine::types::TtlResult::Remaining(s)) => r::integer(s as i64),
            Ok(beyond_kv_engine::types::TtlResult::NoExpiry) => r::integer(-1),
            Ok(beyond_kv_engine::types::TtlResult::NotFound) => r::integer(-2),
            Err(e) => r::error("ERR", &e.to_string()),
        },

        Command::PTtl { key } => match store.pttl(&state.ns, &key).await {
            Ok(beyond_kv_engine::types::TtlResult::Remaining(ms)) => r::integer(ms as i64),
            Ok(beyond_kv_engine::types::TtlResult::NoExpiry) => r::integer(-1),
            Ok(beyond_kv_engine::types::TtlResult::NotFound) => r::integer(-2),
            Err(e) => r::error("ERR", &e.to_string()),
        },

        Command::Persist { key } => match store.persist(&state.ns, &key).await {
            Ok(true) => r::integer(1),
            Ok(false) => r::integer(0),
            Err(e) => r::error("ERR", &e.to_string()),
        },

        Command::MGet { keys } => match dispatch_mget(keys, store, state).await {
            Ok(entries) => {
                let values: Vec<Value> = entries
                    .into_iter()
                    .map(|opt| match opt {
                        Some(entry) => r::bulk(entry.value),
                        None => r::nil(),
                    })
                    .collect();
                r::array(values)
            }
            Err(e) => r::error("ERR", &e),
        },

        Command::MSet { pairs } => match dispatch_mset(pairs, store, state).await {
            Ok(()) => r::ok(),
            Err(e) => r::error("ERR", &e),
        },

        Command::GetSet { key, value } => match store.getset(&state.ns, &key, value).await {
            Ok(Some(old)) => r::bulk(old.value),
            Ok(None) => r::nil(),
            Err(e) => r::error("ERR", &e.to_string()),
        },

        Command::SetNx { key, value } => {
            match store
                .setnx(&state.ns, &key, value, SetOptions::default())
                .await
            {
                Ok(true) => r::integer(1),
                Ok(false) => r::integer(0),
                Err(e) => r::error("ERR", &e.to_string()),
            }
        }

        Command::GetDel { key } => match store.getdel(&state.ns, &key).await {
            Ok(Some(entry)) => r::bulk(entry.value),
            Ok(None) => r::nil(),
            Err(e) => r::error("ERR", &e.to_string()),
        },

        Command::Incr { key } => do_incr(store, &state.ns, &key, 1).await,
        Command::IncrBy { key, delta } => do_incr(store, &state.ns, &key, delta).await,
        Command::Decr { key } => do_incr(store, &state.ns, &key, -1).await,
        Command::DecrBy { key, delta } => do_incr(store, &state.ns, &key, -delta).await,

        Command::GetEx { key, ttl } => {
            use beyond_kv_engine::types::GetExOp;
            let op = ttl.map(|t| match t {
                GetExTtl::Set(spec) => GetExOp::SetTtl(ttl_duration_from_spec(&spec)),
                GetExTtl::Persist => GetExOp::Persist,
            });
            match store.getex(&state.ns, &key, op).await {
                Ok(None) => r::nil(),
                Ok(Some(entry)) => r::bulk(entry.value),
                Err(e) => r::error("ERR", &e.to_string()),
            }
        }

        Command::Keys { pattern } => match dispatch_keys(pattern, store, state).await {
            Ok(keys) => {
                if keys.len() >= KEYS_SCAN_LIMIT {
                    return r::error(
                        "ERR",
                        "KEYS result too large; use SCAN to paginate large keyspaces",
                    );
                }
                r::array(keys.into_iter().map(r::bulk).collect())
            }
            Err(e) => r::error("ERR", &e),
        },

        Command::Scan { cursor, args } => {
            match dispatch_scan(cursor, args.pattern, args.count, store, state).await {
                Ok(page) => r::scan_reply(page.next_cursor, page.keys),
                Err(e) => r::error("ERR", &e),
            }
        }

        Command::DbSize => match dispatch_dbsize(store, state).await {
            Ok(n) => r::integer(n as i64),
            Err(e) => r::error("ERR", &e),
        },

        Command::FlushDb => match dispatch_flushdb(store, state).await {
            Ok(()) => r::ok(),
            Err(e) => r::error("ERR", &e),
        },

        Command::Revision { key } => match store.get(&state.ns, &key).await {
            Ok(Some(entry)) => r::integer(entry.revision as i64),
            Ok(None) => r::integer(-2),
            Err(e) => r::error("ERR", &e.to_string()),
        },

        Command::SetRev {
            key,
            value,
            revision,
            ttl,
        } => {
            let opts = SetOptions {
                ttl: ttl.as_ref().map(ttl_duration_from_spec),
                metadata: None,
                keep_ttl: false,
            };
            match store.setrev(&state.ns, &key, value, opts, revision).await {
                Ok(Some(new_rev)) => r::integer(new_rev as i64),
                Ok(None) => r::error("CONFLICT", "revision mismatch"),
                Err(e) => r::error("ERR", &e.to_string()),
            }
        }

        // Watch commands are intercepted in handle_conn before dispatch reaches here.
        Command::Watch { .. } | Command::PWatch { .. } | Command::Unwatch => r::error(
            "ERR",
            "WATCH must be sent as the first command after HELLO 3",
        ),
    }
}

/// Returns the per-shard senders if fan-out is even possible on this connection.
/// `n_shards == 1` or a missing sender array (test/embedded use) means everything
/// runs on the local shard.
fn fan_out_txs(state: &ConnState) -> Option<&[futures_channel::mpsc::Sender<CrossShardRequest>]> {
    if state.n_shards <= 1 {
        return None;
    }
    state.cross_shard_txs.as_deref()
}

/// Bucket the keys by target shard, preserving each key's original index.
/// Returns `Vec<Option<Vec<(orig_idx, key)>>>` of length `n_shards`.
fn bucket_by_shard(keys: &[Bytes], n_shards: usize) -> Vec<Option<Vec<(usize, Bytes)>>> {
    let mut buckets: Vec<Option<Vec<(usize, Bytes)>>> = (0..n_shards).map(|_| None).collect();
    let approx = keys.len() / n_shards + 1;
    for (i, k) in keys.iter().enumerate() {
        let s = shard_for_key(k.as_ref(), n_shards);
        buckets[s]
            .get_or_insert_with(|| Vec::with_capacity(approx))
            .push((i, k.clone()));
    }
    buckets
}

async fn dispatch_dbsize(store: &ShardStore, state: &ConnState) -> Result<u64, String> {
    let txs = match fan_out_txs(state) {
        None => return store.db_size(&state.ns).await.map_err(|e| e.to_string()),
        Some(txs) => txs,
    };
    let ns = state.ns.clone();
    let futs: Vec<_> = txs
        .iter()
        .map(|tx| {
            let (reply_tx, reply_rx) = oneshot::channel();
            let req = CrossShardRequest::DbSize {
                ns: ns.clone(),
                reply: reply_tx,
            };
            let _ = tx.clone().try_send(req);
            reply_rx
        })
        .collect();
    let results = join_all(futs).await;
    let mut total = 0u64;
    for r in results {
        total += r.map_err(|e| e.to_string())??;
    }
    Ok(total)
}

async fn dispatch_flushdb(store: &ShardStore, state: &ConnState) -> Result<(), String> {
    let txs = match fan_out_txs(state) {
        None => return store.flush_db(&state.ns).await.map_err(|e| e.to_string()),
        Some(txs) => txs,
    };
    let ns = state.ns.clone();
    let futs: Vec<_> = txs
        .iter()
        .map(|tx| {
            let (reply_tx, reply_rx) = oneshot::channel();
            let req = CrossShardRequest::FlushDb {
                ns: ns.clone(),
                reply: reply_tx,
            };
            let _ = tx.clone().try_send(req);
            reply_rx
        })
        .collect();
    let results = join_all(futs).await;
    for r in results {
        r.map_err(|e| e.to_string())??;
    }
    Ok(())
}

async fn dispatch_keys(
    pattern: Option<Bytes>,
    store: &ShardStore,
    state: &ConnState,
) -> Result<Vec<Bytes>, String> {
    let txs = match fan_out_txs(state) {
        None => {
            // Single shard: scan locally.
            let mut all = Vec::new();
            let mut cursor = Bytes::from_static(b"0");
            loop {
                let page = store
                    .scan(&state.ns, &cursor, pattern.as_deref(), 512)
                    .await
                    .map_err(|e| e.to_string())?;
                let done = page.next_cursor == b"0".as_ref();
                all.extend(page.keys);
                cursor = page.next_cursor;
                if done {
                    break;
                }
            }
            return Ok(all);
        }
        Some(txs) => txs,
    };
    let ns = state.ns.clone();
    let futs: Vec<_> = txs
        .iter()
        .map(|tx| {
            let (reply_tx, reply_rx) = oneshot::channel();
            let req = CrossShardRequest::AllKeys {
                ns: ns.clone(),
                pattern: pattern.clone(),
                reply: reply_tx,
            };
            let _ = tx.clone().try_send(req);
            reply_rx
        })
        .collect();
    let results = join_all(futs).await;
    let mut all = Vec::new();
    for r in results {
        all.extend(r.map_err(|e| e.to_string())??);
    }
    Ok(all)
}

/// Scan a single page across shards using a cursor that encodes the target shard.
///
/// Multi-shard cursor format: `\x02 + [shard: u8] + [per-shard cursor bytes]`.
/// The plain `b"0"` sentinel means start at shard 0. The plain `b"0"` return also
/// signals completion. Single-shard deployments use the existing cursor format unchanged.
async fn dispatch_scan(
    cursor: Bytes,
    pattern: Option<Bytes>,
    count: u64,
    store: &ShardStore,
    state: &ConnState,
) -> Result<beyond_kv_engine::types::ScanPage, String> {
    let txs = match fan_out_txs(state) {
        None => {
            return store
                .scan(&state.ns, &cursor, pattern.as_deref(), count)
                .await
                .map_err(|e| e.to_string());
        }
        Some(txs) => txs,
    };

    // Decode cursor → (target_shard, per_shard_cursor).
    let (target_shard, per_shard_cursor) = if cursor.first() == Some(&SCAN_CURSOR_PREFIX) {
        let shard = cursor[1] as usize;
        let inner = cursor.slice(2..);
        (shard, inner)
    } else {
        // b"0" or any other value: start from shard 0.
        (0usize, Bytes::from_static(b"0"))
    };

    // Fetch one page from the target shard.
    let page = if target_shard == state.shard_idx {
        store
            .scan(&state.ns, &per_shard_cursor, pattern.as_deref(), count)
            .await
            .map_err(|e| e.to_string())?
    } else {
        let (reply_tx, reply_rx) = oneshot::channel();
        let req = CrossShardRequest::Scan {
            ns: state.ns.clone(),
            cursor: per_shard_cursor,
            pattern: pattern.clone(),
            count,
            reply: reply_tx,
        };
        let _ = txs[target_shard].clone().try_send(req);
        reply_rx.await.map_err(|e| e.to_string())??
    };

    // Build the outgoing cursor.
    let next = if page.next_cursor == b"0".as_ref() {
        // This shard is exhausted — advance to next shard.
        let next_shard = target_shard + 1;
        if next_shard >= state.n_shards {
            Bytes::from_static(b"0") // all shards done
        } else {
            let mut c = vec![SCAN_CURSOR_PREFIX, next_shard as u8];
            c.extend_from_slice(b"0");
            Bytes::from(c)
        }
    } else {
        let mut c = vec![SCAN_CURSOR_PREFIX, target_shard as u8];
        c.extend_from_slice(&page.next_cursor);
        Bytes::from(c)
    };

    Ok(beyond_kv_engine::types::ScanPage {
        next_cursor: next,
        keys: page.keys,
    })
}

async fn dispatch_mget(
    keys: Vec<Bytes>,
    store: &ShardStore,
    state: &ConnState,
) -> Result<Vec<Option<Entry>>, String> {
    let txs = match fan_out_txs(state) {
        None => {
            // Fast path: single shard, or all keys local by construction.
            let refs: Vec<&[u8]> = keys.iter().map(|k| k.as_ref()).collect();
            return store
                .mget(&state.ns, &refs)
                .await
                .map_err(|e| e.to_string());
        }
        Some(txs) => txs,
    };

    let n_shards = state.n_shards;
    let buckets = bucket_by_shard(&keys, n_shards);
    // All-local fast path: every key bucketed to our shard.
    if buckets
        .iter()
        .enumerate()
        .all(|(s, b)| s == state.shard_idx || b.is_none())
    {
        let refs: Vec<&[u8]> = keys.iter().map(|k| k.as_ref()).collect();
        return store
            .mget(&state.ns, &refs)
            .await
            .map_err(|e| e.to_string());
    }

    let mut results: Vec<Option<Entry>> = vec![None; keys.len()];
    let mut local_bucket: Option<Vec<(usize, Bytes)>> = None;
    let mut pending: Vec<oneshot::Receiver<MGetReply>> = Vec::new();

    // Send to all remote shards first so they start working while we do the
    // local lookup. Stash local keys for after the sends.
    for (shard, bucket) in buckets.into_iter().enumerate() {
        let Some(bucket) = bucket else { continue };
        if shard == state.shard_idx {
            local_bucket = Some(bucket);
            continue;
        }
        let (reply_tx, reply_rx) = oneshot::channel();
        let req = CrossShardRequest::MGet {
            ns: state.ns.clone(),
            keys: bucket,
            reply: reply_tx,
        };
        let mut tx = txs[shard].clone();
        tx.send(req)
            .await
            .map_err(|_| format!("shard {shard} unavailable"))?;
        pending.push(reply_rx);
    }

    // Local lookup runs while remote shards are processing.
    if let Some(bucket) = local_bucket {
        let refs: Vec<&[u8]> = bucket.iter().map(|(_, k)| k.as_ref()).collect();
        let local = store
            .mget(&state.ns, &refs)
            .await
            .map_err(|e| e.to_string())?;
        for ((orig_idx, _), entry) in bucket.into_iter().zip(local) {
            results[orig_idx] = entry;
        }
    }

    for rx in pending {
        let entries = rx
            .await
            .map_err(|_| "cross-shard reply dropped".to_string())??;
        for (orig_idx, entry) in entries {
            results[orig_idx] = entry;
        }
    }
    Ok(results)
}

async fn dispatch_mset(
    pairs: Vec<(Bytes, Bytes)>,
    store: &ShardStore,
    state: &ConnState,
) -> Result<(), String> {
    let txs = match fan_out_txs(state) {
        None => {
            return store
                .mset(&state.ns, &pairs)
                .await
                .map_err(|e| e.to_string());
        }
        Some(txs) => txs,
    };

    let n_shards = state.n_shards;
    // Bucket by shard. Fast path when everything is local.
    let mut buckets: Vec<Option<Vec<(Bytes, Bytes)>>> = (0..n_shards).map(|_| None).collect();
    let approx = pairs.len() / n_shards + 1;
    let mut all_local = true;
    for (k, v) in pairs.into_iter() {
        let s = shard_for_key(k.as_ref(), n_shards);
        if s != state.shard_idx {
            all_local = false;
        }
        buckets[s]
            .get_or_insert_with(|| Vec::with_capacity(approx))
            .push((k, v));
    }
    if all_local {
        let local = buckets[state.shard_idx].take().unwrap_or_default();
        return store
            .mset(&state.ns, &local)
            .await
            .map_err(|e| e.to_string());
    }

    // NOTE: cross-shard MSET is NOT atomic — each shard applies its subset
    // independently. Matches Redis Cluster semantics.
    let mut local_pairs: Option<Vec<(Bytes, Bytes)>> = None;
    let mut pending: Vec<oneshot::Receiver<Result<(), String>>> = Vec::new();
    for (shard, bucket) in buckets.into_iter().enumerate() {
        let Some(bucket) = bucket else { continue };
        if shard == state.shard_idx {
            local_pairs = Some(bucket);
            continue;
        }
        let (reply_tx, reply_rx) = oneshot::channel();
        let req = CrossShardRequest::MSet {
            ns: state.ns.clone(),
            pairs: bucket,
            reply: reply_tx,
        };
        let mut tx = txs[shard].clone();
        tx.send(req)
            .await
            .map_err(|_| format!("shard {shard} unavailable"))?;
        pending.push(reply_rx);
    }

    if let Some(local) = local_pairs {
        store
            .mset(&state.ns, &local)
            .await
            .map_err(|e| e.to_string())?;
    }
    for rx in pending {
        rx.await
            .map_err(|_| "cross-shard reply dropped".to_string())??;
    }
    Ok(())
}

async fn dispatch_del(
    keys: Vec<Bytes>,
    store: &ShardStore,
    state: &ConnState,
) -> Result<u64, String> {
    let txs = match fan_out_txs(state) {
        None => {
            let refs: Vec<&[u8]> = keys.iter().map(|k| k.as_ref()).collect();
            return store.del(&state.ns, &refs).await.map_err(|e| e.to_string());
        }
        Some(txs) => txs,
    };

    let n_shards = state.n_shards;
    // DEL/EXISTS only need the keys, no original index — recipients reduce to a count.
    let mut buckets: Vec<Option<Vec<Bytes>>> = (0..n_shards).map(|_| None).collect();
    let approx = keys.len() / n_shards + 1;
    let mut all_local = true;
    for k in keys.into_iter() {
        let s = shard_for_key(k.as_ref(), n_shards);
        if s != state.shard_idx {
            all_local = false;
        }
        buckets[s]
            .get_or_insert_with(|| Vec::with_capacity(approx))
            .push(k);
    }
    if all_local {
        let local = buckets[state.shard_idx].take().unwrap_or_default();
        let refs: Vec<&[u8]> = local.iter().map(|k| k.as_ref()).collect();
        return store.del(&state.ns, &refs).await.map_err(|e| e.to_string());
    }

    let mut local_keys: Option<Vec<Bytes>> = None;
    let mut pending: Vec<oneshot::Receiver<Result<u64, String>>> = Vec::new();
    for (shard, bucket) in buckets.into_iter().enumerate() {
        let Some(bucket) = bucket else { continue };
        if shard == state.shard_idx {
            local_keys = Some(bucket);
            continue;
        }
        let (reply_tx, reply_rx) = oneshot::channel();
        let req = CrossShardRequest::Del {
            ns: state.ns.clone(),
            keys: bucket,
            reply: reply_tx,
        };
        let mut tx = txs[shard].clone();
        tx.send(req)
            .await
            .map_err(|_| format!("shard {shard} unavailable"))?;
        pending.push(reply_rx);
    }

    let mut total: u64 = 0;
    if let Some(local) = local_keys {
        let refs: Vec<&[u8]> = local.iter().map(|k| k.as_ref()).collect();
        total += store
            .del(&state.ns, &refs)
            .await
            .map_err(|e| e.to_string())?;
    }
    let replies = join_all(pending).await;
    for r in replies {
        total += r.map_err(|_| "cross-shard reply dropped".to_string())??;
    }
    Ok(total)
}

async fn dispatch_exists(
    keys: Vec<Bytes>,
    store: &ShardStore,
    state: &ConnState,
) -> Result<u64, String> {
    let txs = match fan_out_txs(state) {
        None => {
            let refs: Vec<&[u8]> = keys.iter().map(|k| k.as_ref()).collect();
            return store
                .exists(&state.ns, &refs)
                .await
                .map_err(|e| e.to_string());
        }
        Some(txs) => txs,
    };

    let n_shards = state.n_shards;
    let mut buckets: Vec<Option<Vec<Bytes>>> = (0..n_shards).map(|_| None).collect();
    let approx = keys.len() / n_shards + 1;
    let mut all_local = true;
    for k in keys.into_iter() {
        let s = shard_for_key(k.as_ref(), n_shards);
        if s != state.shard_idx {
            all_local = false;
        }
        buckets[s]
            .get_or_insert_with(|| Vec::with_capacity(approx))
            .push(k);
    }
    if all_local {
        let local = buckets[state.shard_idx].take().unwrap_or_default();
        let refs: Vec<&[u8]> = local.iter().map(|k| k.as_ref()).collect();
        return store
            .exists(&state.ns, &refs)
            .await
            .map_err(|e| e.to_string());
    }

    let mut local_keys: Option<Vec<Bytes>> = None;
    let mut pending: Vec<oneshot::Receiver<Result<u64, String>>> = Vec::new();
    for (shard, bucket) in buckets.into_iter().enumerate() {
        let Some(bucket) = bucket else { continue };
        if shard == state.shard_idx {
            local_keys = Some(bucket);
            continue;
        }
        let (reply_tx, reply_rx) = oneshot::channel();
        let req = CrossShardRequest::Exists {
            ns: state.ns.clone(),
            keys: bucket,
            reply: reply_tx,
        };
        let mut tx = txs[shard].clone();
        tx.send(req)
            .await
            .map_err(|_| format!("shard {shard} unavailable"))?;
        pending.push(reply_rx);
    }

    let mut total: u64 = 0;
    if let Some(local) = local_keys {
        let refs: Vec<&[u8]> = local.iter().map(|k| k.as_ref()).collect();
        total += store
            .exists(&state.ns, &refs)
            .await
            .map_err(|e| e.to_string())?;
    }
    let replies = join_all(pending).await;
    for r in replies {
        total += r.map_err(|_| "cross-shard reply dropped".to_string())??;
    }
    Ok(total)
}

fn ttl_duration_from_spec(ttl: &SetTtl) -> Duration {
    match ttl {
        SetTtl::Seconds(s) => Duration::from_secs(*s),
        SetTtl::Millis(ms) => Duration::from_millis(*ms),
        SetTtl::UnixSecs(ts) => {
            let now = now_unix_secs();
            Duration::from_secs(ts.saturating_sub(now))
        }
        SetTtl::UnixMillis(ts) => {
            let now = now_unix_ms();
            Duration::from_millis(ts.saturating_sub(now))
        }
    }
}

fn set_opts_from_args(args: &SetArgs) -> SetOptions {
    SetOptions {
        ttl: args.ttl.as_ref().map(ttl_duration_from_spec),
        metadata: None,
        keep_ttl: args.keep_ttl,
    }
}

fn now_unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

async fn do_incr(store: &ShardStore, ns: &str, key: &[u8], delta: i64) -> Value {
    match store.incr(ns, key, delta).await {
        Ok(n) => r::integer(n),
        Err(e) => r::error("ERR", &e.to_string()),
    }
}

fn cmd_name(cmd: &Command) -> &'static str {
    match cmd {
        Command::Ping { .. } => "PING",
        Command::Hello { .. } => "HELLO",
        Command::Select { .. } => "SELECT",
        Command::BgRewriteAof => "BGREWRITEAOF",
        Command::Quit | Command::Reset => "QUIT/RESET",
        Command::Get { .. } => "GET",
        Command::Set { .. } => "SET",
        Command::Del { .. } => "DEL",
        Command::Exists { .. } => "EXISTS",
        Command::Expire { .. } => "EXPIRE",
        Command::PExpire { .. } => "PEXPIRE",
        Command::ExpireAt { .. } => "EXPIREAT",
        Command::PExpireAt { .. } => "PEXPIREAT",
        Command::Ttl { .. } => "TTL",
        Command::PTtl { .. } => "PTTL",
        Command::Persist { .. } => "PERSIST",
        Command::MGet { .. } => "MGET",
        Command::MSet { .. } => "MSET",
        Command::GetSet { .. } => "GETSET",
        Command::SetNx { .. } => "SETNX",
        Command::GetDel { .. } => "GETDEL",
        Command::GetEx { .. } => "GETEX",
        Command::Incr { .. } => "INCR",
        Command::IncrBy { .. } => "INCRBY",
        Command::Decr { .. } => "DECR",
        Command::DecrBy { .. } => "DECRBY",
        Command::Keys { .. } => "KEYS",
        Command::Scan { .. } => "SCAN",
        Command::DbSize => "DBSIZE",
        Command::FlushDb => "FLUSHDB",
        Command::Watch { .. } => "WATCH",
        Command::PWatch { .. } => "PWATCH",
        Command::Unwatch => "UNWATCH",
        Command::Revision { .. } => "REVISION",
        Command::SetRev { .. } => "SETREV",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use beyond_kv_engine::{store::ShardStore, types::SetOptions as EngineSetOptions};
    use beyond_kv_proto::command::{GetExTtl, SetArgs, SetCondition, SetTtl};
    use beyond_resp::Value;
    use bytes::Bytes;
    use tempfile::TempDir;

    fn rt_block_on<F: std::future::Future>(f: F) -> F::Output {
        monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .build()
            .expect("monoio runtime")
            .block_on(f)
    }

    fn store() -> (ShardStore, TempDir) {
        let tmp = TempDir::new().unwrap();
        let s = rt_block_on(ShardStore::open(tmp.path(), 1 << 20)).unwrap();
        (s, tmp)
    }

    fn state() -> ConnState {
        ConnState::default()
    }

    /// Drive `dispatch` to completion on a fresh monoio runtime.
    fn run(cmd: Command, store: &ShardStore, state: &mut ConnState) -> Value {
        rt_block_on(dispatch(cmd, store, state))
    }

    fn set_key(s: &ShardStore, key: &[u8], value: &[u8]) {
        rt_block_on(s.set(
            "default",
            key,
            Bytes::copy_from_slice(value),
            EngineSetOptions::default(),
        ))
        .unwrap();
    }

    fn set_with_ttl(s: &ShardStore, key: &[u8], value: &[u8], ttl: Duration) {
        rt_block_on(s.set(
            "default",
            key,
            Bytes::copy_from_slice(value),
            EngineSetOptions {
                ttl: Some(ttl),
                metadata: None,
                keep_ttl: false,
            },
        ))
        .unwrap();
    }

    // ── PING / HELLO / SELECT / QUIT ──────────────────────────────────────────

    #[test]
    fn ping_returns_pong() {
        let (s, _t) = store();
        let res = run(Command::Ping { message: None }, &s, &mut state());
        assert!(matches!(res, Value::SimpleString(ref b) if b.as_ref() == b"PONG"));
    }

    #[test]
    fn ping_with_message_echoes_it() {
        let (s, _t) = store();
        let res = run(
            Command::Ping {
                message: Some(Bytes::from_static(b"hi")),
            },
            &s,
            &mut state(),
        );
        assert!(matches!(res, Value::BulkString(ref b) if b.as_ref() == b"hi"));
    }

    #[test]
    fn hello_3_updates_resp_version() {
        let (s, _t) = store();
        let mut st = state();
        run(Command::Hello { version: Some(3) }, &s, &mut st);
        assert_eq!(st.resp_version, 3);
    }

    #[test]
    fn hello_2_resets_resp_version() {
        let (s, _t) = store();
        let mut st = state();
        st.resp_version = 3;
        run(Command::Hello { version: Some(2) }, &s, &mut st);
        assert_eq!(st.resp_version, 2);
    }

    #[test]
    fn select_changes_namespace_in_state() {
        let (s, _t) = store();
        let mut st = state();
        run(Command::Select { db: 7 }, &s, &mut st);
        assert_eq!(st.ns, "db7");
        run(Command::Select { db: 0 }, &s, &mut st);
        assert_eq!(st.ns, "default");
    }

    #[test]
    fn quit_sets_quit_flag() {
        let (s, _t) = store();
        let mut st = state();
        run(Command::Quit, &s, &mut st);
        assert!(st.quit);
    }

    // ── GET / SET ─────────────────────────────────────────────────────────────

    #[test]
    fn get_missing_key_returns_nil() {
        let (s, _t) = store();
        let res = run(
            Command::Get {
                key: Bytes::from_static(b"nope"),
            },
            &s,
            &mut state(),
        );
        assert!(matches!(res, Value::Null));
    }

    #[test]
    fn set_then_get_returns_value() {
        let (s, _t) = store();
        let mut st = state();
        run(
            Command::Set {
                key: Bytes::from_static(b"k"),
                value: Bytes::from_static(b"hello"),
                args: SetArgs {
                    ttl: None,
                    condition: SetCondition::Always,
                    get: false,
                    keep_ttl: false,
                },
            },
            &s,
            &mut st,
        );
        let res = run(
            Command::Get {
                key: Bytes::from_static(b"k"),
            },
            &s,
            &mut st,
        );
        assert!(matches!(res, Value::BulkString(ref b) if b.as_ref() == b"hello"));
    }

    #[test]
    fn set_nx_on_missing_succeeds() {
        let (s, _t) = store();
        let res = run(
            Command::Set {
                key: Bytes::from_static(b"nx"),
                value: Bytes::from_static(b"v"),
                args: SetArgs {
                    ttl: None,
                    condition: SetCondition::Nx,
                    get: false,
                    keep_ttl: false,
                },
            },
            &s,
            &mut state(),
        );
        assert!(matches!(res, Value::SimpleString(_)));
    }

    #[test]
    fn set_nx_on_existing_returns_nil_and_preserves_value() {
        let (s, _t) = store();
        let mut st = state();
        set_key(&s, b"nx-dup", b"original");
        let res = run(
            Command::Set {
                key: Bytes::from_static(b"nx-dup"),
                value: Bytes::from_static(b"clobber"),
                args: SetArgs {
                    ttl: None,
                    condition: SetCondition::Nx,
                    get: false,
                    keep_ttl: false,
                },
            },
            &s,
            &mut st,
        );
        assert!(matches!(res, Value::Null));
        let got = run(
            Command::Get {
                key: Bytes::from_static(b"nx-dup"),
            },
            &s,
            &mut st,
        );
        assert!(matches!(got, Value::BulkString(ref b) if b.as_ref() == b"original"));
    }

    #[test]
    fn set_xx_on_missing_returns_nil() {
        let (s, _t) = store();
        let res = run(
            Command::Set {
                key: Bytes::from_static(b"xx-miss"),
                value: Bytes::from_static(b"v"),
                args: SetArgs {
                    ttl: None,
                    condition: SetCondition::Xx,
                    get: false,
                    keep_ttl: false,
                },
            },
            &s,
            &mut state(),
        );
        assert!(matches!(res, Value::Null));
    }

    #[test]
    fn set_xx_on_existing_succeeds() {
        let (s, _t) = store();
        let mut st = state();
        set_key(&s, b"xx-live", b"old");
        let res = run(
            Command::Set {
                key: Bytes::from_static(b"xx-live"),
                value: Bytes::from_static(b"new"),
                args: SetArgs {
                    ttl: None,
                    condition: SetCondition::Xx,
                    get: false,
                    keep_ttl: false,
                },
            },
            &s,
            &mut st,
        );
        assert!(matches!(res, Value::SimpleString(_)));
        let val = run(
            Command::Get {
                key: Bytes::from_static(b"xx-live"),
            },
            &s,
            &mut st,
        );
        assert!(matches!(val, Value::BulkString(ref b) if b.as_ref() == b"new"));
    }

    #[test]
    fn set_get_flag_returns_old_value() {
        let (s, _t) = store();
        let mut st = state();
        set_key(&s, b"gf", b"old");
        let res = run(
            Command::Set {
                key: Bytes::from_static(b"gf"),
                value: Bytes::from_static(b"new"),
                args: SetArgs {
                    ttl: None,
                    condition: SetCondition::Always,
                    get: true,
                    keep_ttl: false,
                },
            },
            &s,
            &mut st,
        );
        assert!(matches!(res, Value::BulkString(ref b) if b.as_ref() == b"old"));
    }

    // ── DEL / EXISTS ──────────────────────────────────────────────────────────

    #[test]
    fn del_existing_returns_1() {
        let (s, _t) = store();
        let mut st = state();
        set_key(&s, b"del-k", b"v");
        let res = run(
            Command::Del {
                keys: vec![Bytes::from_static(b"del-k")],
            },
            &s,
            &mut st,
        );
        assert!(matches!(res, Value::Integer(1)));
    }

    #[test]
    fn del_missing_returns_0() {
        let (s, _t) = store();
        let res = run(
            Command::Del {
                keys: vec![Bytes::from_static(b"ghost")],
            },
            &s,
            &mut state(),
        );
        assert!(matches!(res, Value::Integer(0)));
    }

    #[test]
    fn exists_live_key_returns_1() {
        let (s, _t) = store();
        let mut st = state();
        set_key(&s, b"ex-k", b"v");
        let res = run(
            Command::Exists {
                keys: vec![Bytes::from_static(b"ex-k")],
            },
            &s,
            &mut st,
        );
        assert!(matches!(res, Value::Integer(1)));
    }

    // ── TTL commands ──────────────────────────────────────────────────────────

    #[test]
    fn ttl_on_missing_returns_neg_two() {
        let (s, _t) = store();
        let res = run(
            Command::Ttl {
                key: Bytes::from_static(b"miss"),
            },
            &s,
            &mut state(),
        );
        assert!(matches!(res, Value::Integer(-2)));
    }

    #[test]
    fn ttl_on_persistent_key_returns_neg_one() {
        let (s, _t) = store();
        let mut st = state();
        set_key(&s, b"no-ttl", b"v");
        let res = run(
            Command::Ttl {
                key: Bytes::from_static(b"no-ttl"),
            },
            &s,
            &mut st,
        );
        assert!(matches!(res, Value::Integer(-1)));
    }

    #[test]
    fn expire_on_live_key_returns_1_and_ttl_visible() {
        let (s, _t) = store();
        let mut st = state();
        set_key(&s, b"exp-live", b"v");
        let res = run(
            Command::Expire {
                key: Bytes::from_static(b"exp-live"),
                secs: 60,
            },
            &s,
            &mut st,
        );
        assert!(matches!(res, Value::Integer(1)));
        let ttl = run(
            Command::Ttl {
                key: Bytes::from_static(b"exp-live"),
            },
            &s,
            &mut st,
        );
        assert!(matches!(ttl, Value::Integer(n) if n > 0));
    }

    #[test]
    fn expire_on_missing_key_returns_0() {
        let (s, _t) = store();
        let res = run(
            Command::Expire {
                key: Bytes::from_static(b"exp-miss"),
                secs: 60,
            },
            &s,
            &mut state(),
        );
        assert!(matches!(res, Value::Integer(0)));
    }

    #[test]
    fn expireat_in_past_deletes_key() {
        let (s, _t) = store();
        let mut st = state();
        set_key(&s, b"expat", b"v");
        let past = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            - 1;
        run(
            Command::ExpireAt {
                key: Bytes::from_static(b"expat"),
                unix_secs: past,
            },
            &s,
            &mut st,
        );
        let got = run(
            Command::Get {
                key: Bytes::from_static(b"expat"),
            },
            &s,
            &mut st,
        );
        assert!(matches!(got, Value::Null));
    }

    #[test]
    fn persist_removes_ttl_returns_1() {
        let (s, _t) = store();
        let mut st = state();
        set_with_ttl(&s, b"persist-k", b"v", Duration::from_secs(60));
        let res = run(
            Command::Persist {
                key: Bytes::from_static(b"persist-k"),
            },
            &s,
            &mut st,
        );
        assert!(matches!(res, Value::Integer(1)));
        let ttl = run(
            Command::Ttl {
                key: Bytes::from_static(b"persist-k"),
            },
            &s,
            &mut st,
        );
        assert!(matches!(ttl, Value::Integer(-1)));
    }

    // ── MSET / MGET ──────────────────────────────────────────────────────────

    #[test]
    fn mset_then_mget_returns_correct_values() {
        let (s, _t) = store();
        let mut st = state();
        run(
            Command::MSet {
                pairs: vec![
                    (Bytes::from_static(b"mk1"), Bytes::from_static(b"mv1")),
                    (Bytes::from_static(b"mk2"), Bytes::from_static(b"mv2")),
                ],
            },
            &s,
            &mut st,
        );
        let res = run(
            Command::MGet {
                keys: vec![
                    Bytes::from_static(b"mk1"),
                    Bytes::from_static(b"mk2"),
                    Bytes::from_static(b"mk-miss"),
                ],
            },
            &s,
            &mut st,
        );
        match res {
            Value::Array(vals) => {
                assert_eq!(vals.len(), 3);
                assert!(matches!(vals[0], Value::BulkString(ref b) if b.as_ref() == b"mv1"));
                assert!(matches!(vals[1], Value::BulkString(ref b) if b.as_ref() == b"mv2"));
                assert!(matches!(vals[2], Value::Null));
            }
            other => panic!("expected array, got {other:?}"),
        }
    }

    // ── GETSET / SETNX / GETDEL / GETEX ─────────────────────────────────────

    #[test]
    fn getset_returns_old_stores_new() {
        let (s, _t) = store();
        let mut st = state();
        set_key(&s, b"gs", b"old");
        let res = run(
            Command::GetSet {
                key: Bytes::from_static(b"gs"),
                value: Bytes::from_static(b"new"),
            },
            &s,
            &mut st,
        );
        assert!(matches!(res, Value::BulkString(ref b) if b.as_ref() == b"old"));
        let got = run(
            Command::Get {
                key: Bytes::from_static(b"gs"),
            },
            &s,
            &mut st,
        );
        assert!(matches!(got, Value::BulkString(ref b) if b.as_ref() == b"new"));
    }

    #[test]
    fn getdel_returns_value_and_removes_key() {
        let (s, _t) = store();
        let mut st = state();
        set_key(&s, b"gd", b"bye");
        let res = run(
            Command::GetDel {
                key: Bytes::from_static(b"gd"),
            },
            &s,
            &mut st,
        );
        assert!(matches!(res, Value::BulkString(ref b) if b.as_ref() == b"bye"));
        let gone = run(
            Command::Get {
                key: Bytes::from_static(b"gd"),
            },
            &s,
            &mut st,
        );
        assert!(matches!(gone, Value::Null));
    }

    #[test]
    fn getex_with_ex_sets_ttl() {
        let (s, _t) = store();
        let mut st = state();
        set_key(&s, b"gex", b"v");
        run(
            Command::GetEx {
                key: Bytes::from_static(b"gex"),
                ttl: Some(GetExTtl::Set(SetTtl::Seconds(60))),
            },
            &s,
            &mut st,
        );
        let ttl = run(
            Command::Ttl {
                key: Bytes::from_static(b"gex"),
            },
            &s,
            &mut st,
        );
        assert!(
            matches!(ttl, Value::Integer(n) if n > 0),
            "GETEX EX should set TTL"
        );
    }

    #[test]
    fn getex_persist_removes_ttl() {
        let (s, _t) = store();
        let mut st = state();
        set_with_ttl(&s, b"gex-ttl", b"v", Duration::from_secs(60));
        run(
            Command::GetEx {
                key: Bytes::from_static(b"gex-ttl"),
                ttl: Some(GetExTtl::Persist),
            },
            &s,
            &mut st,
        );
        let ttl = run(
            Command::Ttl {
                key: Bytes::from_static(b"gex-ttl"),
            },
            &s,
            &mut st,
        );
        assert!(matches!(ttl, Value::Integer(-1)));
    }

    // ── KEYS / SCAN / DBSIZE / FLUSHDB ───────────────────────────────────────

    #[test]
    fn flushdb_clears_all_keys() {
        let (s, _t) = store();
        let mut st = state();
        set_key(&s, b"f1", b"v");
        set_key(&s, b"f2", b"v");
        run(Command::FlushDb, &s, &mut st);
        let size = run(Command::DbSize, &s, &mut st);
        assert!(matches!(size, Value::Integer(0)));
    }

    // ── INCR / INCRBY / DECR / DECRBY ────────────────────────────────────────

    #[test]
    fn incr_missing_key_starts_at_one() {
        let (s, _t) = store();
        let res = run(
            Command::Incr {
                key: Bytes::from_static(b"ctr"),
            },
            &s,
            &mut state(),
        );
        assert!(matches!(res, Value::Integer(1)));
    }

    #[test]
    fn incr_increments_existing_value() {
        let (s, _t) = store();
        let mut st = state();
        set_key(&s, b"ctr2", b"10");
        let res = run(
            Command::Incr {
                key: Bytes::from_static(b"ctr2"),
            },
            &s,
            &mut st,
        );
        assert!(matches!(res, Value::Integer(11)));
    }

    #[test]
    fn incrby_adds_delta() {
        let (s, _t) = store();
        let mut st = state();
        set_key(&s, b"ctr3", b"5");
        let res = run(
            Command::IncrBy {
                key: Bytes::from_static(b"ctr3"),
                delta: 3,
            },
            &s,
            &mut st,
        );
        assert!(matches!(res, Value::Integer(8)));
    }

    #[test]
    fn decr_missing_key_starts_at_minus_one() {
        let (s, _t) = store();
        let res = run(
            Command::Decr {
                key: Bytes::from_static(b"dtr"),
            },
            &s,
            &mut state(),
        );
        assert!(matches!(res, Value::Integer(-1)));
    }

    #[test]
    fn decrby_subtracts_delta() {
        let (s, _t) = store();
        let mut st = state();
        set_key(&s, b"dtr2", b"10");
        let res = run(
            Command::DecrBy {
                key: Bytes::from_static(b"dtr2"),
                delta: 4,
            },
            &s,
            &mut st,
        );
        assert!(matches!(res, Value::Integer(6)));
    }

    #[test]
    fn incr_non_integer_returns_error() {
        let (s, _t) = store();
        let mut st = state();
        set_key(&s, b"badtype", b"notanumber");
        let res = run(
            Command::Incr {
                key: Bytes::from_static(b"badtype"),
            },
            &s,
            &mut st,
        );
        assert!(
            matches!(res, Value::SimpleError(..) | Value::BulkError(..)),
            "expected error for non-integer value, got {res:?}"
        );
    }

    #[test]
    fn incr_preserves_ttl() {
        let (s, _t) = store();
        let mut st = state();
        set_with_ttl(&s, b"ctr-ttl", b"1", Duration::from_secs(60));
        run(
            Command::Incr {
                key: Bytes::from_static(b"ctr-ttl"),
            },
            &s,
            &mut st,
        );
        let ttl = run(
            Command::Ttl {
                key: Bytes::from_static(b"ctr-ttl"),
            },
            &s,
            &mut st,
        );
        assert!(
            matches!(ttl, Value::Integer(n) if n > 0),
            "INCR must preserve TTL"
        );
    }

    #[test]
    fn select_isolates_keys_between_namespaces() {
        let (s, _t) = store();
        let mut st = state();
        set_key(&s, b"ns-k", b"in-default");
        run(Command::Select { db: 3 }, &s, &mut st);
        let res = run(
            Command::Get {
                key: Bytes::from_static(b"ns-k"),
            },
            &s,
            &mut st,
        );
        assert!(
            matches!(res, Value::Null),
            "key from default must be invisible in db3"
        );
    }
}

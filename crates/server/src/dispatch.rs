use std::task::Poll;
use std::time::Duration;

use beyond_kv_engine::store::ShardStore;
use beyond_kv_engine::types::SetOptions;
use beyond_kv_proto::command::{Command, GetExTtl, SetCondition, SetTtl};
use beyond_kv_proto::response::{self as r};
use beyond_resp::Value;

use crate::resp::ConnState;

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
            // but does NOT close the connection (unlike QUIT).
            *state = ConnState::default();
            Value::SimpleString(bytes::Bytes::from_static(b"RESET"))
        }

        Command::Get { key } => match store.get(&state.ns, &key).await {
            Ok(Some(entry)) => r::bulk(entry.value),
            Ok(None) => r::nil(),
            Err(e) => r::error("ERR", &e.to_string()),
        },

        Command::Set { key, value, args } => {
            let opts = set_opts_from_args(&args.ttl);

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
                        Ok(None) => r::nil(),
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

        Command::Del { keys } => {
            let refs: Vec<&[u8]> = keys.iter().map(|k| k.as_ref()).collect();
            match store.del(&state.ns, &refs).await {
                Ok(n) => r::integer(n as i64),
                Err(e) => r::error("ERR", &e.to_string()),
            }
        }

        Command::Exists { keys } => {
            let refs: Vec<&[u8]> = keys.iter().map(|k| k.as_ref()).collect();
            match store.exists(&state.ns, &refs).await {
                Ok(n) => r::integer(n as i64),
                Err(e) => r::error("ERR", &e.to_string()),
            }
        }

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

        Command::MGet { keys } => {
            // Bulk lookup: store.mget batches L1 misses through io_uring via
            // join_all, so a 100-key MGET dispatches all the cold reads
            // concurrently rather than serially awaiting each one.
            let refs: Vec<&[u8]> = keys.iter().map(|k| k.as_ref()).collect();
            match store.mget(&state.ns, &refs).await {
                Err(e) => r::error("ERR", &e.to_string()),
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
            }
        }

        Command::MSet { pairs } => match store.mset(&state.ns, &pairs).await {
            Ok(()) => r::ok(),
            Err(e) => r::error("ERR", &e.to_string()),
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

        Command::Keys { pattern } => {
            const CHUNK: u64 = 512;
            let mut all_keys: Vec<Value> = Vec::new();
            let mut cursor = bytes::Bytes::from_static(b"0");
            loop {
                match store
                    .scan(&state.ns, &cursor, pattern.as_deref(), CHUNK)
                    .await
                {
                    Err(e) => return r::error("ERR", &e.to_string()),
                    Ok(page) => {
                        let done = page.next_cursor == b"0".as_ref();
                        all_keys.extend(page.keys.into_iter().map(r::bulk));
                        if all_keys.len() >= KEYS_SCAN_LIMIT {
                            return r::error(
                                "ERR",
                                "KEYS result too large; use SCAN to paginate large keyspaces",
                            );
                        }
                        cursor = page.next_cursor;
                        if done {
                            break;
                        }
                        yield_now().await;
                    }
                }
            }
            r::array(all_keys)
        }

        Command::Scan { cursor, args } => {
            match store
                .scan(&state.ns, &cursor, args.pattern.as_deref(), args.count)
                .await
            {
                Ok(page) => r::scan_reply(page.next_cursor, page.keys),
                Err(e) => r::error("ERR", &e.to_string()),
            }
        }

        Command::DbSize => match store.db_size(&state.ns).await {
            Ok(n) => r::integer(n as i64),
            Err(e) => r::error("ERR", &e.to_string()),
        },

        Command::FlushDb => match store.flush_db(&state.ns).await {
            Ok(()) => r::ok(),
            Err(e) => r::error("ERR", &e.to_string()),
        },

        // Watch commands are intercepted in handle_conn before dispatch reaches here.
        Command::Watch { .. } | Command::PWatch { .. } | Command::Unwatch => r::error(
            "ERR",
            "WATCH must be sent as the first command after HELLO 3",
        ),
    }
}

async fn yield_now() {
    let mut yielded = false;
    std::future::poll_fn(|cx| {
        if yielded {
            Poll::Ready(())
        } else {
            yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await;
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

fn set_opts_from_args(ttl: &Option<SetTtl>) -> SetOptions {
    SetOptions {
        ttl: ttl.as_ref().map(ttl_duration_from_spec),
        metadata: None,
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
        Command::Keys { .. } => "KEYS",
        Command::Scan { .. } => "SCAN",
        Command::DbSize => "DBSIZE",
        Command::FlushDb => "FLUSHDB",
        Command::Watch { .. } => "WATCH",
        Command::PWatch { .. } => "PWATCH",
        Command::Unwatch => "UNWATCH",
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

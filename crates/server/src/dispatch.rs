use std::time::Duration;

use beyond_kv_engine::store::ShardStore;
use beyond_kv_engine::types::SetOptions;
use beyond_kv_proto::command::{Command, GetExTtl, SetCondition, SetTtl};
use beyond_kv_proto::response::{self as r};
use beyond_resp::Value;

use crate::resp::ConnState;

pub fn dispatch(cmd: Command, store: &ShardStore, state: &mut ConnState) -> Value {
    match cmd {
        Command::Ping { message } => {
            message.map(r::bulk).unwrap_or_else(|| Value::SimpleString(bytes::Bytes::from_static(b"PONG")))
        }

        Command::Hello { version } => {
            let v = version.unwrap_or(2).clamp(2, 3);
            state.resp_version = v;
            r::hello_reply(v)
        }

        Command::Select { db } => {
            state.ns = beyond_kv_engine::store::ns_for_db(db);
            r::ok()
        }

        Command::Quit | Command::Reset => {
            state.quit = true;
            r::ok()
        }

        Command::Get { key } => {
            match store.get(state.ns, &key) {
                Ok(Some(entry)) => r::bulk(entry.value),
                Ok(None) => r::nil(),
                Err(e) => r::error("ERR", &e.to_string()),
            }
        }

        Command::Set { key, value, args } => {
            let opts = set_opts_from_args(&args.ttl);

            // Handle NX / XX conditions
            match args.condition {
                SetCondition::Nx => {
                    match store.setnx(state.ns, &key, value, opts) {
                        Ok(true) => r::ok(),
                        Ok(false) => r::nil(),
                        Err(e) => r::error("ERR", &e.to_string()),
                    }
                }
                SetCondition::Xx => {
                    match store.get(state.ns, &key) {
                        Ok(None) => r::nil(),
                        Ok(Some(_)) => match store.set(state.ns, &key, value, opts) {
                            Ok(()) => r::ok(),
                            Err(e) => r::error("ERR", &e.to_string()),
                        },
                        Err(e) => r::error("ERR", &e.to_string()),
                    }
                }
                SetCondition::Always => {
                    if args.get {
                        match store.getset(state.ns, &key, value) {
                            Ok(Some(old)) => r::bulk(old.value),
                            Ok(None) => r::nil(),
                            Err(e) => r::error("ERR", &e.to_string()),
                        }
                    } else {
                        match store.set(state.ns, &key, value, opts) {
                            Ok(()) => r::ok(),
                            Err(e) => r::error("ERR", &e.to_string()),
                        }
                    }
                }
            }
        }

        Command::Del { keys } => {
            let refs: Vec<&[u8]> = keys.iter().map(|k| k.as_ref()).collect();
            match store.del(state.ns, &refs) {
                Ok(n) => r::integer(n as i64),
                Err(e) => r::error("ERR", &e.to_string()),
            }
        }

        Command::Exists { keys } => {
            let refs: Vec<&[u8]> = keys.iter().map(|k| k.as_ref()).collect();
            match store.exists(state.ns, &refs) {
                Ok(n) => r::integer(n as i64),
                Err(e) => r::error("ERR", &e.to_string()),
            }
        }

        Command::Expire { key, secs } => {
            match store.expire(state.ns, &key, Duration::from_secs(secs)) {
                Ok(true) => r::integer(1),
                Ok(false) => r::integer(0),
                Err(e) => r::error("ERR", &e.to_string()),
            }
        }

        Command::PExpire { key, millis } => {
            match store.expire(state.ns, &key, Duration::from_millis(millis)) {
                Ok(true) => r::integer(1),
                Ok(false) => r::integer(0),
                Err(e) => r::error("ERR", &e.to_string()),
            }
        }

        Command::ExpireAt { key, unix_secs } => {
            let now_secs = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            if unix_secs <= now_secs {
                match store.del(state.ns, &[key.as_ref()]) {
                    Ok(_) => r::integer(1),
                    Err(e) => r::error("ERR", &e.to_string()),
                }
            } else {
                let ttl = Duration::from_secs(unix_secs - now_secs);
                match store.expire(state.ns, &key, ttl) {
                    Ok(true) => r::integer(1),
                    Ok(false) => r::integer(0),
                    Err(e) => r::error("ERR", &e.to_string()),
                }
            }
        }

        Command::PExpireAt { key, unix_millis } => {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            if unix_millis <= now_ms {
                match store.del(state.ns, &[key.as_ref()]) {
                    Ok(_) => r::integer(1),
                    Err(e) => r::error("ERR", &e.to_string()),
                }
            } else {
                let ttl = Duration::from_millis(unix_millis - now_ms);
                match store.expire(state.ns, &key, ttl) {
                    Ok(true) => r::integer(1),
                    Ok(false) => r::integer(0),
                    Err(e) => r::error("ERR", &e.to_string()),
                }
            }
        }

        Command::Ttl { key } => {
            match store.ttl(state.ns, &key) {
                Ok(beyond_kv_engine::types::TtlResult::Remaining(s)) => r::integer(s as i64),
                Ok(beyond_kv_engine::types::TtlResult::NoExpiry) => r::integer(-1),
                Ok(beyond_kv_engine::types::TtlResult::NotFound) => r::integer(-2),
                Err(e) => r::error("ERR", &e.to_string()),
            }
        }

        Command::PTtl { key } => {
            match store.pttl(state.ns, &key) {
                Ok(beyond_kv_engine::types::TtlResult::Remaining(ms)) => r::integer(ms as i64),
                Ok(beyond_kv_engine::types::TtlResult::NoExpiry) => r::integer(-1),
                Ok(beyond_kv_engine::types::TtlResult::NotFound) => r::integer(-2),
                Err(e) => r::error("ERR", &e.to_string()),
            }
        }

        Command::Persist { key } => {
            match store.persist(state.ns, &key) {
                Ok(true) => r::integer(1),
                Ok(false) => r::integer(0),
                Err(e) => r::error("ERR", &e.to_string()),
            }
        }

        Command::MGet { keys } => {
            let values: Vec<Value> = keys.iter().map(|key| {
                match store.get(state.ns, key) {
                    Ok(Some(entry)) => r::bulk(entry.value),
                    Ok(None) => r::nil(),
                    Err(e) => r::error("ERR", &e.to_string()),
                }
            }).collect();
            r::array(values)
        }

        Command::MSet { pairs } => {
            for (key, value) in pairs {
                if let Err(e) = store.set(state.ns, &key, value, SetOptions::default()) {
                    return r::error("ERR", &e.to_string());
                }
            }
            r::ok()
        }

        Command::GetSet { key, value } => {
            match store.getset(state.ns, &key, value) {
                Ok(Some(old)) => r::bulk(old.value),
                Ok(None) => r::nil(),
                Err(e) => r::error("ERR", &e.to_string()),
            }
        }

        Command::SetNx { key, value } => {
            match store.setnx(state.ns, &key, value, SetOptions::default()) {
                Ok(true) => r::integer(1),
                Ok(false) => r::integer(0),
                Err(e) => r::error("ERR", &e.to_string()),
            }
        }

        Command::GetDel { key } => {
            match store.getdel(state.ns, &key) {
                Ok(Some(entry)) => r::bulk(entry.value),
                Ok(None) => r::nil(),
                Err(e) => r::error("ERR", &e.to_string()),
            }
        }

        Command::GetEx { key, ttl } => {
            match store.get(state.ns, &key) {
                Ok(None) => r::nil(),
                Ok(Some(entry)) => {
                    let value = entry.value.clone();
                    match ttl {
                        Some(GetExTtl::Set(ttl_spec)) => {
                            let opts = set_opts_from_args(&Some(ttl_spec));
                            let _ = store.set(state.ns, &key, value.clone(), opts);
                        }
                        Some(GetExTtl::Persist) => {
                            let _ = store.persist(state.ns, &key);
                        }
                        None => {}
                    }
                    r::bulk(value)
                }
                Err(e) => r::error("ERR", &e.to_string()),
            }
        }

        Command::Keys { pattern } => {
            match store.scan(state.ns, b"0", pattern.as_deref(), u64::MAX) {
                Ok(page) => r::array(page.keys.into_iter().map(r::bulk).collect()),
                Err(e) => r::error("ERR", &e.to_string()),
            }
        }

        Command::Scan { cursor, args } => {
            match store.scan(state.ns, &cursor, args.pattern.as_deref(), args.count) {
                Ok(page) => r::scan_reply(page.next_cursor, page.keys),
                Err(e) => r::error("ERR", &e.to_string()),
            }
        }

        Command::DbSize => {
            match store.db_size(state.ns) {
                Ok(n) => r::integer(n as i64),
                Err(e) => r::error("ERR", &e.to_string()),
            }
        }

        Command::FlushDb => {
            match store.flush_db(state.ns) {
                Ok(()) => r::ok(),
                Err(e) => r::error("ERR", &e.to_string()),
            }
        }
    }
}

fn set_opts_from_args(ttl: &Option<SetTtl>) -> SetOptions {
    let ttl = ttl.as_ref().map(|t| match t {
        SetTtl::Seconds(s) => Duration::from_secs(*s),
        SetTtl::Millis(ms) => Duration::from_millis(*ms),
        SetTtl::UnixSecs(ts) => {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            Duration::from_secs(ts.saturating_sub(now))
        }
        SetTtl::UnixMillis(ts) => {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            Duration::from_millis(ts.saturating_sub(now))
        }
    });
    SetOptions { ttl, metadata: None }
}

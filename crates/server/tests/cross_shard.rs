//! Multi-shard integration tests for transparent cross-shard fan-out of
//! MGET / MSET / DEL / EXISTS. The harness spins up `N_SHARDS` real worker
//! threads, each with its own `ShardStore` and monoio runtime, plus an accept
//! thread that peeks each RESP frame's first key to route the connection to
//! the right shard — same shape as `main.rs` but inside the test process.

use std::io::Write as _;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::unix::net::UnixStream;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, SyncSender};
use std::time::Duration;

use beyond_kv::cross_shard;
use beyond_kv::routing::{peek_http_key, peek_resp_key, shard_for_key};
use beyond_kv_engine::store::ShardStore;
use tempfile::TempDir;

const N_SHARDS: usize = 4;

static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

struct ShardedServer {
    _serial: std::sync::MutexGuard<'static, ()>,
    _tmp: TempDir,
    resp_port: u16,
    http_port: u16,
}

impl ShardedServer {
    fn start() -> Self {
        let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let tmp = TempDir::new().unwrap();
        let data_dir = tmp.path().to_owned();

        let resp_listener = TcpListener::bind("0.0.0.0:0").unwrap();
        let resp_port = resp_listener.local_addr().unwrap().port();
        let http_listener = TcpListener::bind("0.0.0.0:0").unwrap();
        let http_port = http_listener.local_addr().unwrap().port();

        let mut resp_senders: Vec<SyncSender<(TcpStream, SocketAddr)>> =
            Vec::with_capacity(N_SHARDS);
        let mut resp_wakeup_writers: Vec<UnixStream> = Vec::with_capacity(N_SHARDS);
        let mut resp_inboxes: Vec<(mpsc::Receiver<(TcpStream, SocketAddr)>, UnixStream)> =
            Vec::with_capacity(N_SHARDS);
        for _ in 0..N_SHARDS {
            let (tx, rx) = mpsc::sync_channel::<(TcpStream, SocketAddr)>(64);
            let (wread, wwrite) = UnixStream::pair().unwrap();
            resp_senders.push(tx);
            resp_wakeup_writers.push(wwrite);
            resp_inboxes.push((rx, wread));
        }

        let mut http_senders: Vec<SyncSender<(TcpStream, SocketAddr)>> =
            Vec::with_capacity(N_SHARDS);
        let mut http_wakeup_writers: Vec<UnixStream> = Vec::with_capacity(N_SHARDS);
        let mut http_inboxes: Vec<(mpsc::Receiver<(TcpStream, SocketAddr)>, UnixStream)> =
            Vec::with_capacity(N_SHARDS);
        for _ in 0..N_SHARDS {
            let (tx, rx) = mpsc::sync_channel::<(TcpStream, SocketAddr)>(64);
            let (wread, wwrite) = UnixStream::pair().unwrap();
            http_senders.push(tx);
            http_wakeup_writers.push(wwrite);
            http_inboxes.push((rx, wread));
        }

        let (cross_txs, cross_rxs) = cross_shard::build_channels(N_SHARDS);
        let cross_shard_txs: Arc<[_]> = Arc::from(cross_txs);

        let iter_data: Vec<_> = (0..N_SHARDS)
            .zip(resp_inboxes)
            .zip(cross_rxs)
            .zip(http_inboxes)
            .collect();
        for (((i, (resp_rx, resp_wake_read)), cross_rx), (http_rx, http_wake_read)) in iter_data {
            let cross_shard_txs = cross_shard_txs.clone();
            let shard_dir = data_dir.join(format!("shard-{i}"));
            std::thread::Builder::new()
                .name(format!("kv-test-shard-{i}"))
                .spawn(move || {
                    monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
                        .enable_timer()
                        .build()
                        .expect("monoio runtime")
                        .block_on(async move {
                            let store =
                                Rc::new(ShardStore::open(&shard_dir, 8 << 20).await.unwrap());
                            let cross_store = store.clone();
                            monoio::spawn(async move {
                                cross_shard::serve(cross_store, cross_rx).await;
                            });
                            let http_store = store.clone();
                            monoio::spawn(async move {
                                beyond_kv::http::serve_routed(
                                    http_store,
                                    http_rx,
                                    http_wake_read,
                                    10_000,
                                    Duration::from_secs(60),
                                    64 * 1024 * 1024,
                                )
                                .await;
                            });
                            beyond_kv::resp::serve(
                                store,
                                resp_rx,
                                resp_wake_read,
                                10_000,
                                Duration::from_secs(60),
                                i,
                                N_SHARDS,
                                cross_shard_txs,
                            )
                            .await;
                        });
                })
                .expect("spawn shard thread");
        }

        // RESP accept thread: peek the first key, route to that shard.
        std::thread::spawn(move || {
            let rr = AtomicUsize::new(0);
            for stream in resp_listener.incoming().flatten() {
                let peer = match stream.peer_addr() {
                    Ok(p) => p,
                    Err(_) => continue,
                };
                let idx = match peek_resp_key(&stream) {
                    Some(k) => shard_for_key(&k, N_SHARDS),
                    None => rr.fetch_add(1, Ordering::Relaxed) % N_SHARDS,
                };
                if resp_senders[idx].send((stream, peer)).is_err() {
                    break;
                }
                if resp_wakeup_writers[idx].write_all(&[1u8]).is_err() {
                    break;
                }
            }
        });

        // HTTP accept thread: peek the URI key, route to that shard.
        std::thread::spawn(move || {
            let rr = AtomicUsize::new(0);
            for stream in http_listener.incoming().flatten() {
                let peer = match stream.peer_addr() {
                    Ok(p) => p,
                    Err(_) => continue,
                };
                let idx = match peek_http_key(&stream) {
                    Some(k) => shard_for_key(&k, N_SHARDS),
                    None => rr.fetch_add(1, Ordering::Relaxed) % N_SHARDS,
                };
                if http_senders[idx].send((stream, peer)).is_err() {
                    break;
                }
                if http_wakeup_writers[idx].write_all(&[1u8]).is_err() {
                    break;
                }
            }
        });

        wait_for_port(resp_port);
        wait_for_port(http_port);
        Self {
            _serial,
            _tmp: tmp,
            resp_port,
            http_port,
        }
    }

    fn resp(&self) -> redis::Connection {
        redis::Client::open(format!("redis://127.0.0.1:{}/", self.resp_port))
            .unwrap()
            .get_connection()
            .unwrap()
    }

    fn http_put(&self, key: &str, value: &[u8]) -> u16 {
        let url = format!(
            "http://127.0.0.1:{}/namespaces/default/values/{}",
            self.http_port,
            urlencoding::encode(key)
        );
        match ureq::put(&url)
            .set("Content-Type", "application/octet-stream")
            .send_bytes(value)
        {
            Ok(r) => r.status(),
            Err(ureq::Error::Status(code, _)) => code,
            Err(e) => panic!("http_put error: {e}"),
        }
    }

    fn http_get(&self, key: &str) -> Option<Vec<u8>> {
        let url = format!(
            "http://127.0.0.1:{}/namespaces/default/values/{}",
            self.http_port,
            urlencoding::encode(key)
        );
        match ureq::get(&url).call() {
            Ok(r) => {
                use std::io::Read as _;
                let mut body = Vec::new();
                r.into_reader().read_to_end(&mut body).unwrap();
                Some(body)
            }
            Err(ureq::Error::Status(404, _)) => None,
            Err(e) => panic!("http_get error: {e}"),
        }
    }

    fn http_delete(&self, key: &str) -> u16 {
        let url = format!(
            "http://127.0.0.1:{}/namespaces/default/values/{}",
            self.http_port,
            urlencoding::encode(key)
        );
        match ureq::delete(&url).call() {
            Ok(r) => r.status(),
            Err(ureq::Error::Status(code, _)) => code,
            Err(e) => panic!("http_delete error: {e}"),
        }
    }
}

fn wait_for_port(port: u16) {
    let addr = format!("127.0.0.1:{port}");
    for _ in 0..2000 {
        if TcpStream::connect(&addr).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    panic!("port {port} never became ready");
}

/// Find one key per shard whose hash maps to that shard. Used to construct
/// command argument lists that are guaranteed to span every shard.
fn keys_one_per_shard() -> [String; N_SHARDS] {
    let mut found: [Option<String>; N_SHARDS] = Default::default();
    let mut filled = 0;
    let mut i = 0u64;
    while filled < N_SHARDS {
        let k = format!("k{i}");
        let s = shard_for_key(k.as_bytes(), N_SHARDS);
        if found[s].is_none() {
            found[s] = Some(k);
            filled += 1;
        }
        i += 1;
        if i > 10_000 {
            panic!("could not find a key for every shard within 10k attempts");
        }
    }
    found.map(|o| o.unwrap())
}

// ── RESP Tests ───────────────────────────────────────────────────────────────

#[test]
fn mset_then_mget_across_shards_returns_values_in_order() {
    let srv = ShardedServer::start();
    let mut con = srv.resp();
    let keys = keys_one_per_shard();

    let mut cmd = redis::cmd("MSET");
    for (i, k) in keys.iter().enumerate() {
        cmd.arg(k).arg(format!("v{i}"));
    }
    let _: () = cmd.query(&mut con).unwrap();

    let mut cmd = redis::cmd("MGET");
    for k in &keys {
        cmd.arg(k);
    }
    // Add a missing key in the middle to verify nil placement is preserved.
    cmd.arg("definitely-missing-key-zzz");
    let vals: Vec<Option<String>> = cmd.query(&mut con).unwrap();

    assert_eq!(vals.len(), N_SHARDS + 1);
    for (i, v) in vals.iter().take(N_SHARDS).enumerate() {
        assert_eq!(v.as_deref(), Some(format!("v{i}").as_str()));
    }
    assert!(vals[N_SHARDS].is_none());
}

#[test]
fn mget_across_shards_returns_nil_for_missing() {
    let srv = ShardedServer::start();
    let mut con = srv.resp();
    let keys = keys_one_per_shard();

    // Single-key SET would route to the connection's pinned shard, not to the
    // key's shard. Use MSET so each key lands on its actual owning shard.
    let _: () = redis::cmd("MSET")
        .arg(&keys[0])
        .arg("a")
        .arg(&keys[2])
        .arg("c")
        .query(&mut con)
        .unwrap();

    let mut cmd = redis::cmd("MGET");
    for k in &keys {
        cmd.arg(k);
    }
    let vals: Vec<Option<String>> = cmd.query(&mut con).unwrap();
    assert_eq!(vals.len(), N_SHARDS);
    assert_eq!(vals[0].as_deref(), Some("a"));
    assert!(vals[1].is_none());
    assert_eq!(vals[2].as_deref(), Some("c"));
    assert!(vals[3].is_none());
}

#[test]
fn del_across_shards_counts_only_present_keys() {
    let srv = ShardedServer::start();
    let mut con = srv.resp();
    let keys = keys_one_per_shard();

    let mut cmd = redis::cmd("MSET");
    for k in &keys {
        cmd.arg(k).arg("v");
    }
    let _: () = cmd.query(&mut con).unwrap();

    let mut cmd = redis::cmd("DEL");
    for k in &keys {
        cmd.arg(k);
    }
    cmd.arg("never-existed-zz");
    let n: u64 = cmd.query(&mut con).unwrap();
    assert_eq!(n, N_SHARDS as u64);

    // Second DEL should report 0.
    let mut cmd = redis::cmd("DEL");
    for k in &keys {
        cmd.arg(k);
    }
    let n: u64 = cmd.query(&mut con).unwrap();
    assert_eq!(n, 0);
}

#[test]
fn exists_across_shards_counts_present_keys() {
    let srv = ShardedServer::start();
    let mut con = srv.resp();
    let keys = keys_one_per_shard();

    // Use MSET so each key reaches its owning shard, not the connection's pinned shard.
    let _: () = redis::cmd("MSET")
        .arg(&keys[0])
        .arg("x")
        .arg(&keys[2])
        .arg("y")
        .query(&mut con)
        .unwrap();

    let mut cmd = redis::cmd("EXISTS");
    for k in &keys {
        cmd.arg(k);
    }
    let n: u64 = cmd.query(&mut con).unwrap();
    assert_eq!(n, 2);
}

#[test]
fn mset_across_shards_is_idempotent() {
    // CLAUDE.md requirement: all state-modifying operations must be idempotent.
    // Running cross-shard MSET twice with the same keys and values must produce
    // the same final state as running it once.
    let srv = ShardedServer::start();
    let mut con = srv.resp();
    let keys = keys_one_per_shard();

    for _ in 0..2 {
        let mut cmd = redis::cmd("MSET");
        for (i, k) in keys.iter().enumerate() {
            cmd.arg(k).arg(format!("v{i}"));
        }
        let _: () = cmd.query(&mut con).unwrap();
    }

    let mut cmd = redis::cmd("MGET");
    for k in &keys {
        cmd.arg(k);
    }
    let vals: Vec<Option<String>> = cmd.query(&mut con).unwrap();
    assert_eq!(vals.len(), N_SHARDS);
    for (i, v) in vals.iter().enumerate() {
        assert_eq!(
            v.as_deref(),
            Some(format!("v{i}").as_str()),
            "key[{i}] incorrect after idempotent cross-shard MSET"
        );
    }
}

#[test]
fn no_crossslot_error_for_multi_shard_command() {
    let srv = ShardedServer::start();
    let mut con = srv.resp();
    let keys = keys_one_per_shard();

    // Sanity: every shard is represented (otherwise this test is vacuous).
    let mut hit = [false; N_SHARDS];
    for k in &keys {
        hit[shard_for_key(k.as_bytes(), N_SHARDS)] = true;
    }
    assert!(hit.iter().all(|h| *h));

    let mut cmd = redis::cmd("MGET");
    for k in &keys {
        cmd.arg(k);
    }
    let res: redis::RedisResult<Vec<Option<String>>> = cmd.query(&mut con);
    let vals = res.expect("MGET across shards must not return CROSSSLOT");
    assert_eq!(vals.len(), N_SHARDS);
}

// ── HTTP Tests ───────────────────────────────────────────────────────────────

#[test]
fn http_put_and_get_across_different_shards() {
    let srv = ShardedServer::start();
    let keys = keys_one_per_shard();

    // PUT each key via HTTP — routing is driven by the URI key, so each lands
    // on its owning shard.
    for (i, k) in keys.iter().enumerate() {
        let status = srv.http_put(k, format!("val{i}").as_bytes());
        assert_eq!(status, 204, "PUT {k} returned {status}");
    }

    // GET each key back — must route to the same shard and find the value.
    for (i, k) in keys.iter().enumerate() {
        let body = srv
            .http_get(k)
            .unwrap_or_else(|| panic!("key {k} not found after PUT"));
        assert_eq!(
            body,
            format!("val{i}").as_bytes(),
            "wrong value for key {k}"
        );
    }
}

#[test]
fn http_delete_across_shards() {
    let srv = ShardedServer::start();
    let keys = keys_one_per_shard();

    for k in &keys {
        srv.http_put(k, b"v");
    }
    for k in &keys {
        let status = srv.http_delete(k);
        assert_eq!(status, 204, "DELETE {k} returned {status}");
    }
    for k in &keys {
        assert!(
            srv.http_get(k).is_none(),
            "key {k} must be absent after DELETE"
        );
    }
}

#[test]
fn http_routing_consistent_with_resp() {
    // Write via HTTP, read via RESP — verifies both listeners share the same store.
    let srv = ShardedServer::start();
    let keys = keys_one_per_shard();
    let mut con = srv.resp();

    for (i, k) in keys.iter().enumerate() {
        srv.http_put(k, format!("x{i}").as_bytes());
    }

    // MGET fans out to every shard, so all HTTP-written keys are visible.
    let mut cmd = redis::cmd("MGET");
    for k in &keys {
        cmd.arg(k);
    }
    let vals: Vec<Option<String>> = cmd.query(&mut con).unwrap();
    assert_eq!(vals.len(), N_SHARDS);
    for (i, v) in vals.iter().enumerate() {
        assert_eq!(
            v.as_deref(),
            Some(format!("x{i}").as_str()),
            "key[{i}] written via HTTP not visible via RESP MGET"
        );
    }
}

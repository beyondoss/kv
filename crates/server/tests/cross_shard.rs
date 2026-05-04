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
use beyond_kv::routing::{peek_resp_key, shard_for_key};
use beyond_kv_engine::store::ShardStore;
use tempfile::TempDir;

const N_SHARDS: usize = 4;

static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

struct ShardedServer {
    _serial: std::sync::MutexGuard<'static, ()>,
    _tmp: TempDir,
    resp_port: u16,
}

impl ShardedServer {
    fn start() -> Self {
        let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let tmp = TempDir::new().unwrap();
        let data_dir = tmp.path().to_owned();

        let resp_listener = TcpListener::bind("0.0.0.0:0").unwrap();
        let resp_port = resp_listener.local_addr().unwrap().port();

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

        let (cross_txs, cross_rxs) = cross_shard::build_channels(N_SHARDS);
        let cross_shard_txs: Arc<[_]> = Arc::from(cross_txs);

        for ((i, (resp_rx, resp_wake_read)), cross_rx) in (0..N_SHARDS)
            .zip(resp_inboxes.into_iter())
            .zip(cross_rxs.into_iter())
        {
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

        // Accept thread: peek the first key, route to that shard.
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

        wait_for_port(resp_port);
        Self {
            _serial,
            _tmp: tmp,
            resp_port,
        }
    }

    fn resp(&self) -> redis::Connection {
        redis::Client::open(format!("redis://127.0.0.1:{}/", self.resp_port))
            .unwrap()
            .get_connection()
            .unwrap()
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

// ── Tests ────────────────────────────────────────────────────────────────────

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

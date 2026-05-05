//! Multi-shard integration tests for transparent cross-shard fan-out of
//! MGET / MSET / DEL / EXISTS. The harness spins up `N_SHARDS` real worker
//! threads, each with its own `ShardStore` and monoio runtime, plus an accept
//! thread that peeks each RESP frame's first key to route the connection to
//! the right shard — same shape as `main.rs` but inside the test process.

use std::io::{Read as _, Write as _};
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

        let (cross_txs, cross_wake_writes, cross_rxs, cross_wake_reads) =
            cross_shard::build_channels(N_SHARDS);
        let cross_shard_txs: Arc<[_]> = Arc::from(cross_txs);
        let cross_shard_wakeups: Arc<[_]> = Arc::from(cross_wake_writes);

        let iter_data: Vec<_> = (0..N_SHARDS)
            .zip(resp_inboxes)
            .zip(cross_rxs)
            .zip(cross_wake_reads)
            .zip(http_inboxes)
            .collect();
        for (
            (((i, (resp_rx, resp_wake_read)), cross_rx), cross_wake_read),
            (http_rx, http_wake_read),
        ) in iter_data
        {
            let cross_shard_txs = cross_shard_txs.clone();
            let cross_shard_wakeups = cross_shard_wakeups.clone();
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
                                cross_shard::serve(cross_store, cross_rx, cross_wake_read).await;
                            });
                            let http_store = store.clone();
                            let http_txs = cross_shard_txs.clone();
                            let http_wakeups = cross_shard_wakeups.clone();
                            monoio::spawn(async move {
                                beyond_kv::http::serve_routed(
                                    http_store,
                                    http_rx,
                                    http_wake_read,
                                    10_000,
                                    Duration::from_secs(60),
                                    64 * 1024 * 1024,
                                    i,
                                    N_SHARDS,
                                    http_txs,
                                    http_wakeups,
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
                                cross_shard_wakeups,
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
            "http://127.0.0.1:{}/v1/kv/{}?ns=0",
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
            "http://127.0.0.1:{}/v1/kv/{}?ns=0",
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
            "http://127.0.0.1:{}/v1/kv/{}?ns=0",
            self.http_port,
            urlencoding::encode(key)
        );
        match ureq::delete(&url).call() {
            Ok(r) => r.status(),
            Err(ureq::Error::Status(code, _)) => code,
            Err(e) => panic!("http_delete error: {e}"),
        }
    }

    /// List all keys via HTTP GET /namespaces/default/keys, following pagination cursors.
    fn http_list_all(&self) -> Vec<String> {
        self.http_list_paged(1000)
    }

    /// List keys via HTTP with a page size limit, following pagination.
    fn http_list_paged(&self, limit: usize) -> Vec<String> {
        let mut all = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let mut url = format!(
                "http://127.0.0.1:{}/v1/kv?ns=0&limit={limit}",
                self.http_port
            );
            if let Some(ref c) = cursor {
                url.push_str(&format!("&cursor={c}"));
            }
            let resp = ureq::get(&url).call().expect("http_list_paged GET failed");
            let mut raw = Vec::new();
            resp.into_reader().read_to_end(&mut raw).unwrap();
            let body: serde_json::Value =
                serde_json::from_slice(&raw).expect("http_list_paged JSON parse failed");
            for entry in body["keys"].as_array().unwrap() {
                all.push(entry["name"].as_str().unwrap().to_owned());
            }
            if body["complete"].as_bool().unwrap_or(true) {
                break;
            }
            cursor = body["cursor"].as_str().map(str::to_owned);
        }
        all
    }

    /// Subscribe to SSE prefix watch; returns a channel that delivers raw JSON event objects.
    fn http_watch_prefix_sse(&self, prefix: &str) -> std::sync::mpsc::Receiver<serde_json::Value> {
        let url = format!(
            "http://127.0.0.1:{}/v1/watch?ns=0&prefix={}",
            self.http_port,
            urlencoding::encode(prefix)
        );
        let (tx, rx) = std::sync::mpsc::sync_channel::<serde_json::Value>(64);
        std::thread::spawn(move || {
            let agent = ureq::AgentBuilder::new()
                .timeout_read(Duration::from_secs(5))
                .build();
            let response = match agent.get(&url).call() {
                Ok(r) => r,
                Err(_) => return,
            };
            use std::io::BufRead as _;
            let mut reader = std::io::BufReader::new(response.into_reader());
            let mut line = String::new();
            loop {
                line.clear();
                match reader.read_line(&mut line) {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {
                        let trimmed = line.trim();
                        if let Some(data) = trimmed.strip_prefix("data: ") {
                            if let Ok(event) = serde_json::from_str::<serde_json::Value>(data) {
                                if tx.send(event).is_err() {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        });
        rx
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

// ── HTTP LIST cross-shard tests ───────────────────────────────────────────────

#[test]
fn http_list_returns_keys_from_all_shards() {
    // PUT one key per shard via HTTP. Each PUT is routed by the URI key to its
    // owning shard. GET /keys must fan out and return all of them.
    let srv = ShardedServer::start();
    let keys = keys_one_per_shard();

    for (i, k) in keys.iter().enumerate() {
        let status = srv.http_put(k, format!("v{i}").as_bytes());
        assert_eq!(status, 204, "PUT {k} returned {status}");
    }

    let found = srv.http_list_all();
    for k in &keys {
        assert!(
            found.contains(k),
            "GET /keys missing key {k} (found {found:?})"
        );
    }
}

#[test]
fn http_list_pagination_covers_all_shards() {
    // Use limit=1 so each page covers exactly one key, forcing the cursor to
    // advance through every shard. The union must contain every written key.
    let srv = ShardedServer::start();
    let keys = keys_one_per_shard();

    for (i, k) in keys.iter().enumerate() {
        let status = srv.http_put(k, format!("v{i}").as_bytes());
        assert_eq!(status, 204, "PUT {k} returned {status}");
    }

    let found = srv.http_list_paged(1);
    // No duplicates across pages.
    let mut deduped = found.clone();
    deduped.sort();
    deduped.dedup();
    assert_eq!(
        deduped.len(),
        found.len(),
        "pagination produced duplicates: {found:?}"
    );
    // All written keys must appear.
    for k in &keys {
        assert!(
            found.contains(k),
            "paginated GET /keys missing {k} (found {found:?})"
        );
    }
}

#[test]
fn http_list_after_mset_via_resp_covers_all_shards() {
    // Write via RESP MSET (guaranteed cross-shard), read via HTTP GET /keys.
    let srv = ShardedServer::start();
    let keys = keys_one_per_shard();
    let mut con = srv.resp();

    let mut cmd = redis::cmd("MSET");
    for (i, k) in keys.iter().enumerate() {
        cmd.arg(k).arg(format!("v{i}"));
    }
    let _: () = cmd.query(&mut con).unwrap();

    let found = srv.http_list_all();
    for k in &keys {
        assert!(
            found.contains(k),
            "HTTP GET /keys missing key {k} written via RESP MSET (found {found:?})"
        );
    }
}

// ── HTTP SSE prefix-watch cross-shard tests ───────────────────────────────────

#[test]
fn http_prefix_watch_sse_receives_events_from_all_shards() {
    // Subscribe to prefix "k" via HTTP SSE. Then write one key per shard via
    // RESP MSET. Every shard must deliver a "set" watch event to the subscriber.
    let srv = ShardedServer::start();
    let keys = keys_one_per_shard();
    // All keys generated by keys_one_per_shard start with "k".

    let sse = srv.http_watch_prefix_sse("k");

    // Wait for the "ready" event before writing.
    let ready = sse
        .recv_timeout(Duration::from_secs(5))
        .expect("timeout waiting for SSE ready event");
    assert_eq!(
        ready["type"], "ready",
        "first SSE event must be ready, got {ready}"
    );

    // Write one key per shard.
    let port = srv.resp_port;
    let keys_clone = keys.clone();
    let (tx, rx) = std::sync::mpsc::channel::<()>();
    std::thread::spawn(move || {
        rx.recv().unwrap();
        let mut con = redis::Client::open(format!("redis://127.0.0.1:{port}/"))
            .unwrap()
            .get_connection()
            .unwrap();
        let mut cmd = redis::cmd("MSET");
        for (i, k) in keys_clone.iter().enumerate() {
            cmd.arg(k).arg(format!("v{i}"));
        }
        let _: () = cmd.query(&mut con).unwrap();
    });
    tx.send(()).unwrap();

    // Collect set events until we've seen all N_SHARDS keys.
    let mut received = std::collections::HashSet::new();
    for _ in 0..N_SHARDS {
        let event = sse
            .recv_timeout(Duration::from_secs(5))
            .expect("timeout waiting for SSE set event");
        if event["type"].as_str() == Some("set") {
            if let Some(k) = event["key"].as_str() {
                received.insert(k.to_owned());
            }
        }
    }
    for k in &keys {
        assert!(
            received.contains(k.as_str()),
            "HTTP SSE prefix watch missing event for key {k} on its shard"
        );
    }
}

// ── SCAN / KEYS / DBSIZE / FLUSHDB cross-shard tests ─────────────────────────

fn scan_all_keys(con: &mut redis::Connection) -> Vec<String> {
    let mut cursor: Vec<u8> = b"0".to_vec();
    let mut all_keys = Vec::new();
    loop {
        let (next, batch): (Vec<u8>, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor.as_slice())
            .arg("COUNT")
            .arg(100u64)
            .query(con)
            .unwrap();
        all_keys.extend(batch);
        if next == b"0" {
            break;
        }
        cursor = next;
    }
    all_keys
}

#[test]
fn scan_returns_keys_from_all_shards() {
    let srv = ShardedServer::start();
    let mut con = srv.resp();
    let keys = keys_one_per_shard();

    let mut cmd = redis::cmd("MSET");
    for (i, k) in keys.iter().enumerate() {
        cmd.arg(k).arg(format!("v{i}"));
    }
    let _: () = cmd.query(&mut con).unwrap();

    let mut scan_con = srv.resp();
    let found = scan_all_keys(&mut scan_con);
    for k in &keys {
        assert!(found.contains(k), "SCAN missing key {k} (found {found:?})");
    }
}

#[test]
fn keys_star_returns_all_keys_across_shards() {
    let srv = ShardedServer::start();
    let mut con = srv.resp();
    let keys = keys_one_per_shard();

    let mut cmd = redis::cmd("MSET");
    for (i, k) in keys.iter().enumerate() {
        cmd.arg(k).arg(format!("v{i}"));
    }
    let _: () = cmd.query(&mut con).unwrap();

    let mut keys_con = srv.resp();
    let found: Vec<String> = redis::cmd("KEYS").arg("*").query(&mut keys_con).unwrap();
    for k in &keys {
        assert!(
            found.contains(k),
            "KEYS * missing key {k} (found {found:?})"
        );
    }
}

#[test]
fn dbsize_counts_keys_on_all_shards() {
    let srv = ShardedServer::start();
    let mut con = srv.resp();
    let keys = keys_one_per_shard();

    let mut cmd = redis::cmd("MSET");
    for (i, k) in keys.iter().enumerate() {
        cmd.arg(k).arg(format!("v{i}"));
    }
    let _: () = cmd.query(&mut con).unwrap();

    let mut size_con = srv.resp();
    let n: u64 = redis::cmd("DBSIZE").query(&mut size_con).unwrap();
    assert_eq!(n, N_SHARDS as u64, "DBSIZE should count keys on all shards");
}

#[test]
fn flushdb_clears_all_shards() {
    let srv = ShardedServer::start();
    let keys = keys_one_per_shard();

    // Write one key per shard.
    let mut con = srv.resp();
    let mut cmd = redis::cmd("MSET");
    for (i, k) in keys.iter().enumerate() {
        cmd.arg(k).arg(format!("v{i}"));
    }
    let _: () = cmd.query(&mut con).unwrap();

    // Flush — must clear every shard.
    let mut flush_con = srv.resp();
    let _: () = redis::cmd("FLUSHDB").query(&mut flush_con).unwrap();

    // All keys must be gone.
    let mut check_con = srv.resp();
    let mut mget = redis::cmd("MGET");
    for k in &keys {
        mget.arg(k);
    }
    let vals: Vec<Option<String>> = mget.query(&mut check_con).unwrap();
    assert!(
        vals.iter().all(|v| v.is_none()),
        "FLUSHDB left keys on unflushed shards: {vals:?}"
    );

    let n: u64 = redis::cmd("DBSIZE").query(&mut check_con).unwrap();
    assert_eq!(n, 0, "DBSIZE after FLUSHDB should be 0");
}

// ── WATCH / PWATCH cross-shard tests ─────────────────────────────────────────

/// Minimal blocking RESP3 client for watch tests.
/// Uses raw TCP so we can stream push frames without ioredis limitations.
struct Resp3Conn {
    w: std::net::TcpStream,
    r: std::io::BufReader<std::net::TcpStream>,
}

impl Resp3Conn {
    fn connect(port: u16) -> Self {
        let s = std::net::TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let r = std::io::BufReader::new(s.try_clone().unwrap());
        Self { w: s, r }
    }

    fn send(&mut self, args: &[&[u8]]) {
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            buf.extend(format!("${}\r\n", a.len()).as_bytes());
            buf.extend_from_slice(a);
            buf.extend_from_slice(b"\r\n");
        }
        self.w.write_all(&buf).unwrap();
    }

    fn read_line(&mut self) -> String {
        use std::io::BufRead as _;
        let mut s = String::new();
        self.r.read_line(&mut s).unwrap();
        s.trim_end_matches("\r\n").to_string()
    }

    /// Read one complete RESP3 value and return its string content.
    /// Compound types (map, array) are consumed and "" is returned.
    fn read_value_as_string(&mut self) -> String {
        let line = self.read_line();
        if line.is_empty() {
            return String::new();
        }
        let (sigil, rest) = (&line[..1], &line[1..]);
        match sigil {
            "$" => {
                use std::io::Read as _;
                let n: usize = rest.parse().unwrap_or(0);
                let mut buf = vec![0u8; n + 2];
                self.r.read_exact(&mut buf).unwrap();
                String::from_utf8_lossy(&buf[..n]).into_owned()
            }
            "+" | "-" | ":" | "," | "(" | "_" | "#" => rest.to_string(),
            "*" | ">" | "~" => {
                let n: usize = rest.parse().unwrap_or(0);
                for _ in 0..n {
                    self.read_value_as_string();
                }
                String::new()
            }
            "%" | "|" => {
                let n: usize = rest.parse().unwrap_or(0);
                for _ in 0..n * 2 {
                    self.read_value_as_string();
                }
                String::new()
            }
            _ => rest.to_string(),
        }
    }

    /// Read the next push (`>`) frame; skip any non-push values encountered first.
    fn next_push(&mut self) -> Vec<String> {
        loop {
            let line = self.read_line();
            if line.is_empty() {
                continue;
            }
            if let Some(n_str) = line.strip_prefix('>') {
                let n: usize = n_str.parse().unwrap_or(0);
                return (0..n).map(|_| self.read_value_as_string()).collect();
            }
            // Non-push value: skip remaining parts after the first line.
            let (sigil, rest) = (&line[..1], &line[1..]);
            match sigil {
                "$" => {
                    use std::io::Read as _;
                    let n: usize = rest.parse().unwrap_or(0);
                    let mut buf = vec![0u8; n + 2];
                    self.r.read_exact(&mut buf).unwrap();
                }
                "*" | "~" => {
                    let n: usize = rest.parse().unwrap_or(0);
                    for _ in 0..n {
                        self.read_value_as_string();
                    }
                }
                "%" | "|" => {
                    let n: usize = rest.parse().unwrap_or(0);
                    for _ in 0..n * 2 {
                        self.read_value_as_string();
                    }
                }
                _ => {} // single-line types, already consumed
            }
        }
    }

    /// Block until the `watch ready` push frame arrives.
    fn wait_ready(&mut self) {
        loop {
            let push = self.next_push();
            if push.get(1).map(String::as_str) == Some("ready") {
                return;
            }
        }
    }
}

#[test]
fn watch_multi_key_receives_event_from_foreign_shard() {
    let srv = ShardedServer::start();
    let keys = keys_one_per_shard();
    // keys[0] → shard 0, keys[1] → shard 1 (guaranteed by keys_one_per_shard).
    let key_a = keys[0].as_bytes().to_vec();
    let key_b = keys[1].as_bytes().to_vec();
    assert_ne!(
        shard_for_key(&key_a, N_SHARDS),
        shard_for_key(&key_b, N_SHARDS),
    );

    // Open RESP3 watch connection (will be pinned to key_a's shard).
    let mut watcher = Resp3Conn::connect(srv.resp_port);
    watcher.send(&[b"HELLO", b"3"]);
    watcher.read_value_as_string(); // skip HELLO response map
    watcher.send(&[b"WATCH", &key_a, &key_b]);
    watcher.wait_ready();

    // Write to both keys from a second connection after ready.
    let port = srv.resp_port;
    let ka = keys[0].clone();
    let kb = keys[1].clone();
    let (tx, rx) = std::sync::mpsc::channel::<()>();
    std::thread::spawn(move || {
        rx.recv().unwrap();
        let mut con = redis::Client::open(format!("redis://127.0.0.1:{port}/"))
            .unwrap()
            .get_connection()
            .unwrap();
        let _: () = redis::cmd("MSET")
            .arg(&ka)
            .arg("va")
            .arg(&kb)
            .arg("vb")
            .query(&mut con)
            .unwrap();
    });
    tx.send(()).unwrap();

    // Both keys must arrive as watch set events.
    let mut received = std::collections::HashSet::new();
    for _ in 0..2 {
        let push = watcher.next_push();
        // push = ["watch", "set", key, value, revision]
        if push.get(1).map(String::as_str) == Some("set") {
            if let Some(k) = push.get(2) {
                received.insert(k.clone());
            }
        }
    }
    let key_a_str = String::from_utf8(key_a).unwrap();
    let key_b_str = String::from_utf8(key_b).unwrap();
    assert!(
        received.contains(&key_a_str),
        "missing watch event for {key_a_str}"
    );
    assert!(
        received.contains(&key_b_str),
        "missing watch event for {key_b_str}"
    );
}

#[test]
fn pwatch_receives_events_from_all_shards() {
    let srv = ShardedServer::start();
    let keys = keys_one_per_shard();
    // All keys start with "k" — use that as the PWATCH prefix.

    let mut watcher = Resp3Conn::connect(srv.resp_port);
    watcher.send(&[b"HELLO", b"3"]);
    watcher.read_value_as_string(); // skip HELLO response map
    watcher.send(&[b"PWATCH", b"k"]);
    watcher.wait_ready();

    // Write all keys from a second connection.
    let port = srv.resp_port;
    let keys_clone = keys.clone();
    let (tx, rx) = std::sync::mpsc::channel::<()>();
    std::thread::spawn(move || {
        rx.recv().unwrap();
        let mut con = redis::Client::open(format!("redis://127.0.0.1:{port}/"))
            .unwrap()
            .get_connection()
            .unwrap();
        let mut cmd = redis::cmd("MSET");
        for (i, k) in keys_clone.iter().enumerate() {
            cmd.arg(k).arg(format!("v{i}"));
        }
        let _: () = cmd.query(&mut con).unwrap();
    });
    tx.send(()).unwrap();

    // Must receive one set event per shard.
    let mut received = std::collections::HashSet::new();
    for _ in 0..N_SHARDS {
        let push = watcher.next_push();
        if push.get(1).map(String::as_str) == Some("set") {
            if let Some(k) = push.get(2) {
                received.insert(k.clone());
            }
        }
    }
    for k in &keys {
        assert!(
            received.contains(k.as_str()),
            "PWATCH missing event for {k}"
        );
    }
}

#![allow(dead_code)]
use beyond_kv_engine::store::ShardStore;
use std::io::Read;
use std::rc::Rc;
use tempfile::TempDir;

fn wait_for_port(port: u16) {
    let addr = format!("127.0.0.1:{port}");
    for _ in 0..2000 {
        if std::net::TcpStream::connect(&addr).is_ok() {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    panic!("port {port} never became ready after 10 s (server thread may have crashed)");
}

// Serialise all TestServer instances so the HTTP integration tests don't
// exhaust macOS's ephemeral port range when running concurrently.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// A single-shard KV server running on two ephemeral ports.
///
/// Both the HTTP and RESP listeners are live by the time `start()` returns.
/// RocksDB data lives in a [`TempDir`] that is removed on drop.
pub struct TestServer {
    // Holds the serial lock for the duration of the test; released on drop.
    _serial: std::sync::MutexGuard<'static, ()>,
    _tmp: TempDir,
    pub http_port: u16,
    pub resp_port: u16,
}

impl TestServer {
    pub fn start() -> Self {
        Self::start_shards(1)
    }

    /// Start a server with `n_shards` shards (all served by one monoio thread).
    /// Keys hash to shards by `FxHash(key) % n_shards`; when n > 1 the HTTP
    /// handler exercises the cross-shard path for foreign-shard keys.
    pub fn start_shards(n_shards: usize) -> Self {
        let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let tmp = TempDir::new().unwrap();
        let data_dir = tmp.path().to_owned();

        let resp_listener = std::net::TcpListener::bind("0.0.0.0:0").unwrap();
        let resp_port = resp_listener.local_addr().unwrap().port();
        let http_listener = std::net::TcpListener::bind("0.0.0.0:0").unwrap();
        let http_port = http_listener.local_addr().unwrap().port();

        let (resp_tx, resp_rx) =
            std::sync::mpsc::sync_channel::<(std::net::TcpStream, std::net::SocketAddr)>(64);
        let (resp_wakeup_read, resp_wakeup_write) = std::os::unix::net::UnixStream::pair().unwrap();
        let (http_tx, http_rx) =
            std::sync::mpsc::sync_channel::<(std::net::TcpStream, std::net::SocketAddr)>(64);
        let (http_wakeup_read, http_wakeup_write) = std::os::unix::net::UnixStream::pair().unwrap();

        std::thread::spawn(move || {
            use std::io::Write as _;
            let mut w = resp_wakeup_write;
            for stream in resp_listener.incoming().flatten() {
                let peer = match stream.peer_addr() {
                    Ok(p) => p,
                    Err(_) => continue,
                };
                if resp_tx.send((stream, peer)).is_err() {
                    break;
                }
                if w.write_all(&[1u8]).is_err() {
                    break;
                }
            }
        });
        std::thread::spawn(move || {
            use std::io::Write as _;
            let mut w = http_wakeup_write;
            for stream in http_listener.incoming().flatten() {
                let peer = match stream.peer_addr() {
                    Ok(p) => p,
                    Err(_) => continue,
                };
                if http_tx.send((stream, peer)).is_err() {
                    break;
                }
                if w.write_all(&[1u8]).is_err() {
                    break;
                }
            }
        });

        std::thread::Builder::new()
            .name(format!("kv-test-shards{n_shards}-{http_port}"))
            .spawn(move || {
                monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
                    .enable_timer()
                    .build()
                    .expect("monoio runtime")
                    .block_on(async move {
                        let (txs, wake_writes, mut rxs, mut wake_reads) =
                            beyond_kv::cross_shard::build_channels(n_shards);
                        let cross_shard_txs: std::sync::Arc<[_]> = std::sync::Arc::from(txs);
                        let cross_shard_wakeups: std::sync::Arc<[_]> =
                            std::sync::Arc::from(wake_writes);

                        // Open one ShardStore per shard and spawn cross-shard servers.
                        let mut stores: Vec<Rc<ShardStore>> = Vec::with_capacity(n_shards);
                        for i in 0..n_shards {
                            let shard_dir = data_dir.join(format!("shard{i}"));
                            std::fs::create_dir_all(&shard_dir).unwrap();
                            let store = Rc::new(
                                ShardStore::open(&shard_dir, 32 << 20)
                                    .await
                                    .expect("ShardStore::open"),
                            );
                            let cross_store = store.clone();
                            let cross_rx = rxs.remove(0);
                            let cross_wake = wake_reads.remove(0);
                            monoio::spawn(async move {
                                beyond_kv::cross_shard::serve(cross_store, cross_rx, cross_wake)
                                    .await;
                            });
                            stores.push(store);
                        }

                        let resp_store = stores[0].clone();
                        let http_store = stores[0].clone();
                        let http_txs = cross_shard_txs.clone();
                        let http_wakeups = cross_shard_wakeups.clone();
                        let http_sync_failures: std::sync::Arc<[std::sync::atomic::AtomicU32]> = (0
                            ..n_shards)
                            .map(|_| std::sync::atomic::AtomicU32::new(0))
                            .collect::<Vec<_>>()
                            .into();
                        monoio::spawn(async move {
                            beyond_kv::http::serve_routed(
                                http_store,
                                http_rx,
                                http_wakeup_read,
                                10_000,
                                std::time::Duration::from_secs(60),
                                64 * 1024 * 1024,
                                0,
                                n_shards,
                                http_txs,
                                http_wakeups,
                                beyond_kv::metrics::Metrics::new(),
                                http_sync_failures,
                                3,
                                None,
                            )
                            .await;
                        });
                        beyond_kv::resp::serve(
                            resp_store,
                            resp_rx,
                            resp_wakeup_read,
                            10_000,
                            std::time::Duration::from_secs(60),
                            0,
                            n_shards,
                            cross_shard_txs,
                            cross_shard_wakeups,
                            beyond_kv::metrics::Metrics::new(),
                            None,
                        )
                        .await;
                    });
            })
            .expect("spawn server thread");

        wait_for_port(http_port);
        wait_for_port(resp_port);

        Self {
            _serial,
            _tmp: tmp,
            http_port,
            resp_port,
        }
    }

    // ── URL construction ──────────────────────────────────────────────────────

    fn base(&self) -> String {
        format!("http://127.0.0.1:{}", self.http_port)
    }

    pub fn value_url(&self, ns: u8, key: &str) -> String {
        format!("{}/v1/kv/{}?ns={ns}", self.base(), urlencoding::encode(key))
    }

    pub fn keys_url(&self, ns: u8) -> String {
        format!("{}/v1/kv?ns={ns}", self.base())
    }

    pub fn livez_url(&self) -> String {
        format!("{}/livez", self.base())
    }

    pub fn readyz_url(&self) -> String {
        format!("{}/readyz", self.base())
    }

    // ── HTTP helpers ──────────────────────────────────────────────────────────

    pub fn get(&self, key: &str) -> KvResponse {
        self.get_ns(0, key)
    }

    pub fn get_ns(&self, ns: u8, key: &str) -> KvResponse {
        raw_call(ureq::get(&self.value_url(ns, key)))
    }

    pub fn put(&self, key: &str, value: &[u8]) -> KvResponse {
        self.put_opts(0, key, value, PutOptions::default())
    }

    pub fn put_ns(&self, ns: u8, key: &str, value: &[u8]) -> KvResponse {
        self.put_opts(ns, key, value, PutOptions::default())
    }

    pub fn put_opts(&self, ns: u8, key: &str, value: &[u8], opts: PutOptions) -> KvResponse {
        // value_url already contains `?ns=N`, so additional params get appended with `&`.
        let mut url = self.value_url(ns, key);
        if opts.nx {
            url.push_str("&nx=1");
        }
        if let Some(t) = opts.ttl_query {
            url.push_str(&format!("&ttl={t}"));
        }

        let mut req = ureq::put(&url).set("Content-Type", "application/octet-stream");
        if let Some(t) = opts.ttl_header {
            req = req.set("x-kv-ttl", &t.to_string());
        }
        if let Some(m) = &opts.metadata {
            req = req.set("x-kv-metadata", &serde_json::to_string(m).unwrap());
        }
        if opts.keep_ttl {
            req = req.set("x-kv-keepttl", "1");
        }
        if opts.return_old {
            req = req.set("x-kv-return-old", "1");
        }
        raw_send(req, value)
    }

    pub fn delete(&self, key: &str) -> KvResponse {
        self.delete_ns(0, key)
    }

    pub fn delete_ns(&self, ns: u8, key: &str) -> KvResponse {
        raw_call(ureq::delete(&self.value_url(ns, key)))
    }

    pub fn list(&self, ns: u8) -> KvResponse {
        self.list_opts(ns, ListOptions::default())
    }

    pub fn list_opts(&self, ns: u8, opts: ListOptions) -> KvResponse {
        // keys_url already contains `?ns=N`, so additional params get appended with `&`.
        let mut url = self.keys_url(ns);
        if let Some(p) = &opts.prefix {
            url.push_str(&format!("&prefix={}", urlencoding::encode(p)));
        }
        if let Some(c) = &opts.cursor {
            url.push_str(&format!("&cursor={}", urlencoding::encode(c)));
        }
        if let Some(l) = opts.limit {
            url.push_str(&format!("&limit={l}"));
        }
        raw_call(ureq::get(&url))
    }

    // ── RESP helper ───────────────────────────────────────────────────────────

    pub fn resp(&self) -> redis::Connection {
        redis::Client::open(format!("redis://127.0.0.1:{}/", self.resp_port))
            .unwrap()
            .get_connection()
            .unwrap()
    }
}

// ── Options ───────────────────────────────────────────────────────────────────

#[derive(Default)]
pub struct PutOptions {
    pub nx: bool,
    pub ttl_header: Option<u64>,
    pub ttl_query: Option<u64>,
    pub metadata: Option<serde_json::Value>,
    pub keep_ttl: bool,
    pub return_old: bool,
}

#[derive(Default)]
pub struct ListOptions {
    pub prefix: Option<String>,
    pub cursor: Option<String>,
    pub limit: Option<u64>,
}

// ── Response ──────────────────────────────────────────────────────────────────

pub struct KvResponse {
    pub status: u16,
    pub body: Vec<u8>,
    pub ttl: Option<u64>,
    pub ttl_ms: Option<u64>,
    pub metadata: Option<serde_json::Value>,
}

impl KvResponse {
    pub fn body_str(&self) -> &str {
        std::str::from_utf8(&self.body).expect("non-UTF-8 response body")
    }

    pub fn json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.body).expect("non-JSON response body")
    }

    pub fn is_ok(&self) -> bool {
        self.status / 100 == 2
    }

    pub fn is_not_found(&self) -> bool {
        self.status == 404
    }

    pub fn is_conflict(&self) -> bool {
        self.status == 409
    }

    pub fn is_method_not_allowed(&self) -> bool {
        self.status == 405
    }
}

// ── RESP scan helper ──────────────────────────────────────────────────────────

/// Drain a full SCAN using the cursor protocol; returns all matching keys.
///
/// Uses a manual loop with `Vec<u8>` cursors rather than `redis::Iter<String>`
/// because our server uses binary (non-ASCII) cursor values that the redis
/// crate's String-based iterator cannot handle.
pub fn scan_all(con: &mut redis::Connection, pattern: Option<&str>) -> Vec<String> {
    let mut results: Vec<String> = Vec::new();
    let mut cursor: Vec<u8> = b"0".to_vec();
    loop {
        let mut cmd = redis::cmd("SCAN");
        cmd.arg(cursor.as_slice());
        if let Some(p) = pattern {
            cmd.arg("MATCH").arg(p);
        }
        let (next_cursor, batch): (Vec<u8>, Vec<String>) = cmd.query(con).unwrap();
        results.extend(batch);
        if next_cursor == b"0" {
            break;
        }
        cursor = next_cursor;
    }
    results
}

// ── Helpers (pub for use in test files) ──────────────────────────────────────

pub fn raw_call_url(req: ureq::Request) -> KvResponse {
    raw_call(req)
}

// ── SSE watch helpers ─────────────────────────────────────────────────────────

/// A handle to a background thread reading SSE events from the server.
///
/// `recv_event` blocks until an event arrives or the 5-second idle timeout
/// fires (the background reader uses `timeout_read`). Drop this handle when
/// done; the background thread exits on the next failed send.
pub struct SseReceiver(std::sync::mpsc::Receiver<serde_json::Value>);

impl SseReceiver {
    pub fn recv_event(&self) -> Option<serde_json::Value> {
        self.0.recv_timeout(std::time::Duration::from_secs(5)).ok()
    }
}

/// Open an SSE stream on `/namespaces/{ns}/watch/{key}`.
///
/// Pass `since` to replay missed events (architecture: `tstamp_ms > since`).
pub fn watch_key_sse(port: u16, ns: u8, key: &str, since: Option<u64>) -> SseReceiver {
    let mut url = format!(
        "http://127.0.0.1:{port}/v1/watch/{}?ns={ns}",
        urlencoding::encode(key),
    );
    if let Some(s) = since {
        url.push_str(&format!("&since={s}"));
    }
    sse_stream(url)
}

/// Open an SSE stream on `/v1/watch?ns={ns}&prefix={prefix}`.
pub fn watch_prefix_sse(port: u16, ns: u8, prefix: &str, since: Option<u64>) -> SseReceiver {
    let mut url = format!(
        "http://127.0.0.1:{port}/v1/watch?ns={ns}&prefix={}",
        urlencoding::encode(prefix),
    );
    if let Some(s) = since {
        url.push_str(&format!("&since={s}"));
    }
    sse_stream(url)
}

fn sse_stream(url: String) -> SseReceiver {
    let (tx, rx) = std::sync::mpsc::sync_channel::<serde_json::Value>(64);
    std::thread::spawn(move || {
        // 5-second read timeout so the thread exits cleanly when no more
        // events arrive (e.g. after the test finishes).
        let agent = ureq::AgentBuilder::new()
            .timeout_read(std::time::Duration::from_secs(5))
            .build();
        let response = match agent.get(&url).call() {
            Ok(r) => r,
            Err(_) => return,
        };
        let mut reader = std::io::BufReader::new(response.into_reader());
        let mut line = String::new();
        loop {
            line.clear();
            match std::io::BufRead::read_line(&mut reader, &mut line) {
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
    SseReceiver(rx)
}

// ── Internal helpers ──────────────────────────────────────────────────────────

fn raw_call(req: ureq::Request) -> KvResponse {
    let res = match req.call() {
        Ok(r) => r,
        Err(ureq::Error::Status(_, r)) => r,
        Err(e) => panic!("HTTP transport error: {e}"),
    };
    read_response(res)
}

fn raw_send(req: ureq::Request, body: &[u8]) -> KvResponse {
    let res = match req.send_bytes(body) {
        Ok(r) => r,
        Err(ureq::Error::Status(_, r)) => r,
        Err(e) => panic!("HTTP transport error: {e}"),
    };
    read_response(res)
}

fn read_response(res: ureq::Response) -> KvResponse {
    let status = res.status();
    let ttl = res.header("x-kv-ttl").and_then(|s| s.parse().ok());
    let ttl_ms = res.header("x-kv-ttl-ms").and_then(|s| s.parse().ok());
    let metadata = res
        .header("x-kv-metadata")
        .and_then(|s| serde_json::from_str(s).ok());
    let mut body = Vec::new();
    res.into_reader().read_to_end(&mut body).unwrap();
    KvResponse {
        status,
        body,
        ttl,
        ttl_ms,
        metadata,
    }
}

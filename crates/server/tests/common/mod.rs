use std::io::Read;
use std::rc::Rc;

use beyond_kv_engine::store::ShardStore;
use tempfile::TempDir;

fn wait_for_port(port: u16) {
    let addr = format!("127.0.0.1:{port}");
    for _ in 0..500 {
        if std::net::TcpStream::connect(&addr).is_ok() {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    panic!("port {port} never became ready (server thread may have crashed)");
}

/// A single-shard KV server running on two ephemeral ports.
///
/// Both the HTTP and RESP listeners are live by the time `start()` returns.
/// RocksDB data lives in a [`TempDir`] that is removed on drop.
pub struct TestServer {
    _tmp: TempDir,
    pub http_port: u16,
    pub resp_port: u16,
}

impl TestServer {
    pub fn start() -> Self {
        let tmp = TempDir::new().unwrap();
        let data_dir = tmp.path().to_owned();

        // Both ports are OS-assigned (port 0) to avoid cross-binary collisions.
        let resp_listener = std::net::TcpListener::bind("0.0.0.0:0").unwrap();
        let resp_port = resp_listener.local_addr().unwrap().port();
        let http_listener = std::net::TcpListener::bind("0.0.0.0:0").unwrap();
        let http_port = http_listener.local_addr().unwrap().port();

        let (resp_tx, resp_rx) =
            std::sync::mpsc::sync_channel::<(std::net::TcpStream, std::net::SocketAddr)>(64);
        let (resp_wakeup_read, resp_wakeup_write) =
            std::os::unix::net::UnixStream::pair().unwrap();
        let (http_tx, http_rx) =
            std::sync::mpsc::sync_channel::<(std::net::TcpStream, std::net::SocketAddr)>(64);
        let (http_wakeup_read, http_wakeup_write) =
            std::os::unix::net::UnixStream::pair().unwrap();

        // RESP accept thread.
        std::thread::spawn(move || {
            use std::io::Write as _;
            let mut wakeup_write = resp_wakeup_write;
            for stream in resp_listener.incoming().flatten() {
                let peer = match stream.peer_addr() {
                    Ok(p) => p,
                    Err(_) => continue,
                };
                if resp_tx.send((stream, peer)).is_err() {
                    break;
                }
                if wakeup_write.write_all(&[1u8]).is_err() {
                    break;
                }
            }
        });

        // HTTP accept thread.
        std::thread::spawn(move || {
            use std::io::Write as _;
            let mut wakeup_write = http_wakeup_write;
            for stream in http_listener.incoming().flatten() {
                let peer = match stream.peer_addr() {
                    Ok(p) => p,
                    Err(_) => continue,
                };
                if http_tx.send((stream, peer)).is_err() {
                    break;
                }
                if wakeup_write.write_all(&[1u8]).is_err() {
                    break;
                }
            }
        });

        std::thread::Builder::new()
            .name(format!("kv-test-{http_port}"))
            .spawn(move || {
                let store = Rc::new(
                    ShardStore::open(&data_dir, 32 << 20).expect("ShardStore::open"),
                );
                monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
                    .enable_timer()
                    .build()
                    .expect("monoio runtime")
                    .block_on(async move {
                        let http_store = store.clone();
                        monoio::spawn(async move {
                            beyond_kv::http::serve_routed(
                                http_store,
                                http_rx,
                                http_wakeup_read,
                            )
                            .await;
                        });
                        beyond_kv::resp::serve(store, resp_rx, resp_wakeup_read).await;
                    });
            })
            .expect("spawn server thread");

        wait_for_port(http_port);
        wait_for_port(resp_port);

        Self { _tmp: tmp, http_port, resp_port }
    }

    // ── URL construction ──────────────────────────────────────────────────────

    fn base(&self) -> String {
        format!("http://127.0.0.1:{}", self.http_port)
    }

    pub fn value_url(&self, ns: &str, key: &str) -> String {
        format!("{}/namespaces/{ns}/values/{}", self.base(), urlencoding::encode(key))
    }

    pub fn keys_url(&self, ns: &str) -> String {
        format!("{}/namespaces/{ns}/keys", self.base())
    }

    pub fn healthz_url(&self) -> String {
        format!("{}/healthz", self.base())
    }

    // ── HTTP helpers ──────────────────────────────────────────────────────────

    pub fn get(&self, key: &str) -> KvResponse {
        self.get_ns("default", key)
    }

    pub fn get_ns(&self, ns: &str, key: &str) -> KvResponse {
        raw_call(ureq::get(&self.value_url(ns, key)))
    }

    pub fn put(&self, key: &str, value: &[u8]) -> KvResponse {
        self.put_opts("default", key, value, PutOptions::default())
    }

    pub fn put_ns(&self, ns: &str, key: &str, value: &[u8]) -> KvResponse {
        self.put_opts(ns, key, value, PutOptions::default())
    }

    pub fn put_opts(&self, ns: &str, key: &str, value: &[u8], opts: PutOptions) -> KvResponse {
        let mut url = self.value_url(ns, key);
        let mut qp: Vec<String> = Vec::new();
        if opts.nx {
            qp.push("nx=1".into());
        }
        if let Some(t) = opts.ttl_query {
            qp.push(format!("ttl={t}"));
        }
        if !qp.is_empty() {
            url.push('?');
            url.push_str(&qp.join("&"));
        }

        let mut req = ureq::put(&url).set("Content-Type", "application/octet-stream");
        if let Some(t) = opts.ttl_header {
            req = req.set("x-kv-ttl", &t.to_string());
        }
        if let Some(m) = &opts.metadata {
            req = req.set("x-kv-metadata", &serde_json::to_string(m).unwrap());
        }
        raw_send(req, value)
    }

    pub fn delete(&self, key: &str) -> KvResponse {
        self.delete_ns("default", key)
    }

    pub fn delete_ns(&self, ns: &str, key: &str) -> KvResponse {
        raw_call(ureq::delete(&self.value_url(ns, key)))
    }

    pub fn list(&self, ns: &str) -> KvResponse {
        self.list_opts(ns, ListOptions::default())
    }

    pub fn list_opts(&self, ns: &str, opts: ListOptions) -> KvResponse {
        let mut url = self.keys_url(ns);
        let mut qp: Vec<String> = Vec::new();
        if let Some(p) = &opts.prefix {
            qp.push(format!("prefix={}", urlencoding::encode(p)));
        }
        if let Some(c) = &opts.cursor {
            qp.push(format!("cursor={}", urlencoding::encode(c)));
        }
        if let Some(l) = opts.limit {
            qp.push(format!("limit={l}"));
        }
        if !qp.is_empty() {
            url.push('?');
            url.push_str(&qp.join("&"));
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
    let metadata = res.header("x-kv-metadata").and_then(|s| serde_json::from_str(s).ok());
    let mut body = Vec::new();
    res.into_reader().read_to_end(&mut body).unwrap();
    KvResponse { status, body, ttl, metadata }
}

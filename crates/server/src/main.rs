#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::hash::Hasher;
use std::io::Write as _;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::unix::net::UnixStream;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, SyncSender};
use std::sync::Arc;

use rustc_hash::FxHasher;

fn shard_for_key(key: &[u8], n: usize) -> usize {
    let mut h = FxHasher::default();
    h.write(key);
    (h.finish() as usize) % n
}

fn route(key: Option<Vec<u8>>, n: usize, rr: &AtomicUsize) -> usize {
    match key {
        Some(k) => shard_for_key(&k, n),
        None => rr.fetch_add(1, Ordering::Relaxed) % n,
    }
}

fn peek_routing_key_bytes(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn percent_decode_routing(bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(h), Some(l)) =
                (peek_routing_key_bytes(bytes[i + 1]), peek_routing_key_bytes(bytes[i + 2]))
            {
                out.push(h << 4 | l);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    out
}

/// Peek the leading bytes of an incoming RESP connection, returning the key
/// (the second bulk-string argument of an array command) when one is present.
fn peek_resp_key(stream: &TcpStream) -> Option<Vec<u8>> {
    let _ = stream.set_nonblocking(true);
    let mut buf = [0u8; 4096];
    let n = stream.peek(&mut buf).unwrap_or(0);
    let _ = stream.set_nonblocking(false);
    if n == 0 {
        return None;
    }
    let buf = &buf[..n];

    if buf.first().copied() != Some(b'*') {
        return None;
    }
    // Find first \n
    let nl1 = buf.iter().position(|&b| b == b'\n')?;
    // Array count between buf[1..nl1-1] (strip \r)
    let count_end = if nl1 > 0 && buf[nl1 - 1] == b'\r' { nl1 - 1 } else { nl1 };
    let count_str = std::str::from_utf8(&buf[1..count_end]).ok()?;
    let count: usize = count_str.parse().ok()?;
    if count < 2 {
        return None;
    }

    // Skip first bulk-string element: $len\r\ncmd\r\n
    let mut i = nl1 + 1;
    if i >= buf.len() || buf[i] != b'$' {
        return None;
    }
    let len_start = i + 1;
    let nl2 = len_start + buf[len_start..].iter().position(|&b| b == b'\n')?;
    let len_end = if nl2 > 0 && buf[nl2 - 1] == b'\r' { nl2 - 1 } else { nl2 };
    let cmd_len: usize = std::str::from_utf8(&buf[len_start..len_end]).ok()?.parse().ok()?;
    i = nl2 + 1 + cmd_len + 2; // skip cmd bytes + \r\n

    // Read second bulk-string element's value
    if i >= buf.len() || buf[i] != b'$' {
        return None;
    }
    let len_start = i + 1;
    let nl3 = len_start + buf[len_start..].iter().position(|&b| b == b'\n')?;
    let len_end = if nl3 > 0 && buf[nl3 - 1] == b'\r' { nl3 - 1 } else { nl3 };
    let key_len: usize = std::str::from_utf8(&buf[len_start..len_end]).ok()?.parse().ok()?;
    let key_start = nl3 + 1;
    let key_end = key_start.checked_add(key_len)?;
    if key_end > buf.len() {
        return None;
    }
    Some(buf[key_start..key_end].to_vec())
}

/// Peek the leading bytes of an incoming HTTP connection, returning the key
/// extracted from `/values/{key}` segments when one is present.
fn peek_http_key(stream: &TcpStream) -> Option<Vec<u8>> {
    let _ = stream.set_nonblocking(true);
    let mut buf = [0u8; 4096];
    let n = stream.peek(&mut buf).unwrap_or(0);
    let _ = stream.set_nonblocking(false);
    if n == 0 {
        return None;
    }
    let buf = &buf[..n];

    // Find end of request line
    let nl = buf.iter().position(|&b| b == b'\n')?;
    let line = &buf[..nl];
    // Parse: METHOD SP /path SP HTTP/1.1
    let mut parts = line.splitn(3, |&b| b == b' ');
    let _method = parts.next()?;
    let path = parts.next()?;
    let needle = b"/values/";
    let pos = path.windows(needle.len()).position(|w| w == needle)?;
    let after = &path[pos + needle.len()..];
    // Stop at query string or end of request line; slashes are part of the key.
    let key_end = after
        .iter()
        .position(|&b| b == b'?' || b == b' ')
        .unwrap_or(after.len());
    if key_end == 0 {
        return None;
    }
    // Percent-decode so the routing key matches the stored key.
    Some(percent_decode_routing(&after[..key_end]))
}

fn accept_one(
    stream: TcpStream,
    peer: SocketAddr,
    peek_key: fn(&TcpStream) -> Option<Vec<u8>>,
    senders: &[SyncSender<(TcpStream, SocketAddr)>],
    wakeup_writers: &mut [UnixStream],
    rr: &AtomicUsize,
) -> bool {
    let idx = route(peek_key(&stream), senders.len(), rr);
    if senders[idx].send((stream, peer)).is_err() {
        return false;
    }
    if let Err(e) = wakeup_writers[idx].write_all(&[1u8]) {
        tracing::error!(worker = idx, error = %e, "wakeup pipe write failed; worker likely dead");
        return false;
    }
    true
}

fn main() -> anyhow::Result<()> {
    let cfg = beyond_kv::config::Config::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "beyond_kv=info".into()),
        )
        .init();

    let n_threads = cfg.threads.unwrap_or_else(num_cpus::get).max(1);
    tracing::info!(threads = n_threads, resp_port = cfg.resp_port, "starting beyond-kv");

    let data_dir = cfg.data_dir.clone();
    let resp_port = cfg.resp_port;
    let http_port = cfg.http_port;
    let memory_per_shard = cfg.memory_bytes / n_threads;
    let reclaim_sealed_threshold = cfg.reclaim_sealed_threshold;
    let reclaim_interval_secs = cfg.reclaim_interval_secs;
    tracing::info!(http_port, "HTTP server enabled");

    // Per-worker, per-protocol channel + wakeup pipe.
    let mut resp_senders: Vec<SyncSender<(TcpStream, SocketAddr)>> = Vec::with_capacity(n_threads);
    let mut resp_wakeup_writers: Vec<UnixStream> = Vec::with_capacity(n_threads);
    let mut http_senders: Vec<SyncSender<(TcpStream, SocketAddr)>> = Vec::with_capacity(n_threads);
    let mut http_wakeup_writers: Vec<UnixStream> = Vec::with_capacity(n_threads);
    let mut worker_inboxes: Vec<(
        mpsc::Receiver<(TcpStream, SocketAddr)>,
        UnixStream,
        mpsc::Receiver<(TcpStream, SocketAddr)>,
        UnixStream,
    )> = Vec::with_capacity(n_threads);

    for _ in 0..n_threads {
        let (resp_tx, resp_rx) = mpsc::sync_channel::<(TcpStream, SocketAddr)>(64);
        let (resp_wake_read, resp_wake_write) = UnixStream::pair()?;
        let (http_tx, http_rx) = mpsc::sync_channel::<(TcpStream, SocketAddr)>(64);
        let (http_wake_read, http_wake_write) = UnixStream::pair()?;
        resp_senders.push(resp_tx);
        resp_wakeup_writers.push(resp_wake_write);
        http_senders.push(http_tx);
        http_wakeup_writers.push(http_wake_write);
        worker_inboxes.push((resp_rx, resp_wake_read, http_rx, http_wake_read));
    }

    let handles: Vec<_> = (0..n_threads)
        .zip(worker_inboxes)
        .map(|(i, (resp_rx, resp_wake_read, http_rx, http_wake_read))| {
            let data_dir = data_dir.clone();
            std::thread::Builder::new()
                .name(format!("kv-worker-{i}"))
                .spawn(move || {
                    let shard_dir = data_dir.join(format!("shard-{i}"));
                    monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
                        .enable_timer()
                        .build()
                        .expect("failed to build monoio runtime")
                        .block_on(async {
                            let store = beyond_kv_engine::store::ShardStore::open(
                                &shard_dir,
                                memory_per_shard,
                            )
                            .await
                            .expect("failed to open store");
                            let store = Rc::new(store);
                            let sweep_store = store.clone();
                            monoio::spawn(async move {
                                loop {
                                    monoio::time::sleep(std::time::Duration::from_secs(30)).await;
                                    sweep_store.sweep_cache();
                                }
                            });
                            let sync_store = store.clone();
                            monoio::spawn(async move {
                                loop {
                                    monoio::time::sleep(std::time::Duration::from_secs(1)).await;
                                    if let Err(e) = sync_store.sync_logs().await {
                                        tracing::warn!(error = %e, "periodic log sync failed");
                                    }
                                }
                            });
                            if reclaim_sealed_threshold > 0 {
                                let reclaim_store = store.clone();
                                monoio::spawn(async move {
                                    loop {
                                        monoio::time::sleep(std::time::Duration::from_secs(
                                            reclaim_interval_secs,
                                        ))
                                        .await;
                                        if let Err(e) = reclaim_store
                                            .reclaim_if_needed(reclaim_sealed_threshold)
                                            .await
                                        {
                                            tracing::warn!(error = %e, "auto-reclaim failed");
                                        }
                                    }
                                });
                            }
                            let http_store = store.clone();
                            monoio::spawn(async move {
                                beyond_kv::http::serve_routed(http_store, http_rx, http_wake_read)
                                    .await;
                            });
                            beyond_kv::resp::serve(store, resp_rx, resp_wake_read).await;
                        })
                })
                .expect("failed to spawn worker thread")
        })
        .collect();

    let rr = Arc::new(AtomicUsize::new(0));

    // HTTP accept thread.
    let http_addr = format!("0.0.0.0:{http_port}");
    let http_listener = TcpListener::bind(&http_addr)?;
    tracing::info!("HTTP listening on {http_addr}");

    {
        let rr = rr.clone();
        let mut http_wakeup_writers = http_wakeup_writers;
        let http_senders = http_senders;
        std::thread::Builder::new()
            .name("kv-http-accept".into())
            .spawn(move || {
                for result in http_listener.incoming() {
                    match result {
                        Ok(stream) => {
                            let peer = match stream.peer_addr() {
                                Ok(p) => p,
                                Err(e) => {
                                    tracing::debug!("HTTP peer_addr: {e}");
                                    continue;
                                }
                            };
                            if !accept_one(
                                stream,
                                peer,
                                peek_http_key,
                                &http_senders,
                                &mut http_wakeup_writers,
                                &rr,
                            ) {
                                tracing::warn!("HTTP channel closed, stopping accept loop");
                                break;
                            }
                        }
                        Err(e) => tracing::error!("HTTP accept error: {e}"),
                    }
                }
            })
            .expect("failed to spawn http accept thread");
    }

    // RESP accept loop runs on the main thread.
    let resp_addr = format!("0.0.0.0:{resp_port}");
    let resp_listener = TcpListener::bind(&resp_addr)?;
    tracing::info!("RESP listening on {resp_addr}");

    for result in resp_listener.incoming() {
        match result {
            Ok(stream) => {
                let peer = match stream.peer_addr() {
                    Ok(p) => p,
                    Err(e) => {
                        tracing::debug!("peer_addr: {e}");
                        continue;
                    }
                };
                if !accept_one(
                    stream,
                    peer,
                    peek_resp_key,
                    &resp_senders,
                    &mut resp_wakeup_writers,
                    &rr,
                ) {
                    tracing::warn!("RESP channel closed, stopping accept loop");
                    break;
                }
            }
            Err(e) => tracing::error!("accept error: {e}"),
        }
    }

    for h in handles {
        let _ = h.join();
    }

    Ok(())
}

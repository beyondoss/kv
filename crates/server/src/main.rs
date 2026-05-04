#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::io::Write as _;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::unix::net::UnixStream;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{self, SyncSender};
use std::time::Duration;

fn route(key: Option<Vec<u8>>, n: usize, rr: &AtomicUsize) -> usize {
    match key {
        Some(k) => beyond_kv::routing::shard_for_key(&k, n),
        None => rr.fetch_add(1, Ordering::Relaxed) % n,
    }
}

use beyond_kv::routing::{peek_http_key, peek_resp_key};

/// Write a minimal RESP error to a freshly-accepted stream that can't be
/// dispatched (inbox full). Does not panic on I/O failure.
fn reject_resp(mut stream: TcpStream) {
    let _ = stream.write_all(b"-ERR server busy, please retry later\r\n");
}

/// Write a minimal HTTP 503 to a freshly-accepted stream that can't be
/// dispatched (inbox full). Does not panic on I/O failure.
fn reject_http(mut stream: TcpStream) {
    let _ = stream.write_all(
        b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
    );
}

/// Route one accepted connection to a worker shard.
///
/// Returns `false` only when a worker channel has been permanently
/// disconnected (dead worker), indicating the accept loop should stop.
/// A full inbox sheds the connection and returns `true` so the caller
/// keeps accepting.
fn accept_one(
    stream: TcpStream,
    peer: SocketAddr,
    peek_key: fn(&TcpStream) -> Option<Vec<u8>>,
    on_reject: fn(TcpStream),
    senders: &[SyncSender<(TcpStream, SocketAddr)>],
    wakeup_writers: &mut [UnixStream],
    rr: &AtomicUsize,
) -> bool {
    let idx = route(peek_key(&stream), senders.len(), rr);
    match senders[idx].try_send((stream, peer)) {
        Ok(()) => {}
        Err(mpsc::TrySendError::Full((stream, _))) => {
            tracing::warn!(worker = idx, %peer, "worker inbox full; shedding connection");
            on_reject(stream);
            return true;
        }
        Err(mpsc::TrySendError::Disconnected(_)) => {
            tracing::error!(
                worker = idx,
                "worker channel disconnected; stopping accept loop"
            );
            return false;
        }
    }
    if let Err(e) = wakeup_writers[idx].write_all(&[1u8]) {
        tracing::error!(worker = idx, error = %e, "wakeup pipe write failed; worker likely dead");
        return false;
    }
    true
}

/// Non-blocking accept loop shared by both protocols. Exits when the shutdown
/// flag is set or a worker channel is permanently disconnected.
fn accept_loop(
    listener: &TcpListener,
    peek_key: fn(&TcpStream) -> Option<Vec<u8>>,
    on_reject: fn(TcpStream),
    senders: &[SyncSender<(TcpStream, SocketAddr)>],
    wakeup_writers: &mut [UnixStream],
    rr: &AtomicUsize,
    shutdown: &AtomicBool,
    label: &str,
) {
    loop {
        if shutdown.load(Ordering::Relaxed) {
            tracing::info!("{label} accept loop: shutdown signal received, draining");
            break;
        }
        match listener.accept() {
            Ok((stream, peer)) => {
                if !accept_one(
                    stream,
                    peer,
                    peek_key,
                    on_reject,
                    senders,
                    wakeup_writers,
                    rr,
                ) {
                    break;
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(e) => tracing::error!("{label} accept error: {e}"),
        }
    }
}

fn main() -> anyhow::Result<()> {
    let cfg = beyond_kv::config::Config::parse();
    cfg.validate()?;

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "beyond_kv=info".into()),
        )
        .init();

    // Log panics (including those in worker threads) before the process
    // aborts (release) or the thread unwinds (debug).
    std::panic::set_hook(Box::new(|info| {
        tracing::error!(panic = %info, "thread panicked");
        eprintln!("PANIC: {info}");
    }));

    let n_threads = cfg.threads.unwrap_or_else(num_cpus::get).max(1);
    tracing::info!(
        threads = n_threads,
        resp_port = cfg.resp_port,
        "starting beyond-kv"
    );

    let data_dir = cfg.data_dir.clone();
    let resp_port = cfg.resp_port;
    let http_port = cfg.http_port;
    let memory_per_shard = cfg.memory_bytes / n_threads;
    let reclaim_sealed_threshold = cfg.reclaim_sealed_threshold;
    let reclaim_interval_secs = cfg.reclaim_interval_secs;
    let max_conns = cfg.max_conns_per_shard;
    let idle_timeout = Duration::from_secs(cfg.idle_timeout_secs);
    let max_value_bytes = cfg.max_value_bytes;
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
                                    monoio::time::sleep(Duration::from_secs(30)).await;
                                    sweep_store.sweep_cache();
                                }
                            });

                            let sync_store = store.clone();
                            monoio::spawn(async move {
                                let mut consecutive_failures = 0u32;
                                loop {
                                    monoio::time::sleep(Duration::from_secs(1)).await;
                                    match sync_store.sync_logs().await {
                                        Ok(()) => consecutive_failures = 0,
                                        Err(e) => {
                                            consecutive_failures += 1;
                                            if consecutive_failures >= 3 {
                                                tracing::error!(
                                                    error = %e,
                                                    consecutive = consecutive_failures,
                                                    "periodic log sync failing repeatedly; \
                                                     durability degraded"
                                                );
                                            } else {
                                                tracing::warn!(
                                                    error = %e,
                                                    "periodic log sync failed"
                                                );
                                            }
                                        }
                                    }
                                }
                            });

                            if reclaim_sealed_threshold > 0 {
                                let reclaim_store = store.clone();
                                monoio::spawn(async move {
                                    loop {
                                        monoio::time::sleep(Duration::from_secs(
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
                                beyond_kv::http::serve_routed(
                                    http_store,
                                    http_rx,
                                    http_wake_read,
                                    max_conns,
                                    idle_timeout,
                                    max_value_bytes,
                                )
                                .await;
                            });

                            beyond_kv::resp::serve(
                                store.clone(),
                                resp_rx,
                                resp_wake_read,
                                max_conns,
                                idle_timeout,
                                i,
                                n_threads,
                            )
                            .await;

                            // Flush WAL before the worker exits so that all acked
                            // writes are durable even when we shut down mid-interval.
                            if let Err(e) = store.sync_logs().await {
                                tracing::error!(error = %e, "final log sync failed on shutdown");
                            } else {
                                tracing::debug!("worker {i}: final log sync complete");
                            }
                        })
                })
                .expect("failed to spawn worker thread")
        })
        .collect();

    let rr = Arc::new(AtomicUsize::new(0));

    // Register SIGTERM and SIGINT to set the shutdown flag atomically.
    let shutdown = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(signal_hook::consts::SIGTERM, Arc::clone(&shutdown))?;
    signal_hook::flag::register(signal_hook::consts::SIGINT, Arc::clone(&shutdown))?;

    // HTTP accept thread (non-blocking listener + shutdown-aware loop).
    let http_addr = format!("0.0.0.0:{http_port}");
    let http_listener = TcpListener::bind(&http_addr)?;
    http_listener.set_nonblocking(true)?;
    tracing::info!("HTTP listening on {http_addr}");

    let http_thread = {
        let rr = rr.clone();
        let shutdown = Arc::clone(&shutdown);
        std::thread::Builder::new()
            .name("kv-http-accept".into())
            .spawn(move || {
                accept_loop(
                    &http_listener,
                    peek_http_key,
                    reject_http,
                    &http_senders,
                    &mut http_wakeup_writers,
                    &rr,
                    &shutdown,
                    "HTTP",
                );
                // Dropping http_senders + http_wakeup_writers here signals workers.
            })?
    };

    // RESP accept loop runs on the main thread.
    let resp_addr = format!("0.0.0.0:{resp_port}");
    let resp_listener = TcpListener::bind(&resp_addr)?;
    resp_listener.set_nonblocking(true)?;
    tracing::info!("RESP listening on {resp_addr}");

    accept_loop(
        &resp_listener,
        peek_resp_key,
        reject_resp,
        &resp_senders,
        &mut resp_wakeup_writers,
        &rr,
        &shutdown,
        "RESP",
    );

    // Dropping senders + wakeup writers closes the channels and pipes,
    // which causes workers' serve() loops to return so they can flush.
    drop(resp_senders);
    drop(resp_wakeup_writers);

    // Ensure the HTTP thread has also finished and released its resources.
    shutdown.store(true, Ordering::Relaxed);
    if let Err(e) = http_thread.join() {
        tracing::error!("HTTP accept thread panicked: {e:?}");
    }

    tracing::info!("waiting for workers to flush and exit");
    for h in handles {
        if let Err(e) = h.join() {
            tracing::error!("worker thread panicked: {e:?}");
        }
    }
    tracing::info!("shutdown complete");

    Ok(())
}

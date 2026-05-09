#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

use clap::Parser as _;

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
#[allow(clippy::too_many_arguments)]
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

#[derive(clap::Parser)]
#[command(name = "beyond-kv", about = "Beyond KV server")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(clap::Subcommand)]
enum Command {
    /// Run the KV server.
    Serve(Box<beyond_kv::config::Config>),
    /// Write openapi/v1.json from the annotated routes and exit.
    GenerateOpenapi,
}

fn generate_openapi() -> anyhow::Result<()> {
    use utoipa::OpenApi as _;
    let doc = beyond_kv::http::ApiDoc::openapi();
    let json = serde_json::to_string_pretty(&doc)?;
    std::fs::create_dir_all("openapi")?;
    std::fs::write("openapi/v1.json", json)?;
    println!("wrote openapi/v1.json");
    Ok(())
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let cfg = match cli.command {
        Command::GenerateOpenapi => return generate_openapi(),
        Command::Serve(cfg) => *cfg,
    };
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
    let readyz_sync_failure_threshold = cfg.readyz_sync_failure_threshold;
    tracing::info!(http_port, "HTTP server enabled");

    // Per-worker, per-protocol channel + wakeup pipe.
    let mut resp_senders: Vec<SyncSender<(TcpStream, SocketAddr)>> = Vec::with_capacity(n_threads);
    let mut resp_wakeup_writers: Vec<UnixStream> = Vec::with_capacity(n_threads);
    let mut http_senders: Vec<SyncSender<(TcpStream, SocketAddr)>> = Vec::with_capacity(n_threads);
    let mut http_wakeup_writers: Vec<UnixStream> = Vec::with_capacity(n_threads);
    #[allow(clippy::type_complexity)]
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

    // Cross-shard request channels: one inbox per shard, shared sender array.
    // Senders are cheap to clone; the `Arc<[Sender]>` lets every connection
    // route a sub-request to any shard without taking a lock.
    //
    // Each shard also gets a wakeup pipe so a remote sender can interrupt the
    // target shard's `io_uring_enter` sleep — bare futures wakers cannot do this.
    let (cross_shard_tx_vec, cross_shard_wake_writes, cross_shard_rx_vec, cross_shard_wake_reads) =
        beyond_kv::cross_shard::build_channels(n_threads);
    let cross_shard_txs: Arc<[_]> = Arc::from(cross_shard_tx_vec);
    let cross_shard_wakeups: Arc<[_]> = Arc::from(cross_shard_wake_writes);
    let shutdown_error = Arc::new(AtomicBool::new(false));
    let metrics = beyond_kv::metrics::Metrics::new();

    // Per-shard counter of consecutive log-sync failures. /readyz reports 503
    // when any shard reaches the failure threshold so load balancers can take
    // it out of rotation while durability is degraded.
    let sync_failures: Arc<[std::sync::atomic::AtomicU32]> = (0..n_threads)
        .map(|_| std::sync::atomic::AtomicU32::new(0))
        .collect::<Vec<_>>()
        .into();

    let handles: Vec<_> = (0..n_threads)
        .zip(worker_inboxes)
        .zip(cross_shard_rx_vec)
        .zip(cross_shard_wake_reads)
        .map(
            |(
                ((i, (resp_rx, resp_wake_read, http_rx, http_wake_read)), cross_shard_rx),
                cross_shard_wake_read,
            )| {
                let data_dir = data_dir.clone();
                let cross_shard_txs = cross_shard_txs.clone();
                let cross_shard_wakeups = cross_shard_wakeups.clone();
                let shutdown_error = shutdown_error.clone();
                let metrics = metrics.clone();
                let sync_failures = Arc::clone(&sync_failures);
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
                            let counters = store.cache_counters();
                            metrics.register_cache_counters(counters.hits, counters.misses);
                            let store = Rc::new(store);

                            let sweep_store = store.clone();
                            let sweep_metrics = metrics.clone();
                            let shard_label = format!("{i}");
                            monoio::spawn(async move {
                                loop {
                                    monoio::time::sleep(Duration::from_secs(30)).await;
                                    let expired = sweep_store.sweep_cache();
                                    if expired > 0 {
                                        sweep_metrics.keys_expired_total.with_label_values(&[&shard_label]).inc_by(expired as f64);
                                    }
                                    sweep_metrics.cache_size_bytes.with_label_values(&[&shard_label]).set(sweep_store.cache_bytes_used() as f64);
                                    sweep_metrics.cache_entries.with_label_values(&[&shard_label]).set(sweep_store.cache_len() as f64);
                                    sweep_metrics.namespaces_open.with_label_values(&[&shard_label]).set(sweep_store.namespace_count() as f64);
                                    sweep_metrics.sealed_segments.with_label_values(&[&shard_label]).set(sweep_store.sealed_segment_count() as f64);
                                }
                            });

                            let sync_store = store.clone();
                            let sync_fail_counter = Arc::clone(&sync_failures);
                            let sync_metrics = metrics.clone();
                            let sync_shard_label = format!("{i}");
                            monoio::spawn(async move {
                                loop {
                                    monoio::time::sleep(Duration::from_secs(1)).await;
                                    let sync_start = std::time::Instant::now();
                                    match sync_store.sync_logs().await {
                                        Ok(()) => {
                                            sync_metrics.log_sync_duration_seconds.with_label_values(&[&sync_shard_label]).observe(sync_start.elapsed().as_secs_f64());
                                            sync_fail_counter[i].store(0, Ordering::Relaxed);
                                        }
                                        Err(e) => {
                                            sync_metrics.log_sync_failures_total.with_label_values(&[&sync_shard_label]).inc();
                                            let n = sync_fail_counter[i]
                                                .fetch_add(1, Ordering::Relaxed)
                                                + 1;
                                            if n >= readyz_sync_failure_threshold {
                                                tracing::error!(
                                                    error = %e,
                                                    consecutive = n,
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
                                let reclaim_metrics = metrics.clone();
                                let reclaim_shard_label = format!("{i}");
                                monoio::spawn(async move {
                                    loop {
                                        monoio::time::sleep(Duration::from_secs(
                                            reclaim_interval_secs,
                                        ))
                                        .await;
                                        match reclaim_store
                                            .reclaim_if_needed(reclaim_sealed_threshold)
                                            .await
                                        {
                                            Ok(summary) if summary.namespaces_reclaimed > 0 => {
                                                reclaim_metrics.reclaim_runs_total.with_label_values(&[&reclaim_shard_label]).inc_by(summary.namespaces_reclaimed as f64);
                                                reclaim_metrics.reclaim_files_freed_total.with_label_values(&[&reclaim_shard_label]).inc_by(summary.files_freed as f64);
                                            }
                                            Ok(_) => {}
                                            Err(e) => {
                                                tracing::warn!(error = %e, "auto-reclaim failed");
                                            }
                                        }
                                    }
                                });
                            }

                            let http_store = store.clone();
                            let http_txs = cross_shard_txs.clone();
                            let http_wakeups = cross_shard_wakeups.clone();
                            let http_metrics = metrics.clone();
                            let http_sync_failures = Arc::clone(&sync_failures);
                            monoio::spawn(async move {
                                beyond_kv::http::serve_routed(
                                    http_store,
                                    http_rx,
                                    http_wake_read,
                                    max_conns,
                                    idle_timeout,
                                    max_value_bytes,
                                    i,
                                    n_threads,
                                    http_txs,
                                    http_wakeups,
                                    http_metrics,
                                    http_sync_failures,
                                    readyz_sync_failure_threshold,
                                )
                                .await;
                            });

                            // Cross-shard request handler: drains this shard's
                            // inbox of MGET/MSET/DEL/EXISTS sub-requests sent by
                            // other shards, runs them against the local store.
                            let cross_store = store.clone();
                            monoio::spawn(async move {
                                beyond_kv::cross_shard::serve(
                                    cross_store,
                                    cross_shard_rx,
                                    cross_shard_wake_read,
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
                                cross_shard_txs,
                                cross_shard_wakeups,
                                metrics.clone(),
                            )
                            .await;

                            // Flush WAL before the worker exits so that all acked
                            // writes are durable even when we shut down mid-interval.
                            if let Err(e) = store.sync_logs().await {
                                tracing::error!(error = %e, "final log sync failed on shutdown");
                            } else {
                                tracing::debug!("worker {i}: final log sync complete");
                            }
                            // Seal active files so the next startup reads footers
                            // instead of replaying records.
                            if let Err(e) = store.seal_all_for_shutdown().await {
                                tracing::error!(error = %e, "seal on shutdown failed");
                                shutdown_error.store(true, Ordering::Relaxed);
                            }
                        })
                    })
                    .expect("failed to spawn worker thread")
            },
        )
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
    let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
    std::thread::Builder::new()
        .name("kv-shutdown-watchdog".into())
        .spawn(move || {
            for h in handles {
                if let Err(e) = h.join() {
                    tracing::error!("worker thread panicked: {e:?}");
                }
            }
            let _ = done_tx.send(());
        })?;
    const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);
    match done_rx.recv_timeout(SHUTDOWN_TIMEOUT) {
        Ok(()) => tracing::info!("shutdown complete"),
        Err(_) => {
            tracing::error!("workers did not exit within {SHUTDOWN_TIMEOUT:?}; forcing abort");
            std::process::abort();
        }
    }

    if shutdown_error.load(Ordering::Relaxed) {
        anyhow::bail!("one or more workers failed to seal log files on shutdown");
    }
    Ok(())
}

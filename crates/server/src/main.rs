#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod config;
mod dispatch;
mod http;
mod resp;

fn main() -> anyhow::Result<()> {
    let cfg = config::Config::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "beyond_kv=info".into()),
        )
        .init();

    let n_threads = cfg.threads.unwrap_or_else(num_cpus::get);
    tracing::info!(threads = n_threads, resp_port = cfg.resp_port, "starting beyond-kv");

    let data_dir = cfg.data_dir.clone();
    let resp_port = cfg.resp_port;
    let http_port = cfg.http_port;
    tracing::info!(http_port, "HTTP server enabled");

    let handles: Vec<_> = (0..n_threads)
        .map(|i| {
            let data_dir = data_dir.clone();
            std::thread::Builder::new()
                .name(format!("kv-worker-{i}"))
                .spawn(move || {
                    let shard_dir = data_dir.join(format!("shard-{i}"));
                    let store = beyond_kv_engine::store::ShardStore::open(&shard_dir, cfg.memory_bytes / n_threads)
                        .expect("failed to open store");
                    let store = std::rc::Rc::new(store);

                    monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
                        .enable_timer()
                        .build()
                        .expect("failed to build monoio runtime")
                        .block_on(async {
                            // Background TTL sweeper
                            let sweep_store = store.clone();
                            monoio::spawn(async move {
                                loop {
                                    monoio::time::sleep(std::time::Duration::from_secs(30)).await;
                                    sweep_store.sweep_cache();
                                }
                            });
                            let http_store = store.clone();
                            monoio::spawn(async move {
                                http::serve(http_store, http_port).await;
                            });
                            resp::serve(store, resp_port).await;
                        })
                })
                .expect("failed to spawn worker thread")
        })
        .collect();

    for h in handles {
        let _ = h.join();
    }

    Ok(())
}

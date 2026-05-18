pub mod config;
pub mod cross_shard;
pub mod dispatch;
pub mod handoff;
pub mod http;
pub mod metrics;
pub mod resp;
pub mod routing;
pub mod tls;

pub use tls::{BeyondStream, TlsAcceptor};

use std::cell::Cell;
use std::net::SocketAddr;
use std::os::unix::net::UnixStream as StdUnixStream;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::mpsc;

use monoio::io::AsyncReadRent;
use monoio::net::{TcpStream, UnixStream};

use crate::metrics::Metrics;

/// RAII guard that decrements a shared connection counter and the per-shard
/// `kv_active_connections` gauge on drop.
pub(crate) struct ConnGuard {
    count: Rc<Cell<usize>>,
    gauge: prometheus::Gauge,
}

impl ConnGuard {
    fn new(count: Rc<Cell<usize>>, gauge: prometheus::Gauge) -> Self {
        gauge.inc();
        Self { count, gauge }
    }
}

impl Drop for ConnGuard {
    fn drop(&mut self) {
        self.count.set(self.count.get().saturating_sub(1));
        self.gauge.dec();
    }
}

/// Wakeup-pipe driven accept loop shared by HTTP and RESP servers.
///
/// Reads from the wakeup pipe, drains the connection inbox, enforces `max_conns`,
/// and calls `on_conn` for each accepted stream. The `ConnGuard` passed to
/// `on_conn` should be held until the connection task completes so the counter
/// stays accurate.
pub(crate) async fn serve_loop(
    rx: mpsc::Receiver<(std::net::TcpStream, SocketAddr)>,
    wakeup_read: StdUnixStream,
    max_conns: usize,
    label: &'static str,
    shard_idx: usize,
    metrics: Arc<Metrics>,
    mut on_conn: impl FnMut(TcpStream, SocketAddr, ConnGuard),
) {
    if let Err(e) = wakeup_read.set_nonblocking(true) {
        tracing::error!("{label} wakeup set_nonblocking failed: {e}");
        return;
    }
    let mut wakeup = match UnixStream::from_std(wakeup_read) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!("failed to create {label} wakeup stream: {e}");
            return;
        }
    };
    let conn_count = Rc::new(Cell::new(0usize));
    let mut buf = vec![0u8; 64];
    loop {
        let res;
        (res, buf) = wakeup.read(buf).await;
        match res {
            Ok(0) | Err(_) => break,
            Ok(_) => {}
        }
        while let Ok((stream, peer)) = rx.try_recv() {
            if conn_count.get() >= max_conns {
                tracing::warn!(
                    %peer,
                    limit = max_conns,
                    "{label} connection limit reached; dropping connection"
                );
                drop(stream);
                continue;
            }
            if stream.set_nonblocking(true).is_err() {
                tracing::error!(%peer, "{label} set_nonblocking failed");
                continue;
            }
            match TcpStream::from_std(stream) {
                Ok(s) => {
                    tracing::debug!(%peer, "accepted {label} connection");
                    conn_count.set(conn_count.get() + 1);
                    let shard_label = shard_idx.to_string();
                    let gauge = metrics
                        .active_connections
                        .with_label_values(&[shard_label.as_str(), label]);
                    let guard = ConnGuard::new(conn_count.clone(), gauge);
                    on_conn(s, peer, guard);
                }
                Err(e) => tracing::error!(%peer, "{label} TcpStream::from_std: {e}"),
            }
        }
    }
}

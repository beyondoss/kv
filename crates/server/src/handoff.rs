//! Bridges the sync `handoff::Drainable` trait into monoio worker threads.
//!
//! Topology mirrors the cross-shard channel pattern in `cross_shard.rs`:
//!
//! - One `mpsc::Receiver<HandoffOp>` lives on each monoio worker, drained by
//!   `serve_handoff_inbox` (an async task spawned alongside the RESP/HTTP
//!   accept loops).
//! - One `UnixStream::pair()` per worker provides the wakeup pipe so a remote
//!   sender can interrupt `io_uring_enter` — bare futures wakers do NOT wake
//!   a sleeping monoio thread.
//! - `KvHandoff` (the [`handoff::Drainable`] impl) is owned by the dedicated
//!   handoff control thread (where `handoff::Incumbent::serve` runs). It
//!   sends one op per worker and gathers all replies before returning.
//!
//! The `accept_closed` atomic is shared with the main-thread RESP accept loop
//! and the HTTP accept thread so they can stop accepting new connections
//! when a drain is in progress.

use std::io::Write as _;
use std::os::unix::net::UnixStream as StdUnixStream;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::time::{Duration, Instant};

use beyond_kv_engine::store::ShardStore;
use handoff::{DrainReport, Drainable, SealReport, StateSnapshot};
use monoio::io::AsyncReadRent;
use monoio::net::UnixStream;

use crate::metrics::Metrics;

/// Internal operations sent from the handoff control thread to each worker.
pub enum HandoffOp {
    /// fsync any unsynced writes.
    Drain {
        reply: SyncSender<Result<(), String>>,
    },
    /// Write a footer to each active namespace file (existing
    /// `seal_all_for_shutdown` behavior); report this shard's per-namespace
    /// last revisions.
    Seal {
        reply: SyncSender<Result<Vec<u64>, String>>,
    },
    /// Open a fresh active segment for each namespace that was previously
    /// sealed via [`HandoffOp::Seal`]. Used by the supervisor's
    /// `ResumeAfterAbort` path.
    Resume {
        reply: SyncSender<Result<(), String>>,
    },
    /// Cheap read of shard last revisions.
    Snapshot { reply: SyncSender<Vec<u64>> },
}

/// `Drainable` implementation that lives in the handoff control thread.
/// Each method sends one [`HandoffOp`] per worker and aggregates replies.
pub struct KvHandoff {
    senders: Vec<SyncSender<HandoffOp>>,
    wakeups: Vec<StdUnixStream>,
    /// Set by `drain` (so accept loops stop accepting). Cleared by `resume`.
    pub accept_closed: Arc<AtomicBool>,
    metrics: Arc<Metrics>,
    n_shards: usize,
}

impl KvHandoff {
    pub fn new(
        senders: Vec<SyncSender<HandoffOp>>,
        wakeups: Vec<StdUnixStream>,
        accept_closed: Arc<AtomicBool>,
        metrics: Arc<Metrics>,
    ) -> Self {
        let n_shards = senders.len();
        Self {
            senders,
            wakeups,
            accept_closed,
            metrics,
            n_shards,
        }
    }

    /// Sum the per-shard, per-proto `kv_active_connections` gauges. Cheap
    /// (atomic load per gauge).
    fn total_open_conns(&self) -> u32 {
        let mut total = 0u32;
        for shard in 0..self.n_shards {
            let label = shard.to_string();
            for proto in ["RESP", "HTTP"] {
                let g = self
                    .metrics
                    .active_connections
                    .with_label_values(&[label.as_str(), proto]);
                total = total.saturating_add(g.get().max(0.0) as u32);
            }
        }
        total
    }

    fn fan_out<R: Send + 'static>(
        &self,
        mut make_op: impl FnMut(SyncSender<R>) -> HandoffOp,
    ) -> handoff::Result<Vec<Receiver<R>>> {
        let mut replies = Vec::with_capacity(self.senders.len());
        for (i, s) in self.senders.iter().enumerate() {
            let (tx, rx) = sync_channel::<R>(1);
            s.send(make_op(tx)).map_err(|_| handoff::Error::Channel)?;
            // Interrupt the worker's `io_uring_enter` sleep.
            let _ = (&self.wakeups[i]).write(&[1u8]);
            replies.push(rx);
        }
        Ok(replies)
    }
}

impl Drainable for KvHandoff {
    fn drain(&self, deadline: Instant) -> handoff::Result<DrainReport> {
        let started = Instant::now();
        // Stop new accepts immediately.
        self.accept_closed.store(true, Ordering::SeqCst);
        // Trigger sync on every shard; wait for completion.
        let replies = self.fan_out(|reply| HandoffOp::Drain { reply })?;
        for rx in replies {
            let timeout = deadline.saturating_duration_since(Instant::now());
            rx.recv_timeout(timeout)
                .map_err(|_| handoff::Error::Timeout("drain reply"))?
                .map_err(|e| handoff::Error::Protocol(format!("drain failed: {e}")))?;
        }
        // Wait for in-flight connections to close. Poll the prometheus gauges.
        while self.total_open_conns() > 0 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(25));
        }
        self.metrics
            .handoff_drain_seconds
            .observe(started.elapsed().as_secs_f64());
        Ok(DrainReport {
            open_conns_remaining: self.total_open_conns(),
            accept_closed: true,
        })
    }

    fn seal(&self) -> handoff::Result<SealReport> {
        let started = Instant::now();
        let replies = self.fan_out(|reply| HandoffOp::Seal { reply })?;
        let mut last_revision_per_shard = Vec::with_capacity(self.senders.len());
        for rx in replies {
            let revs = rx
                .recv()
                .map_err(|_| handoff::Error::Channel)?
                .map_err(|e| {
                    self.metrics.handoff_seal_failures_total.inc();
                    self.metrics
                        .handoff_handoffs_total
                        .with_label_values(&["seal_failed"])
                        .inc();
                    handoff::Error::Protocol(format!("seal failed: {e}"))
                })?;
            // Per-shard last revision = max across this shard's namespaces.
            last_revision_per_shard.push(revs.into_iter().max().unwrap_or(0));
        }
        self.metrics
            .handoff_seal_seconds
            .observe(started.elapsed().as_secs_f64());
        Ok(SealReport {
            last_revision_per_shard,
            data_dir_fingerprint: [0u8; 32],
        })
    }

    fn resume_after_abort(&self) -> handoff::Result<()> {
        let replies = self.fan_out(|reply| HandoffOp::Resume { reply })?;
        for rx in replies {
            rx.recv()
                .map_err(|_| handoff::Error::Channel)?
                .map_err(|e| handoff::Error::Protocol(format!("resume failed: {e}")))?;
        }
        // Re-open accept loops.
        self.accept_closed.store(false, Ordering::SeqCst);
        self.metrics.handoff_rolled_back_total.inc();
        self.metrics
            .handoff_handoffs_total
            .with_label_values(&["resumed"])
            .inc();
        Ok(())
    }

    fn snapshot_state(&self) -> StateSnapshot {
        let replies = match self.fan_out(|reply| HandoffOp::Snapshot { reply }) {
            Ok(r) => r,
            Err(_) => return StateSnapshot::default(),
        };
        let mut last_revision_per_shard = Vec::with_capacity(self.senders.len());
        for rx in replies {
            if let Ok(revs) = rx.recv_timeout(Duration::from_secs(1)) {
                last_revision_per_shard.push(revs.into_iter().max().unwrap_or(0));
            }
        }
        StateSnapshot {
            shard_count: self.senders.len() as u32,
            open_conns: self.total_open_conns(),
            last_revision_per_shard,
        }
    }
}

/// Build per-worker channels. Mirrors `cross_shard::build_channels`.
#[allow(clippy::type_complexity)]
pub fn build_channels(
    n: usize,
) -> (
    Vec<SyncSender<HandoffOp>>,
    Vec<StdUnixStream>,
    Vec<Receiver<HandoffOp>>,
    Vec<StdUnixStream>,
) {
    let mut txs = Vec::with_capacity(n);
    let mut rxs = Vec::with_capacity(n);
    let mut wake_writes = Vec::with_capacity(n);
    let mut wake_reads = Vec::with_capacity(n);
    for _ in 0..n {
        let (tx, rx) = sync_channel::<HandoffOp>(8);
        let (wake_read, wake_write) =
            StdUnixStream::pair().expect("handoff wakeup unix socket pair");
        txs.push(tx);
        rxs.push(rx);
        wake_writes.push(wake_write);
        wake_reads.push(wake_read);
    }
    (txs, wake_writes, rxs, wake_reads)
}

/// Per-worker monoio task that drains the handoff inbox.
pub async fn serve_handoff_inbox(
    store: Rc<ShardStore>,
    rx: Receiver<HandoffOp>,
    wakeup_read: StdUnixStream,
) {
    if let Err(e) = wakeup_read.set_nonblocking(true) {
        tracing::error!("handoff inbox wakeup set_nonblocking failed: {e}");
        return;
    }
    let mut wakeup = match UnixStream::from_std(wakeup_read) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!("failed to register handoff wakeup stream: {e}");
            return;
        }
    };
    let mut buf = vec![0u8; 64];
    loop {
        let res;
        (res, buf) = wakeup.read(buf).await;
        if matches!(res, Ok(0) | Err(_)) {
            return;
        }
        loop {
            match rx.try_recv() {
                Ok(op) => handle_op(op, &store).await,
                Err(std::sync::mpsc::TryRecvError::Empty) => break,
                Err(std::sync::mpsc::TryRecvError::Disconnected) => return,
            }
        }
    }
}

async fn handle_op(op: HandoffOp, store: &Rc<ShardStore>) {
    match op {
        HandoffOp::Drain { reply } => {
            let res = store.sync_logs().await.map_err(|e| e.to_string());
            let _ = reply.send(res);
        }
        HandoffOp::Seal { reply } => {
            let res = match store.seal_all_for_shutdown().await {
                Ok(()) => Ok(store.last_revision_per_namespace()),
                Err(e) => Err(e.to_string()),
            };
            let _ = reply.send(res);
        }
        HandoffOp::Resume { reply } => {
            let res = store.resume_after_abort().await.map_err(|e| e.to_string());
            let _ = reply.send(res);
        }
        HandoffOp::Snapshot { reply } => {
            let _ = reply.send(store.last_revision_per_namespace());
        }
    }
}

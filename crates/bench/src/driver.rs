//! The benchmark driver.
//!
//! Two modes, one code path:
//!
//! - **Closed loop** — every worker runs flat-out. Measures saturation
//!   throughput. Latency numbers are deeply misleading (no queueing model),
//!   but the throughput number is real.
//!
//! - **Open loop** — every worker schedules its own arrivals from a Poisson
//!   process at `rate / concurrency` ops/sec. Critically, when a worker falls
//!   behind, the next scheduled time is *not* shifted forward — so response
//!   time is measured from the *intended* send time, not the *actual* one.
//!   This is coordinated-omission correction (Gil Tene), and without it tail
//!   latencies under load are systematically understated.
//!
//! The driver runs an optional populate phase, an optional warmup phase, then
//! the measured phase. Each phase uses an independent seed but identical
//! workload parameters, so two runs at the same plan are byte-identical
//! across targets.

use std::sync::Arc;
use std::time::{Duration, Instant};

use rand::{rngs::StdRng, Rng, SeedableRng};
use rand_distr::{Distribution, Exp};
use tokio::time::sleep_until;

use crate::client::Target;
use crate::metrics::{Report, WorkerStats};
use crate::workload::{Op, Workload};

#[derive(Debug, Clone, Copy)]
pub enum Mode {
    /// Saturate the server: workers loop as fast as possible.
    Closed,
    /// Issue at a fixed rate with Poisson arrivals (CO-corrected).
    Open { rate_per_sec: f64 },
}

#[derive(Debug, Clone)]
pub struct Plan {
    pub mode: Mode,
    pub concurrency: usize,
    pub duration: Duration,
    pub warmup: Duration,
    /// If true, the driver pre-populates the keyspace via SETs before warmup.
    pub populate: bool,
    pub seed: u64,
}

pub struct Driver {
    pub workload: Arc<Workload>,
    pub plan: Plan,
}

impl Driver {
    pub fn new(workload: Workload, plan: Plan) -> Self {
        Self { workload: Arc::new(workload), plan }
    }

    /// Reset the target's keyspace and (optionally) repopulate it.
    /// Call once per target before any number of [`Driver::measure`] calls.
    pub async fn prepare(&self, target: Arc<dyn Target>) -> anyhow::Result<()> {
        target.reset().await?;
        if self.plan.populate {
            tracing::info!(target = target.name(), "populating keyspace");
            populate(target, self.workload.clone(), self.plan.concurrency).await?;
        }
        Ok(())
    }

    /// Run one warmup + measurement phase at `mode`. The keyspace is left as-is;
    /// this is what makes rate sweeps cheap — populate once, measure many.
    pub async fn measure(
        &self,
        target: Arc<dyn Target>,
        mode: Mode,
    ) -> anyhow::Result<Report> {
        if self.plan.warmup > Duration::ZERO {
            tracing::info!(target = target.name(), mode = ?mode, warmup = ?self.plan.warmup, "warmup");
            let _ = self.run_phase(target.clone(), mode, self.plan.warmup, self.plan.seed).await?;
        }
        tracing::info!(target = target.name(), mode = ?mode, duration = ?self.plan.duration, "measuring");
        let elapsed_start = Instant::now();
        let workers = self
            .run_phase(target.clone(), mode, self.plan.duration, self.plan.seed.wrapping_add(1))
            .await?;
        let mut report =
            Report::from_workers(target.name().to_string(), workers, elapsed_start.elapsed());
        report.target_rate_per_sec = match mode {
            Mode::Open { rate_per_sec } => Some(rate_per_sec),
            Mode::Closed => None,
        };
        Ok(report)
    }

    /// Convenience: prepare then measure once at `self.plan.mode`.
    pub async fn run(&self, target: Arc<dyn Target>) -> anyhow::Result<Report> {
        self.prepare(target.clone()).await?;
        self.measure(target, self.plan.mode).await
    }

    async fn run_phase(
        &self,
        target: Arc<dyn Target>,
        mode: Mode,
        duration: Duration,
        seed: u64,
    ) -> anyhow::Result<Vec<WorkerStats>> {
        let rate_per_worker = match mode {
            Mode::Open { rate_per_sec } => Some(rate_per_sec / self.plan.concurrency as f64),
            Mode::Closed => None,
        };

        // Sync barrier: every worker starts at the same wall-clock instant so
        // arrival schedules across workers compose into the requested aggregate
        // rate without drift from staggered spawning.
        let start_at = Instant::now() + Duration::from_millis(50);
        let deadline = start_at + duration;

        let mut handles = Vec::with_capacity(self.plan.concurrency);
        for w in 0..self.plan.concurrency {
            let target = target.clone();
            let workload = self.workload.clone();
            let worker_seed = seed.wrapping_add(w as u64);
            handles.push(tokio::spawn(async move {
                run_worker(target, workload, rate_per_worker, start_at, deadline, worker_seed).await
            }));
        }

        let mut out = Vec::with_capacity(handles.len());
        for h in handles {
            out.push(h.await??);
        }
        Ok(out)
    }
}

async fn run_worker(
    target: Arc<dyn Target>,
    workload: Arc<Workload>,
    rate_per_worker: Option<f64>,
    start_at: Instant,
    deadline: Instant,
    seed: u64,
) -> anyhow::Result<WorkerStats> {
    let mut client = target.connect().await?;
    let mut rng = StdRng::seed_from_u64(seed);
    let mut stats = WorkerStats::new();

    sleep_until(start_at.into()).await;

    match rate_per_worker {
        // Open-loop with Poisson arrivals.
        Some(rate) => {
            let exp = Exp::new(rate).map_err(|e| anyhow::anyhow!("invalid rate: {e:?}"))?;
            let mut next_at = start_at;
            while next_at < deadline {
                if next_at > Instant::now() {
                    sleep_until(next_at.into()).await;
                }
                execute_one(&mut *client, &workload, &mut rng, Some(next_at), &mut stats).await;
                let dt = exp.sample(&mut rng);
                next_at += Duration::from_secs_f64(dt);
            }
        }
        // Closed-loop: send as fast as the server can answer.
        None => {
            while Instant::now() < deadline {
                execute_one(&mut *client, &workload, &mut rng, None, &mut stats).await;
            }
        }
    }

    Ok(stats)
}

#[inline]
async fn execute_one<R: Rng>(
    client: &mut dyn crate::client::Client,
    workload: &Workload,
    rng: &mut R,
    scheduled_at: Option<Instant>,
    stats: &mut WorkerStats,
) {
    let op = workload.next(rng);
    let started = Instant::now();
    let result = client.execute(&op).await;
    let completed = Instant::now();

    match result {
        Ok(()) => {
            let service_us = us(completed - started);
            // In closed-loop mode, response == service: there is no schedule.
            let response_us = scheduled_at.map_or(service_us, |s| us(completed - s));
            stats.record(op.kind(), service_us, response_us, op.keys());
        }
        Err(err) => {
            stats.errors += 1;
            tracing::debug!(?op, %err, "op failed");
        }
    }
}

#[inline]
fn us(d: Duration) -> u64 {
    // Sub-microsecond ops still need a positive bucket; clamp to 1µs.
    d.as_micros().max(1) as u64
}

/// Populate phase: deterministic SETs across the entire keyspace, sharded
/// across workers. Runs to completion regardless of `duration`.
async fn populate(
    target: Arc<dyn Target>,
    workload: Arc<Workload>,
    concurrency: usize,
) -> anyhow::Result<()> {
    let n = workload.keyspace().size();
    let mut handles = Vec::with_capacity(concurrency);
    for w in 0..concurrency {
        let target = target.clone();
        let workload = workload.clone();
        let start = (w as u64) * n / (concurrency as u64);
        let end = ((w as u64) + 1) * n / (concurrency as u64);
        handles.push(tokio::spawn(async move {
            let mut client = target.connect().await?;
            let value = workload.value().clone();
            for idx in start..end {
                let key = workload.keyspace().nth(idx);
                client.execute(&Op::Set { key, value: value.clone() }).await?;
            }
            anyhow::Ok(())
        }));
    }
    for h in handles { h.await??; }
    Ok(())
}

//! Per-worker stats and merged report.
//!
//! Each worker records into its own [`Histogram`]s — no contention on the hot
//! path. Histograms merge associatively at the end of the run.
//!
//! We track latencies **per [`OpKind`]** in addition to overall, because GET
//! and SET have completely different cost models (read path vs write path,
//! WAL/AOF involvement, cache placement) and averaging them together hides
//! exactly the things you'd want to compare.
//!
//! Two distributions per op:
//!
//! - **service time** — how long the server took once the request was on the
//!   wire. The server-internal cost.
//! - **response time** — CO-corrected: how long since the request was
//!   *scheduled to be sent*. Includes any time the client spent backed up.
//!   This is what real users would observe under load.
//!
//! The gap between them is the queueing penalty.

use hdrhistogram::Histogram;
use serde::Serialize;
use std::time::Duration;

use crate::workload::OpKind;

/// One archived benchmark run — metadata + plan + per-target reports.
///
/// Designed to be saved as JSON next to other runs so a future "after the
/// rewrite" run can be diffed against today's baseline. Methodology drift is
/// the enemy here: the plan and metadata are stored verbatim alongside the
/// numbers so the comparison stays honest.
#[derive(Debug, Serialize)]
pub struct BenchRun {
    pub metadata: RunMetadata,
    pub plan: PlanSummary,
    pub reports: Vec<Report>,
}

#[derive(Debug, Serialize)]
pub struct RunMetadata {
    /// RFC3339 UTC timestamp.
    pub timestamp: String,
    /// User-supplied label. Becomes part of the filename and aids `jq` greps.
    pub label: Option<String>,
    /// `git rev-parse --short HEAD` at the time of the run, if available.
    pub git_sha: Option<String>,
    /// `uname -srm` from inside the bench container.
    pub kernel: Option<String>,
    pub cpu_count: usize,
    /// Memory budget shared by both servers (`--memory-bytes` on Beyond,
    /// `--maxmemory` on Redis).
    pub memory_bytes: Option<u64>,
    /// Verbatim `redis-server --version` output.
    pub redis_version: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct PlanSummary {
    pub concurrency: usize,
    pub duration_secs: f64,
    pub warmup_secs: f64,
    pub populate: bool,
    pub seed: u64,
    pub batch: usize,
    pub workload: String,
    pub keys: u64,
    pub value_size: usize,
    pub keydist: String,
    pub modes: Vec<ModeSummary>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ModeSummary {
    Closed,
    Open { rate_per_sec: f64 },
}

/// 1µs–60s, 3 significant figures — covers everything from in-memory hits to
/// pathological tail events without losing precision.
const HIST_LO: u64 = 1;
const HIST_HI: u64 = 60_000_000;
const HIST_SIG: u8 = 3;

pub fn new_histogram() -> Histogram<u64> {
    Histogram::<u64>::new_with_bounds(HIST_LO, HIST_HI, HIST_SIG)
        .expect("histogram bounds are valid")
}

/// Latency record for a single op kind on one worker.
pub struct KindStats {
    pub service: Histogram<u64>,
    pub response: Histogram<u64>,
    /// Number of *calls* completed (one per network round-trip).
    pub completed: u64,
    /// Number of *logical keys* served. For single-key ops this equals
    /// `completed`; for MGET/MSET it's the sum of batch sizes.
    pub keys: u64,
}

impl KindStats {
    pub fn new() -> Self {
        Self {
            service: new_histogram(),
            response: new_histogram(),
            completed: 0,
            keys: 0,
        }
    }

    pub fn record(&mut self, service_us: u64, response_us: u64, keys: u64) {
        let _ = self.service.record(service_us);
        let _ = self.response.record(response_us);
        self.completed += 1;
        self.keys += keys;
    }

    pub fn merge_into(&self, into: &mut KindStats) {
        into.service.add(&self.service).expect("compatible histograms");
        into.response.add(&self.response).expect("compatible histograms");
        into.completed += self.completed;
        into.keys += self.keys;
    }
}

impl Default for KindStats {
    fn default() -> Self { Self::new() }
}

pub struct WorkerStats {
    /// Indexed by `OpKind as usize`.
    pub by_kind: [KindStats; OpKind::COUNT],
    pub errors: u64,
}

impl WorkerStats {
    pub fn new() -> Self {
        Self {
            by_kind: std::array::from_fn(|_| KindStats::new()),
            errors: 0,
        }
    }

    pub fn record(&mut self, kind: OpKind, service_us: u64, response_us: u64, keys: u64) {
        self.by_kind[kind as usize].record(service_us, response_us, keys);
    }
}

impl Default for WorkerStats {
    fn default() -> Self { Self::new() }
}

#[derive(Debug, Serialize)]
pub struct Report {
    pub target: String,
    /// `Some(r)` for open-loop runs (the *requested* rate, not the achieved
    /// throughput); `None` for closed-loop saturation runs.
    pub target_rate_per_sec: Option<f64>,
    pub elapsed_secs: f64,
    pub completed: u64,
    pub errors: u64,
    pub throughput_ops_per_sec: f64,
    pub overall: KindReport,
    /// One entry per [`OpKind`] that actually saw traffic.
    pub by_op: Vec<KindReport>,
}

#[derive(Debug, Serialize)]
pub struct KindReport {
    pub kind: &'static str,
    /// Calls completed (one per round-trip).
    pub completed: u64,
    /// Logical keys served (`completed` × batch size for MGET/MSET).
    pub keys: u64,
    /// Calls per second.
    pub throughput_ops_per_sec: f64,
    /// Logical keys per second.
    pub throughput_keys_per_sec: f64,
    pub service: LatencyDigest,
    pub response: LatencyDigest,
}

#[derive(Debug, Serialize)]
pub struct LatencyDigest {
    pub mean_us: f64,
    pub p50_us: u64,
    pub p90_us: u64,
    pub p99_us: u64,
    pub p999_us: u64,
    pub max_us: u64,
}

impl LatencyDigest {
    fn from(h: &Histogram<u64>) -> Self {
        Self {
            mean_us: h.mean(),
            p50_us: h.value_at_quantile(0.50),
            p90_us: h.value_at_quantile(0.90),
            p99_us: h.value_at_quantile(0.99),
            p999_us: h.value_at_quantile(0.999),
            max_us: h.max(),
        }
    }
}

impl KindReport {
    fn from(name: &'static str, k: &KindStats, elapsed_secs: f64) -> Self {
        Self {
            kind: name,
            completed: k.completed,
            keys: k.keys,
            throughput_ops_per_sec: k.completed as f64 / elapsed_secs,
            throughput_keys_per_sec: k.keys as f64 / elapsed_secs,
            service: LatencyDigest::from(&k.service),
            response: LatencyDigest::from(&k.response),
        }
    }
}

impl Report {
    pub fn from_workers(target: String, workers: Vec<WorkerStats>, elapsed: Duration) -> Self {
        // Merge all workers' per-kind histograms.
        let mut merged: [KindStats; OpKind::COUNT] = std::array::from_fn(|_| KindStats::new());
        let mut errors = 0u64;
        for w in workers {
            for (i, k) in w.by_kind.iter().enumerate() {
                k.merge_into(&mut merged[i]);
            }
            errors += w.errors;
        }
        // Build the overall histogram by adding the per-kind histograms.
        let mut overall = KindStats::new();
        for k in &merged {
            k.merge_into(&mut overall);
        }

        let elapsed_secs = elapsed.as_secs_f64().max(f64::EPSILON);
        let by_op: Vec<KindReport> = OpKind::ALL
            .iter()
            .zip(merged.iter())
            .filter(|(_, k)| k.completed > 0)
            .map(|(kind, k)| KindReport::from(kind.name(), k, elapsed_secs))
            .collect();

        Self {
            target,
            target_rate_per_sec: None,
            elapsed_secs,
            completed: overall.completed,
            errors,
            throughput_ops_per_sec: overall.completed as f64 / elapsed_secs,
            overall: KindReport::from("ALL", &overall, elapsed_secs),
            by_op,
        }
    }
}

//! `kv-bench` — honest, reproducible KV benchmarks.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use beyond_kv_bench::client::Target;
use beyond_kv_bench::driver::{Driver, Mode, Plan};
use beyond_kv_bench::keyspace::{KeyDist, Keyspace};
use beyond_kv_bench::metrics::{BenchRun, KindReport, ModeSummary, PlanSummary, Report, RunMetadata};
use beyond_kv_bench::targets::Resp;
use beyond_kv_bench::workload::{OpMix, Workload};

use clap::Parser;
use tabled::{settings::Style, Table, Tabled};

#[derive(Parser, Debug)]
#[command(name = "kv-bench", about = "Honest KV benchmarks for Beyond and friends")]
struct Cli {
    /// One or more `name=url` targets, e.g. `--target beyond=redis://127.0.0.1:6379`.
    #[arg(long = "target", required = true, value_parser = Resp::parse_spec)]
    targets: Vec<Resp>,

    /// Op mix: `get`, `set`, or `mixed:<read_pct>` (e.g. `mixed:80`).
    #[arg(long, default_value = "mixed:80")]
    workload: OpMix,

    /// Number of distinct keys.
    #[arg(long, default_value_t = 1_000_000)]
    keys: u64,

    /// Key distribution: `uniform`, `zipf`, or `zipf:<theta>`.
    #[arg(long, default_value = "uniform")]
    keydist: KeyDist,

    /// Value size in bytes.
    #[arg(long, default_value_t = 256)]
    value_size: usize,

    /// Batch size. `1` ⇒ single-key GET/SET; `>1` ⇒ MGET/MSET of N keys per
    /// call. Throughput is reported in both calls/s and keys/s; latency is
    /// per-call. Schedule rates are call rates, not key rates.
    #[arg(long, default_value_t = 1)]
    batch: usize,

    /// Number of concurrent connections / workers.
    #[arg(long, default_value_t = 64)]
    concurrency: usize,

    /// Total measurement duration (e.g. `30s`, `2m`).
    #[arg(long, default_value = "30s", value_parser = parse_duration)]
    duration: Duration,

    /// Warmup duration, discarded from the report.
    #[arg(long, default_value = "5s", value_parser = parse_duration)]
    warmup: Duration,

    /// Open-loop target rate in ops/sec. Omit for closed-loop saturation.
    /// Conflicts with `--sweep`. Accepts `k`/`m` suffixes (e.g. `50k`, `1m`).
    #[arg(long, conflicts_with = "sweep", value_parser = parse_rate)]
    rate: Option<f64>,

    /// Comma-separated rates for an open-loop sweep, low → high. Each rate runs
    /// a full warmup + measurement against every target, sharing a single
    /// populate. Produces the latency-throughput curve in one invocation.
    /// Example: `--sweep 10k,25k,50k,100k,200k,400k`.
    #[arg(long, conflicts_with = "rate")]
    sweep: Option<String>,

    /// Pre-populate the keyspace with SETs before measuring.
    #[arg(long, default_value_t = false)]
    populate: bool,

    /// Deterministic seed — fix this for reproducible runs.
    #[arg(long, default_value_t = 0xBE7011D_DEAD_BEEFu64)]
    seed: u64,

    /// Emit machine-readable JSON instead of a table.
    #[arg(long, default_value_t = false)]
    json: bool,

    /// Save the full run (metadata + plan + reports) as JSON to this path.
    /// The table is still printed to stdout. Use this to archive baselines
    /// for diffing across code changes (e.g. before/after a storage rewrite).
    #[arg(long, value_name = "FILE")]
    out: Option<PathBuf>,

    /// Optional label stored in the run metadata. Useful as a free-text tag
    /// (`rocksdb-baseline`, `lsm-rewrite-v1`, …) that survives in JSON greps.
    #[arg(long)]
    label: Option<String>,

    /// Total number of Beyond shards (= `--threads` passed to beyond-kv).
    /// When > 1, each target must pair with a `--shard-index`.
    #[arg(long, default_value_t = 1)]
    shards: usize,

    /// Which shard this run targets (0-based). The keyspace is pre-filtered to
    /// only the keys that Beyond's router sends to this shard, so the shard's
    /// L1 cache covers exactly its 1/N slice of the dataset.
    #[arg(long, default_value_t = 0)]
    shard_index: usize,
}

fn parse_duration(s: &str) -> anyhow::Result<Duration> {
    humantime::parse_duration(s).map_err(Into::into)
}

/// Parse a rate spec with `k`/`m` suffixes: `50k` → 50_000, `1.5m` → 1_500_000.
fn parse_rate(s: &str) -> anyhow::Result<f64> {
    let s = s.trim();
    let (num, mult) = match s.chars().last() {
        Some('k') | Some('K') => (&s[..s.len() - 1], 1_000.0),
        Some('m') | Some('M') => (&s[..s.len() - 1], 1_000_000.0),
        _ => (s, 1.0),
    };
    Ok(num.parse::<f64>()? * mult)
}

fn parse_sweep(s: &str) -> anyhow::Result<Vec<f64>> {
    let mut rates: Vec<f64> = s.split(',').map(parse_rate).collect::<Result<_, _>>()?;
    anyhow::ensure!(!rates.is_empty(), "sweep needs at least one rate");
    // Run low → high so the curve renders in a natural reading order.
    rates.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    Ok(rates)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    let mode = match cli.rate {
        Some(r) => Mode::Open { rate_per_sec: r },
        None => Mode::Closed,
    };
    let plan = Plan {
        mode,
        concurrency: cli.concurrency,
        duration: cli.duration,
        warmup: cli.warmup,
        populate: cli.populate,
        seed: cli.seed,
    };

    if cli.shards > 1 {
        eprintln!(
            "shard filter: shard {}/{} — pre-filtering keyspace (this may take a moment for large key counts)",
            cli.shard_index, cli.shards,
        );
    }
    let keyspace = Keyspace::new_sharded(cli.keys, cli.keydist, cli.shard_index, cli.shards)?;
    if cli.shards > 1 {
        eprintln!("shard filter: {} keys in this shard's partition", keyspace.size());
    }
    let workload = Workload::new(keyspace, cli.workload, cli.value_size, cli.batch);
    let driver = Driver::new(workload, plan);

    print_plan(&cli, &driver.plan);

    // Modes for this run: either a single mode (closed or one open rate) or a
    // sweep of open rates that share a single populate phase per target.
    let modes: Vec<Mode> = match cli.sweep.as_deref() {
        Some(spec) => parse_sweep(spec)?
            .into_iter()
            .map(|r| Mode::Open { rate_per_sec: r })
            .collect(),
        None => vec![mode],
    };

    let mut reports = Vec::with_capacity(cli.targets.len() * modes.len());
    // Sequential across targets and rates: parallel runs contaminate each
    // other's measurements (NIC, scheduler, page cache).
    for t in cli.targets {
        let target: Arc<dyn Target> = Arc::new(t);
        driver.prepare(target.clone()).await?;
        for &m in &modes {
            reports.push(driver.measure(target.clone(), m).await?);
        }
    }

    if cli.json {
        println!("{}", serde_json::to_string_pretty(&reports)?);
    } else {
        print_table(&reports);
    }

    if let Some(ref path) = cli.out {
        let run = BenchRun {
            metadata: RunMetadata {
                timestamp: env_or("BENCH_TIMESTAMP", current_timestamp()),
                label: cli.label.clone(),
                git_sha: std::env::var("BENCH_GIT_SHA").ok().filter(|s| !s.is_empty()),
                kernel: std::env::var("BENCH_KERNEL").ok().filter(|s| !s.is_empty()),
                cpu_count: num_cpus(),
                memory_bytes: std::env::var("BENCH_MEMORY_BYTES").ok().and_then(|s| s.parse().ok()),
                redis_version: std::env::var("BENCH_REDIS_VERSION").ok().filter(|s| !s.is_empty()),
            },
            plan: PlanSummary {
                concurrency: driver.plan.concurrency,
                duration_secs: driver.plan.duration.as_secs_f64(),
                warmup_secs: driver.plan.warmup.as_secs_f64(),
                populate: driver.plan.populate,
                seed: driver.plan.seed,
                batch: cli.batch,
                workload: format!("{:?}", cli.workload),
                keys: cli.keys,
                value_size: cli.value_size,
                keydist: format!("{:?}", cli.keydist),
                modes: modes.iter().map(mode_summary).collect(),
            },
            reports,
        };
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, serde_json::to_vec_pretty(&run)?)?;
        eprintln!("saved: {}", path.display());
    }
    Ok(())
}

fn env_or(key: &str, fallback: String) -> String {
    std::env::var(key).ok().filter(|s| !s.is_empty()).unwrap_or(fallback)
}

fn current_timestamp() -> String {
    // RFC3339 UTC, second precision — minimal so we don't pull `chrono`.
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
    // Days-since-epoch + h/m/s decomposition (UTC, leap-seconds ignored — fine
    // for a "when did this run happen" tag).
    let (days, sec_of_day) = (secs / 86_400, secs % 86_400);
    let (h, m, s) = (sec_of_day / 3600, (sec_of_day / 60) % 60, sec_of_day % 60);
    let (y, mo, d) = civil_from_days(days as i64);
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{m:02}:{s:02}Z")
}

/// Howard Hinnant's days→{year,month,day} algorithm. Public domain.
fn civil_from_days(z: i64) -> (i32, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = y + if m <= 2 { 1 } else { 0 };
    (y as i32, m as u32, d as u32)
}

fn num_cpus() -> usize {
    std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1)
}

fn mode_summary(m: &Mode) -> ModeSummary {
    match *m {
        Mode::Closed => ModeSummary::Closed,
        Mode::Open { rate_per_sec } => ModeSummary::Open { rate_per_sec },
    }
}

fn print_plan(cli: &Cli, plan: &Plan) {
    let shard_info = if cli.shards > 1 {
        format!(" shard={}/{}", cli.shard_index, cli.shards)
    } else {
        String::new()
    };
    eprintln!(
        "plan: workload={:?} keys={}{} keydist={:?} value_size={}B \
         concurrency={} duration={:?} warmup={:?} mode={:?} populate={} seed={:#x}",
        cli.workload,
        cli.keys,
        shard_info,
        cli.keydist,
        cli.value_size,
        plan.concurrency,
        plan.duration,
        plan.warmup,
        plan.mode,
        plan.populate,
        plan.seed,
    );
}

/// One row per (target, rate, op-kind), with an `ALL` rollup carrying errors.
/// Service time = server-internal cost. Response time = CO-corrected — what
/// users actually see under load. The gap between them is the queueing penalty.
#[derive(Tabled)]
struct Row {
    target: String,
    /// Requested call rate. `max` for closed-loop saturation runs.
    rate: String,
    op: &'static str,
    /// Calls per second (network round-trips).
    #[tabled(rename = "calls/s")]
    calls: String,
    /// Logical keys per second (`calls × batch_size` for MGET/MSET).
    #[tabled(rename = "keys/s")]
    keys: String,
    #[tabled(rename = "svc p50 µs")]
    svc_p50: u64,
    #[tabled(rename = "svc p99 µs")]
    svc_p99: u64,
    #[tabled(rename = "rsp p50 µs")]
    rsp_p50: u64,
    #[tabled(rename = "rsp p99 µs")]
    rsp_p99: u64,
    #[tabled(rename = "rsp p999 µs")]
    rsp_p999: u64,
    #[tabled(rename = "rsp max µs")]
    rsp_max: u64,
    #[tabled(rename = "err")]
    errors: String,
}

impl Row {
    fn from(target: &str, rate: &str, k: &KindReport, errors: Option<u64>) -> Self {
        Self {
            target: target.to_string(),
            rate: rate.to_string(),
            op: k.kind,
            calls: format!("{:.0}", k.throughput_ops_per_sec),
            keys: format!("{:.0}", k.throughput_keys_per_sec),
            svc_p50: k.service.p50_us,
            svc_p99: k.service.p99_us,
            rsp_p50: k.response.p50_us,
            rsp_p99: k.response.p99_us,
            rsp_p999: k.response.p999_us,
            rsp_max: k.response.max_us,
            errors: errors.map_or_else(|| "·".to_string(), |e| e.to_string()),
        }
    }
}

/// Format a rate for the table: `50000` → `50k`, `1500000` → `1.5m`.
fn fmt_rate(rate: Option<f64>) -> String {
    match rate {
        None => "max".to_string(),
        Some(r) if r >= 1_000_000.0 => format!("{:.1}m", r / 1_000_000.0),
        Some(r) if r >= 1_000.0 => format!("{:.0}k", r / 1_000.0),
        Some(r) => format!("{r:.0}"),
    }
}

fn print_table(reports: &[Report]) {
    let mut rows = Vec::with_capacity(reports.len() * 4);
    for r in reports {
        let rate = fmt_rate(r.target_rate_per_sec);
        for k in &r.by_op {
            rows.push(Row::from(&r.target, &rate, k, None));
        }
        rows.push(Row::from(&r.target, &rate, &r.overall, Some(r.errors)));
    }
    let mut table = Table::new(rows);
    table.with(Style::rounded());
    println!("{table}");
}

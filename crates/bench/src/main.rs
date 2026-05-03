//! `kv-bench` — honest, reproducible KV benchmarks.

use std::sync::Arc;
use std::time::Duration;

use beyond_kv_bench::client::Target;
use beyond_kv_bench::driver::{Driver, Mode, Plan};
use beyond_kv_bench::keyspace::{KeyDist, Keyspace};
use beyond_kv_bench::metrics::{KindReport, Report};
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

    let keyspace = Keyspace::new(cli.keys, cli.keydist)?;
    let workload = Workload::new(keyspace, cli.workload, cli.value_size);
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
    Ok(())
}

fn print_plan(cli: &Cli, plan: &Plan) {
    eprintln!(
        "plan: workload={:?} keys={} keydist={:?} value_size={}B \
         concurrency={} duration={:?} warmup={:?} mode={:?} populate={} seed={:#x}",
        cli.workload,
        cli.keys,
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
    /// Requested rate in ops/s. `max` for closed-loop saturation runs.
    rate: String,
    op: &'static str,
    #[tabled(rename = "ops/s")]
    throughput: String,
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
            throughput: format!("{:.0}", k.throughput_ops_per_sec),
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

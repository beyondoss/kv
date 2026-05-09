use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

#[allow(unused_imports)]
use prometheus::{
    Counter, CounterVec, Encoder as _, Gauge, GaugeVec, Histogram, HistogramOpts, HistogramVec,
    Opts, Registry, TextEncoder,
};

macro_rules! define_metrics {
    (
        $(#[$struct_meta:meta])*
        $vis:vis struct $name:ident {
            $(
                $metric_type:ident $field:ident($metric_name:literal)
                $([$($label:literal),+ $(,)?])?
                $(buckets = $buckets:expr)?
                => $help:literal
            ),* $(,)?
        }
    ) => {
        $(#[$struct_meta])*
        $vis struct $name {
            pub registry: Registry,
            $(pub $field: define_metrics!(@field_type $metric_type $([$($label),+])?),)*
        }

        impl $name {
            pub fn new() -> Self {
                let registry = Registry::new();
                $(
                    let $field = define_metrics!(
                        @create $metric_type $metric_name $help
                        $([$($label),+])?
                        $(buckets = $buckets)?
                    );
                    registry.register(Box::new($field.clone())).expect("metric not yet registered");
                )*
                Self { registry, $($field,)* }
            }

            #[allow(dead_code)]
            pub fn registry(&self) -> &Registry { &self.registry }

            pub fn encode(&self) -> String {
                let mut buf = Vec::new();
                TextEncoder::new().encode(&self.registry.gather(), &mut buf)
                    .expect("encoding to vec never fails");
                String::from_utf8(buf).expect("prometheus outputs valid utf-8")
            }
        }

        impl Default for $name {
            fn default() -> Self { Self::new() }
        }
    };

    (@field_type counter) => { Counter };
    (@field_type counter [$($label:literal),+]) => { CounterVec };
    (@field_type counter_vec) => { CounterVec };
    (@field_type counter_vec [$($label:literal),+]) => { CounterVec };
    (@field_type gauge) => { Gauge };
    (@field_type gauge [$($label:literal),+]) => { GaugeVec };
    (@field_type gauge_vec) => { GaugeVec };
    (@field_type gauge_vec [$($label:literal),+]) => { GaugeVec };
    (@field_type histogram) => { Histogram };
    (@field_type histogram [$($label:literal),+]) => { HistogramVec };
    (@field_type histogram_vec) => { HistogramVec };
    (@field_type histogram_vec [$($label:literal),+]) => { HistogramVec };

    (@create counter $name:literal $help:literal) => {
        Counter::new($name, $help).expect("valid metric")
    };
    (@create counter $name:literal $help:literal [$($label:literal),+]) => {
        CounterVec::new(Opts::new($name, $help), &[$($label),+]).expect("valid metric")
    };
    (@create counter_vec $name:literal $help:literal [$($label:literal),+]) => {
        CounterVec::new(Opts::new($name, $help), &[$($label),+]).expect("valid metric")
    };
    (@create gauge $name:literal $help:literal) => {
        Gauge::new($name, $help).expect("valid metric")
    };
    (@create gauge $name:literal $help:literal [$($label:literal),+]) => {
        GaugeVec::new(Opts::new($name, $help), &[$($label),+]).expect("valid metric")
    };
    (@create gauge_vec $name:literal $help:literal [$($label:literal),+]) => {
        GaugeVec::new(Opts::new($name, $help), &[$($label),+]).expect("valid metric")
    };
    (@create histogram $name:literal $help:literal) => {
        Histogram::with_opts(HistogramOpts::new($name, $help)).expect("valid metric")
    };
    (@create histogram $name:literal $help:literal buckets = $buckets:expr) => {
        Histogram::with_opts(
            HistogramOpts::new($name, $help).buckets($buckets.to_vec())
        ).expect("valid metric")
    };
    (@create histogram $name:literal $help:literal [$($label:literal),+]) => {
        HistogramVec::new(HistogramOpts::new($name, $help), &[$($label),+]).expect("valid metric")
    };
    (@create histogram $name:literal $help:literal [$($label:literal),+] buckets = $buckets:expr) => {
        HistogramVec::new(
            HistogramOpts::new($name, $help).buckets($buckets.to_vec()),
            &[$($label),+],
        ).expect("valid metric")
    };
    (@create histogram_vec $name:literal $help:literal [$($label:literal),+]) => {
        HistogramVec::new(HistogramOpts::new($name, $help), &[$($label),+]).expect("valid metric")
    };
    (@create histogram_vec $name:literal $help:literal [$($label:literal),+] buckets = $buckets:expr) => {
        HistogramVec::new(
            HistogramOpts::new($name, $help).buckets($buckets.to_vec()),
            &[$($label),+],
        ).expect("valid metric")
    };
}

// Bucket sets grouped by operation latency profile.

/// KV operations — from 25µs L1 cache hits through 10s SCAN/FLUSHDB.
const KV_OP_BUCKETS: &[f64] = &[
    0.000025, 0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25,
    0.5, 1.0, 5.0, 10.0,
];
/// Storage I/O and cross-shard fan-out — 100µs to 5s.
const STORAGE_BUCKETS: &[f64] = &[
    0.0001, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0,
];
/// Database-backed operations (log sync) — 1ms fast-path through 1s.
const DB_OP_BUCKETS: &[f64] = &[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0];

define_metrics! {
    pub struct MetricsInner {
        // ── Operations ────────────────────────────────────────────────────────
        counter_vec ops_total("kv_ops_total")["op", "result"]
            => "Total KV operations",

        histogram op_duration_seconds("kv_op_duration_seconds")["op"]
            buckets = KV_OP_BUCKETS
            => "KV operation duration in seconds",

        // ── Connections ───────────────────────────────────────────────────────
        gauge active_connections("kv_active_connections")["shard", "proto"]
            => "Live client connections per shard and protocol",

        // ── Cross-shard ───────────────────────────────────────────────────────
        counter_vec cross_shard_ops_total("kv_cross_shard_ops_total")["op"]
            => "Operations that required cross-shard fan-out",

        histogram cross_shard_op_duration_seconds("kv_cross_shard_op_duration_seconds")["op"]
            buckets = STORAGE_BUCKETS
            => "Cross-shard fan-out operation duration in seconds",

        // ── Cache ─────────────────────────────────────────────────────────────
        counter_vec cache_ops_total("kv_cache_ops_total")["result"]
            => "Total L1 cache lookups",

        gauge_vec cache_size_bytes("kv_cache_size_bytes")["shard"]
            => "L1 cache memory in use per shard in bytes",

        gauge_vec cache_entries("kv_cache_entries")["shard"]
            => "L1 cache entry count per shard",

        counter_vec keys_expired_total("kv_keys_expired_total")["shard"]
            => "Keys removed by TTL sweep per shard",

        // ── Durability ────────────────────────────────────────────────────────
        counter_vec log_sync_failures_total("kv_log_sync_failures_total")["shard"]
            => "Total log sync failures per shard",

        histogram log_sync_duration_seconds("kv_log_sync_duration_seconds")["shard"]
            buckets = DB_OP_BUCKETS
            => "Log sync (fsync) duration per shard in seconds",

        // ── Storage ───────────────────────────────────────────────────────────
        gauge_vec sealed_segments("kv_sealed_segments")["shard"]
            => "Sealed log segments awaiting compaction per shard",

        counter_vec reclaim_runs_total("kv_reclaim_runs_total")["shard"]
            => "Compaction (reclaim) runs completed per shard",

        counter_vec reclaim_files_freed_total("kv_reclaim_files_freed_total")["shard"]
            => "Log files freed by compaction per shard",

        // ── Namespaces ────────────────────────────────────────────────────────
        gauge_vec namespaces_open("kv_namespaces_open")["shard"]
            => "Open namespace count per shard (limit 1024)",
    }
}

pub struct Metrics {
    inner: MetricsInner,
    // Arc<AtomicU64> pairs from each shard's ShardStore, read at encode time.
    cache_shards: std::sync::Mutex<Vec<(Arc<AtomicU64>, Arc<AtomicU64>)>>,
    // Last-observed totals for delta-inc into the registered cache_ops_total counters.
    last_cache_hits: std::sync::Mutex<u64>,
    last_cache_misses: std::sync::Mutex<u64>,
}

impl std::ops::Deref for Metrics {
    type Target = MetricsInner;
    fn deref(&self) -> &MetricsInner {
        &self.inner
    }
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: MetricsInner::new(),
            cache_shards: std::sync::Mutex::new(Vec::new()),
            last_cache_hits: std::sync::Mutex::new(0),
            last_cache_misses: std::sync::Mutex::new(0),
        })
    }

    pub fn register_cache_counters(&self, hits: Arc<AtomicU64>, misses: Arc<AtomicU64>) {
        if let Ok(mut v) = self.cache_shards.lock() {
            v.push((hits, misses));
        }
    }

    /// Flush the AtomicU64 cache counters into the Prometheus-registered
    /// CounterVec so they appear in the registry gather output.
    fn flush_cache_counters(&self) {
        let (total_hits, total_misses) = match self.cache_shards.lock() {
            Ok(v) => v.iter().fold((0u64, 0u64), |(h, m), (hits, misses)| {
                (
                    h + hits.load(Ordering::Relaxed),
                    m + misses.load(Ordering::Relaxed),
                )
            }),
            Err(_) => return,
        };

        if let (Ok(mut last_h), Ok(mut last_m)) =
            (self.last_cache_hits.lock(), self.last_cache_misses.lock())
        {
            let delta_hits = total_hits.saturating_sub(*last_h);
            let delta_misses = total_misses.saturating_sub(*last_m);
            if delta_hits > 0 {
                self.inner
                    .cache_ops_total
                    .with_label_values(&["hit"])
                    .inc_by(delta_hits as f64);
                *last_h = total_hits;
            }
            if delta_misses > 0 {
                self.inner
                    .cache_ops_total
                    .with_label_values(&["miss"])
                    .inc_by(delta_misses as f64);
                *last_m = total_misses;
            }
        }
    }

    pub fn encode(&self) -> String {
        self.flush_cache_counters();
        self.inner.encode()
    }
}

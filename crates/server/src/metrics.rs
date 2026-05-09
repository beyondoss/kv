use std::sync::Arc;

use prometheus::{CounterVec, HistogramOpts, HistogramVec, Opts, Registry};

pub struct Metrics {
    registry: Registry,
    pub ops_total: CounterVec,
    pub op_duration_seconds: HistogramVec,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        let registry = Registry::new();

        let ops_total = CounterVec::new(
            Opts::new("kv_ops_total", "Total KV operations"),
            &["op", "result"],
        )
        .expect("kv_ops_total");
        registry.register(Box::new(ops_total.clone())).expect("register kv_ops_total");

        let op_duration_seconds = HistogramVec::new(
            HistogramOpts::new("kv_op_duration_seconds", "KV operation duration in seconds")
                .buckets(vec![
                    0.0001, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0,
                ]),
            &["op"],
        )
        .expect("kv_op_duration_seconds");
        registry
            .register(Box::new(op_duration_seconds.clone()))
            .expect("register kv_op_duration_seconds");

        Arc::new(Self {
            registry,
            ops_total,
            op_duration_seconds,
        })
    }

    pub fn encode(&self) -> String {
        let encoder = prometheus::TextEncoder::new();
        let mut buf = String::new();
        encoder
            .encode_utf8(&self.registry.gather(), &mut buf)
            .expect("metrics encode");
        buf
    }
}

//! The two-trait abstraction every benchmark target plugs into.
//!
//! A [`Target`] is a *server* — knows how to mint fresh connections and how to
//! reset state between runs. A [`Client`] is a *single connection* — executes
//! one operation at a time and waits for the response. Workers own one client
//! each; we never multiplex, because hidden pipelining destroys the meaning of
//! per-op latency.

use async_trait::async_trait;

use crate::workload::Op;

#[async_trait]
pub trait Target: Send + Sync + 'static {
    /// Display name used in reports (`beyond`, `redis`, …).
    fn name(&self) -> &str;

    /// Mint a fresh dedicated connection. Called once per worker.
    async fn connect(&self) -> anyhow::Result<Box<dyn Client>>;

    /// Wipe the keyspace before a run. Best-effort; called once per phase.
    async fn reset(&self) -> anyhow::Result<()>;
}

#[async_trait]
pub trait Client: Send {
    /// Execute one op and await its response. Drops the response payload —
    /// the benchmark cares about latency and throughput, not values.
    async fn execute(&mut self, op: &Op) -> anyhow::Result<()>;
}

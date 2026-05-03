//! Honest, reproducible KV benchmarks.
//!
//! The harness is built around two small traits — [`Target`] (a server we can
//! connect to) and [`Client`] (a single open connection that executes [`Op`]s) —
//! and a single [`Driver`] that replays a deterministic [`Workload`] against
//! every target with identical seeds.
//!
//! See [`driver`] for the open-loop scheduler and coordinated-omission
//! correction; see [`metrics`] for HDR-histogram aggregation.
//!
//! [`Op`]: workload::Op
//! [`Workload`]: workload::Workload
//! [`Driver`]: driver::Driver

pub mod client;
pub mod driver;
pub mod keyspace;
pub mod metrics;
pub mod targets;
pub mod workload;

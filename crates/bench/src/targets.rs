// `aio::Connection` is "deprecated" in favour of `MultiplexedConnection`, but
// the whole point of this benchmark is to measure honest single-op latency on
// a dedicated socket. We deliberately want the non-multiplexed path.
#![allow(deprecated)]

//! Concrete [`Target`] implementations.
//!
//! Beyond and Redis both speak RESP, so they share a single [`Resp`] target —
//! only the URL differs. New baselines (memcached, Dragonfly, KeyDB) plug in by
//! adding a struct that implements [`Target`].

use anyhow::Context;
use async_trait::async_trait;
use redis::AsyncCommands;

use crate::client::{Client, Target};
use crate::workload::Op;

/// A RESP-speaking server (Beyond KV, Redis, KeyDB, Dragonfly, …).
#[derive(Debug, Clone)]
pub struct Resp {
    name: String,
    url: String,
}

impl Resp {
    pub fn new(name: impl Into<String>, url: impl Into<String>) -> Self {
        Self { name: name.into(), url: url.into() }
    }

    /// Parse a `name=url` spec from the CLI, e.g. `beyond=redis://127.0.0.1:6379`.
    pub fn parse_spec(spec: &str) -> anyhow::Result<Self> {
        let (name, url) = spec
            .split_once('=')
            .ok_or_else(|| anyhow::anyhow!("expected name=url, got: {spec}"))?;
        Ok(Self::new(name, url))
    }
}

#[async_trait]
impl Target for Resp {
    fn name(&self) -> &str { &self.name }

    async fn connect(&self) -> anyhow::Result<Box<dyn Client>> {
        let client = redis::Client::open(self.url.as_str())
            .with_context(|| format!("invalid redis url for {}: {}", self.name, self.url))?;
        // Dedicated single-socket connection — no multiplexing, no hidden pipelining.
        let conn = client
            .get_async_connection()
            .await
            .with_context(|| format!("connect {} ({})", self.name, self.url))?;
        Ok(Box::new(RespClient { conn }))
    }

    async fn reset(&self) -> anyhow::Result<()> {
        let client = redis::Client::open(self.url.as_str())?;
        let mut conn = client.get_async_connection().await?;
        let _: () = redis::cmd("FLUSHDB")
            .query_async(&mut conn)
            .await
            .with_context(|| format!("FLUSHDB on {}", self.name))?;
        Ok(())
    }
}

struct RespClient {
    conn: redis::aio::Connection,
}

#[async_trait]
impl Client for RespClient {
    async fn execute(&mut self, op: &Op) -> anyhow::Result<()> {
        match op {
            Op::Get { key } => {
                let _: Option<Vec<u8>> = self.conn.get(key.as_ref()).await?;
            }
            Op::Set { key, value } => {
                let _: () = self.conn.set(key.as_ref(), value.as_ref()).await?;
            }
            Op::Del { key } => {
                let _: i64 = self.conn.del(key.as_ref()).await?;
            }
        }
        Ok(())
    }
}

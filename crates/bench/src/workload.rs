//! Workload definition: what operations to issue, against which keys.
//!
//! A [`Workload`] is a *deterministic* generator parameterised by an op-mix and
//! a [`Keyspace`]. Each worker draws from its own RNG seeded from the run seed
//! plus the worker index — so two runs of the same plan produce byte-identical
//! op streams across targets, which is what lets us compare them honestly.

use bytes::Bytes;
use rand::Rng;

use crate::keyspace::Keyspace;

#[derive(Debug, Clone)]
pub enum Op {
    Get { key: Bytes },
    Set { key: Bytes, value: Bytes },
    Del { key: Bytes },
    /// Batched read. One round-trip, `keys.len()` logical operations.
    MGet { keys: Vec<Bytes> },
    /// Batched write. One round-trip, `pairs.len()` logical operations.
    MSet { pairs: Vec<(Bytes, Bytes)> },
}

impl Op {
    /// Logical operation count. For batched ops this is the batch size; for
    /// single-key ops it is 1. Used to convert calls/sec → keys/sec.
    pub fn keys(&self) -> u64 {
        match self {
            Op::Get { .. } | Op::Set { .. } | Op::Del { .. } => 1,
            Op::MGet { keys } => keys.len() as u64,
            Op::MSet { pairs } => pairs.len() as u64,
        }
    }
}

/// Compact, indexable op classification used by the metrics layer.
///
/// `as usize` indexes the per-kind histogram arrays — so the order here is
/// load-bearing. Adding a variant means widening those arrays.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum OpKind {
    Get = 0,
    Set = 1,
    Del = 2,
    MGet = 3,
    MSet = 4,
}

impl OpKind {
    pub const COUNT: usize = 5;
    pub const ALL: [OpKind; Self::COUNT] =
        [OpKind::Get, OpKind::Set, OpKind::Del, OpKind::MGet, OpKind::MSet];
    pub fn name(self) -> &'static str {
        match self {
            OpKind::Get => "GET",
            OpKind::Set => "SET",
            OpKind::Del => "DEL",
            OpKind::MGet => "MGET",
            OpKind::MSet => "MSET",
        }
    }
}

impl Op {
    pub fn kind(&self) -> OpKind {
        match self {
            Op::Get { .. } => OpKind::Get,
            Op::Set { .. } => OpKind::Set,
            Op::Del { .. } => OpKind::Del,
            Op::MGet { .. } => OpKind::MGet,
            Op::MSet { .. } => OpKind::MSet,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum OpMix {
    GetOnly,
    SetOnly,
    /// Mix of GET/SET. `read_pct` is in `[0, 100]`.
    Mixed { read_pct: u8 },
}

impl std::str::FromStr for OpMix {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> anyhow::Result<Self> {
        match s {
            "get" | "get-only" => Ok(OpMix::GetOnly),
            "set" | "set-only" => Ok(OpMix::SetOnly),
            other => {
                if let Some(rest) = other.strip_prefix("mixed:") {
                    let pct: u8 = rest.parse()?;
                    anyhow::ensure!(pct <= 100, "read_pct must be 0..=100");
                    Ok(OpMix::Mixed { read_pct: pct })
                } else {
                    anyhow::bail!("unknown workload: {other} (try get, set, mixed:80)")
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct Workload {
    keyspace: Keyspace,
    mix: OpMix,
    /// Pre-built value buffer. Cloning a `Bytes` is a refcount bump, so all
    /// SETs share one backing allocation.
    value: Bytes,
    /// Batch size. `1` ⇒ GET/SET; `>1` ⇒ MGET/MSET of this many keys.
    batch: usize,
}

impl Workload {
    pub fn new(keyspace: Keyspace, mix: OpMix, value_size: usize, batch: usize) -> Self {
        // Deterministic, non-zero filler so wire size matches the user's request.
        let mut buf = vec![0u8; value_size];
        for (i, b) in buf.iter_mut().enumerate() {
            *b = (i as u8).wrapping_add(0x20);
        }
        Self { keyspace, mix, value: Bytes::from(buf), batch: batch.max(1) }
    }

    pub fn keyspace(&self) -> &Keyspace { &self.keyspace }
    pub fn value(&self) -> &Bytes { &self.value }
    pub fn batch(&self) -> usize { self.batch }

    pub fn next<R: Rng>(&self, rng: &mut R) -> Op {
        let pick_read = match self.mix {
            OpMix::GetOnly => true,
            OpMix::SetOnly => false,
            OpMix::Mixed { read_pct } => rng.gen_range(0u8..100) < read_pct,
        };
        if self.batch == 1 {
            let key = self.keyspace.sample(rng);
            if pick_read {
                Op::Get { key }
            } else {
                Op::Set { key, value: self.value.clone() }
            }
        } else if pick_read {
            let keys: Vec<Bytes> =
                (0..self.batch).map(|_| self.keyspace.sample(rng)).collect();
            Op::MGet { keys }
        } else {
            let pairs: Vec<(Bytes, Bytes)> = (0..self.batch)
                .map(|_| (self.keyspace.sample(rng), self.value.clone()))
                .collect();
            Op::MSet { pairs }
        }
    }
}

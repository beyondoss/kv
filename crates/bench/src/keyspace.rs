//! Key sampling distributions.
//!
//! Real workloads are rarely uniform — caches see hot keys. We support both
//! `Uniform` (worst case for caches, best case for hash-ring fairness) and
//! `Zipfian` (the canonical hot-key distribution, parameterised by `theta`).
//! Higher `theta` ⇒ more skew; `theta = 0.99` is the YCSB default.

use bytes::Bytes;
use rand::Rng;
use rand_distr::{Distribution, Zipf};

use std::fmt::Write;

#[derive(Debug, Clone, Copy)]
pub enum KeyDist {
    Uniform,
    Zipfian { theta: f64 },
}

impl std::str::FromStr for KeyDist {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> anyhow::Result<Self> {
        if let Some(rest) = s.strip_prefix("zipf:") {
            Ok(KeyDist::Zipfian { theta: rest.parse()? })
        } else {
            match s {
                "uniform" => Ok(KeyDist::Uniform),
                "zipf" | "zipfian" => Ok(KeyDist::Zipfian { theta: 0.99 }),
                other => anyhow::bail!("unknown key distribution: {other}"),
            }
        }
    }
}

#[derive(Clone)]
pub struct Keyspace {
    size: u64,
    dist: KeyDist,
    zipf: Option<Zipf<f64>>,
}

impl Keyspace {
    pub fn new(size: u64, dist: KeyDist) -> anyhow::Result<Self> {
        anyhow::ensure!(size > 0, "keyspace size must be positive");
        let zipf = match dist {
            KeyDist::Zipfian { theta } => Some(
                Zipf::new(size, theta).map_err(|e| anyhow::anyhow!("invalid zipf: {e:?}"))?,
            ),
            KeyDist::Uniform => None,
        };
        Ok(Self { size, dist, zipf })
    }

    pub fn size(&self) -> u64 { self.size }

    /// Sample one key. Allocates a small `Bytes` per call — keys are tiny
    /// (~20 bytes) so this stays well under the per-op overhead floor.
    pub fn sample<R: Rng>(&self, rng: &mut R) -> Bytes {
        let idx = match self.dist {
            KeyDist::Uniform => rng.gen_range(0..self.size),
            KeyDist::Zipfian { .. } => {
                // Zipf::sample returns 1..=size; subtract 1 for 0-based.
                self.zipf.as_ref().unwrap().sample(rng) as u64 - 1
            }
        };
        format_key(idx)
    }

    /// Deterministic enumeration — used by the populate phase.
    pub fn nth(&self, idx: u64) -> Bytes { format_key(idx % self.size) }
}

fn format_key(idx: u64) -> Bytes {
    // Fixed-width hex keeps key length stable across the run.
    let mut s = String::with_capacity(18);
    let _ = write!(&mut s, "k:{:016x}", idx);
    Bytes::from(s)
}

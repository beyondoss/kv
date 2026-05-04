//! Key sampling distributions.
//!
//! Real workloads are rarely uniform — caches see hot keys. We support both
//! `Uniform` (worst case for caches, best case for hash-ring fairness) and
//! `Zipfian` (the canonical hot-key distribution, parameterised by `theta`).
//! Higher `theta` ⇒ more skew; `theta = 0.99` is the YCSB default.
//!
//! When `--shards N --shard-index I` are given, the keyspace pre-filters its
//! key indices to only those that route to shard I under Beyond's routing
//! function (`FxHasher(key) % N`). This lets the bench driver send each
//! connection only the keys that belong to that connection's shard, so every
//! shard owns exactly 1/N of the dataset and its L1 cache covers it fully.

use std::fmt::Write;
use std::hash::Hasher as _;

use bytes::Bytes;
use rand::Rng;
use rand_distr::{Distribution, Zipf};
use rustc_hash::FxHasher;

#[derive(Debug, Clone, Copy)]
pub enum KeyDist {
    Uniform,
    Zipfian { theta: f64 },
}

impl std::str::FromStr for KeyDist {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> anyhow::Result<Self> {
        if let Some(rest) = s.strip_prefix("zipf:") {
            Ok(KeyDist::Zipfian {
                theta: rest.parse()?,
            })
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
    /// Number of keys in this shard's partition (= total keys when unsharded).
    size: u64,
    dist: KeyDist,
    zipf: Option<Zipf<f64>>,
    /// Pre-filtered key indices for sharded mode. `None` = use 0..size directly.
    indices: Option<Vec<u64>>,
}

impl Keyspace {
    pub fn new(size: u64, dist: KeyDist) -> anyhow::Result<Self> {
        Self::new_sharded(size, dist, 0, 1)
    }

    /// Build a keyspace restricted to the keys that route to `shard_index` out
    /// of `num_shards` shards. Uses the same routing function as the server:
    /// `FxHasher(key_bytes) % num_shards`.
    pub fn new_sharded(
        total_keys: u64,
        dist: KeyDist,
        shard_index: usize,
        num_shards: usize,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(total_keys > 0, "keyspace size must be positive");
        anyhow::ensure!(num_shards > 0, "shard count must be positive");
        anyhow::ensure!(shard_index < num_shards, "shard-index must be < shards");

        let indices: Option<Vec<u64>> = if num_shards == 1 {
            None
        } else {
            let v: Vec<u64> = (0..total_keys)
                .filter(|&i| {
                    let key = format_key(i);
                    let mut h = FxHasher::default();
                    h.write(&key);
                    (h.finish() as usize) % num_shards == shard_index
                })
                .collect();
            anyhow::ensure!(
                !v.is_empty(),
                "no keys in keyspace route to shard {shard_index}/{num_shards} \
                 — increase --keys or check --shards"
            );
            Some(v)
        };

        let size = indices.as_ref().map_or(total_keys, |v| v.len() as u64);

        let zipf = match dist {
            KeyDist::Zipfian { theta } => {
                Some(Zipf::new(size, theta).map_err(|e| anyhow::anyhow!("invalid zipf: {e:?}"))?)
            }
            KeyDist::Uniform => None,
        };

        Ok(Self {
            size,
            dist,
            zipf,
            indices,
        })
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    /// Sample one key from this shard's partition.
    pub fn sample<R: Rng>(&self, rng: &mut R) -> Bytes {
        let pos = match self.dist {
            KeyDist::Uniform => rng.gen_range(0..self.size),
            KeyDist::Zipfian { .. } => self.zipf.as_ref().unwrap().sample(rng) as u64 - 1,
        };
        let idx = self.indices.as_ref().map_or(pos, |v| v[pos as usize]);
        format_key(idx)
    }

    /// Deterministic enumeration — used by the populate phase.
    pub fn nth(&self, i: u64) -> Bytes {
        let idx = match &self.indices {
            Some(v) => v[(i % v.len() as u64) as usize],
            None => i % self.size,
        };
        format_key(idx)
    }
}

fn format_key(idx: u64) -> Bytes {
    // Fixed-width hex keeps key length stable across the run.
    let mut s = String::with_capacity(18);
    let _ = write!(&mut s, "k:{:016x}", idx);
    Bytes::from(s)
}

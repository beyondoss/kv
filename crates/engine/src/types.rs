use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;

#[derive(Debug, Clone)]
pub struct Entry {
    pub value: Bytes,
    pub expires_at: Option<Instant>,
    pub metadata: Option<Arc<serde_json::Value>>,
    /// Monotonically-increasing write timestamp (ms since epoch). Used as a
    /// revision for compare-and-swap. Populated on all reads; 0 if unknown.
    pub revision: u64,
}

#[derive(Debug, Clone)]
pub struct SetOptions {
    pub ttl: Option<Duration>,
    pub metadata: Option<Arc<serde_json::Value>>,
    /// When true, preserve the key's existing TTL instead of clearing it.
    /// Implements Redis KEEPTTL semantics: `SET key value KEEPTTL`.
    pub keep_ttl: bool,
}

impl Default for SetOptions {
    fn default() -> Self {
        Self {
            ttl: None,
            metadata: None,
            keep_ttl: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TtlResult {
    NoExpiry,
    NotFound,
    Remaining(u64),
}

#[derive(Debug, Clone)]
pub enum GetExOp {
    SetTtl(Duration),
    Persist,
}

#[derive(Debug)]
pub struct ScanPage {
    pub next_cursor: Bytes,
    pub keys: Vec<Bytes>,
}

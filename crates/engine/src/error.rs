use thiserror::Error;

#[derive(Debug, Error)]
pub enum EngineError {
    #[error("io: {source}")]
    Io {
        #[from]
        source: std::io::Error,
    },
    #[error("crc mismatch at offset {offset}")]
    CrcMismatch { offset: u64 },
    #[error("malformed record at offset {offset}: {reason}")]
    BadRecord { offset: u64, reason: &'static str },
    #[error("invalid namespace name: {name:?}")]
    InvalidNamespace { name: String },
    #[error("metadata json: {source}")]
    MetadataJson {
        #[from]
        source: serde_json::Error,
    },
    #[error("reclaim or flush already in progress on this namespace")]
    ReclamationBusy,
    #[error("capacity exceeded: {reason}")]
    CapacityExceeded { reason: &'static str },
    #[error("invalid input: {reason}")]
    InvalidInput { reason: &'static str },
    #[error("conflict: {reason}")]
    Conflict { reason: &'static str },
    #[error("write rejected: store is frozen for handoff")]
    Frozen,
    /// Triggered only when the `KV_TEST_FAIL_ONCE_FILE` env var points at a
    /// file that exists at the moment of a seal. The file is unlinked on
    /// trigger so a subsequent seal succeeds. Used by handoff-failure tests
    /// to exercise the SealFailed protocol path with a real KV process.
    #[error("test-only seal failure (KV_TEST_FAIL_ONCE_FILE signal was present)")]
    TestSealFailure,
}

pub type Result<T> = std::result::Result<T, EngineError>;

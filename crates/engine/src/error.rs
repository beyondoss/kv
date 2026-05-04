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
}

pub type Result<T> = std::result::Result<T, EngineError>;

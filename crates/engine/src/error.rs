use thiserror::Error;

#[derive(Debug, Error)]
pub enum EngineError {
    #[error("rocksdb: {source}")]
    RocksDb { #[from] source: rocksdb::Error },
    #[error("encode: {source}")]
    Encode { #[from] source: postcard::Error },
    #[error("io: {source}")]
    Io { #[from] source: std::io::Error },
    #[error("invalid namespace name: {name:?}")]
    InvalidNamespace { name: String },
}

pub type Result<T> = std::result::Result<T, EngineError>;

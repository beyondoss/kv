use thiserror::Error;

#[must_use = "errors must be handled or explicitly ignored with `let _ =`"]
#[derive(Debug, Error)]
pub enum ProtoError {
    #[error("wrong number of arguments for '{cmd}' command")]
    WrongArity { cmd: &'static str },
    #[error("value is not an integer or out of range: {raw:?}")]
    InvalidInteger { raw: bytes::Bytes },
    #[error("invalid expire time: {raw:?}")]
    InvalidExpiry { raw: bytes::Bytes },
    #[error("syntax error near {token:?}")]
    Syntax { token: bytes::Bytes },
    #[error("unknown command '{}'", cmd.escape_ascii())]
    UnknownCommand { cmd: bytes::Bytes },
    #[error("invalid command format: {reason}")]
    InvalidFormat { reason: &'static str },
    #[error("DB index is out of range")]
    DbIndexOutOfRange,
}

pub type Result<T> = std::result::Result<T, ProtoError>;

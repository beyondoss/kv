use bytes::Bytes;

use crate::error::ProtoError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetCondition {
    Always,
    Nx,
    Xx,
    /// Compare-and-swap: only write if the current revision equals this value.
    Rev(u64),
}

#[derive(Debug, Clone)]
pub enum SetTtl {
    Seconds(u64),
    Millis(u64),
    UnixSecs(u64),
    UnixMillis(u64),
}

#[derive(Debug, Clone)]
pub struct SetArgs {
    pub ttl: Option<SetTtl>,
    pub condition: SetCondition,
    pub get: bool,
}

#[derive(Debug, Clone)]
pub struct ScanArgs {
    pub pattern: Option<Bytes>,
    pub count: u64,
}

/// TTL modification for GETEX. Distinct from `Option<SetTtl>` so that
/// `Persist` (remove TTL) is not conflated with "no TTL option given".
#[derive(Debug, Clone)]
pub enum GetExTtl {
    Set(SetTtl),
    Persist,
}

#[derive(Debug, Clone)]
pub enum Command {
    Get {
        key: Bytes,
    },
    Set {
        key: Bytes,
        value: Bytes,
        args: SetArgs,
    },
    Del {
        keys: Vec<Bytes>,
    },
    Exists {
        keys: Vec<Bytes>,
    },
    Expire {
        key: Bytes,
        secs: u64,
    },
    PExpire {
        key: Bytes,
        millis: u64,
    },
    ExpireAt {
        key: Bytes,
        unix_secs: u64,
    },
    PExpireAt {
        key: Bytes,
        unix_millis: u64,
    },
    Ttl {
        key: Bytes,
    },
    PTtl {
        key: Bytes,
    },
    Persist {
        key: Bytes,
    },
    Keys {
        pattern: Option<Bytes>,
    },
    Scan {
        cursor: Bytes,
        args: ScanArgs,
    },
    MGet {
        keys: Vec<Bytes>,
    },
    MSet {
        pairs: Vec<(Bytes, Bytes)>,
    },
    GetSet {
        key: Bytes,
        value: Bytes,
    },
    SetNx {
        key: Bytes,
        value: Bytes,
    },
    GetDel {
        key: Bytes,
    },
    GetEx {
        key: Bytes,
        ttl: Option<GetExTtl>,
    },
    Incr {
        key: Bytes,
    },
    IncrBy {
        key: Bytes,
        delta: i64,
    },
    Decr {
        key: Bytes,
    },
    DecrBy {
        key: Bytes,
        delta: i64,
    },
    Hello {
        version: Option<u8>,
    },
    Ping {
        message: Option<Bytes>,
    },
    Select {
        db: u64,
    },
    DbSize,
    FlushDb,
    BgRewriteAof,
    Quit,
    Reset,
    /// WATCH key [key ...] [SINCE <revision>]
    Watch {
        keys: Vec<Bytes>,
        since: Option<u64>,
    },
    /// PWATCH prefix [SINCE <revision>]
    PWatch {
        prefix: Bytes,
        since: Option<u64>,
    },
    Unwatch,
    /// REVISION key → current revision (ms since epoch) or -2 if the key is missing.
    Revision {
        key: Bytes,
    },
    /// SETREV key value revision [EX n | PX n | EXAT n | PXAT n]
    ///
    /// Compare-and-swap write: atomically sets `key` to `value` only when the
    /// current revision equals `revision`. Returns the new revision on success,
    /// nil on mismatch or missing key.
    SetRev {
        key: Bytes,
        value: Bytes,
        revision: u64,
        ttl: Option<SetTtl>,
    },
}

fn bulk(v: &beyond_resp::Value) -> Result<Bytes, ProtoError> {
    match v {
        beyond_resp::Value::BulkString(b) => Ok(b.clone()),
        beyond_resp::Value::SimpleString(b) => Ok(b.clone()),
        _ => Err(ProtoError::InvalidFormat {
            reason: "expected bulk string",
        }),
    }
}

fn parse_u64(v: &beyond_resp::Value) -> Result<u64, ProtoError> {
    let b = bulk(v)?;
    std::str::from_utf8(&b)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| ProtoError::InvalidInteger { raw: b })
}

fn parse_i64(v: &beyond_resp::Value) -> Result<i64, ProtoError> {
    let b = bulk(v)?;
    std::str::from_utf8(&b)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| ProtoError::InvalidInteger { raw: b })
}

impl Command {
    pub fn parse(value: beyond_resp::Value) -> Result<Self, ProtoError> {
        let args = match value {
            beyond_resp::Value::Array(v) if !v.is_empty() => v,
            _ => {
                return Err(ProtoError::InvalidFormat {
                    reason: "expected non-empty array",
                });
            }
        };

        let name_bytes = bulk(&args[0])?;
        // uppercase on stack for names ≤ 16 bytes, heap otherwise
        let mut buf = [0u8; 16];
        let name: &[u8] = if name_bytes.len() <= 16 {
            let n = name_bytes.len();
            for (i, b) in name_bytes.iter().enumerate() {
                buf[i] = b.to_ascii_uppercase();
            }
            &buf[..n]
        } else {
            return Err(ProtoError::UnknownCommand { cmd: name_bytes });
        };

        match name {
            b"GET" => {
                if args.len() != 2 {
                    return Err(ProtoError::WrongArity { cmd: "GET" });
                }
                Ok(Command::Get {
                    key: bulk(&args[1])?,
                })
            }
            b"SET" => parse_set(&args),
            b"DEL" => {
                if args.len() < 2 {
                    return Err(ProtoError::WrongArity { cmd: "DEL" });
                }
                let keys = args[1..].iter().map(bulk).collect::<Result<_, _>>()?;
                Ok(Command::Del { keys })
            }
            b"EXISTS" => {
                if args.len() < 2 {
                    return Err(ProtoError::WrongArity { cmd: "EXISTS" });
                }
                let keys = args[1..].iter().map(bulk).collect::<Result<_, _>>()?;
                Ok(Command::Exists { keys })
            }
            b"EXPIRE" => {
                if args.len() != 3 {
                    return Err(ProtoError::WrongArity { cmd: "EXPIRE" });
                }
                Ok(Command::Expire {
                    key: bulk(&args[1])?,
                    secs: parse_u64(&args[2])?,
                })
            }
            b"PEXPIRE" => {
                if args.len() != 3 {
                    return Err(ProtoError::WrongArity { cmd: "PEXPIRE" });
                }
                Ok(Command::PExpire {
                    key: bulk(&args[1])?,
                    millis: parse_u64(&args[2])?,
                })
            }
            b"EXPIREAT" => {
                if args.len() != 3 {
                    return Err(ProtoError::WrongArity { cmd: "EXPIREAT" });
                }
                Ok(Command::ExpireAt {
                    key: bulk(&args[1])?,
                    unix_secs: parse_u64(&args[2])?,
                })
            }
            b"PEXPIREAT" => {
                if args.len() != 3 {
                    return Err(ProtoError::WrongArity { cmd: "PEXPIREAT" });
                }
                Ok(Command::PExpireAt {
                    key: bulk(&args[1])?,
                    unix_millis: parse_u64(&args[2])?,
                })
            }
            b"TTL" => {
                if args.len() != 2 {
                    return Err(ProtoError::WrongArity { cmd: "TTL" });
                }
                Ok(Command::Ttl {
                    key: bulk(&args[1])?,
                })
            }
            b"PTTL" => {
                if args.len() != 2 {
                    return Err(ProtoError::WrongArity { cmd: "PTTL" });
                }
                Ok(Command::PTtl {
                    key: bulk(&args[1])?,
                })
            }
            b"PERSIST" => {
                if args.len() != 2 {
                    return Err(ProtoError::WrongArity { cmd: "PERSIST" });
                }
                Ok(Command::Persist {
                    key: bulk(&args[1])?,
                })
            }
            b"KEYS" => {
                let pattern = if args.len() >= 2 {
                    Some(bulk(&args[1])?)
                } else {
                    None
                };
                Ok(Command::Keys { pattern })
            }
            b"SCAN" => {
                if args.len() < 2 {
                    return Err(ProtoError::WrongArity { cmd: "SCAN" });
                }
                let cursor = bulk(&args[1])?;
                let mut pattern = None;
                let mut count = 10u64;
                let mut i = 2;
                while i < args.len() {
                    let opt = bulk(&args[i])?;
                    let mut opt_upper = [0u8; 5];
                    let opt_up: &[u8] = if opt.len() <= 5 {
                        for (j, b) in opt.iter().enumerate() {
                            opt_upper[j] = b.to_ascii_uppercase();
                        }
                        &opt_upper[..opt.len()]
                    } else {
                        return Err(ProtoError::Syntax { token: opt });
                    };
                    match opt_up {
                        b"MATCH" => {
                            i += 1;
                            if i >= args.len() {
                                return Err(ProtoError::WrongArity { cmd: "SCAN" });
                            }
                            pattern = Some(bulk(&args[i])?);
                        }
                        b"COUNT" => {
                            i += 1;
                            if i >= args.len() {
                                return Err(ProtoError::WrongArity { cmd: "SCAN" });
                            }
                            count = parse_u64(&args[i])?;
                        }
                        _ => return Err(ProtoError::Syntax { token: opt }),
                    }
                    i += 1;
                }
                Ok(Command::Scan {
                    cursor,
                    args: ScanArgs { pattern, count },
                })
            }
            b"MGET" => {
                if args.len() < 2 {
                    return Err(ProtoError::WrongArity { cmd: "MGET" });
                }
                let keys = args[1..].iter().map(bulk).collect::<Result<_, _>>()?;
                Ok(Command::MGet { keys })
            }
            b"MSET" => {
                if args.len() < 3 || (args.len() - 1) % 2 != 0 {
                    return Err(ProtoError::WrongArity { cmd: "MSET" });
                }
                let pairs = args[1..]
                    .chunks(2)
                    .map(|c| Ok((bulk(&c[0])?, bulk(&c[1])?)))
                    .collect::<Result<_, ProtoError>>()?;
                Ok(Command::MSet { pairs })
            }
            b"GETSET" => {
                if args.len() != 3 {
                    return Err(ProtoError::WrongArity { cmd: "GETSET" });
                }
                Ok(Command::GetSet {
                    key: bulk(&args[1])?,
                    value: bulk(&args[2])?,
                })
            }
            b"SETNX" => {
                if args.len() != 3 {
                    return Err(ProtoError::WrongArity { cmd: "SETNX" });
                }
                Ok(Command::SetNx {
                    key: bulk(&args[1])?,
                    value: bulk(&args[2])?,
                })
            }
            b"GETDEL" => {
                if args.len() != 2 {
                    return Err(ProtoError::WrongArity { cmd: "GETDEL" });
                }
                Ok(Command::GetDel {
                    key: bulk(&args[1])?,
                })
            }
            b"GETEX" => parse_getex(&args),
            b"INCR" => {
                if args.len() != 2 {
                    return Err(ProtoError::WrongArity { cmd: "INCR" });
                }
                Ok(Command::Incr {
                    key: bulk(&args[1])?,
                })
            }
            b"INCRBY" => {
                if args.len() != 3 {
                    return Err(ProtoError::WrongArity { cmd: "INCRBY" });
                }
                Ok(Command::IncrBy {
                    key: bulk(&args[1])?,
                    delta: parse_i64(&args[2])?,
                })
            }
            b"DECR" => {
                if args.len() != 2 {
                    return Err(ProtoError::WrongArity { cmd: "DECR" });
                }
                Ok(Command::Decr {
                    key: bulk(&args[1])?,
                })
            }
            b"DECRBY" => {
                if args.len() != 3 {
                    return Err(ProtoError::WrongArity { cmd: "DECRBY" });
                }
                Ok(Command::DecrBy {
                    key: bulk(&args[1])?,
                    delta: parse_i64(&args[2])?,
                })
            }
            b"HELLO" => {
                let version = if args.len() >= 2 {
                    let raw = bulk(&args[1])?;
                    let v: u64 = std::str::from_utf8(&raw)
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .ok_or_else(|| ProtoError::InvalidInteger { raw: raw.clone() })?;
                    let ver = u8::try_from(v).map_err(|_| ProtoError::InvalidInteger { raw })?;
                    Some(ver)
                } else {
                    None
                };
                Ok(Command::Hello { version })
            }
            b"PING" => {
                let message = if args.len() >= 2 {
                    Some(bulk(&args[1])?)
                } else {
                    None
                };
                Ok(Command::Ping { message })
            }
            b"SELECT" => {
                if args.len() != 2 {
                    return Err(ProtoError::WrongArity { cmd: "SELECT" });
                }
                let db = parse_u64(&args[1])?;
                Ok(Command::Select { db })
            }
            b"DBSIZE" => Ok(Command::DbSize),
            b"BGREWRITEAOF" => Ok(Command::BgRewriteAof),
            b"FLUSHDB" => {
                if args.len() != 1 {
                    return Err(ProtoError::WrongArity { cmd: "FLUSHDB" });
                }
                Ok(Command::FlushDb)
            }
            b"QUIT" => Ok(Command::Quit),
            b"RESET" => Ok(Command::Reset),
            b"WATCH" => {
                if args.len() < 2 {
                    return Err(ProtoError::WrongArity { cmd: "WATCH" });
                }
                // Parse: WATCH key [key ...] [SINCE <u64>]
                // If last two args are SINCE <n>, strip them.
                let mut since = None;
                let mut key_end = args.len();
                if args.len() >= 3 {
                    let maybe_since = bulk(&args[args.len() - 2])?;
                    let mut buf = [0u8; 5];
                    if maybe_since.len() <= 5 {
                        for (i, b) in maybe_since.iter().enumerate() {
                            buf[i] = b.to_ascii_uppercase();
                        }
                        if &buf[..maybe_since.len()] == b"SINCE" {
                            since = Some(parse_u64(&args[args.len() - 1])?);
                            key_end = args.len() - 2;
                        }
                    }
                }
                let keys = args[1..key_end]
                    .iter()
                    .map(bulk)
                    .collect::<Result<_, _>>()?;
                Ok(Command::Watch { keys, since })
            }
            b"PWATCH" => {
                if args.len() < 2 {
                    return Err(ProtoError::WrongArity { cmd: "PWATCH" });
                }
                let mut since = None;
                let prefix = bulk(&args[1])?;
                if args.len() >= 4 {
                    let maybe_since = bulk(&args[2])?;
                    let mut buf = [0u8; 5];
                    if maybe_since.len() <= 5 {
                        for (i, b) in maybe_since.iter().enumerate() {
                            buf[i] = b.to_ascii_uppercase();
                        }
                        if &buf[..maybe_since.len()] == b"SINCE" {
                            since = Some(parse_u64(&args[3])?);
                        }
                    }
                }
                Ok(Command::PWatch { prefix, since })
            }
            b"UNWATCH" => Ok(Command::Unwatch),
            b"REVISION" => {
                if args.len() != 2 {
                    return Err(ProtoError::WrongArity { cmd: "REVISION" });
                }
                Ok(Command::Revision {
                    key: bulk(&args[1])?,
                })
            }
            b"SETREV" => parse_setrev(&args),
            // Satisfy clients that probe server capabilities
            b"COMMAND" => Ok(Command::Ping { message: None }),
            _ => Err(ProtoError::UnknownCommand { cmd: name_bytes }),
        }
    }
}

fn parse_set(args: &[beyond_resp::Value]) -> Result<Command, ProtoError> {
    if args.len() < 3 {
        return Err(ProtoError::WrongArity { cmd: "SET" });
    }
    let key = bulk(&args[1])?;
    let value = bulk(&args[2])?;
    let mut ttl = None;
    let mut condition = SetCondition::Always;
    let mut get = false;
    let mut i = 3;
    while i < args.len() {
        let opt = bulk(&args[i])?;
        // Buffer sized for the longest option: KEEPTTL (7 bytes)
        let mut buf = [0u8; 7];
        let opt_up: &[u8] = if opt.len() <= 7 {
            for (j, b) in opt.iter().enumerate() {
                buf[j] = b.to_ascii_uppercase();
            }
            &buf[..opt.len()]
        } else {
            return Err(ProtoError::Syntax { token: opt });
        };
        match opt_up {
            b"EX" => {
                i += 1;
                if i >= args.len() {
                    return Err(ProtoError::WrongArity { cmd: "SET" });
                }
                let raw = bulk(&args[i])?;
                let v: u64 = std::str::from_utf8(&raw)
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .ok_or_else(|| ProtoError::InvalidInteger { raw: raw.clone() })?;
                if v == 0 {
                    return Err(ProtoError::InvalidExpiry { raw });
                }
                ttl = Some(SetTtl::Seconds(v));
            }
            b"PX" => {
                i += 1;
                if i >= args.len() {
                    return Err(ProtoError::WrongArity { cmd: "SET" });
                }
                let raw = bulk(&args[i])?;
                let v: u64 = std::str::from_utf8(&raw)
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .ok_or_else(|| ProtoError::InvalidInteger { raw: raw.clone() })?;
                if v == 0 {
                    return Err(ProtoError::InvalidExpiry { raw });
                }
                ttl = Some(SetTtl::Millis(v));
            }
            b"EXAT" => {
                i += 1;
                if i >= args.len() {
                    return Err(ProtoError::WrongArity { cmd: "SET" });
                }
                ttl = Some(SetTtl::UnixSecs(parse_u64(&args[i])?));
            }
            b"PXAT" => {
                i += 1;
                if i >= args.len() {
                    return Err(ProtoError::WrongArity { cmd: "SET" });
                }
                ttl = Some(SetTtl::UnixMillis(parse_u64(&args[i])?));
            }
            b"NX" => condition = SetCondition::Nx,
            b"XX" => condition = SetCondition::Xx,
            b"GET" => get = true,
            b"KEEPTTL" => {} // preserve existing TTL — engine is responsible for the semantics
            b"REV" => {
                i += 1;
                if i >= args.len() {
                    return Err(ProtoError::WrongArity { cmd: "SET" });
                }
                condition = SetCondition::Rev(parse_u64(&args[i])?);
            }
            _ => return Err(ProtoError::Syntax { token: opt }),
        }
        i += 1;
    }
    Ok(Command::Set {
        key,
        value,
        args: SetArgs {
            ttl,
            condition,
            get,
        },
    })
}

fn parse_setrev(args: &[beyond_resp::Value]) -> Result<Command, ProtoError> {
    if args.len() < 4 {
        return Err(ProtoError::WrongArity { cmd: "SETREV" });
    }
    let key = bulk(&args[1])?;
    let value = bulk(&args[2])?;
    let revision = parse_u64(&args[3])?;
    let mut ttl = None;
    let mut i = 4;
    while i < args.len() {
        let opt = bulk(&args[i])?;
        // Buffer sized for the longest option: EXAT/PXAT (4 bytes)
        let mut buf = [0u8; 4];
        let opt_up: &[u8] = if opt.len() <= 4 {
            for (j, b) in opt.iter().enumerate() {
                buf[j] = b.to_ascii_uppercase();
            }
            &buf[..opt.len()]
        } else {
            return Err(ProtoError::Syntax { token: opt });
        };
        match opt_up {
            b"EX" => {
                i += 1;
                if i >= args.len() {
                    return Err(ProtoError::WrongArity { cmd: "SETREV" });
                }
                let raw = bulk(&args[i])?;
                let v: u64 = std::str::from_utf8(&raw)
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .ok_or_else(|| ProtoError::InvalidInteger { raw: raw.clone() })?;
                if v == 0 {
                    return Err(ProtoError::InvalidExpiry { raw });
                }
                ttl = Some(SetTtl::Seconds(v));
            }
            b"PX" => {
                i += 1;
                if i >= args.len() {
                    return Err(ProtoError::WrongArity { cmd: "SETREV" });
                }
                let raw = bulk(&args[i])?;
                let v: u64 = std::str::from_utf8(&raw)
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .ok_or_else(|| ProtoError::InvalidInteger { raw: raw.clone() })?;
                if v == 0 {
                    return Err(ProtoError::InvalidExpiry { raw });
                }
                ttl = Some(SetTtl::Millis(v));
            }
            b"EXAT" => {
                i += 1;
                if i >= args.len() {
                    return Err(ProtoError::WrongArity { cmd: "SETREV" });
                }
                ttl = Some(SetTtl::UnixSecs(parse_u64(&args[i])?));
            }
            b"PXAT" => {
                i += 1;
                if i >= args.len() {
                    return Err(ProtoError::WrongArity { cmd: "SETREV" });
                }
                ttl = Some(SetTtl::UnixMillis(parse_u64(&args[i])?));
            }
            _ => return Err(ProtoError::Syntax { token: opt }),
        }
        i += 1;
    }
    Ok(Command::SetRev {
        key,
        value,
        revision,
        ttl,
    })
}

fn parse_getex(args: &[beyond_resp::Value]) -> Result<Command, ProtoError> {
    if args.len() < 2 {
        return Err(ProtoError::WrongArity { cmd: "GETEX" });
    }
    let key = bulk(&args[1])?;
    let mut ttl: Option<GetExTtl> = None;
    let mut i = 2;
    while i < args.len() {
        let opt = bulk(&args[i])?;
        // Buffer sized for the longest option: PERSIST (7 bytes)
        let mut buf = [0u8; 7];
        let opt_up: &[u8] = if opt.len() <= 7 {
            for (j, b) in opt.iter().enumerate() {
                buf[j] = b.to_ascii_uppercase();
            }
            &buf[..opt.len()]
        } else {
            return Err(ProtoError::Syntax { token: opt });
        };
        match opt_up {
            b"EX" => {
                i += 1;
                if i >= args.len() {
                    return Err(ProtoError::WrongArity { cmd: "GETEX" });
                }
                let raw = bulk(&args[i])?;
                let v: u64 = std::str::from_utf8(&raw)
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .ok_or_else(|| ProtoError::InvalidInteger { raw: raw.clone() })?;
                if v == 0 {
                    return Err(ProtoError::InvalidExpiry { raw });
                }
                ttl = Some(GetExTtl::Set(SetTtl::Seconds(v)));
            }
            b"PX" => {
                i += 1;
                if i >= args.len() {
                    return Err(ProtoError::WrongArity { cmd: "GETEX" });
                }
                let raw = bulk(&args[i])?;
                let v: u64 = std::str::from_utf8(&raw)
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .ok_or_else(|| ProtoError::InvalidInteger { raw: raw.clone() })?;
                if v == 0 {
                    return Err(ProtoError::InvalidExpiry { raw });
                }
                ttl = Some(GetExTtl::Set(SetTtl::Millis(v)));
            }
            b"EXAT" => {
                i += 1;
                if i >= args.len() {
                    return Err(ProtoError::WrongArity { cmd: "GETEX" });
                }
                ttl = Some(GetExTtl::Set(SetTtl::UnixSecs(parse_u64(&args[i])?)));
            }
            b"PXAT" => {
                i += 1;
                if i >= args.len() {
                    return Err(ProtoError::WrongArity { cmd: "GETEX" });
                }
                ttl = Some(GetExTtl::Set(SetTtl::UnixMillis(parse_u64(&args[i])?)));
            }
            b"PERSIST" => ttl = Some(GetExTtl::Persist),
            _ => return Err(ProtoError::Syntax { token: opt }),
        }
        i += 1;
    }
    Ok(Command::GetEx { key, ttl })
}

#[cfg(test)]
mod tests {
    use super::*;
    use beyond_resp::Value;

    fn arr(parts: &[&[u8]]) -> Value {
        Value::Array(
            parts
                .iter()
                .map(|b| Value::BulkString(Bytes::copy_from_slice(b)))
                .collect(),
        )
    }

    // --- GET ---

    #[test]
    fn get_ok() {
        let cmd = Command::parse(arr(&[b"GET", b"mykey"])).unwrap();
        assert!(matches!(cmd, Command::Get { key } if key == "mykey"));
    }

    #[test]
    fn get_wrong_arity() {
        assert!(matches!(
            Command::parse(arr(&[b"GET"])),
            Err(ProtoError::WrongArity { cmd: "GET" })
        ));
    }

    // --- SET ---

    #[test]
    fn set_basic() {
        let cmd = Command::parse(arr(&[b"SET", b"k", b"v"])).unwrap();
        assert!(matches!(cmd, Command::Set { .. }));
    }

    #[test]
    fn set_ex() {
        let cmd = Command::parse(arr(&[b"SET", b"k", b"v", b"EX", b"60"])).unwrap();
        match cmd {
            Command::Set { args, .. } => assert!(matches!(args.ttl, Some(SetTtl::Seconds(60)))),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn set_px() {
        let cmd = Command::parse(arr(&[b"SET", b"k", b"v", b"PX", b"1000"])).unwrap();
        match cmd {
            Command::Set { args, .. } => assert!(matches!(args.ttl, Some(SetTtl::Millis(1000)))),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn set_ex_zero_rejected() {
        assert!(matches!(
            Command::parse(arr(&[b"SET", b"k", b"v", b"EX", b"0"])),
            Err(ProtoError::InvalidExpiry { .. })
        ));
    }

    #[test]
    fn set_px_zero_rejected() {
        assert!(matches!(
            Command::parse(arr(&[b"SET", b"k", b"v", b"PX", b"0"])),
            Err(ProtoError::InvalidExpiry { .. })
        ));
    }

    #[test]
    fn set_ex_missing_value_panics_not() {
        assert!(matches!(
            Command::parse(arr(&[b"SET", b"k", b"v", b"EX"])),
            Err(ProtoError::WrongArity { cmd: "SET" })
        ));
    }

    #[test]
    fn set_px_missing_value() {
        assert!(matches!(
            Command::parse(arr(&[b"SET", b"k", b"v", b"PX"])),
            Err(ProtoError::WrongArity { cmd: "SET" })
        ));
    }

    #[test]
    fn set_exat_missing_value() {
        assert!(matches!(
            Command::parse(arr(&[b"SET", b"k", b"v", b"EXAT"])),
            Err(ProtoError::WrongArity { cmd: "SET" })
        ));
    }

    #[test]
    fn set_pxat_missing_value() {
        assert!(matches!(
            Command::parse(arr(&[b"SET", b"k", b"v", b"PXAT"])),
            Err(ProtoError::WrongArity { cmd: "SET" })
        ));
    }

    #[test]
    fn set_nx() {
        let cmd = Command::parse(arr(&[b"SET", b"k", b"v", b"NX"])).unwrap();
        match cmd {
            Command::Set { args, .. } => assert_eq!(args.condition, SetCondition::Nx),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn set_xx() {
        let cmd = Command::parse(arr(&[b"SET", b"k", b"v", b"XX"])).unwrap();
        match cmd {
            Command::Set { args, .. } => assert_eq!(args.condition, SetCondition::Xx),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn set_get_flag() {
        let cmd = Command::parse(arr(&[b"SET", b"k", b"v", b"GET"])).unwrap();
        match cmd {
            Command::Set { args, .. } => assert!(args.get),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn set_keepttl() {
        // Previously broken: buffer was 6 bytes, KEEPTTL is 7 — would return Syntax error
        let cmd = Command::parse(arr(&[b"SET", b"k", b"v", b"KEEPTTL"])).unwrap();
        assert!(matches!(cmd, Command::Set { .. }));
    }

    #[test]
    fn set_wrong_arity() {
        assert!(matches!(
            Command::parse(arr(&[b"SET", b"k"])),
            Err(ProtoError::WrongArity { cmd: "SET" })
        ));
    }

    #[test]
    fn set_syntax_error() {
        assert!(matches!(
            Command::parse(arr(&[b"SET", b"k", b"v", b"BOGUS"])),
            Err(ProtoError::Syntax { .. })
        ));
    }

    // --- DEL / EXISTS ---

    #[test]
    fn del_single() {
        let cmd = Command::parse(arr(&[b"DEL", b"k"])).unwrap();
        assert!(matches!(cmd, Command::Del { keys } if keys.len() == 1));
    }

    #[test]
    fn del_multi() {
        let cmd = Command::parse(arr(&[b"DEL", b"a", b"b", b"c"])).unwrap();
        assert!(matches!(cmd, Command::Del { keys } if keys.len() == 3));
    }

    #[test]
    fn del_wrong_arity() {
        assert!(matches!(
            Command::parse(arr(&[b"DEL"])),
            Err(ProtoError::WrongArity { cmd: "DEL" })
        ));
    }

    #[test]
    fn exists_multi() {
        let cmd = Command::parse(arr(&[b"EXISTS", b"a", b"b"])).unwrap();
        assert!(matches!(cmd, Command::Exists { keys } if keys.len() == 2));
    }

    // --- TTL commands ---

    #[test]
    fn expire_ok() {
        let cmd = Command::parse(arr(&[b"EXPIRE", b"k", b"30"])).unwrap();
        assert!(matches!(cmd, Command::Expire { secs: 30, .. }));
    }

    #[test]
    fn pexpire_ok() {
        let cmd = Command::parse(arr(&[b"PEXPIRE", b"k", b"5000"])).unwrap();
        assert!(matches!(cmd, Command::PExpire { millis: 5000, .. }));
    }

    #[test]
    fn ttl_ok() {
        let cmd = Command::parse(arr(&[b"TTL", b"k"])).unwrap();
        assert!(matches!(cmd, Command::Ttl { .. }));
    }

    #[test]
    fn persist_ok() {
        let cmd = Command::parse(arr(&[b"PERSIST", b"k"])).unwrap();
        assert!(matches!(cmd, Command::Persist { .. }));
    }

    // --- SCAN ---

    #[test]
    fn scan_basic() {
        let cmd = Command::parse(arr(&[b"SCAN", b"0"])).unwrap();
        assert!(matches!(cmd, Command::Scan { ref cursor, .. } if cursor.as_ref() == b"0"));
    }

    #[test]
    fn scan_with_match() {
        let cmd = Command::parse(arr(&[b"SCAN", b"0", b"MATCH", b"foo*"])).unwrap();
        match cmd {
            Command::Scan { args, .. } => {
                assert_eq!(args.pattern.as_deref(), Some(b"foo*".as_ref()))
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn scan_with_count() {
        let cmd = Command::parse(arr(&[b"SCAN", b"0", b"COUNT", b"100"])).unwrap();
        match cmd {
            Command::Scan { args, .. } => assert_eq!(args.count, 100),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn scan_match_missing_value() {
        assert!(matches!(
            Command::parse(arr(&[b"SCAN", b"0", b"MATCH"])),
            Err(ProtoError::WrongArity { cmd: "SCAN" })
        ));
    }

    #[test]
    fn scan_count_missing_value() {
        assert!(matches!(
            Command::parse(arr(&[b"SCAN", b"0", b"COUNT"])),
            Err(ProtoError::WrongArity { cmd: "SCAN" })
        ));
    }

    // --- MGET / MSET ---

    #[test]
    fn mget_ok() {
        let cmd = Command::parse(arr(&[b"MGET", b"a", b"b"])).unwrap();
        assert!(matches!(cmd, Command::MGet { keys } if keys.len() == 2));
    }

    #[test]
    fn mset_ok() {
        let cmd = Command::parse(arr(&[b"MSET", b"k1", b"v1", b"k2", b"v2"])).unwrap();
        assert!(matches!(cmd, Command::MSet { pairs } if pairs.len() == 2));
    }

    #[test]
    fn mset_odd_args_rejected() {
        assert!(matches!(
            Command::parse(arr(&[b"MSET", b"k1", b"v1", b"k2"])),
            Err(ProtoError::WrongArity { cmd: "MSET" })
        ));
    }

    // --- GETEX ---

    #[test]
    fn getex_no_ttl() {
        let cmd = Command::parse(arr(&[b"GETEX", b"k"])).unwrap();
        assert!(matches!(cmd, Command::GetEx { ttl: None, .. }));
    }

    #[test]
    fn getex_persist() {
        let cmd = Command::parse(arr(&[b"GETEX", b"k", b"PERSIST"])).unwrap();
        assert!(matches!(
            cmd,
            Command::GetEx {
                ttl: Some(GetExTtl::Persist),
                ..
            }
        ));
    }

    #[test]
    fn getex_ex() {
        let cmd = Command::parse(arr(&[b"GETEX", b"k", b"EX", b"60"])).unwrap();
        assert!(matches!(
            cmd,
            Command::GetEx {
                ttl: Some(GetExTtl::Set(SetTtl::Seconds(60))),
                ..
            }
        ));
    }

    #[test]
    fn getex_ex_zero_rejected() {
        assert!(matches!(
            Command::parse(arr(&[b"GETEX", b"k", b"EX", b"0"])),
            Err(ProtoError::InvalidExpiry { .. })
        ));
    }

    #[test]
    fn getex_ex_missing_value() {
        assert!(matches!(
            Command::parse(arr(&[b"GETEX", b"k", b"EX"])),
            Err(ProtoError::WrongArity { cmd: "GETEX" })
        ));
    }

    // --- SELECT ---

    #[test]
    fn select_ok() {
        let cmd = Command::parse(arr(&[b"SELECT", b"0"])).unwrap();
        assert!(matches!(cmd, Command::Select { db: 0 }));
    }

    #[test]
    fn select_max_ok() {
        let cmd = Command::parse(arr(&[b"SELECT", b"15"])).unwrap();
        assert!(matches!(cmd, Command::Select { db: 15 }));
    }

    #[test]
    fn select_large_db_ok() {
        let cmd = Command::parse(arr(&[b"SELECT", b"16"])).unwrap();
        assert!(matches!(cmd, Command::Select { db: 16 }));
        let cmd = Command::parse(arr(&[b"SELECT", b"999"])).unwrap();
        assert!(matches!(cmd, Command::Select { db: 999 }));
    }

    #[test]
    fn select_non_integer() {
        assert!(matches!(
            Command::parse(arr(&[b"SELECT", b"abc"])),
            Err(ProtoError::InvalidInteger { .. })
        ));
    }

    // --- Misc ---

    #[test]
    fn ping_no_message() {
        let cmd = Command::parse(arr(&[b"PING"])).unwrap();
        assert!(matches!(cmd, Command::Ping { message: None }));
    }

    #[test]
    fn ping_with_message() {
        let cmd = Command::parse(arr(&[b"PING", b"hello"])).unwrap();
        assert!(matches!(cmd, Command::Ping { message: Some(_) }));
    }

    #[test]
    fn hello_no_version() {
        let cmd = Command::parse(arr(&[b"HELLO"])).unwrap();
        assert!(matches!(cmd, Command::Hello { version: None }));
    }

    #[test]
    fn hello_with_version() {
        let cmd = Command::parse(arr(&[b"HELLO", b"3"])).unwrap();
        assert!(matches!(cmd, Command::Hello { version: Some(3) }));
    }

    #[test]
    fn unknown_command() {
        assert!(matches!(
            Command::parse(arr(&[b"NOTACOMMAND"])),
            Err(ProtoError::UnknownCommand { .. })
        ));
    }

    #[test]
    fn empty_array_rejected() {
        assert!(matches!(
            Command::parse(Value::Array(vec![])),
            Err(ProtoError::InvalidFormat { .. })
        ));
    }

    #[test]
    fn case_insensitive() {
        let cmd = Command::parse(arr(&[b"get", b"k"])).unwrap();
        assert!(matches!(cmd, Command::Get { .. }));
        let cmd = Command::parse(arr(&[b"Set", b"k", b"v"])).unwrap();
        assert!(matches!(cmd, Command::Set { .. }));
    }

    #[test]
    fn dbsize_ok() {
        let cmd = Command::parse(arr(&[b"DBSIZE"])).unwrap();
        assert!(matches!(cmd, Command::DbSize));
    }

    #[test]
    fn flushdb_ok() {
        let cmd = Command::parse(arr(&[b"FLUSHDB"])).unwrap();
        assert!(matches!(cmd, Command::FlushDb));
    }

    #[test]
    fn flushdb_extra_args_rejected() {
        assert!(matches!(
            Command::parse(arr(&[b"FLUSHDB", b"ASYNC"])),
            Err(ProtoError::WrongArity { cmd: "FLUSHDB" })
        ));
    }

    #[test]
    fn quit_ok() {
        let cmd = Command::parse(arr(&[b"QUIT"])).unwrap();
        assert!(matches!(cmd, Command::Quit));
    }

    #[test]
    fn hello_version_overflow_rejected() {
        assert!(matches!(
            Command::parse(arr(&[b"HELLO", b"256"])),
            Err(ProtoError::InvalidInteger { .. })
        ));
    }

    #[test]
    fn set_oversized_option_rejected() {
        assert!(matches!(
            Command::parse(arr(&[b"SET", b"k", b"v", b"TOOLONGOPTION"])),
            Err(ProtoError::Syntax { .. })
        ));
    }

    #[test]
    fn scan_oversized_option_rejected() {
        assert!(matches!(
            Command::parse(arr(&[b"SCAN", b"0", b"TOOLONGOPTION"])),
            Err(ProtoError::Syntax { .. })
        ));
    }

    #[test]
    fn getex_oversized_option_rejected() {
        assert!(matches!(
            Command::parse(arr(&[b"GETEX", b"k", b"TOOLONGOPTION"])),
            Err(ProtoError::Syntax { .. })
        ));
    }

    // --- WATCH / PWATCH / UNWATCH ---

    #[test]
    fn watch_single_key() {
        let cmd = Command::parse(arr(&[b"WATCH", b"mykey"])).unwrap();
        assert!(matches!(cmd, Command::Watch { keys, since: None } if keys.len() == 1));
    }

    #[test]
    fn watch_multi_key() {
        let cmd = Command::parse(arr(&[b"WATCH", b"a", b"b", b"c"])).unwrap();
        assert!(matches!(cmd, Command::Watch { keys, since: None } if keys.len() == 3));
    }

    #[test]
    fn watch_with_since() {
        let cmd = Command::parse(arr(&[b"WATCH", b"k", b"SINCE", b"42"])).unwrap();
        match cmd {
            Command::Watch { since, .. } => assert_eq!(since, Some(42)),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn watch_wrong_arity() {
        assert!(matches!(
            Command::parse(arr(&[b"WATCH"])),
            Err(ProtoError::WrongArity { cmd: "WATCH" })
        ));
    }

    #[test]
    fn pwatch_basic() {
        let cmd = Command::parse(arr(&[b"PWATCH", b"user:"])).unwrap();
        assert!(matches!(cmd, Command::PWatch { since: None, .. }));
    }

    #[test]
    fn pwatch_with_since() {
        let cmd = Command::parse(arr(&[b"PWATCH", b"user:", b"SINCE", b"100"])).unwrap();
        match cmd {
            Command::PWatch { since, .. } => assert_eq!(since, Some(100)),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn pwatch_wrong_arity() {
        assert!(matches!(
            Command::parse(arr(&[b"PWATCH"])),
            Err(ProtoError::WrongArity { cmd: "PWATCH" })
        ));
    }

    #[test]
    fn unwatch_ok() {
        let cmd = Command::parse(arr(&[b"UNWATCH"])).unwrap();
        assert!(matches!(cmd, Command::Unwatch));
    }

    // --- REVISION ---

    #[test]
    fn revision_ok() {
        let cmd = Command::parse(arr(&[b"REVISION", b"mykey"])).unwrap();
        assert!(matches!(cmd, Command::Revision { key } if key == "mykey"));
    }

    #[test]
    fn revision_wrong_arity() {
        assert!(matches!(
            Command::parse(arr(&[b"REVISION"])),
            Err(ProtoError::WrongArity { cmd: "REVISION" })
        ));
    }

    // --- SETREV ---

    #[test]
    fn setrev_basic() {
        let cmd = Command::parse(arr(&[b"SETREV", b"k", b"v", b"99"])).unwrap();
        assert!(matches!(
            cmd,
            Command::SetRev {
                revision: 99,
                ttl: None,
                ..
            }
        ));
    }

    #[test]
    fn setrev_with_ex() {
        let cmd = Command::parse(arr(&[b"SETREV", b"k", b"v", b"99", b"EX", b"30"])).unwrap();
        assert!(matches!(
            cmd,
            Command::SetRev {
                revision: 99,
                ttl: Some(SetTtl::Seconds(30)),
                ..
            }
        ));
    }

    #[test]
    fn setrev_wrong_arity() {
        assert!(matches!(
            Command::parse(arr(&[b"SETREV", b"k", b"v"])),
            Err(ProtoError::WrongArity { cmd: "SETREV" })
        ));
    }

    #[test]
    fn setrev_invalid_revision() {
        assert!(matches!(
            Command::parse(arr(&[b"SETREV", b"k", b"v", b"notanumber"])),
            Err(ProtoError::InvalidInteger { .. })
        ));
    }

    // --- SET REV condition ---

    #[test]
    fn set_rev_condition() {
        let cmd = Command::parse(arr(&[b"SET", b"k", b"v", b"REV", b"7"])).unwrap();
        match cmd {
            Command::Set { args, .. } => assert_eq!(args.condition, SetCondition::Rev(7)),
            _ => panic!("wrong variant"),
        }
    }

    // --- KEYS ---

    #[test]
    fn keys_no_pattern() {
        let cmd = Command::parse(arr(&[b"KEYS"])).unwrap();
        assert!(matches!(cmd, Command::Keys { pattern: None }));
    }

    #[test]
    fn keys_with_pattern() {
        let cmd = Command::parse(arr(&[b"KEYS", b"user:*"])).unwrap();
        match cmd {
            Command::Keys { pattern: Some(p) } => assert_eq!(p.as_ref(), b"user:*"),
            _ => panic!("wrong variant"),
        }
    }

    // --- GETSET ---

    #[test]
    fn getset_ok() {
        let cmd = Command::parse(arr(&[b"GETSET", b"k", b"v"])).unwrap();
        assert!(matches!(cmd, Command::GetSet { .. }));
    }

    #[test]
    fn getset_wrong_arity() {
        assert!(matches!(
            Command::parse(arr(&[b"GETSET", b"k"])),
            Err(ProtoError::WrongArity { cmd: "GETSET" })
        ));
    }

    // --- INCR / INCRBY / DECR / DECRBY ---

    #[test]
    fn incr_ok() {
        let cmd = Command::parse(arr(&[b"INCR", b"counter"])).unwrap();
        assert!(matches!(cmd, Command::Incr { .. }));
    }

    #[test]
    fn incrby_ok() {
        let cmd = Command::parse(arr(&[b"INCRBY", b"counter", b"5"])).unwrap();
        assert!(matches!(cmd, Command::IncrBy { delta: 5, .. }));
    }

    #[test]
    fn incrby_negative() {
        let cmd = Command::parse(arr(&[b"INCRBY", b"counter", b"-3"])).unwrap();
        assert!(matches!(cmd, Command::IncrBy { delta: -3, .. }));
    }

    #[test]
    fn incrby_wrong_arity() {
        assert!(matches!(
            Command::parse(arr(&[b"INCRBY", b"counter"])),
            Err(ProtoError::WrongArity { cmd: "INCRBY" })
        ));
    }

    #[test]
    fn decr_ok() {
        let cmd = Command::parse(arr(&[b"DECR", b"counter"])).unwrap();
        assert!(matches!(cmd, Command::Decr { .. }));
    }

    #[test]
    fn decrby_ok() {
        let cmd = Command::parse(arr(&[b"DECRBY", b"counter", b"10"])).unwrap();
        assert!(matches!(cmd, Command::DecrBy { delta: 10, .. }));
    }

    #[test]
    fn decrby_wrong_arity() {
        assert!(matches!(
            Command::parse(arr(&[b"DECRBY", b"counter"])),
            Err(ProtoError::WrongArity { cmd: "DECRBY" })
        ));
    }
}

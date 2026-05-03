use beyond_resp::Value;
use bytes::Bytes;

pub fn ok() -> Value {
    Value::SimpleString(Bytes::from_static(b"OK"))
}

pub fn nil() -> Value {
    Value::Null
}

pub fn integer(n: i64) -> Value {
    Value::Integer(n)
}

pub fn bulk(b: impl Into<Bytes>) -> Value {
    Value::BulkString(b.into())
}

pub fn error(kind: &str, msg: &str) -> Value {
    Value::SimpleError(Bytes::from(format!("{kind} {msg}")))
}

pub fn err_wrong_type() -> Value {
    error("WRONGTYPE", "Operation against a key holding the wrong kind of value")
}

pub fn array(items: Vec<Value>) -> Value {
    Value::Array(items)
}

pub fn scan_reply(cursor: Bytes, keys: Vec<Bytes>) -> Value {
    Value::Array(vec![
        Value::BulkString(cursor),
        Value::Array(keys.into_iter().map(Value::BulkString).collect()),
    ])
}

pub fn hello_reply(version: u8) -> Value {
    // RESP3 map response for HELLO
    Value::Map(vec![
        (Value::SimpleString(Bytes::from_static(b"server")), Value::SimpleString(Bytes::from_static(b"beyond-kv"))),
        (Value::SimpleString(Bytes::from_static(b"version")), Value::SimpleString(Bytes::from_static(env!("CARGO_PKG_VERSION").as_bytes()))),
        (Value::SimpleString(Bytes::from_static(b"proto")), Value::Integer(version as i64)),
        (Value::SimpleString(Bytes::from_static(b"mode")), Value::SimpleString(Bytes::from_static(b"standalone"))),
        (Value::SimpleString(Bytes::from_static(b"role")), Value::SimpleString(Bytes::from_static(b"master"))),
    ])
}

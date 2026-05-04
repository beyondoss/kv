use std::net::SocketAddr;
use std::os::unix::net::UnixStream as StdUnixStream;
use std::rc::Rc;
use std::sync::{Arc, mpsc};
use std::time::Duration;

use base64::Engine as _;
use beyond_kv_engine::log::now_ms;
use beyond_kv_engine::store::ShardStore;
use beyond_kv_engine::types::SetOptions;
use beyond_kv_engine::watch::{KeyFilter, WatchEvent};
use bytes::Bytes;
use futures_channel::mpsc::Receiver;
use futures_util::StreamExt as FuturesStreamExt;
use futures_util::stream::SelectAll;
use monoio::io::{
    AsyncWriteRentExt, OwnedReadHalf, OwnedWriteHalf, Splitable, sink::Sink, stream::Stream,
};
use monoio::net::TcpStream;
use monoio_http::common::body::{Body, BodyExt, FixedBody, HttpBody};
use monoio_http::h1::codec::decoder::FillPayload;
use monoio_http::h1::codec::decoder::RequestDecoder;
use monoio_http::h1::codec::encoder::GenericEncoder;
use monoio_http::h1::payload::Payload;

pub async fn serve_routed(
    store: Rc<ShardStore>,
    rx: mpsc::Receiver<(std::net::TcpStream, SocketAddr)>,
    wakeup_read: StdUnixStream,
    max_conns: usize,
    idle_timeout: Duration,
    max_value_bytes: usize,
) {
    crate::serve_loop(rx, wakeup_read, max_conns, "HTTP", |s, _peer, guard| {
        let store = store.clone();
        monoio::spawn(async move {
            let _guard = guard;
            handle_conn(s, store, idle_timeout, max_value_bytes).await;
        });
    })
    .await;
}

async fn handle_conn(
    stream: TcpStream,
    store: Rc<ShardStore>,
    idle_timeout: Duration,
    max_value_bytes: usize,
) {
    // Split so we can keep the write half for SSE streaming later.
    let (r, mut w) = stream.into_split();
    let mut decoder: RequestDecoder<OwnedReadHalf<TcpStream>> = RequestDecoder::new(r);

    loop {
        let req = match monoio::time::timeout(idle_timeout, Stream::next(&mut decoder)).await {
            Ok(Some(Ok(r))) => r,
            Ok(Some(Err(e))) => {
                tracing::debug!("HTTP decode error: {e}");
                break;
            }
            Ok(None) => break,
            Err(_) => {
                tracing::debug!("HTTP connection idle timeout");
                break;
            }
        };

        // SSE watch paths take over the connection — intercept before filling payload.
        if let Some(wp) = parse_watch_params(req.uri().path(), req.uri().query().unwrap_or("")) {
            if req.method() != http::Method::GET {
                let mut enc = GenericEncoder::new(&mut w);
                let _ = Sink::send(&mut enc, method_not_allowed()).await;
                break;
            }
            handle_watch_sse(&mut w, &store, wp).await;
            return;
        }

        // Reject oversized bodies before reading them.
        let content_len: Option<usize> = req
            .headers()
            .get(http::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse().ok());
        if content_len.map_or(false, |n| n > max_value_bytes) {
            let mut enc = GenericEncoder::new(&mut w);
            let _ = Sink::send(&mut enc, payload_too_large()).await;
            break; // close connection — body not drained
        }

        if decoder.fill_payload().await.is_err() {
            break;
        }

        let (parts, body) = req.into_parts();
        let body_bytes = read_body(body).await;

        // Secondary check on actual body size (handles chunked transfer).
        if body_bytes.len() > max_value_bytes {
            let mut enc = GenericEncoder::new(&mut w);
            let _ = Sink::send(&mut enc, payload_too_large()).await;
            break;
        }

        let response = route(&parts, body_bytes, &store).await;

        let mut enc = GenericEncoder::new(&mut w);
        if Sink::send(&mut enc, response).await.is_err() {
            break;
        }
        if Sink::<http::Response<HttpBody>>::flush(&mut enc)
            .await
            .is_err()
        {
            break;
        }
    }
}

async fn read_body(body: Payload) -> Bytes {
    match body.stream_hint() {
        monoio_http::common::body::StreamHint::None => Bytes::new(),
        _ => body.bytes().await.unwrap_or_default(),
    }
}

async fn route(
    parts: &http::request::Parts,
    body: Bytes,
    store: &ShardStore,
) -> http::Response<HttpBody> {
    let path = parts.uri.path();
    let method = &parts.method;

    // Route: /namespaces/{ns}/values/{key}
    if let Some(rest) = path.strip_prefix("/namespaces/") {
        if let Some((ns, rest)) = split_once(rest, '/') {
            if let Some(key_encoded) = rest.strip_prefix("values/") {
                let key = percent_decode(key_encoded);
                return match *method {
                    http::Method::GET => handle_get(ns, &key, store).await,
                    http::Method::PUT => handle_put(ns, &key, body, parts, store).await,
                    http::Method::DELETE => handle_delete(ns, &key, store).await,
                    _ => method_not_allowed(),
                };
            }
            if rest == "keys" {
                if *method == http::Method::GET {
                    let query = parts.uri.query().unwrap_or("");
                    return handle_list(ns, query, store).await;
                }
                return method_not_allowed();
            }
        }
    }

    if path == "/healthz" {
        return ok_text("ok");
    }

    not_found_json("not_found", "endpoint not found")
}

async fn handle_get(ns: &str, key: &[u8], store: &ShardStore) -> http::Response<HttpBody> {
    match store.get(ns, key).await {
        Err(e) => internal_error(&e.to_string()),
        Ok(None) => not_found_json("not_found", "key does not exist"),
        Ok(Some(entry)) => {
            let ttl_secs = entry.expires_at.map(|t| {
                t.saturating_duration_since(std::time::Instant::now())
                    .as_secs()
            });
            let mut builder = http::Response::builder().status(200);
            if let Some(ttl) = ttl_secs {
                builder = builder.header("X-KV-TTL", ttl.to_string());
            }
            if entry.revision > 0 {
                builder = builder.header("X-KV-Revision", entry.revision.to_string());
            }
            if let Some(meta) = &entry.metadata {
                if let Ok(json) = serde_json::to_string(meta) {
                    if let Ok(hv) = http::HeaderValue::from_str(&json) {
                        builder = builder.header("X-KV-Metadata", hv);
                    }
                }
            }
            builder
                .header("Content-Type", "application/octet-stream")
                .body(HttpBody::fixed_body(Some(entry.value)))
                .unwrap_or_else(|_| internal_error("response build failed"))
        }
    }
}

async fn handle_put(
    ns: &str,
    key: &[u8],
    body: Bytes,
    parts: &http::request::Parts,
    store: &ShardStore,
) -> http::Response<HttpBody> {
    let ttl = parse_ttl_from_request(parts);
    let metadata = parts
        .headers
        .get("x-kv-metadata")
        .and_then(|v| v.to_str().ok())
        .filter(|s| s.len() <= 64 * 1024)
        .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok())
        .map(Arc::new);

    let query = parts.uri.query().unwrap_or("");
    let nx = query.contains("nx=1");
    let xx = query.contains("xx=1");
    let if_match: Option<u64> = parts
        .headers
        .get("if-match")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse().ok());

    let opts = SetOptions { ttl, metadata };

    if let Some(expected_rev) = if_match {
        match store.setrev(ns, key, body, opts, expected_rev).await {
            Ok(Some(new_rev)) => http::Response::builder()
                .status(204)
                .header("X-KV-Revision", new_rev.to_string())
                .body(HttpBody::fixed_body(None))
                .unwrap_or_else(|_| internal_error("response build failed")),
            Ok(None) => json_response(
                409,
                &serde_json::json!({ "error": "conflict", "message": "revision mismatch" }),
            ),
            Err(e) => internal_error(&e.to_string()),
        }
    } else if nx {
        match store.setnx(ns, key, body, opts).await {
            Ok(true) => no_content(),
            Ok(false) => json_response(
                409,
                &serde_json::json!({ "error": "conflict", "message": "key already exists" }),
            ),
            Err(e) => internal_error(&e.to_string()),
        }
    } else if xx {
        match store.setxx(ns, key, body, opts).await {
            Ok(true) => no_content(),
            Ok(false) => json_response(
                409,
                &serde_json::json!({ "error": "conflict", "message": "key does not exist" }),
            ),
            Err(e) => internal_error(&e.to_string()),
        }
    } else {
        match store.set(ns, key, body, opts).await {
            Ok(()) => no_content(),
            Err(e) => internal_error(&e.to_string()),
        }
    }
}

async fn handle_delete(ns: &str, key: &[u8], store: &ShardStore) -> http::Response<HttpBody> {
    match store.del(ns, &[key]).await {
        Ok(_) => no_content(),
        Err(e) => internal_error(&e.to_string()),
    }
}

async fn handle_list(ns: &str, query: &str, store: &ShardStore) -> http::Response<HttpBody> {
    let prefix_pattern = query_param(query, "prefix").map(|raw| {
        let mut p = percent_decode(raw);
        p.push(b'*');
        p
    });
    // Cursor is a decimal u64 string (URL-safe). "0" or absent = start of scan.
    let cursor_bytes: Vec<u8> = match query_param(query, "cursor") {
        None | Some("0") => b"0".to_vec(),
        Some(s) => match s.parse::<u64>() {
            Ok(0) => b"0".to_vec(),
            Ok(pos) => pos.to_le_bytes().to_vec(),
            Err(_) => b"0".to_vec(), // invalid cursor: restart
        },
    };
    let limit: u64 = query_param(query, "limit")
        .and_then(|s| s.parse().ok())
        .unwrap_or(100)
        .min(1000);

    match store
        .scan(ns, &cursor_bytes, prefix_pattern.as_deref(), limit)
        .await
    {
        Err(e) => internal_error(&e.to_string()),
        Ok(page) => {
            let keys: Vec<serde_json::Value> = page
                .keys
                .iter()
                .map(|k| {
                    let name = String::from_utf8(k.to_vec())
                        .unwrap_or_else(|e| percent_encode_bytes(e.as_bytes()));
                    serde_json::json!({ "name": name })
                })
                .collect();
            let complete = page.next_cursor == b"0".as_ref();
            let mut body = serde_json::json!({ "keys": keys, "complete": complete });
            if !complete {
                // Emit cursor as decimal u64 — URL-safe, no re-encoding needed by clients.
                let pos = if page.next_cursor.len() == 8 {
                    page.next_cursor
                        .as_ref()
                        .try_into()
                        .map(u64::from_le_bytes)
                        .unwrap_or(0)
                } else {
                    0u64
                };
                body["cursor"] = serde_json::Value::String(pos.to_string());
            }
            json_response(200, &body)
        }
    }
}

// ── SSE watch ────────────────────────────────────────────────────────────────

struct WatchParams {
    ns: String,
    key: Vec<u8>,
    is_prefix: bool,
    since: Option<u64>,
}

fn parse_watch_params(path: &str, query: &str) -> Option<WatchParams> {
    let rest = path.strip_prefix("/namespaces/")?;
    let (ns, rest) = split_once(rest, '/')?;

    if let Some(key_encoded) = rest.strip_prefix("watch/") {
        return Some(WatchParams {
            ns: ns.to_string(),
            key: percent_decode(key_encoded),
            is_prefix: false,
            since: query_param(query, "since").and_then(|s| s.parse().ok()),
        });
    }

    if rest == "watch" {
        let prefix_raw = query_param(query, "prefix")?;
        return Some(WatchParams {
            ns: ns.to_string(),
            key: percent_decode(prefix_raw),
            is_prefix: true,
            since: query_param(query, "since").and_then(|s| s.parse().ok()),
        });
    }

    None
}

async fn handle_watch_sse(
    w: &mut OwnedWriteHalf<TcpStream>,
    store: &ShardStore,
    params: WatchParams,
) {
    let headers = b"HTTP/1.1 200 OK\r\n\
        Content-Type: text/event-stream\r\n\
        Cache-Control: no-cache\r\n\
        X-Accel-Buffering: no\r\n\
        Connection: keep-alive\r\n\
        \r\n"
        .to_vec();
    let (res, _) = w.write_all(headers).await;
    if res.is_err() {
        return;
    }

    let filter = if params.is_prefix {
        KeyFilter::Prefix(&params.key)
    } else {
        KeyFilter::Exact(&params.key)
    };
    let since_rev = params.since.unwrap_or(0);

    let (initial, rx) = match store.watch_subscribe(&params.ns, filter, since_rev).await {
        Ok(v) => v,
        Err(e) => {
            let msg = format!(
                "data: {{\"type\":\"error\",\"message\":{}}}\n\n",
                serde_json::json!(e.to_string())
            );
            let _ = w.write_all(msg.into_bytes()).await;
            return;
        }
    };

    for event in &initial {
        let data = format!("data: {}\n\n", sse_event_json(event));
        let (res, _) = w.write_all(data.into_bytes()).await;
        if res.is_err() {
            return;
        }
    }

    let (res, _) = w
        .write_all(b"data: {\"type\":\"ready\",\"revision\":0}\n\n".to_vec())
        .await;
    if res.is_err() {
        return;
    }

    let mut rx_stream: SelectAll<Receiver<WatchEvent>> = SelectAll::new();
    rx_stream.push(rx);

    loop {
        match monoio::time::timeout(
            Duration::from_secs(25),
            FuturesStreamExt::next(&mut rx_stream),
        )
        .await
        {
            Ok(Some(event)) => {
                let data = format!("data: {}\n\n", sse_event_json(&event));
                let (res, _) = w.write_all(data.into_bytes()).await;
                if res.is_err() {
                    return;
                }
            }
            Ok(None) => return,
            Err(_timeout) => {
                let (res, _) = w.write_all(b": heartbeat\n\n".to_vec()).await;
                if res.is_err() {
                    return;
                }
            }
        }
    }
}

fn sse_event_json(event: &WatchEvent) -> String {
    match event {
        WatchEvent::Set {
            key,
            value,
            metadata,
            expires_at_ms,
            revision,
        } => {
            let key_str = String::from_utf8_lossy(key);
            let value_b64 = base64::engine::general_purpose::STANDARD.encode(value);
            let mut obj = serde_json::json!({
                "type": "set",
                "key": key_str,
                "value": value_b64,
                "revision": revision,
            });
            if let Some(exp_ms) = expires_at_ms {
                let ttl_secs = exp_ms.saturating_sub(now_ms()) / 1000;
                obj["ttl"] = serde_json::Value::Number(ttl_secs.into());
            }
            if let Some(meta) = metadata {
                obj["metadata"] = meta.as_ref().clone();
            }
            obj.to_string()
        }
        WatchEvent::Del { key, revision } => serde_json::json!({
            "type": "del",
            "key": String::from_utf8_lossy(key),
            "revision": revision,
        })
        .to_string(),
    }
}

// ── helpers ──────────────────────────────────────────────────────────────────

fn parse_ttl_from_request(parts: &http::request::Parts) -> Option<Duration> {
    let header_ttl = parts
        .headers
        .get("x-kv-ttl")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .map(Duration::from_secs);

    header_ttl.or_else(|| {
        query_param(parts.uri.query().unwrap_or(""), "ttl")
            .and_then(|s| s.parse::<u64>().ok())
            .map(Duration::from_secs)
    })
}

fn query_param<'a>(query: &'a str, name: &str) -> Option<&'a str> {
    query.split('&').find_map(|pair| {
        let (k, v) = pair.split_once('=')?;
        if k == name { Some(v) } else { None }
    })
}

fn percent_encode_bytes(bytes: &[u8]) -> String {
    const HEX: &[u8] = b"0123456789ABCDEF";
    let mut out = Vec::with_capacity(bytes.len());
    for &b in bytes {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b);
        } else {
            out.push(b'%');
            out.push(HEX[(b >> 4) as usize]);
            out.push(HEX[(b & 0xf) as usize]);
        }
    }
    // Output contains only ASCII so this is always valid UTF-8.
    String::from_utf8(out).unwrap_or_default()
}

fn percent_decode(s: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(h), Some(l)) = (hex_val(bytes[i + 1]), hex_val(bytes[i + 2])) {
                out.push(h << 4 | l);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    out
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn split_once(s: &str, delim: char) -> Option<(&str, &str)> {
    let pos = s.find(delim)?;
    Some((&s[..pos], &s[pos + 1..]))
}

fn fallback_500() -> http::Response<HttpBody> {
    http::Response::builder()
        .status(500)
        .header("Content-Type", "text/plain")
        .body(HttpBody::fixed_body(Some(Bytes::from_static(
            b"internal error",
        ))))
        .expect("static response must build")
}

fn json_response(status: u16, body: &serde_json::Value) -> http::Response<HttpBody> {
    let json = serde_json::to_vec(body).unwrap_or_default();
    http::Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .body(HttpBody::fixed_body(Some(Bytes::from(json))))
        .unwrap_or_else(|_| fallback_500())
}

fn ok_text(msg: &'static str) -> http::Response<HttpBody> {
    http::Response::builder()
        .status(200)
        .body(HttpBody::fixed_body(Some(Bytes::from_static(
            msg.as_bytes(),
        ))))
        .unwrap_or_else(|_| fallback_500())
}

fn no_content() -> http::Response<HttpBody> {
    http::Response::builder()
        .status(204)
        .body(HttpBody::fixed_body(None))
        .unwrap_or_else(|_| fallback_500())
}

fn payload_too_large() -> http::Response<HttpBody> {
    json_response(
        413,
        &serde_json::json!({
            "error": "payload_too_large",
            "message": "request body exceeds maximum allowed size"
        }),
    )
}

fn not_found_json(code: &str, msg: &str) -> http::Response<HttpBody> {
    json_response(404, &serde_json::json!({ "error": code, "message": msg }))
}

fn internal_error(msg: &str) -> http::Response<HttpBody> {
    json_response(
        500,
        &serde_json::json!({ "error": "internal_error", "message": msg }),
    )
}

fn method_not_allowed() -> http::Response<HttpBody> {
    json_response(
        405,
        &serde_json::json!({ "error": "method_not_allowed", "message": "method not allowed" }),
    )
}

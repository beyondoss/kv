use std::io::Write as _;
use std::net::SocketAddr;
use std::os::unix::net::UnixStream as StdUnixStream;
use std::rc::Rc;
use std::sync::{Arc, mpsc};
use std::time::Duration;

use crate::cross_shard::{CrossShardRequest, OwnedKeyFilter, ShardSenders};

use base64::Engine as _;
use beyond_kv_engine::error::EngineError;
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

#[allow(clippy::too_many_arguments)]
pub async fn serve_routed(
    store: Rc<ShardStore>,
    rx: mpsc::Receiver<(std::net::TcpStream, SocketAddr)>,
    wakeup_read: StdUnixStream,
    max_conns: usize,
    idle_timeout: Duration,
    max_value_bytes: usize,
    shard_idx: usize,
    n_shards: usize,
    cross_shard_txs: ShardSenders,
    cross_shard_wakeups: Arc<[StdUnixStream]>,
) {
    crate::serve_loop(rx, wakeup_read, max_conns, "HTTP", |s, _peer, guard| {
        let store = store.clone();
        let txs = cross_shard_txs.clone();
        let wakeups = cross_shard_wakeups.clone();
        monoio::spawn(async move {
            let _guard = guard;
            handle_conn(
                s,
                store,
                idle_timeout,
                max_value_bytes,
                shard_idx,
                n_shards,
                txs,
                wakeups,
            )
            .await;
        });
    })
    .await;
}

async fn handle_conn(
    stream: TcpStream,
    store: Rc<ShardStore>,
    idle_timeout: Duration,
    max_value_bytes: usize,
    shard_idx: usize,
    n_shards: usize,
    cross_shard_txs: ShardSenders,
    cross_shard_wakeups: Arc<[StdUnixStream]>,
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
            handle_watch_sse(
                &mut w,
                &store,
                wp,
                shard_idx,
                n_shards,
                &cross_shard_txs,
                &cross_shard_wakeups,
            )
            .await;
            return;
        }

        // Reject oversized bodies before reading them.
        let content_len: Option<usize> = req
            .headers()
            .get(http::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse().ok());
        if content_len.is_some_and(|n| n > max_value_bytes) {
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

        let response = route(
            &parts,
            body_bytes,
            &store,
            shard_idx,
            n_shards,
            &cross_shard_txs,
            &cross_shard_wakeups,
        )
        .await;

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
    shard_idx: usize,
    n_shards: usize,
    cross_shard_txs: &ShardSenders,
    cross_shard_wakeups: &[StdUnixStream],
) -> http::Response<HttpBody> {
    let path = parts.uri.path();
    let method = &parts.method;
    let query = parts.uri.query().unwrap_or("");

    if path == "/healthz" {
        return ok_text("ok");
    }

    if path == "/v1/openapi.json" {
        if *method != http::Method::GET {
            return method_not_allowed();
        }
        return http::Response::builder()
            .status(200)
            .header("Content-Type", "application/json")
            .body(HttpBody::fixed_body(Some(openapi_json().clone())))
            .unwrap_or_else(|_| fallback_500());
    }

    // GET /v1/kv → list endpoint (no key in path).
    if path == "/v1/kv" || path == "/v1/kv/" {
        if *method != http::Method::GET {
            return method_not_allowed();
        }
        let ns = match parse_ns(query) {
            Ok(n) => n,
            Err(r) => return r,
        };
        return handle_list(
            ns,
            query,
            store,
            shard_idx,
            n_shards,
            cross_shard_txs,
            cross_shard_wakeups,
        )
        .await;
    }

    // /v1/kv/{key}[/incr]
    if let Some(rest) = path.strip_prefix("/v1/kv/") {
        let ns = match parse_ns(query) {
            Ok(n) => n,
            Err(r) => return r,
        };
        // POST /v1/kv/{key}/incr
        if *method == http::Method::POST {
            if let Some(key_encoded) = rest.strip_suffix("/incr") {
                let key = percent_decode(key_encoded);
                if key.is_empty() {
                    return not_found_json("not_found", "endpoint not found");
                }
                let delta = query_param(query, "delta")
                    .and_then(|s| s.parse::<i64>().ok())
                    .unwrap_or(1);
                return handle_incr(ns, &key, delta, store).await;
            }
            return method_not_allowed();
        }
        // Reject /v1/kv/{key}/incr for non-POST
        let key_encoded = rest.strip_suffix("/incr").unwrap_or(rest);
        let key = percent_decode(key_encoded);
        if key.is_empty() {
            return not_found_json("not_found", "endpoint not found");
        }
        return match *method {
            http::Method::GET => handle_get(ns, &key, store).await,
            http::Method::PUT => handle_put(ns, &key, body, parts, store).await,
            http::Method::DELETE => handle_delete(ns, &key, parts, store).await,
            _ => method_not_allowed(),
        };
    }

    not_found_json("not_found", "endpoint not found")
}

/// Fetch the raw bytes stored at `key`. The value is returned as `application/octet-stream`.
/// Response headers carry key metadata: `X-KV-TTL` is the remaining TTL in whole seconds
/// (absent when the key has no expiry), `X-KV-Revision` is the current revision counter
/// (absent if the key was never written via `If-Match`), and `X-KV-Metadata` is the
/// JSON blob attached to the key (absent if none was stored).
#[utoipa::path(
    get,
    path = "/v1/kv/{key}",
    operation_id = "get_value",
    tag = "kv",
    params(
        ("key" = String, Path, description = "Key to retrieve. Percent-encoded; all bytes are valid except `\\0`."),
        ("ns" = Option<u8>, Query, description = "Namespace (0–15). Namespaces are independent keyspaces. Defaults to 0."),
    ),
    responses(
        (status = 200, description = "Key found. Value bytes in body.", content_type = "application/octet-stream",
            headers(
                ("X-KV-TTL" = u64, description = "Remaining TTL in whole seconds. Absent if the key has no expiry."),
                ("X-KV-Revision" = u64, description = "Current revision counter. Absent if the key was never written via `If-Match`."),
                ("X-KV-Metadata" = String, description = "JSON metadata blob attached to the key. Absent if none was stored."),
            )
        ),
        (status = 404, body = ErrorResponse, description = "Key does not exist in this namespace."),
    )
)]
async fn handle_get(ns: &str, key: &[u8], store: &ShardStore) -> http::Response<HttpBody> {
    match store.get(ns, key).await {
        Err(e) => engine_error_response(e),
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

/// Store raw bytes at `key`. The request body is the value (`application/octet-stream`).
///
/// **Conditional writes** — at most one condition may be active per request:
/// - `nx=1` — write only if the key does **not** exist; returns 409 if it does.
/// - `xx=1` — write only if the key **already exists**; returns 409 if it does not.
/// - `If-Match: <rev>` — write only if the stored revision equals `<rev>`; returns 409
///   on mismatch. On success, the new revision is returned in `X-KV-Revision`.
///
/// **TTL** — set `ttl=<seconds>` (query) or `X-KV-TTL: <seconds>` (header; takes
/// precedence). Use `X-KV-KeepTTL: 1` to preserve an existing expiry when overwriting a
/// key; mutually exclusive with any TTL option.
///
/// **Atomic read-modify** — `X-KV-Return-Old: 1` returns the previous value (200) in a
/// single atomic swap; 204 when the key did not exist. Mutually exclusive with all
/// conditional-write options.
///
/// **Metadata** — `X-KV-Metadata: <json>` attaches an arbitrary JSON value to the key
/// (max 64 KiB serialized). Retrievable on GET via `X-KV-Metadata`.
#[utoipa::path(
    put,
    path = "/v1/kv/{key}",
    operation_id = "put_value",
    tag = "kv",
    params(
        ("key" = String, Path, description = "Key to set. Percent-encoded; all bytes are valid except `\\0`."),
        ("ns" = Option<u8>, Query, description = "Namespace (0–15). Defaults to 0."),
        ("nx" = Option<u8>, Query, description = "Set `nx=1` to write only if the key does **not** exist. Mutually exclusive with `xx` and `If-Match`."),
        ("xx" = Option<u8>, Query, description = "Set `xx=1` to write only if the key **already exists**. Mutually exclusive with `nx` and `If-Match`."),
        ("ttl" = Option<u64>, Query, description = "Key expiry in seconds from now. Overridden by the `X-KV-TTL` header when both are present."),
        ("If-Match" = Option<u64>, Header, description = "Write only if the stored revision equals this value. Returns 409 on mismatch. On success, the new revision is in `X-KV-Revision`."),
        ("X-KV-TTL" = Option<u64>, Header, description = "Key expiry in seconds from now. Takes precedence over the `ttl` query parameter."),
        ("X-KV-KeepTTL" = Option<String>, Header, description = "Set to any value to preserve the existing TTL when overwriting a key. Mutually exclusive with `ttl` / `X-KV-TTL`."),
        ("X-KV-Return-Old" = Option<String>, Header, description = "Set to any value to atomically swap and return the previous value. Returns 200 with the old bytes (or 204 if the key did not exist). Mutually exclusive with conditional writes and `X-KV-KeepTTL`."),
        ("X-KV-Metadata" = Option<String>, Header, description = "Arbitrary JSON value to attach to the key (max 64 KiB serialized). Readable on GET via `X-KV-Metadata`."),
    ),
    request_body(content_type = "application/octet-stream", description = "Raw bytes to store. Empty body is valid."),
    responses(
        (status = 200, description = "Swap succeeded. Body contains the **previous** value (`X-KV-Return-Old` path only).", content_type = "application/octet-stream"),
        (status = 204, description = "Stored. Also returned by `X-KV-Return-Old` when the key did not previously exist.",
            headers(
                ("X-KV-Revision" = u64, description = "New revision after a successful `If-Match` write. Absent for unconditional writes."),
            )
        ),
        (status = 400, body = ErrorResponse, description = "Incompatible options (e.g. `X-KV-KeepTTL` with a TTL option, or `X-KV-Return-Old` with a conditional write)."),
        (status = 409, body = ErrorResponse, description = "Conditional write failed: key already exists (`nx`), does not exist (`xx`), or revision mismatch (`If-Match`)."),
    )
)]
async fn handle_put(
    ns: &str,
    key: &[u8],
    body: Bytes,
    parts: &http::request::Parts,
    store: &ShardStore,
) -> http::Response<HttpBody> {
    let ttl = parse_ttl_from_request(parts);
    let keep_ttl = parts.headers.contains_key("x-kv-keepttl");
    let return_old = parts.headers.contains_key("x-kv-return-old");
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

    if keep_ttl && ttl.is_some() {
        return json_response(
            400,
            &serde_json::json!({
                "error": "invalid_request",
                "message": "x-kv-keepttl cannot be combined with a TTL option"
            }),
        );
    }
    if return_old && (nx || xx || if_match.is_some() || keep_ttl) {
        return json_response(
            400,
            &serde_json::json!({
                "error": "invalid_request",
                "message": "x-kv-return-old cannot be combined with conditional writes or x-kv-keepttl"
            }),
        );
    }

    let opts = SetOptions {
        ttl,
        metadata,
        keep_ttl,
    };

    if return_old {
        return match store.getset(ns, key, body).await {
            Ok(Some(old)) => http::Response::builder()
                .status(200)
                .header("Content-Type", "application/octet-stream")
                .body(HttpBody::fixed_body(Some(old.value)))
                .unwrap_or_else(|_| internal_error("response build failed")),
            Ok(None) => no_content(),
            Err(e) => engine_error_response(e),
        };
    }

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
            Err(e) => engine_error_response(e),
        }
    } else if nx {
        match store.setnx(ns, key, body, opts).await {
            Ok(true) => no_content(),
            Ok(false) => json_response(
                409,
                &serde_json::json!({ "error": "conflict", "message": "key already exists" }),
            ),
            Err(e) => engine_error_response(e),
        }
    } else if xx {
        match store.setxx(ns, key, body, opts).await {
            Ok(true) => no_content(),
            Ok(false) => json_response(
                409,
                &serde_json::json!({ "error": "conflict", "message": "key does not exist" }),
            ),
            Err(e) => engine_error_response(e),
        }
    } else {
        match store.set(ns, key, body, opts).await {
            Ok(()) => no_content(),
            Err(e) => engine_error_response(e),
        }
    }
}

/// Delete `key` from the store. Idempotent — returns 204 whether or not the key existed.
/// Supply `If-Match: <rev>` for a conditional delete: returns 409 if the stored revision
/// does not match, leaving the key untouched.
#[utoipa::path(
    delete,
    path = "/v1/kv/{key}",
    operation_id = "delete_value",
    tag = "kv",
    params(
        ("key" = String, Path, description = "Key to delete. Percent-encoded."),
        ("ns" = Option<u8>, Query, description = "Namespace (0–15). Defaults to 0."),
        ("If-Match" = Option<u64>, Header, description = "Delete only if the stored revision equals this value. Returns 409 on mismatch, leaving the key untouched."),
    ),
    responses(
        (status = 204, description = "Deleted. Also returned when the key did not exist (idempotent)."),
        (status = 409, body = ErrorResponse, description = "Revision mismatch — `If-Match` was supplied but the stored revision differs."),
    )
)]
async fn handle_delete(
    ns: &str,
    key: &[u8],
    parts: &http::request::Parts,
    store: &ShardStore,
) -> http::Response<HttpBody> {
    let if_match: Option<u64> = parts
        .headers
        .get("if-match")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse().ok());

    if let Some(expected_rev) = if_match {
        return match store.delrev(ns, key, expected_rev).await {
            Ok(Some(())) => no_content(),
            Ok(None) => json_response(
                409,
                &serde_json::json!({ "error": "conflict", "message": "revision mismatch" }),
            ),
            Err(e) => engine_error_response(e),
        };
    }

    match store.del(ns, &[key]).await {
        Ok(_) => no_content(),
        Err(e) => engine_error_response(e),
    }
}

/// Atomically increment a 64-bit signed integer counter stored at `key` by `delta`
/// (default 1). If the key does not exist it is initialised to 0 before incrementing.
/// Returns the new value. Returns 400 if the stored bytes cannot be interpreted as a
/// decimal integer, or if the operation would overflow i64.
#[utoipa::path(
    post,
    path = "/v1/kv/{key}/incr",
    operation_id = "increment_value",
    tag = "kv",
    params(
        ("key" = String, Path, description = "Counter key. Created as 0 if it does not exist."),
        ("ns" = Option<u8>, Query, description = "Namespace (0–15). Defaults to 0."),
        ("delta" = Option<i64>, Query, description = "Amount to add. May be negative for decrement. Defaults to 1."),
    ),
    responses(
        (status = 200, body = IncrResponse, description = "Increment applied. Body contains the new counter value."),
        (status = 400, body = ErrorResponse, description = "Stored value is not a valid decimal integer, or the result would overflow i64."),
    )
)]
async fn handle_incr(
    ns: &str,
    key: &[u8],
    delta: i64,
    store: &ShardStore,
) -> http::Response<HttpBody> {
    match store.incr(ns, key, delta).await {
        Ok(n) => json_response(200, &serde_json::json!({ "value": n })),
        Err(e) => engine_error_response(e),
    }
}

/// Multi-shard cursor prefix used in HTTP LIST responses. Format: `\x02 + shard_byte + per_shard_cursor`.
/// Distinct from the engine's `\x01` continuation cursor so we can detect which format is in use.
const LIST_CURSOR_PREFIX: u8 = 0x02;

/// List keys in a namespace, optionally filtered by prefix. Results are returned in
/// lexicographic order. Pagination is cursor-based: when `complete` is `false`, pass the
/// returned `cursor` value as the `cursor` query parameter on the next request to fetch
/// the subsequent page. Omit `cursor` (or pass `0`) to start from the beginning.
/// Across multiple shards, the cursor encodes per-shard positions so fan-out is handled
/// transparently by the server.
#[utoipa::path(
    get,
    path = "/v1/kv",
    operation_id = "list_keys",
    tag = "kv",
    params(
        ("ns" = Option<u8>, Query, description = "Namespace (0–15). Defaults to 0."),
        ("prefix" = Option<String>, Query, description = "Return only keys that begin with this string. Percent-encoded. Omit to return all keys."),
        ("cursor" = Option<String>, Query, description = "Opaque pagination cursor from a previous `ListResponse`. Omit or pass `0` to start from the beginning."),
        ("limit" = Option<u64>, Query, description = "Maximum keys to return per page (1–1000). Defaults to 100."),
    ),
    responses(
        (status = 200, body = ListResponse, description = "Page of matching keys in lexicographic order."),
    )
)]
#[allow(clippy::too_many_arguments)]
async fn handle_list(
    ns: &str,
    query: &str,
    store: &ShardStore,
    shard_idx: usize,
    n_shards: usize,
    cross_shard_txs: &ShardSenders,
    cross_shard_wakeups: &[StdUnixStream],
) -> http::Response<HttpBody> {
    let prefix_pattern: Option<Vec<u8>> = query_param(query, "prefix").map(|raw| {
        let mut p = percent_decode(raw);
        p.push(b'*');
        p
    });
    let limit: u64 = query_param(query, "limit")
        .and_then(|s| s.parse().ok())
        .unwrap_or(100)
        .min(1000);

    // Single-shard fast path: no fan-out needed.
    if n_shards <= 1 {
        // Cursor is a base64-encoded key or "0" for start. Wrap in the \x01 prefix
        // that store::scan uses to distinguish continuation cursors from the sentinel.
        let cursor_bytes: Vec<u8> = match query_param(query, "cursor") {
            None | Some("0") => b"0".to_vec(),
            Some(s) => match base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(s) {
                Ok(key) => {
                    let mut v = Vec::with_capacity(1 + key.len());
                    v.push(0x01u8);
                    v.extend_from_slice(&key);
                    v
                }
                Err(_) => b"0".to_vec(),
            },
        };
        return match store
            .scan(ns, &cursor_bytes, prefix_pattern.as_deref(), limit)
            .await
        {
            Err(e) => engine_error_response(e),
            Ok(page) => build_list_response_single(page),
        };
    }

    // Multi-shard path.
    // Cursor blob: \x02 + shard_byte + per_shard_cursor (raw engine cursor).
    let (target_shard, per_shard_cursor): (usize, Bytes) = {
        let raw = query_param(query, "cursor").unwrap_or("0");
        if raw == "0" {
            (0, Bytes::from_static(b"0"))
        } else {
            match base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(raw) {
                Ok(blob) if blob.first() == Some(&LIST_CURSOR_PREFIX) && blob.len() >= 2 => {
                    (blob[1] as usize, Bytes::copy_from_slice(&blob[2..]))
                }
                _ => (0, Bytes::from_static(b"0")), // bad cursor → restart
            }
        }
    };

    // Clamp to valid shard range.
    let target_shard = target_shard.min(n_shards - 1);

    // Fetch one page from the target shard.
    let page_result: Result<beyond_kv_engine::types::ScanPage, String> =
        if target_shard == shard_idx {
            store
                .scan(ns, &per_shard_cursor, prefix_pattern.as_deref(), limit)
                .await
                .map_err(|e| e.to_string())
        } else {
            let (reply_tx, reply_rx) = futures_channel::oneshot::channel();
            let req = CrossShardRequest::Scan {
                ns: ns.to_string(),
                cursor: per_shard_cursor,
                pattern: prefix_pattern.as_deref().map(Bytes::copy_from_slice),
                count: limit,
                reply: reply_tx,
            };
            if cross_shard_txs[target_shard].clone().try_send(req).is_err() {
                return json_response(
                    503,
                    &serde_json::json!({"error":"shard_unavailable","message":"shard inbox full"}),
                );
            }
            let _ = (&cross_shard_wakeups[target_shard]).write_all(&[1u8]);
            match reply_rx.await {
                Ok(r) => r,
                Err(e) => Err(e.to_string()),
            }
        };

    let page = match page_result {
        Ok(p) => p,
        Err(e) => return internal_error(&e),
    };

    // Build the outgoing cursor and determine completeness.
    let shard_done = page.next_cursor == b"0".as_ref();
    let (complete, cursor_out) = if shard_done {
        let next_shard = target_shard + 1;
        if next_shard >= n_shards {
            (true, None)
        } else {
            // Advance to the next shard from its beginning.
            let mut blob = vec![LIST_CURSOR_PREFIX, next_shard as u8];
            blob.extend_from_slice(b"0");
            (
                false,
                Some(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&blob)),
            )
        }
    } else {
        let mut blob = vec![LIST_CURSOR_PREFIX, target_shard as u8];
        blob.extend_from_slice(&page.next_cursor);
        (
            false,
            Some(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&blob)),
        )
    };

    let keys: Vec<serde_json::Value> = page
        .keys
        .iter()
        .map(|k| {
            let name = String::from_utf8(k.to_vec())
                .unwrap_or_else(|e| percent_encode_bytes(e.as_bytes()));
            serde_json::json!({ "name": name })
        })
        .collect();

    let mut body = serde_json::json!({ "keys": keys, "complete": complete });
    if let Some(cursor) = cursor_out {
        body["cursor"] = serde_json::Value::String(cursor);
    }
    json_response(200, &body)
}

fn build_list_response_single(page: beyond_kv_engine::types::ScanPage) -> http::Response<HttpBody> {
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
        // Cursor is b"\x01" + last_key — strip prefix, base64-encode the key.
        let key = page
            .next_cursor
            .strip_prefix(b"\x01")
            .unwrap_or(&page.next_cursor);
        body["cursor"] =
            serde_json::Value::String(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(key));
    }
    json_response(200, &body)
}

// ── SSE watch ────────────────────────────────────────────────────────────────

struct WatchParams {
    ns: String,
    key: Vec<u8>,
    is_prefix: bool,
    since: Option<u64>,
}

fn parse_watch_params(path: &str, query: &str) -> Option<WatchParams> {
    // /v1/watch/{key} or /v1/watch (with ?prefix=)
    let ns = parse_ns(query).ok()?.to_string();

    if let Some(key_encoded) = path.strip_prefix("/v1/watch/") {
        if key_encoded.is_empty() {
            return None;
        }
        return Some(WatchParams {
            ns,
            key: percent_decode(key_encoded),
            is_prefix: false,
            since: query_param(query, "since").and_then(|s| s.parse().ok()),
        });
    }

    if path == "/v1/watch" {
        let prefix_raw = query_param(query, "prefix")?;
        return Some(WatchParams {
            ns,
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
    shard_idx: usize,
    n_shards: usize,
    cross_shard_txs: &ShardSenders,
    cross_shard_wakeups: &[StdUnixStream],
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

    let since_rev = params.since.unwrap_or(0);
    let mut rx_stream: SelectAll<Receiver<WatchEvent>> = SelectAll::new();

    if params.is_prefix && n_shards > 1 {
        // Prefix watch must subscribe on every shard.
        for shard in 0..n_shards {
            let result = if shard == shard_idx {
                store
                    .watch_subscribe(&params.ns, KeyFilter::Prefix(&params.key), since_rev)
                    .await
                    .map_err(|e| e.to_string())
            } else {
                let (reply_tx, reply_rx) = futures_channel::oneshot::channel();
                let req = CrossShardRequest::WatchSubscribe {
                    ns: params.ns.clone(),
                    filter: OwnedKeyFilter::Prefix(Bytes::copy_from_slice(&params.key)),
                    since: since_rev,
                    reply: reply_tx,
                };
                if cross_shard_txs[shard].clone().try_send(req).is_err() {
                    let msg = b"data: {\"type\":\"error\",\"message\":\"shard inbox full\"}\n\n";
                    let _ = w.write_all(msg.to_vec()).await;
                    return;
                }
                let _ = (&cross_shard_wakeups[shard]).write_all(&[1u8]);
                match reply_rx.await {
                    Ok(r) => r,
                    Err(e) => Err(e.to_string()),
                }
            };
            match result {
                Ok((initial, rx)) => {
                    for event in &initial {
                        let data = format!("data: {}\n\n", sse_event_json(event));
                        if w.write_all(data.into_bytes()).await.0.is_err() {
                            return;
                        }
                    }
                    rx_stream.push(rx);
                }
                Err(e) => {
                    let msg = format!(
                        "data: {{\"type\":\"error\",\"message\":{}}}\n\n",
                        serde_json::json!(e)
                    );
                    let _ = w.write_all(msg.into_bytes()).await;
                    return;
                }
            }
        }
    } else {
        // Exact-key watch: the connection is already routed to the key's shard.
        let filter = if params.is_prefix {
            KeyFilter::Prefix(&params.key)
        } else {
            KeyFilter::Exact(&params.key)
        };
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
            if w.write_all(data.into_bytes()).await.0.is_err() {
                return;
            }
        }
        rx_stream.push(rx);
    }

    let (res, _) = w
        .write_all(b"data: {\"type\":\"ready\",\"revision\":0}\n\n".to_vec())
        .await;
    if res.is_err() {
        return;
    }

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
            let value_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(value);
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

/// Maps the `?ns=N` query param (numeric u8 0–15) to a namespace name.
static NS_NAMES: [&str; 16] = [
    "default", "db1", "db2", "db3", "db4", "db5", "db6", "db7", "db8", "db9", "db10", "db11",
    "db12", "db13", "db14", "db15",
];

fn parse_ns(query: &str) -> Result<&'static str, http::Response<HttpBody>> {
    let n = query_param(query, "ns")
        .and_then(|s| s.parse::<u8>().ok())
        .unwrap_or(0);
    NS_NAMES.get(n as usize).copied().ok_or_else(|| {
        json_response(
            400,
            &serde_json::json!({
                "error": "invalid_namespace",
                "message": "ns must be 0-15"
            }),
        )
    })
}

// ── OpenAPI schemas ──────────────────────────────────────────────────────────

#[derive(serde::Serialize, utoipa::ToSchema)]
#[allow(dead_code)]
struct IncrResponse {
    /// New counter value after applying the delta.
    value: i64,
}

#[derive(serde::Serialize, utoipa::ToSchema)]
#[allow(dead_code)]
struct KeyItem {
    /// Key name (percent-decoded).
    name: String,
}

#[derive(serde::Serialize, utoipa::ToSchema)]
#[allow(dead_code)]
struct ListResponse {
    /// Matching keys in lexicographic order.
    keys: Vec<KeyItem>,
    /// `true` when all matching keys have been returned and there are no further pages.
    /// `false` means a `cursor` is present and more results may be fetched.
    complete: bool,
    /// Opaque pagination cursor. Pass as the `cursor` query parameter on the next request
    /// to fetch the subsequent page. Absent when `complete` is `true`.
    #[serde(skip_serializing_if = "Option::is_none")]
    cursor: Option<String>,
}

#[derive(serde::Serialize, utoipa::ToSchema)]
#[allow(dead_code)]
struct ErrorResponse {
    /// Machine-readable error code (e.g. `not_found`, `conflict`, `invalid_request`,
    /// `invalid_namespace`, `engine_error`).
    error: String,
    /// Human-readable description of what went wrong.
    message: String,
}

#[derive(utoipa::OpenApi)]
#[openapi(
    info(
        title = "beyond/kv",
        version = "1",
        description = "Low-latency key-value store with namespaces, TTL, revision-based \
            conditional writes, atomic increment, and cursor-paginated key listing. \
            All keys and values are raw bytes; values are transmitted as \
            `application/octet-stream`. Namespaces (0–15) are independent keyspaces \
            sharing the same physical store."
    ),
    paths(handle_get, handle_put, handle_delete, handle_incr, handle_list),
    components(schemas(IncrResponse, KeyItem, ListResponse, ErrorResponse)),
    tags(
        (name = "kv", description = "Key-value operations: get, put, delete, increment, and list."),
    )
)]
pub struct ApiDoc;

fn openapi_json() -> &'static Bytes {
    static SPEC: std::sync::OnceLock<Bytes> = std::sync::OnceLock::new();
    SPEC.get_or_init(|| {
        use utoipa::OpenApi as _;
        Bytes::from(ApiDoc::openapi().to_json().expect("valid OpenAPI spec"))
    })
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

fn engine_error_response(e: EngineError) -> http::Response<HttpBody> {
    match e {
        EngineError::InvalidNamespace { .. } => json_response(
            400,
            &serde_json::json!({ "error": "invalid_namespace", "message": e.to_string() }),
        ),
        EngineError::CapacityExceeded { .. } => json_response(
            400,
            &serde_json::json!({ "error": "capacity_exceeded", "message": e.to_string() }),
        ),
        EngineError::InvalidInput { .. } => json_response(
            400,
            &serde_json::json!({ "error": "invalid_value", "message": e.to_string() }),
        ),
        EngineError::Conflict { .. } => json_response(
            409,
            &serde_json::json!({ "error": "conflict", "message": e.to_string() }),
        ),
        _ => internal_error(&e.to_string()),
    }
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

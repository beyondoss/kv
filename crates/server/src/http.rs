use std::io::Write as _;
use std::net::SocketAddr;
use std::os::unix::net::UnixStream as StdUnixStream;

/// Non-blocking wakeup: write one byte to the target shard's pipe so its
/// io_uring sleep is interrupted. WouldBlock means a wakeup is already queued.
#[inline]
fn poke_wakeup(wakeups: &[StdUnixStream], shard: usize) {
    if let Some(w) = wakeups.get(shard) {
        match (&*w).write(&[1u8]) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(e) => tracing::warn!(shard, error = %e, "cross-shard wakeup write failed"),
        }
    }
}
use std::rc::Rc;
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

use crate::cross_shard::{CrossShardRequest, OwnedKeyFilter, ShardSenders};
use crate::metrics::Metrics;
use crate::routing::shard_for_key;

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
    metrics: Arc<Metrics>,
) {
    crate::serve_loop(rx, wakeup_read, max_conns, "HTTP", |s, _peer, guard| {
        let store = store.clone();
        let txs = cross_shard_txs.clone();
        let wakeups = cross_shard_wakeups.clone();
        let metrics = metrics.clone();
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
                metrics,
            )
            .await;
        });
    })
    .await;
}

#[allow(clippy::too_many_arguments)]
async fn handle_conn(
    stream: TcpStream,
    store: Rc<ShardStore>,
    idle_timeout: Duration,
    max_value_bytes: usize,
    shard_idx: usize,
    n_shards: usize,
    cross_shard_txs: ShardSenders,
    cross_shard_wakeups: Arc<[StdUnixStream]>,
    metrics: Arc<Metrics>,
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

        let op = http_op(&parts.method, parts.uri.path());
        let start = Instant::now();
        let response = route(
            &parts,
            body_bytes,
            &store,
            shard_idx,
            n_shards,
            &cross_shard_txs,
            &cross_shard_wakeups,
            metrics.as_ref(),
        )
        .await;
        let label = match response.status().as_u16() {
            200..=299 => "ok",
            404 => "nil",
            _ => "error",
        };
        metrics.ops_total.with_label_values(&[op, label]).inc();
        metrics.op_duration_seconds.with_label_values(&[op]).observe(start.elapsed().as_secs_f64());

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
    metrics: &Metrics,
) -> http::Response<HttpBody> {
    let path = parts.uri.path();
    let method = &parts.method;
    let query = parts.uri.query().unwrap_or("");

    if path == "/healthz" {
        return ok_text("ok");
    }

    if path == "/metrics" {
        return http::Response::builder()
            .status(200)
            .header("Content-Type", "text/plain; version=0.0.4")
            .body(HttpBody::fixed_body(Some(Bytes::from(metrics.encode()))))
            .unwrap_or_else(|_| fallback_500());
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

    if path == "/v1/admin/compact" {
        if *method != http::Method::POST {
            return method_not_allowed();
        }
        let ns = match parse_ns(query) {
            Ok(n) => n,
            Err(r) => return r,
        };
        return handle_compact(ns, store).await;
    }

    // POST /v1/kv/batch — bulk mixed operations.
    if path == "/v1/kv/batch" {
        if *method != http::Method::POST {
            return method_not_allowed();
        }
        let ns = match parse_ns(query) {
            Ok(n) => n,
            Err(r) => return r,
        };
        return handle_batch(
            ns,
            body,
            store,
            shard_idx,
            n_shards,
            cross_shard_txs,
            cross_shard_wakeups,
        )
        .await;
    }

    // GET/DELETE /v1/kv → list or flush (no key in path).
    if path == "/v1/kv" || path == "/v1/kv/" {
        let ns = match parse_ns(query) {
            Ok(n) => n,
            Err(r) => return r,
        };
        return match *method {
            http::Method::GET => {
                if query_param(query, "count").is_some() {
                    handle_dbsize(
                        ns,
                        store,
                        shard_idx,
                        n_shards,
                        cross_shard_txs,
                        cross_shard_wakeups,
                    )
                    .await
                } else {
                    handle_list(
                        ns,
                        query,
                        store,
                        shard_idx,
                        n_shards,
                        cross_shard_txs,
                        cross_shard_wakeups,
                    )
                    .await
                }
            }
            http::Method::DELETE => {
                handle_flushdb(
                    ns,
                    store,
                    shard_idx,
                    n_shards,
                    cross_shard_txs,
                    cross_shard_wakeups,
                )
                .await
            }
            _ => method_not_allowed(),
        };
    }

    // /v1/kv/{key}[/incr]
    if let Some(rest) = path.strip_prefix("/v1/kv/") {
        let ns = match parse_ns(query) {
            Ok(n) => n,
            Err(r) => return r,
        };
        // /v1/kv/{key}/incr — only POST is allowed.
        if let Some(key_encoded) = rest.strip_suffix("/incr") {
            if *method != http::Method::POST {
                return method_not_allowed();
            }
            let key = percent_decode(key_encoded);
            if key.is_empty() {
                return not_found_json("not_found", "endpoint not found");
            }
            let delta = query_param(query, "delta")
                .and_then(|s| s.parse::<i64>().ok())
                .unwrap_or(1);
            return handle_incr(ns, &key, delta, store).await;
        }
        let key = percent_decode(rest);
        if key.is_empty() {
            return not_found_json("not_found", "endpoint not found");
        }
        return match *method {
            http::Method::GET => handle_get(ns, &key, store).await,
            http::Method::HEAD => handle_head(ns, &key, store).await,
            http::Method::PUT => handle_put(ns, &key, body, parts, store).await,
            http::Method::DELETE => handle_delete(ns, &key, parts, store).await,
            http::Method::PATCH => handle_patch(ns, &key, parts, store).await,
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
                ("X-KV-TTL-MS" = u64, description = "Remaining TTL in milliseconds. Absent if the key has no expiry."),
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
            let ttl_remaining = entry
                .expires_at
                .map(|t| t.saturating_duration_since(std::time::Instant::now()));
            let mut builder = http::Response::builder().status(200);
            if let Some(rem) = ttl_remaining {
                builder = builder.header("X-KV-TTL", rem.as_secs().to_string());
                builder = builder.header("X-KV-TTL-MS", (rem.as_millis() as u64).to_string());
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

/// Check whether `key` exists without fetching its value. Returns key metadata in response
/// headers: `X-KV-TTL` (remaining seconds, absent if no expiry), `X-KV-Revision` (absent
/// if never written via `If-Match`), and `X-KV-Metadata` (absent if none stored).
/// Returns 200 if the key exists, 404 if it does not.
#[utoipa::path(
    head,
    path = "/v1/kv/{key}",
    operation_id = "head_value",
    tag = "kv",
    params(
        ("key" = String, Path, description = "Key to check. Percent-encoded."),
        ("ns" = Option<u8>, Query, description = "Namespace (0–15). Defaults to 0."),
    ),
    responses(
        (status = 200, description = "Key exists. Metadata in headers; no body.",
            headers(
                ("X-KV-TTL" = u64, description = "Remaining TTL in whole seconds. Absent if no expiry."),
                ("X-KV-TTL-MS" = u64, description = "Remaining TTL in milliseconds. Absent if no expiry."),
                ("X-KV-Revision" = u64, description = "Current revision counter. Absent if never written via `If-Match`."),
                ("X-KV-Metadata" = String, description = "JSON metadata blob. Absent if none was stored."),
            )
        ),
        (status = 404, description = "Key does not exist."),
    )
)]
async fn handle_head(ns: &str, key: &[u8], store: &ShardStore) -> http::Response<HttpBody> {
    match store.get(ns, key).await {
        Err(e) => engine_error_response(e),
        Ok(None) => http::Response::builder()
            .status(404)
            .body(HttpBody::fixed_body(None))
            .unwrap_or_else(|_| fallback_500()),
        Ok(Some(entry)) => {
            let ttl_remaining = entry
                .expires_at
                .map(|t| t.saturating_duration_since(std::time::Instant::now()));
            let mut builder = http::Response::builder().status(200);
            if let Some(rem) = ttl_remaining {
                builder = builder.header("X-KV-TTL", rem.as_secs().to_string());
                builder = builder.header("X-KV-TTL-MS", (rem.as_millis() as u64).to_string());
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
                .body(HttpBody::fixed_body(None))
                .unwrap_or_else(|_| internal_error("response build failed"))
        }
    }
}

/// Modify the TTL of `key` without changing its value. Exactly one TTL option must be
/// supplied: `ttl` (seconds from now), `ttl_ms` (millis from now), `ttl_at` (absolute
/// unix seconds), `ttl_at_ms` (absolute unix millis), or `persist=1` to remove the TTL.
///
/// Set `X-KV-Return-Value: 1` to atomically fetch the current value in the same operation
/// (GETEX semantics) — the response body is the value bytes and status is 200.
/// Without that header the response is 204.
///
/// Returns 404 if the key does not exist.
#[utoipa::path(
    patch,
    path = "/v1/kv/{key}",
    operation_id = "patch_ttl",
    tag = "kv",
    params(
        ("key" = String, Path, description = "Key to update. Percent-encoded."),
        ("ns" = Option<u8>, Query, description = "Namespace (0–15). Defaults to 0."),
        ("ttl" = Option<u64>, Query, description = "New TTL in seconds from now."),
        ("ttl_ms" = Option<u64>, Query, description = "New TTL in milliseconds from now."),
        ("ttl_at" = Option<u64>, Query, description = "Absolute expiry as a Unix timestamp in seconds."),
        ("ttl_at_ms" = Option<u64>, Query, description = "Absolute expiry as a Unix timestamp in milliseconds."),
        ("persist" = Option<u8>, Query, description = "Set `persist=1` to remove the TTL entirely."),
        ("X-KV-Return-Value" = Option<String>, Header, description = "Set to any value to return the current value bytes in the response body (GETEX semantics)."),
    ),
    responses(
        (status = 200, description = "TTL updated. Body contains the current value (`X-KV-Return-Value` path only).", content_type = "application/octet-stream",
            headers(
                ("X-KV-TTL" = u64, description = "New remaining TTL in seconds. Absent if persist was requested."),
                ("X-KV-TTL-MS" = u64, description = "New remaining TTL in milliseconds. Absent if persist was requested."),
                ("X-KV-Revision" = u64, description = "Current revision. Absent if key was never written via `If-Match`."),
                ("X-KV-Metadata" = String, description = "JSON metadata. Absent if none stored."),
            )
        ),
        (status = 204, description = "TTL updated. No body."),
        (status = 400, body = ErrorResponse, description = "No TTL option supplied, or multiple supplied."),
        (status = 404, description = "Key does not exist."),
    )
)]
async fn handle_patch(
    ns: &str,
    key: &[u8],
    parts: &http::request::Parts,
    store: &ShardStore,
) -> http::Response<HttpBody> {
    let query = parts.uri.query().unwrap_or("");
    let return_value = parts.headers.contains_key("x-kv-return-value");

    let getex_op = parse_ttl_op(query);

    if return_value {
        // GETEX: atomically get the value and optionally update TTL.
        match store.getex(ns, key, getex_op).await {
            Err(e) => engine_error_response(e),
            Ok(None) => http::Response::builder()
                .status(404)
                .body(HttpBody::fixed_body(None))
                .unwrap_or_else(|_| fallback_500()),
            Ok(Some(entry)) => {
                let ttl_remaining = entry
                    .expires_at
                    .map(|t| t.saturating_duration_since(std::time::Instant::now()));
                let mut builder = http::Response::builder().status(200);
                if let Some(rem) = ttl_remaining {
                    builder = builder.header("X-KV-TTL", rem.as_secs().to_string());
                    builder = builder.header("X-KV-TTL-MS", (rem.as_millis() as u64).to_string());
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
    } else {
        // TTL-only update: no value read.
        match getex_op {
            None => err(
                400,
                "invalid_request",
                "supply exactly one of: ttl, ttl_ms, ttl_at, ttl_at_ms, persist=1",
            ),
            Some(beyond_kv_engine::types::GetExOp::Persist) => match store.persist(ns, key).await {
                Ok(true) => no_content(),
                Ok(false) => http::Response::builder()
                    .status(404)
                    .body(HttpBody::fixed_body(None))
                    .unwrap_or_else(|_| fallback_500()),
                Err(e) => engine_error_response(e),
            },
            Some(beyond_kv_engine::types::GetExOp::SetTtl(dur)) => {
                match store.expire(ns, key, dur).await {
                    Ok(true) => no_content(),
                    Ok(false) => http::Response::builder()
                        .status(404)
                        .body(HttpBody::fixed_body(None))
                        .unwrap_or_else(|_| fallback_500()),
                    Err(e) => engine_error_response(e),
                }
            }
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
        return err(
            400,
            "invalid_request",
            "x-kv-keepttl cannot be combined with a TTL option",
        );
    }
    if return_old && (nx || xx || if_match.is_some() || keep_ttl) {
        return err(
            400,
            "invalid_request",
            "x-kv-return-old cannot be combined with conditional writes or x-kv-keepttl",
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
            Ok(None) => err(409, "conflict", "revision mismatch"),
            Err(e) => engine_error_response(e),
        }
    } else if nx {
        match store.setnx(ns, key, body, opts).await {
            Ok(true) => no_content(),
            Ok(false) => err(409, "conflict", "key already exists"),
            Err(e) => engine_error_response(e),
        }
    } else if xx {
        match store.setxx(ns, key, body, opts).await {
            Ok(true) => no_content(),
            Ok(false) => err(409, "conflict", "key does not exist"),
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
///
/// **Conditional delete** — supply `If-Match: <rev>`: returns 409 if the stored revision
/// does not match, leaving the key untouched. Mutually exclusive with `X-KV-Return-Old`.
///
/// **Atomic read-then-delete** — set `X-KV-Return-Old: 1` to atomically delete the key
/// and return its previous value (200) in a single operation. Returns 204 when the key
/// did not exist. Mutually exclusive with `If-Match`.
#[utoipa::path(
    delete,
    path = "/v1/kv/{key}",
    operation_id = "delete_value",
    tag = "kv",
    params(
        ("key" = String, Path, description = "Key to delete. Percent-encoded."),
        ("ns" = Option<u8>, Query, description = "Namespace (0–15). Defaults to 0."),
        ("If-Match" = Option<u64>, Header, description = "Delete only if the stored revision equals this value. Returns 409 on mismatch, leaving the key untouched. Mutually exclusive with `X-KV-Return-Old`."),
        ("X-KV-Return-Old" = Option<String>, Header, description = "Set to any value to atomically delete and return the previous value (200). Returns 204 when the key did not exist. Mutually exclusive with `If-Match`."),
    ),
    responses(
        (status = 200, description = "Key deleted. Body contains the previous value (`X-KV-Return-Old` path only).", content_type = "application/octet-stream",
            headers(
                ("X-KV-TTL" = u64, description = "Remaining TTL in whole seconds the deleted entry had. Absent if it had no expiry."),
                ("X-KV-TTL-MS" = u64, description = "Remaining TTL in milliseconds the deleted entry had. Absent if it had no expiry."),
                ("X-KV-Revision" = u64, description = "Revision of the deleted entry."),
                ("X-KV-Metadata" = String, description = "JSON metadata blob the deleted entry had. Absent if none was stored."),
            )
        ),
        (status = 204, description = "Deleted (or key did not exist — idempotent)."),
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
    let return_old = parts.headers.contains_key("x-kv-return-old");

    if let Some(expected_rev) = if_match {
        return match store.delrev(ns, key, expected_rev).await {
            Ok(Some(())) => no_content(),
            Ok(None) => err(409, "conflict", "revision mismatch"),
            Err(e) => engine_error_response(e),
        };
    }

    if return_old {
        return match store.getdel(ns, key).await {
            Err(e) => engine_error_response(e),
            Ok(None) => no_content(),
            Ok(Some(entry)) => {
                let ttl_remaining = entry
                    .expires_at
                    .map(|t| t.saturating_duration_since(std::time::Instant::now()));
                let mut builder = http::Response::builder().status(200);
                if let Some(rem) = ttl_remaining {
                    builder = builder.header("X-KV-TTL", rem.as_secs().to_string());
                    builder = builder.header("X-KV-TTL-MS", (rem.as_millis() as u64).to_string());
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

#[allow(clippy::too_many_arguments)]
async fn handle_dbsize(
    ns: &str,
    store: &ShardStore,
    shard_idx: usize,
    n_shards: usize,
    cross_shard_txs: &ShardSenders,
    cross_shard_wakeups: &[StdUnixStream],
) -> http::Response<HttpBody> {
    if n_shards <= 1 {
        return match store.db_size(ns).await {
            Ok(count) => json_response(200, &serde_json::json!({ "count": count })),
            Err(e) => engine_error_response(e),
        };
    }

    let mut total: u64 = 0;
    let mut reply_rxs = Vec::with_capacity(n_shards - 1);

    for shard in 0..n_shards {
        if shard == shard_idx {
            match store.db_size(ns).await {
                Ok(n) => total += n,
                Err(e) => return engine_error_response(e),
            }
        } else {
            let (reply_tx, reply_rx) = futures_channel::oneshot::channel();
            let req = CrossShardRequest::DbSize {
                ns: ns.to_string(),
                reply: reply_tx,
            };
            if cross_shard_txs[shard].clone().try_send(req).is_err() {
                return err(503, "shard_unavailable", "shard inbox full");
            }
            poke_wakeup(cross_shard_wakeups, shard);
            reply_rxs.push(reply_rx);
        }
    }

    for rx in reply_rxs {
        match rx.await {
            Ok(Ok(n)) => total += n,
            Ok(Err(e)) => return internal_error(&e),
            Err(e) => return internal_error(&e.to_string()),
        }
    }

    json_response(200, &serde_json::json!({ "count": total }))
}

/// Delete all keys in the namespace. Returns 204 even if the namespace was already empty.
#[utoipa::path(
    delete,
    path = "/v1/kv",
    operation_id = "flush_namespace",
    tag = "kv",
    params(
        ("ns" = Option<u8>, Query, description = "Namespace (0–15). Defaults to 0."),
    ),
    responses(
        (status = 204, description = "All keys deleted (idempotent)."),
    )
)]
#[allow(clippy::too_many_arguments)]
async fn handle_flushdb(
    ns: &str,
    store: &ShardStore,
    shard_idx: usize,
    n_shards: usize,
    cross_shard_txs: &ShardSenders,
    cross_shard_wakeups: &[StdUnixStream],
) -> http::Response<HttpBody> {
    if n_shards <= 1 {
        return match store.flush_db(ns).await {
            Ok(()) => no_content(),
            Err(e) => engine_error_response(e),
        };
    }

    let mut reply_rxs = Vec::with_capacity(n_shards - 1);

    for shard in 0..n_shards {
        if shard == shard_idx {
            if let Err(e) = store.flush_db(ns).await {
                return engine_error_response(e);
            }
        } else {
            let (reply_tx, reply_rx) = futures_channel::oneshot::channel();
            let req = CrossShardRequest::FlushDb {
                ns: ns.to_string(),
                reply: reply_tx,
            };
            if cross_shard_txs[shard].clone().try_send(req).is_err() {
                return err(503, "shard_unavailable", "shard inbox full");
            }
            poke_wakeup(cross_shard_wakeups, shard);
            reply_rxs.push(reply_rx);
        }
    }

    for rx in reply_rxs {
        match rx.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return internal_error(&e),
            Err(e) => return internal_error(&e.to_string()),
        }
    }

    no_content()
}

/// Trigger a background log compaction (equivalent to BGREWRITEAOF). Returns immediately;
/// compaction runs asynchronously.
#[utoipa::path(
    post,
    path = "/v1/admin/compact",
    operation_id = "compact",
    tag = "admin",
    params(
        ("ns" = Option<u8>, Query, description = "Namespace (0–15). Defaults to 0."),
    ),
    responses(
        (status = 204, description = "Compaction started in the background."),
    )
)]
async fn handle_compact(ns: &str, store: &ShardStore) -> http::Response<HttpBody> {
    match store.reclaim(ns).await {
        Ok(()) => no_content(),
        Err(e) => engine_error_response(e),
    }
}

/// Execute a batch of mixed key-value operations in a single round-trip. Operations are
/// executed in order and results are returned in the same order. There is no cross-operation
/// atomicity guarantee — each operation is individually atomic.
///
/// Supported operations:
/// - `get`: returns `{"value":"<base64url>","revision":N,"ttl":N,"metadata":{...}}` or `null` if not found
/// - `set`: stores a value; returns `null`
/// - `delete`: removes a key; returns `null`
/// - `incr`: atomically increments a counter; returns `{"value":N}`
///
/// Set values and get results are base64url-encoded (no padding), matching the SSE watch format.
#[utoipa::path(
    post,
    path = "/v1/kv/batch",
    operation_id = "batch",
    tag = "kv",
    params(
        ("ns" = Option<u8>, Query, description = "Namespace (0–15). Defaults to 0."),
    ),
    request_body(content_type = "application/json", description = "Array of operations."),
    responses(
        (status = 200, description = "Array of results in the same order as the request."),
        (status = 400, body = ErrorResponse, description = "Malformed request body."),
    )
)]
#[allow(clippy::too_many_arguments)]
async fn handle_batch(
    ns: &str,
    body: Bytes,
    store: &ShardStore,
    shard_idx: usize,
    n_shards: usize,
    cross_shard_txs: &ShardSenders,
    cross_shard_wakeups: &[StdUnixStream],
) -> http::Response<HttpBody> {
    #[derive(serde::Deserialize)]
    #[serde(tag = "op", rename_all = "lowercase")]
    enum BatchOp {
        Get {
            key: String,
        },
        Set {
            key: String,
            #[serde(default, deserialize_with = "deser_b64_opt")]
            value: Option<Bytes>,
            /// TTL in whole seconds. Overridden by `ttl_ms` when both are present.
            #[serde(default)]
            ttl: Option<u64>,
            /// TTL in milliseconds. Takes priority over `ttl` when both are present.
            #[serde(default, rename = "ttlMs")]
            ttl_ms: Option<u64>,
            #[serde(default)]
            metadata: Option<serde_json::Value>,
            #[serde(default)]
            nx: bool,
            #[serde(default)]
            xx: bool,
            #[serde(rename = "ifMatch")]
            if_match: Option<u64>,
            /// Preserve the existing TTL when overwriting a key.
            #[serde(default, rename = "keepTtl")]
            keep_ttl: bool,
        },
        Delete {
            key: String,
            #[serde(rename = "ifMatch")]
            if_match: Option<u64>,
            /// Atomically return the previous value before deleting.
            #[serde(default, rename = "returnOld")]
            return_old: bool,
        },
        Incr {
            key: String,
            #[serde(default = "default_delta")]
            delta: i64,
        },
        Exists {
            key: String,
        },
    }

    fn default_delta() -> i64 {
        1
    }

    fn deser_b64_opt<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<Bytes>, D::Error> {
        use serde::Deserialize as _;
        let s = Option::<String>::deserialize(d)?;
        match s {
            None => Ok(None),
            Some(s) => base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(s.as_bytes())
                .map(|b| Some(Bytes::from(b)))
                .map_err(serde::de::Error::custom),
        }
    }

    let ops: Vec<BatchOp> = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return err(400, "invalid_request", e.to_string());
        }
    };

    let mut results: Vec<serde_json::Value> = Vec::with_capacity(ops.len());

    for op in ops {
        let result = match op {
            BatchOp::Get { key } => {
                let raw_key = percent_decode(&key);
                let entry = if n_shards > 1 {
                    let target = shard_for_key(&raw_key, n_shards);
                    if target == shard_idx {
                        store.get(ns, &raw_key).await.map_err(|e| e.to_string())
                    } else {
                        let (reply_tx, reply_rx) = futures_channel::oneshot::channel();
                        let req = CrossShardRequest::MGet {
                            ns: ns.to_string(),
                            keys: vec![(0, Bytes::copy_from_slice(&raw_key))],
                            reply: reply_tx,
                        };
                        if cross_shard_txs[target].clone().try_send(req).is_err() {
                            return err(503, "shard_unavailable", "shard inbox full");
                        }
                        poke_wakeup(cross_shard_wakeups, target);
                        match reply_rx.await {
                            Ok(Ok(mut v)) => Ok(v.pop().and_then(|(_, e)| e)),
                            Ok(Err(e)) => Err(e),
                            Err(e) => Err(e.to_string()),
                        }
                    }
                } else {
                    store.get(ns, &raw_key).await.map_err(|e| e.to_string())
                };

                match entry {
                    Err(e) => return internal_error(&e),
                    Ok(None) => serde_json::Value::Null,
                    Ok(Some(e)) => {
                        let mut obj = serde_json::json!({
                            "value": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&e.value),
                            "revision": e.revision,
                        });
                        if let Some(exp) = e.expires_at {
                            let rem = exp.saturating_duration_since(std::time::Instant::now());
                            obj["ttl"] = rem.as_secs().into();
                            obj["ttl_ms"] = (rem.as_millis() as u64).into();
                        }
                        if let Some(meta) = e.metadata {
                            obj["metadata"] = meta.as_ref().clone();
                        }
                        obj
                    }
                }
            }

            BatchOp::Set {
                key,
                value,
                ttl,
                ttl_ms,
                metadata,
                nx,
                xx,
                if_match,
                keep_ttl,
            } => {
                let raw_key = percent_decode(&key);
                let value_bytes = value.unwrap_or_default();
                let opts = SetOptions {
                    ttl: ttl_ms
                        .map(Duration::from_millis)
                        .or_else(|| ttl.map(Duration::from_secs)),
                    metadata: metadata.map(Arc::new),
                    keep_ttl,
                };

                let outcome: BatchSetOutcome = if n_shards > 1 {
                    let target = shard_for_key(&raw_key, n_shards);
                    if target == shard_idx {
                        batch_set_local(ns, &raw_key, value_bytes, opts, nx, xx, if_match, store)
                            .await
                    } else if let Some(rev) = if_match {
                        let (reply_tx, reply_rx) = futures_channel::oneshot::channel();
                        let req = CrossShardRequest::SetRev {
                            ns: ns.to_string(),
                            key: Bytes::copy_from_slice(&raw_key),
                            value: value_bytes,
                            opts,
                            revision: rev,
                            reply: reply_tx,
                        };
                        if cross_shard_txs[target].clone().try_send(req).is_err() {
                            return err(503, "shard_unavailable", "shard inbox full");
                        }
                        poke_wakeup(cross_shard_wakeups, target);
                        match reply_rx.await {
                            Ok(Ok(Some(_))) => BatchSetOutcome::Ok,
                            Ok(Ok(None)) => BatchSetOutcome::Conflict("revision mismatch"),
                            Ok(Err(e)) => BatchSetOutcome::Err(e),
                            Err(e) => BatchSetOutcome::Err(e.to_string()),
                        }
                    } else if nx {
                        let (reply_tx, reply_rx) = futures_channel::oneshot::channel();
                        let req = CrossShardRequest::SetNx {
                            ns: ns.to_string(),
                            key: Bytes::copy_from_slice(&raw_key),
                            value: value_bytes,
                            opts,
                            reply: reply_tx,
                        };
                        if cross_shard_txs[target].clone().try_send(req).is_err() {
                            return err(503, "shard_unavailable", "shard inbox full");
                        }
                        poke_wakeup(cross_shard_wakeups, target);
                        match reply_rx.await {
                            Ok(Ok(true)) => BatchSetOutcome::Ok,
                            Ok(Ok(false)) => BatchSetOutcome::Conflict("key already exists"),
                            Ok(Err(e)) => BatchSetOutcome::Err(e),
                            Err(e) => BatchSetOutcome::Err(e.to_string()),
                        }
                    } else if xx {
                        let (reply_tx, reply_rx) = futures_channel::oneshot::channel();
                        let req = CrossShardRequest::SetXx {
                            ns: ns.to_string(),
                            key: Bytes::copy_from_slice(&raw_key),
                            value: value_bytes,
                            opts,
                            reply: reply_tx,
                        };
                        if cross_shard_txs[target].clone().try_send(req).is_err() {
                            return err(503, "shard_unavailable", "shard inbox full");
                        }
                        poke_wakeup(cross_shard_wakeups, target);
                        match reply_rx.await {
                            Ok(Ok(true)) => BatchSetOutcome::Ok,
                            Ok(Ok(false)) => BatchSetOutcome::Conflict("key does not exist"),
                            Ok(Err(e)) => BatchSetOutcome::Err(e),
                            Err(e) => BatchSetOutcome::Err(e.to_string()),
                        }
                    } else {
                        let (reply_tx, reply_rx) = futures_channel::oneshot::channel();
                        let req = CrossShardRequest::Set {
                            ns: ns.to_string(),
                            key: Bytes::copy_from_slice(&raw_key),
                            value: value_bytes,
                            opts,
                            reply: reply_tx,
                        };
                        if cross_shard_txs[target].clone().try_send(req).is_err() {
                            return err(503, "shard_unavailable", "shard inbox full");
                        }
                        poke_wakeup(cross_shard_wakeups, target);
                        match reply_rx.await {
                            Ok(Ok(())) => BatchSetOutcome::Ok,
                            Ok(Err(e)) => BatchSetOutcome::Err(e),
                            Err(e) => BatchSetOutcome::Err(e.to_string()),
                        }
                    }
                } else {
                    batch_set_local(ns, &raw_key, value_bytes, opts, nx, xx, if_match, store).await
                };

                match outcome {
                    BatchSetOutcome::Ok => serde_json::Value::Null,
                    BatchSetOutcome::Conflict(msg) => {
                        return err(409, "conflict", msg);
                    }
                    BatchSetOutcome::Err(e) => return internal_error(&e),
                }
            }

            BatchOp::Delete {
                key,
                if_match,
                return_old,
            } => {
                let raw_key = percent_decode(&key);

                // `return_old` path: atomically get-then-delete, return Entry JSON.
                if return_old {
                    let entry_result: Result<Option<beyond_kv_engine::types::Entry>, String> =
                        if n_shards > 1 {
                            let target = shard_for_key(&raw_key, n_shards);
                            if target == shard_idx {
                                store.getdel(ns, &raw_key).await.map_err(|e| e.to_string())
                            } else {
                                let (reply_tx, reply_rx) = futures_channel::oneshot::channel();
                                let req = CrossShardRequest::GetDel {
                                    ns: ns.to_string(),
                                    key: Bytes::copy_from_slice(&raw_key),
                                    orig_idx: 0,
                                    reply: reply_tx,
                                };
                                if cross_shard_txs[target].clone().try_send(req).is_err() {
                                    return err(503, "shard_unavailable", "shard inbox full");
                                }
                                poke_wakeup(cross_shard_wakeups, target);
                                match reply_rx.await {
                                    Ok(Ok((_, e))) => Ok(e),
                                    Ok(Err(e)) => Err(e),
                                    Err(e) => Err(e.to_string()),
                                }
                            }
                        } else {
                            store.getdel(ns, &raw_key).await.map_err(|e| e.to_string())
                        };

                    match entry_result {
                        Err(e) => return internal_error(&e),
                        Ok(None) => serde_json::Value::Null,
                        Ok(Some(e)) => {
                            let mut obj = serde_json::json!({
                                "value": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&e.value),
                                "revision": e.revision,
                            });
                            if let Some(exp) = e.expires_at {
                                let rem = exp.saturating_duration_since(std::time::Instant::now());
                                obj["ttl"] = rem.as_secs().into();
                                obj["ttl_ms"] = (rem.as_millis() as u64).into();
                            }
                            if let Some(meta) = e.metadata {
                                obj["metadata"] = meta.as_ref().clone();
                            }
                            obj
                        }
                    }
                } else {
                    // Standard delete (or conditional-by-revision delete).
                    // Returns Ok(true)=deleted, Ok(false)=conflict (rev mismatch), Err=engine error.
                    let del_result: Result<bool, String> = if n_shards > 1 {
                        let target = shard_for_key(&raw_key, n_shards);
                        if target == shard_idx {
                            if let Some(rev) = if_match {
                                store
                                    .delrev(ns, &raw_key, rev)
                                    .await
                                    .map(|opt| opt.is_some())
                                    .map_err(|e| e.to_string())
                            } else {
                                store
                                    .del(ns, &[raw_key.as_slice()])
                                    .await
                                    .map(|_| true)
                                    .map_err(|e| e.to_string())
                            }
                        } else if let Some(rev) = if_match {
                            let (reply_tx, reply_rx) = futures_channel::oneshot::channel();
                            let req = CrossShardRequest::DelRev {
                                ns: ns.to_string(),
                                key: Bytes::copy_from_slice(&raw_key),
                                revision: rev,
                                reply: reply_tx,
                            };
                            if cross_shard_txs[target].clone().try_send(req).is_err() {
                                return err(503, "shard_unavailable", "shard inbox full");
                            }
                            poke_wakeup(cross_shard_wakeups, target);
                            match reply_rx.await {
                                Ok(r) => r.map(|opt| opt.is_some()),
                                Err(e) => Err(e.to_string()),
                            }
                        } else {
                            let (reply_tx, reply_rx) = futures_channel::oneshot::channel();
                            let req = CrossShardRequest::Del {
                                ns: ns.to_string(),
                                keys: vec![Bytes::copy_from_slice(&raw_key)],
                                reply: reply_tx,
                            };
                            if cross_shard_txs[target].clone().try_send(req).is_err() {
                                return err(503, "shard_unavailable", "shard inbox full");
                            }
                            poke_wakeup(cross_shard_wakeups, target);
                            match reply_rx.await {
                                Ok(r) => r.map(|_| true),
                                Err(e) => Err(e.to_string()),
                            }
                        }
                    } else if let Some(rev) = if_match {
                        store
                            .delrev(ns, &raw_key, rev)
                            .await
                            .map(|opt| opt.is_some())
                            .map_err(|e| e.to_string())
                    } else {
                        store
                            .del(ns, &[raw_key.as_slice()])
                            .await
                            .map(|_| true)
                            .map_err(|e| e.to_string())
                    };

                    match del_result {
                        Err(e) => return internal_error(&e),
                        Ok(false) => {
                            return err(409, "conflict", "revision mismatch");
                        }
                        Ok(true) => serde_json::Value::Null,
                    }
                }
            }

            BatchOp::Exists { key } => {
                let raw_key = percent_decode(&key);
                let count: Result<u64, String> = if n_shards > 1 {
                    let target = shard_for_key(&raw_key, n_shards);
                    if target == shard_idx {
                        store
                            .exists(ns, &[raw_key.as_slice()])
                            .await
                            .map_err(|e| e.to_string())
                    } else {
                        let (reply_tx, reply_rx) = futures_channel::oneshot::channel();
                        let req = CrossShardRequest::Exists {
                            ns: ns.to_string(),
                            keys: vec![Bytes::copy_from_slice(&raw_key)],
                            reply: reply_tx,
                        };
                        if cross_shard_txs[target].clone().try_send(req).is_err() {
                            return err(503, "shard_unavailable", "shard inbox full");
                        }
                        poke_wakeup(cross_shard_wakeups, target);
                        match reply_rx.await {
                            Ok(r) => r,
                            Err(e) => Err(e.to_string()),
                        }
                    }
                } else {
                    store
                        .exists(ns, &[raw_key.as_slice()])
                        .await
                        .map_err(|e| e.to_string())
                };

                match count {
                    Err(e) => return internal_error(&e),
                    Ok(n) => serde_json::Value::Bool(n > 0),
                }
            }

            BatchOp::Incr { key, delta } => {
                let raw_key = percent_decode(&key);
                let res: Result<i64, String> = if n_shards > 1 {
                    let target = shard_for_key(&raw_key, n_shards);
                    if target == shard_idx {
                        store
                            .incr(ns, &raw_key, delta)
                            .await
                            .map_err(|e| e.to_string())
                    } else {
                        let (reply_tx, reply_rx) = futures_channel::oneshot::channel();
                        let req = CrossShardRequest::Incr {
                            ns: ns.to_string(),
                            key: Bytes::copy_from_slice(&raw_key),
                            delta,
                            reply: reply_tx,
                        };
                        if cross_shard_txs[target].clone().try_send(req).is_err() {
                            return err(503, "shard_unavailable", "shard inbox full");
                        }
                        poke_wakeup(cross_shard_wakeups, target);
                        match reply_rx.await {
                            Ok(r) => r,
                            Err(e) => Err(e.to_string()),
                        }
                    }
                } else {
                    store
                        .incr(ns, &raw_key, delta)
                        .await
                        .map_err(|e| e.to_string())
                };

                match res {
                    Err(e) => return internal_error(&e),
                    Ok(n) => serde_json::json!({ "value": n }),
                }
            }
        };

        results.push(result);
    }

    json_response(200, &serde_json::Value::Array(results))
}

enum BatchSetOutcome {
    Ok,
    Conflict(&'static str),
    Err(String),
}

#[allow(clippy::too_many_arguments)]
async fn batch_set_local(
    ns: &str,
    key: &[u8],
    value: Bytes,
    opts: SetOptions,
    nx: bool,
    xx: bool,
    if_match: Option<u64>,
    store: &ShardStore,
) -> BatchSetOutcome {
    if let Some(rev) = if_match {
        match store.setrev(ns, key, value, opts, rev).await {
            Ok(Some(_)) => BatchSetOutcome::Ok,
            Ok(None) => BatchSetOutcome::Conflict("revision mismatch"),
            Err(e) => BatchSetOutcome::Err(e.to_string()),
        }
    } else if nx {
        match store.setnx(ns, key, value, opts).await {
            Ok(true) => BatchSetOutcome::Ok,
            Ok(false) => BatchSetOutcome::Conflict("key already exists"),
            Err(e) => BatchSetOutcome::Err(e.to_string()),
        }
    } else if xx {
        match store.setxx(ns, key, value, opts).await {
            Ok(true) => BatchSetOutcome::Ok,
            Ok(false) => BatchSetOutcome::Conflict("key does not exist"),
            Err(e) => BatchSetOutcome::Err(e.to_string()),
        }
    } else {
        match store.set(ns, key, value, opts).await {
            Ok(()) => BatchSetOutcome::Ok,
            Err(e) => BatchSetOutcome::Err(e.to_string()),
        }
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
///
/// Pass `count=1` to return only the total key count instead of a key listing.
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
        ("count" = Option<u8>, Query, description = "Set `count=1` to return only the total key count instead of a listing."),
    ),
    responses(
        (status = 200, body = ListResponse, description = "Page of matching keys in lexicographic order. When `count=1` is supplied, returns `{\"count\": N}` instead."),
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
                return err(503, "shard_unavailable", "shard inbox full");
            }
            poke_wakeup(cross_shard_wakeups, target_shard);
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
    let (_complete, cursor_out) = if shard_done {
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

    let mut body = serde_json::json!({ "keys": keys });
    if let Some(cursor) = cursor_out {
        body["next_cursor"] = serde_json::Value::String(cursor);
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
    let done = page.next_cursor == b"0".as_ref();
    let mut body = serde_json::json!({ "keys": keys });
    if !done {
        // Cursor is b"\x01" + last_key — strip prefix, base64-encode the key.
        let key = page
            .next_cursor
            .strip_prefix(b"\x01")
            .unwrap_or(&page.next_cursor);
        body["next_cursor"] =
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
                poke_wakeup(cross_shard_wakeups, shard);
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
            let key_str = String::from_utf8(key.to_vec())
                .unwrap_or_else(|e| percent_encode_bytes(e.as_bytes()));
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
            "key": String::from_utf8(key.to_vec()).unwrap_or_else(|e| percent_encode_bytes(e.as_bytes())),
            "revision": revision,
        })
        .to_string(),
    }
}

// ── helpers ──────────────────────────────────────────────────────────────────

/// Parse TTL modification options from the query string into a `GetExOp`.
/// Returns `None` when no TTL option is present (valid for GETEX with no TTL change,
/// invalid for PATCH where at least one option is required).
fn parse_ttl_op(query: &str) -> Option<beyond_kv_engine::types::GetExOp> {
    use beyond_kv_engine::types::GetExOp;
    if query_param(query, "persist").is_some() {
        return Some(GetExOp::Persist);
    }
    if let Some(s) = query_param(query, "ttl") {
        if let Ok(secs) = s.parse::<u64>() {
            return Some(GetExOp::SetTtl(Duration::from_secs(secs)));
        }
    }
    if let Some(s) = query_param(query, "ttl_ms") {
        if let Ok(ms) = s.parse::<u64>() {
            return Some(GetExOp::SetTtl(Duration::from_millis(ms)));
        }
    }
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    if let Some(s) = query_param(query, "ttl_at") {
        if let Ok(at) = s.parse::<u64>() {
            let remaining = at.saturating_sub(now_secs);
            return Some(GetExOp::SetTtl(Duration::from_secs(remaining)));
        }
    }
    if let Some(s) = query_param(query, "ttl_at_ms") {
        if let Ok(at_ms) = s.parse::<u64>() {
            let now_ms = now_secs * 1000;
            let remaining_ms = at_ms.saturating_sub(now_ms);
            return Some(GetExOp::SetTtl(Duration::from_millis(remaining_ms)));
        }
    }
    None
}

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

#[allow(clippy::result_large_err)]
fn parse_ns(query: &str) -> Result<&'static str, http::Response<HttpBody>> {
    let n = query_param(query, "ns")
        .and_then(|s| s.parse::<u8>().ok())
        .unwrap_or(0);
    NS_NAMES
        .get(n as usize)
        .copied()
        .ok_or_else(|| err(400, "invalid_namespace", "ns must be 0-15"))
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
struct CountResponse {
    /// Total number of keys in the namespace.
    count: u64,
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
    /// Opaque pagination cursor. Pass as the `cursor` query parameter on the next request
    /// to fetch the subsequent page. Absent when there are no further pages.
    #[schema(nullable)]
    #[serde(skip_serializing_if = "Option::is_none")]
    next_cursor: Option<String>,
}

#[derive(serde::Serialize, utoipa::ToSchema)]
struct ErrorBody {
    /// Machine-readable error code, e.g. `"not_found"`, `"conflict"`, `"invalid_request"`.
    code: String,
    /// Human-readable description.
    message: String,
    /// Optional actionable guidance.
    #[serde(skip_serializing_if = "Option::is_none")]
    hint: Option<String>,
}

#[derive(serde::Serialize, utoipa::ToSchema)]
struct ErrorResponse {
    error: ErrorBody,
}

#[derive(utoipa::OpenApi)]
#[openapi(
    info(
        title = "beyond/kv",
        version = "1",
        description = "Low-latency key-value store with namespaces, TTL, revision-based \
            conditional writes, atomic increment, cursor-paginated key listing, and batch \
            operations. All keys and values are raw bytes; values are transmitted as \
            `application/octet-stream`. Namespaces (0–15) are independent keyspaces \
            sharing the same physical store."
    ),
    paths(
        handle_get, handle_head, handle_put, handle_patch, handle_delete,
        handle_incr, handle_list, handle_flushdb,
        handle_compact, handle_batch,
    ),
    components(schemas(IncrResponse, CountResponse, KeyItem, ListResponse, ErrorBody, ErrorResponse)),
    tags(
        (name = "kv", description = "Key-value operations: get, put, delete, increment, list, and batch."),
        (name = "admin", description = "Administrative operations: compaction."),
    )
)]
pub struct ApiDoc;

fn http_op(method: &http::Method, path: &str) -> &'static str {
    match path {
        "/healthz" => "healthz",
        "/metrics" => "metrics",
        "/v1/openapi.json" => "openapi",
        "/v1/kv/batch" => "batch",
        "/v1/admin/compact" => "compact",
        p if p == "/v1/kv" || p.starts_with("/v1/kv?") => "list",
        p if p.starts_with("/v1/kv/") => match *method {
            http::Method::GET => "get",
            http::Method::HEAD => "head",
            http::Method::PUT => "put",
            http::Method::DELETE => "delete",
            http::Method::PATCH => "patch",
            _ => "other",
        },
        _ => "other",
    }
}

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

fn err(status: u16, code: &str, msg: impl Into<String>) -> http::Response<HttpBody> {
    err_hint(status, code, msg, None)
}

fn err_hint(
    status: u16,
    code: &str,
    msg: impl Into<String>,
    hint: Option<&str>,
) -> http::Response<HttpBody> {
    let mut body = serde_json::json!({ "error": { "code": code, "message": msg.into() } });
    if let Some(h) = hint {
        body["error"]["hint"] = serde_json::Value::String(h.to_owned());
    }
    json_response(status, &body)
}

fn payload_too_large() -> http::Response<HttpBody> {
    err(
        413,
        "payload_too_large",
        "request body exceeds maximum allowed size",
    )
}

fn not_found_json(code: &str, msg: &str) -> http::Response<HttpBody> {
    err(404, code, msg)
}

fn engine_error_response(e: EngineError) -> http::Response<HttpBody> {
    match e {
        EngineError::InvalidNamespace { .. } => err(400, "invalid_namespace", e.to_string()),
        EngineError::CapacityExceeded { .. } => err(400, "capacity_exceeded", e.to_string()),
        EngineError::InvalidInput { .. } => err(400, "invalid_value", e.to_string()),
        EngineError::Conflict { .. } => err_hint(
            409,
            "conflict",
            e.to_string(),
            Some("read the current revision from the X-KV-Revision response header and retry"),
        ),
        _ => internal_error(&e.to_string()),
    }
}

fn internal_error(msg: &str) -> http::Response<HttpBody> {
    err(500, "internal_error", msg)
}

fn method_not_allowed() -> http::Response<HttpBody> {
    err(405, "method_not_allowed", "method not allowed")
}

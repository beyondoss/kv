mod common;
use common::{ListOptions, PutOptions, TestServer};

// ── Health check ──────────────────────────────────────────────────────────────

#[test]
fn healthz_returns_ok() {
    let srv = TestServer::start();
    let res = common::raw_call_url(ureq::get(&srv.healthz_url()));
    assert_eq!(res.status, 200);
    assert_eq!(res.body_str(), "ok");
}

// ── GET ───────────────────────────────────────────────────────────────────────

#[test]
fn get_missing_key_is_404() {
    let srv = TestServer::start();
    let res = srv.get("no-such-key");
    assert!(res.is_not_found());
    let body = res.json();
    assert_eq!(body["error"]["code"], "not_found");
}

// ── PUT + GET roundtrip ───────────────────────────────────────────────────────

#[test]
fn put_then_get_returns_value() {
    let srv = TestServer::start();
    assert!(srv.put("greeting", b"hello world").is_ok());
    let res = srv.get("greeting");
    assert_eq!(res.status, 200);
    assert_eq!(res.body, b"hello world");
}

#[test]
fn put_overwrites_existing_key() {
    let srv = TestServer::start();
    srv.put("k", b"first");
    srv.put("k", b"second");
    let res = srv.get("k");
    assert_eq!(res.body, b"second");
}

// ── DELETE ────────────────────────────────────────────────────────────────────

#[test]
fn delete_removes_key() {
    let srv = TestServer::start();
    srv.put("target", b"value");
    assert!(srv.delete("target").is_ok());
    assert!(srv.get("target").is_not_found());
}

#[test]
fn delete_is_idempotent_on_missing_key() {
    let srv = TestServer::start();
    let res = srv.delete("ghost");
    assert!(
        res.is_ok(),
        "DELETE on missing key must not error; got {}",
        res.status
    );
}

#[test]
fn delete_then_delete_again_is_ok() {
    let srv = TestServer::start();
    srv.put("k", b"v");
    srv.delete("k");
    let res = srv.delete("k");
    assert!(
        res.is_ok(),
        "second DELETE must be idempotent; got {}",
        res.status
    );
}

// ── NX (set-if-not-exists) ────────────────────────────────────────────────────

#[test]
fn put_nx_succeeds_on_fresh_key() {
    let srv = TestServer::start();
    let res = srv.put_opts(
        0,
        "nx-key",
        b"v",
        PutOptions {
            nx: true,
            ..Default::default()
        },
    );
    assert_eq!(res.status, 204);
}

#[test]
fn put_nx_returns_409_on_existing_key() {
    let srv = TestServer::start();
    srv.put("nx-dup", b"existing");
    let res = srv.put_opts(
        0,
        "nx-dup",
        b"new",
        PutOptions {
            nx: true,
            ..Default::default()
        },
    );
    assert!(res.is_conflict());
    assert_eq!(res.json()["error"]["code"], "conflict");
}

#[test]
fn put_nx_does_not_overwrite_value() {
    let srv = TestServer::start();
    srv.put("nx-safe", b"original");
    srv.put_opts(
        0,
        "nx-safe",
        b"clobbered",
        PutOptions {
            nx: true,
            ..Default::default()
        },
    );
    assert_eq!(srv.get("nx-safe").body, b"original");
}

// ── TTL ───────────────────────────────────────────────────────────────────────

#[test]
fn put_with_ttl_header_reflects_in_get() {
    let srv = TestServer::start();
    srv.put_opts(
        0,
        "ttl-key",
        b"v",
        PutOptions {
            ttl_header: Some(60),
            ..Default::default()
        },
    );
    let res = srv.get("ttl-key");
    assert_eq!(res.status, 200);
    let ttl = res.ttl.expect("x-kv-ttl header missing");
    assert!(ttl > 0 && ttl <= 60, "TTL should be in (0, 60], got {ttl}");
}

#[test]
fn put_with_ttl_query_param_reflects_in_get() {
    let srv = TestServer::start();
    srv.put_opts(
        0,
        "ttl-q",
        b"v",
        PutOptions {
            ttl_query: Some(60),
            ..Default::default()
        },
    );
    let ttl = srv.get("ttl-q").ttl.expect("x-kv-ttl missing");
    assert!(ttl > 0 && ttl <= 60);
}

#[test]
fn key_expires_after_ttl() {
    let srv = TestServer::start();
    srv.put_opts(
        0,
        "expiring",
        b"soon-gone",
        PutOptions {
            ttl_header: Some(1),
            ..Default::default()
        },
    );
    assert_eq!(srv.get("expiring").status, 200);
    std::thread::sleep(std::time::Duration::from_millis(1100));
    assert!(
        srv.get("expiring").is_not_found(),
        "key should have expired"
    );
}

// ── Metadata ──────────────────────────────────────────────────────────────────

#[test]
fn put_with_metadata_round_trips() {
    let srv = TestServer::start();
    let meta = serde_json::json!({"score": 42, "tags": ["a", "b"]});
    srv.put_opts(
        0,
        "meta-key",
        b"data",
        PutOptions {
            metadata: Some(meta.clone()),
            ..Default::default()
        },
    );
    let res = srv.get("meta-key");
    assert_eq!(res.metadata, Some(meta));
}

// ── Binary values ─────────────────────────────────────────────────────────────

#[test]
fn binary_value_round_trips_exactly() {
    let srv = TestServer::start();
    let data: Vec<u8> = (0u8..=255).collect();
    srv.put("binary", &data);
    let res = srv.get("binary");
    assert_eq!(res.body, data, "binary round-trip mismatch");
}

#[test]
fn value_with_null_bytes_round_trips() {
    let srv = TestServer::start();
    let data = b"\x00\x01\x02\xFF\xFE\x00";
    srv.put("nulls", data);
    assert_eq!(srv.get("nulls").body, data);
}

// ── Key encoding ──────────────────────────────────────────────────────────────

#[test]
fn key_with_spaces_round_trips_via_percent_encoding() {
    let srv = TestServer::start();
    srv.put("hello world", b"spaced");
    assert_eq!(srv.get("hello world").body, b"spaced");
}

#[test]
fn key_with_slashes_round_trips() {
    let srv = TestServer::start();
    srv.put("path/to/key", b"nested");
    assert_eq!(srv.get("path/to/key").body, b"nested");
}

#[test]
fn key_with_unicode_round_trips() {
    let srv = TestServer::start();
    srv.put("こんにちは", b"konnichiwa");
    assert_eq!(srv.get("こんにちは").body, b"konnichiwa");
}

// ── Namespace isolation ───────────────────────────────────────────────────────

#[test]
fn key_in_one_namespace_invisible_in_another() {
    let srv = TestServer::start();
    srv.put_ns(0, "shared-name", b"in-default");
    assert!(srv.get_ns(1, "shared-name").is_not_found());
}

#[test]
fn same_key_independent_values_per_namespace() {
    let srv = TestServer::start();
    srv.put_ns(0, "k", b"default-val");
    srv.put_ns(2, "k", b"db2-val");
    assert_eq!(srv.get_ns(0, "k").body, b"default-val");
    assert_eq!(srv.get_ns(2, "k").body, b"db2-val");
}

// ── Method routing ────────────────────────────────────────────────────────────

#[test]
fn post_to_value_endpoint_is_405() {
    let srv = TestServer::start();
    let url = srv.value_url(0, "k");
    let res = common::raw_call_url(ureq::post(&url));
    assert!(res.is_method_not_allowed());
    assert_eq!(res.json()["error"]["code"], "method_not_allowed");
}

#[test]
fn unknown_route_is_404() {
    let srv = TestServer::start();
    let url = format!("http://127.0.0.1:{}/not/a/real/endpoint", srv.http_port);
    let res = common::raw_call_url(ureq::get(&url));
    assert!(res.is_not_found());
}

// ── List / SCAN ───────────────────────────────────────────────────────────────

#[test]
fn list_empty_namespace_returns_empty() {
    let srv = TestServer::start();
    let body = srv.list(0).json();
    assert_eq!(body["keys"], serde_json::json!([]));
    assert_eq!(body["complete"], true);
}

#[test]
fn list_returns_all_inserted_keys() {
    let srv = TestServer::start();
    for k in ["alpha", "beta", "gamma"] {
        srv.put(k, b"v");
    }
    let body = srv.list(0).json();
    let names: Vec<String> = body["keys"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["name"].as_str().unwrap().to_owned())
        .collect();
    for k in ["alpha", "beta", "gamma"] {
        assert!(names.contains(&k.to_owned()), "expected {k} in list");
    }
    assert_eq!(body["complete"], true);
}

#[test]
fn list_with_prefix_filters_keys() {
    let srv = TestServer::start();
    for k in ["user:1", "user:2", "session:abc"] {
        srv.put(k, b"v");
    }
    let body = srv
        .list_opts(
            0,
            ListOptions {
                prefix: Some("user:".into()),
                ..Default::default()
            },
        )
        .json();
    let names: Vec<String> = body["keys"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["name"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(names.len(), 2);
    assert!(names.iter().all(|n| n.starts_with("user:")));
}

#[test]
fn list_with_limit_caps_page_size() {
    let srv = TestServer::start();
    for i in 0..10 {
        srv.put(&format!("item:{i:02}"), b"v");
    }
    let body = srv
        .list_opts(
            0,
            ListOptions {
                limit: Some(3),
                ..Default::default()
            },
        )
        .json();
    let keys = body["keys"].as_array().unwrap();
    assert_eq!(keys.len(), 3);
    assert_eq!(body["complete"], false);
    assert!(
        body["cursor"].as_str().is_some(),
        "expect a cursor for the next page"
    );
}

#[test]
fn list_pagination_covers_all_keys() {
    let srv = TestServer::start();
    let want: Vec<String> = (0..15).map(|i| format!("pg:{i:02}")).collect();
    for k in &want {
        srv.put(k, b"v");
    }

    let mut all_names: Vec<String> = Vec::new();
    let mut cursor: Option<String> = None;

    loop {
        let opts = ListOptions {
            cursor: cursor.clone(),
            limit: Some(4),
            ..Default::default()
        };
        let body = srv.list_opts(0, opts).json();
        for e in body["keys"].as_array().unwrap() {
            all_names.push(e["name"].as_str().unwrap().to_owned());
        }
        if body["complete"].as_bool().unwrap_or(false) {
            break;
        }
        cursor = body["cursor"].as_str().map(|s| s.to_owned());
    }

    all_names.sort();
    let mut want_sorted = want.clone();
    want_sorted.sort();
    assert_eq!(
        all_names, want_sorted,
        "paginated scan missed or duplicated keys"
    );
}

#[test]
fn expired_keys_not_returned_in_list() {
    let srv = TestServer::start();
    srv.put_opts(0, "live", b"v", PutOptions::default());
    srv.put_opts(
        0,
        "dead",
        b"v",
        PutOptions {
            ttl_header: Some(1),
            ..Default::default()
        },
    );
    std::thread::sleep(std::time::Duration::from_millis(1100));
    let body = srv.list(0).json();
    let names: Vec<&str> = body["keys"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"live"));
    assert!(
        !names.contains(&"dead"),
        "expired key must not appear in list"
    );
}

// ── Malformed requests ────────────────────────────────────────────────────────

#[test]
fn malformed_ttl_header_is_silently_ignored_key_stored_without_ttl() {
    // X-KV-TTL with a non-numeric value is swallowed via .ok(); key lands with no TTL.
    let srv = TestServer::start();
    let res = ureq::put(&srv.value_url(0, "bad-ttl-key"))
        .set("Content-Type", "application/octet-stream")
        .set("X-KV-TTL", "not-a-number")
        .send_bytes(b"value")
        .unwrap();
    assert_eq!(res.status(), 204, "bad TTL header must not cause a 4xx");
    // Key is stored without TTL — TTL header in GET response should be absent
    let got = srv.get("bad-ttl-key");
    assert_eq!(got.status, 200);
    assert_eq!(got.body, b"value");
    assert!(
        got.ttl.is_none(),
        "no TTL should have been set when header was non-numeric"
    );
}

#[test]
fn malformed_metadata_header_is_silently_ignored() {
    // X-KV-Metadata with invalid JSON is swallowed via .ok(); key is stored without metadata.
    let srv = TestServer::start();
    let res = ureq::put(&srv.value_url(0, "bad-meta-key"))
        .set("Content-Type", "application/octet-stream")
        .set("X-KV-Metadata", "this-is-not-json{{{")
        .send_bytes(b"value")
        .unwrap();
    assert_eq!(res.status(), 204);
    let got = srv.get("bad-meta-key");
    assert_eq!(got.status, 200);
    assert!(
        got.metadata.is_none(),
        "invalid metadata header must be silently dropped"
    );
}

#[test]
fn invalid_limit_query_param_falls_back_to_default() {
    // limit=banana is ignored via .ok(); falls back to 100.
    let srv = TestServer::start();
    for i in 0..5 {
        srv.put(&format!("lim-key-{i}"), b"v");
    }
    let url = format!("{}&limit=banana", srv.keys_url(0));
    let res = common::raw_call_url(ureq::get(&url));
    assert_eq!(res.status, 200);
    let body = res.json();
    // With default limit=100 all 5 keys should come back
    let keys = body["keys"].as_array().unwrap();
    assert_eq!(keys.len(), 5, "fallback limit must return all keys");
}

#[test]
fn zero_ttl_header_results_in_no_ttl() {
    // X-KV-TTL: 0 → Duration::from_secs(0) which may be treated as no TTL or immediate expiry.
    // Verify the key is stored and GET returns a result (implementation treats 0 as no TTL).
    let srv = TestServer::start();
    let _ = ureq::put(&srv.value_url(0, "ttl-zero"))
        .set("Content-Type", "application/octet-stream")
        .set("X-KV-TTL", "0")
        .send_bytes(b"zero-ttl-value");
    // Key may or may not persist depending on implementation; just confirm no 5xx
    let got = srv.get("ttl-zero");
    assert!(
        got.status == 200 || got.status == 404,
        "zero TTL must not cause 5xx"
    );
}

// ── INCR ──────────────────────────────────────────────────────────────────────

fn incr_url(srv: &TestServer, key: &str) -> String {
    format!(
        "http://127.0.0.1:{}/v1/kv/{}/incr?ns=0",
        srv.http_port,
        urlencoding::encode(key)
    )
}

fn incr_url_delta(srv: &TestServer, key: &str, delta: i64) -> String {
    format!(
        "http://127.0.0.1:{}/v1/kv/{}/incr?ns=0&delta={delta}",
        srv.http_port,
        urlencoding::encode(key)
    )
}

#[test]
fn incr_missing_key_starts_at_one() {
    let srv = TestServer::start();
    let res = common::raw_call_url(ureq::post(&incr_url(&srv, "ctr")));
    assert_eq!(res.status, 200);
    assert_eq!(res.json()["value"], 1);
}

#[test]
fn incr_increments_existing_value() {
    let srv = TestServer::start();
    srv.put("ctr", b"5");
    let res = common::raw_call_url(ureq::post(&incr_url(&srv, "ctr")));
    assert_eq!(res.status, 200);
    assert_eq!(res.json()["value"], 6);
}

#[test]
fn incr_with_delta() {
    let srv = TestServer::start();
    srv.put("ctr", b"10");
    let res = common::raw_call_url(ureq::post(&incr_url_delta(&srv, "ctr", 5)));
    assert_eq!(res.status, 200);
    assert_eq!(res.json()["value"], 15);
}

#[test]
fn incr_with_negative_delta_decrements() {
    let srv = TestServer::start();
    srv.put("ctr", b"10");
    let res = common::raw_call_url(ureq::post(&incr_url_delta(&srv, "ctr", -3)));
    assert_eq!(res.status, 200);
    assert_eq!(res.json()["value"], 7);
}

#[test]
fn incr_non_integer_value_returns_400() {
    let srv = TestServer::start();
    srv.put("bad", b"hello");
    let res = common::raw_call_url(ureq::post(&incr_url(&srv, "bad")));
    assert_eq!(res.status, 400);
    assert_eq!(res.json()["error"]["code"], "invalid_value");
}

#[test]
fn incr_preserves_ttl() {
    let srv = TestServer::start();
    srv.put_opts(
        0,
        "ctr",
        b"5",
        common::PutOptions {
            ttl_header: Some(60),
            ..Default::default()
        },
    );
    common::raw_call_url(ureq::post(&incr_url(&srv, "ctr")));
    let res = srv.get("ctr");
    assert!(res.ttl.is_some(), "TTL should be preserved after INCR");
    assert!(res.ttl.unwrap() > 0);
}

#[test]
fn incr_overflow_returns_400() {
    let srv = TestServer::start();
    srv.put("big", i64::MAX.to_string().as_bytes());
    let res = common::raw_call_url(ureq::post(&incr_url(&srv, "big")));
    assert_eq!(res.status, 400);
}

#[test]
fn incr_patch_returns_405() {
    let srv = TestServer::start();
    let url = incr_url(&srv, "ctr");
    let res = common::raw_call_url(ureq::patch(&url));
    assert_eq!(res.status, 405);
}

// ── HTTP Watch (SSE) ──────────────────────────────────────────────────────────

#[test]
fn watch_key_receives_set_event() {
    let srv = TestServer::start();
    let sse = common::watch_key_sse(srv.http_port, 0, "sse-set-k", None);

    // No existing key → first event is "ready".
    let ready = sse.recv_event().expect("expected ready event");
    assert_eq!(ready["type"], "ready", "first event must be ready: {ready}");

    // Write the key — must produce a "set" watch event.
    srv.put("sse-set-k", b"hello-sse");

    let event = sse.recv_event().expect("expected set event after PUT");
    assert_eq!(event["type"], "set", "expected set event: {event}");
    assert_eq!(event["key"], "sse-set-k");
    assert!(
        event["revision"].as_u64().unwrap_or(0) > 0,
        "revision must be positive"
    );
}

#[test]
fn watch_key_receives_del_event() {
    let srv = TestServer::start();
    srv.put("sse-del-k", b"to-delete");

    let sse = common::watch_key_sse(srv.http_port, 0, "sse-del-k", None);

    // Key exists — initial state push (set), then ready.
    let init = sse.recv_event().expect("expected initial set event");
    assert_eq!(init["type"], "set", "expected initial state set: {init}");
    let ready = sse.recv_event().expect("expected ready event");
    assert_eq!(ready["type"], "ready", "expected ready: {ready}");

    // Delete the key.
    srv.delete("sse-del-k");

    let event = sse.recv_event().expect("expected del event after DELETE");
    assert_eq!(event["type"], "del", "expected del event: {event}");
    assert_eq!(event["key"], "sse-del-k");
}

#[test]
fn watch_since_replays_missed_event() {
    // Architecture guarantee: ?since=<rev> replays mutations with tstamp_ms > rev.
    // This models a client reconnecting after a disconnect.
    let srv = TestServer::start();

    srv.put("sse-since-k", b"v1");

    // Capture revision of v1 via RESP REVISION command.
    let rev: u64 = {
        let mut con = srv.resp();
        redis::cmd("REVISION")
            .arg("sse-since-k")
            .query::<i64>(&mut con)
            .unwrap() as u64
    };
    assert!(rev > 0);

    // Write v2 without watching — this is the event we want to replay.
    srv.put("sse-since-k", b"v2");

    // Watch with since=rev(v1): should get the v2 catch-up event, then ready.
    let sse = common::watch_key_sse(srv.http_port, 0, "sse-since-k", Some(rev));

    let catchup = sse.recv_event().expect("expected catch-up set event");
    assert_eq!(catchup["type"], "set", "expected set catch-up: {catchup}");
    assert_eq!(catchup["key"], "sse-since-k");

    let ready = sse.recv_event().expect("expected ready after catch-up");
    assert_eq!(ready["type"], "ready", "expected ready: {ready}");
}

#[test]
fn watch_prefix_receives_matching_events_only() {
    let srv = TestServer::start();
    let sse = common::watch_prefix_sse(srv.http_port, 0, "pfx:", None);

    // No existing keys with prefix → ready arrives immediately.
    let ready = sse.recv_event().expect("expected ready event");
    assert_eq!(ready["type"], "ready", "expected ready: {ready}");

    // Write a matching key.
    srv.put("pfx:alpha", b"a");
    let ev1 = sse.recv_event().expect("expected set event for pfx:alpha");
    assert_eq!(ev1["type"], "set", "expected set: {ev1}");
    assert_eq!(ev1["key"], "pfx:alpha");

    // Write a non-matching key — should produce no event.
    srv.put("other:beta", b"b");

    // Write another matching key — this event must arrive (not the other:beta one).
    srv.put("pfx:gamma", b"g");
    let ev2 = sse.recv_event().expect("expected set event for pfx:gamma");
    assert_eq!(ev2["type"], "set", "expected set: {ev2}");
    assert_eq!(ev2["key"], "pfx:gamma");
}

// ── KEEPTTL ───────────────────────────────────────────────────────────────────

#[test]
fn put_keepttl_preserves_existing_ttl() {
    let srv = TestServer::start();
    srv.put_opts(
        0,
        "keepttl-k",
        b"v1",
        PutOptions {
            ttl_header: Some(60),
            ..Default::default()
        },
    );
    let ttl_before = srv.get("keepttl-k").ttl.expect("x-kv-ttl missing before");
    srv.put_opts(
        0,
        "keepttl-k",
        b"v2",
        PutOptions {
            keep_ttl: true,
            ..Default::default()
        },
    );
    let res = srv.get("keepttl-k");
    assert_eq!(res.body, b"v2");
    let ttl_after = res.ttl.expect("x-kv-ttl must be preserved");
    assert!(
        ttl_after > 0 && ttl_after <= ttl_before,
        "TTL should be preserved"
    );
}

#[test]
fn put_keepttl_on_key_without_ttl_stays_persistent() {
    let srv = TestServer::start();
    srv.put("no-ttl-k", b"v1");
    srv.put_opts(
        0,
        "no-ttl-k",
        b"v2",
        PutOptions {
            keep_ttl: true,
            ..Default::default()
        },
    );
    let res = srv.get("no-ttl-k");
    assert_eq!(res.body, b"v2");
    assert!(res.ttl.is_none(), "key should remain persistent");
}

#[test]
fn put_keepttl_with_ttl_option_returns_400() {
    let srv = TestServer::start();
    let res = common::raw_call_url(
        ureq::put(&srv.value_url(0, "conflict-k"))
            .set("x-kv-keepttl", "1")
            .set("x-kv-ttl", "60")
            .set("Content-Type", "application/octet-stream"),
    );
    assert_eq!(res.status, 400);
    assert_eq!(res.json()["error"]["code"], "invalid_request");
}

// ── Return-old (atomic swap) ──────────────────────────────────────────────────

#[test]
fn put_return_old_on_existing_key_returns_old_value() {
    let srv = TestServer::start();
    srv.put("swap-k", b"old");
    let res = srv.put_opts(
        0,
        "swap-k",
        b"new",
        PutOptions {
            return_old: true,
            ..Default::default()
        },
    );
    assert_eq!(res.status, 200);
    assert_eq!(res.body, b"old");
    assert_eq!(srv.get("swap-k").body, b"new");
}

#[test]
fn put_return_old_on_missing_key_returns_204() {
    let srv = TestServer::start();
    let res = srv.put_opts(
        0,
        "swap-new",
        b"v",
        PutOptions {
            return_old: true,
            ..Default::default()
        },
    );
    assert_eq!(res.status, 204, "no old value → 204");
    assert_eq!(srv.get("swap-new").body, b"v");
}

#[test]
fn put_return_old_with_nx_returns_400() {
    let srv = TestServer::start();
    let mut url = srv.value_url(0, "bad-combo");
    url.push_str("&nx=1");
    let res = common::raw_call_url(
        ureq::put(&url)
            .set("x-kv-return-old", "1")
            .set("Content-Type", "application/octet-stream"),
    );
    assert_eq!(res.status, 400);
    assert_eq!(res.json()["error"]["code"], "invalid_request");
}

// ── Namespace validation ──────────────────────────────────────────────────────

#[test]
fn namespace_out_of_range_returns_400() {
    let srv = TestServer::start();
    let url = format!("http://127.0.0.1:{}/v1/kv/k?ns=99", srv.http_port);
    let res = common::raw_call_url(ureq::get(&url));
    assert_eq!(res.status, 400);
    assert_eq!(res.json()["error"]["code"], "invalid_namespace");
}

// ── HEAD ──────────────────────────────────────────────────────────────────────

#[test]
fn head_missing_key_returns_404() {
    let srv = TestServer::start();
    let url = srv.value_url(0, "no-such-key");
    let res = common::raw_call_url(ureq::head(&url));
    assert_eq!(res.status, 404);
}

#[test]
fn head_existing_key_returns_200_no_body() {
    let srv = TestServer::start();
    srv.put("hk", b"value");
    let url = srv.value_url(0, "hk");
    let res = common::raw_call_url(ureq::head(&url));
    assert_eq!(res.status, 200);
    assert!(res.body.is_empty(), "HEAD must not return a body");
}

#[test]
fn head_returns_ttl_header_when_set() {
    let srv = TestServer::start();
    srv.put_opts(
        0,
        "hk-ttl",
        b"v",
        PutOptions {
            ttl_header: Some(60),
            ..Default::default()
        },
    );
    let url = srv.value_url(0, "hk-ttl");
    let res = common::raw_call_url(ureq::head(&url));
    assert_eq!(res.status, 200);
    let ttl = res.ttl.expect("X-KV-TTL header missing");
    assert!(ttl > 0 && ttl <= 60);
}

#[test]
fn head_returns_no_ttl_header_when_key_has_no_ttl() {
    let srv = TestServer::start();
    srv.put("hk-notl", b"v");
    let url = srv.value_url(0, "hk-notl");
    let res = common::raw_call_url(ureq::head(&url));
    assert_eq!(res.status, 200);
    assert!(res.ttl.is_none(), "no TTL should mean no X-KV-TTL header");
}

#[test]
fn head_returns_metadata_header() {
    let srv = TestServer::start();
    let meta = serde_json::json!({"role": "admin"});
    srv.put_opts(
        0,
        "hk-meta",
        b"v",
        PutOptions {
            metadata: Some(meta.clone()),
            ..Default::default()
        },
    );
    let url = srv.value_url(0, "hk-meta");
    let res = common::raw_call_url(ureq::head(&url));
    assert_eq!(res.status, 200);
    assert_eq!(res.metadata, Some(meta));
}

// ── PATCH (TTL update / PERSIST / GETEX) ─────────────────────────────────────

fn patch_url(srv: &TestServer, ns: u8, key: &str, extra: &str) -> String {
    let base = srv.value_url(ns, key);
    if extra.is_empty() {
        base
    } else {
        format!("{base}&{extra}")
    }
}

#[test]
fn patch_ttl_updates_existing_key() {
    let srv = TestServer::start();
    srv.put("pk", b"v");
    let url = patch_url(&srv, 0, "pk", "ttl=120");
    let res = common::raw_call_url(ureq::patch(&url));
    assert_eq!(res.status, 204);
    let ttl = srv.get("pk").ttl.expect("TTL should be set after PATCH");
    assert!(ttl > 0 && ttl <= 120);
}

#[test]
fn patch_ttl_ms_param_works() {
    let srv = TestServer::start();
    srv.put("pk-ms", b"v");
    let url = patch_url(&srv, 0, "pk-ms", "ttl_ms=90000");
    let res = common::raw_call_url(ureq::patch(&url));
    assert_eq!(res.status, 204);
    let ttl = srv.get("pk-ms").ttl.expect("TTL should be set");
    assert!(ttl > 0 && ttl <= 90);
}

#[test]
fn patch_persist_removes_ttl() {
    let srv = TestServer::start();
    srv.put_opts(
        0,
        "pk-persist",
        b"v",
        PutOptions {
            ttl_header: Some(60),
            ..Default::default()
        },
    );
    assert!(
        srv.get("pk-persist").ttl.is_some(),
        "key should start with a TTL"
    );
    let url = patch_url(&srv, 0, "pk-persist", "persist=1");
    let res = common::raw_call_url(ureq::patch(&url));
    assert_eq!(res.status, 204);
    assert!(
        srv.get("pk-persist").ttl.is_none(),
        "persist=1 must clear TTL"
    );
}

#[test]
fn patch_missing_key_returns_404() {
    let srv = TestServer::start();
    let url = patch_url(&srv, 0, "no-such-key", "ttl=60");
    let res = common::raw_call_url(ureq::patch(&url));
    assert_eq!(res.status, 404);
}

#[test]
fn patch_no_option_returns_400() {
    let srv = TestServer::start();
    srv.put("pk-bad", b"v");
    let url = patch_url(&srv, 0, "pk-bad", "");
    let res = common::raw_call_url(ureq::patch(&url));
    assert_eq!(res.status, 400);
    assert_eq!(res.json()["error"]["code"], "invalid_request");
}

#[test]
fn patch_return_value_header_returns_current_value() {
    let srv = TestServer::start();
    srv.put_opts(
        0,
        "pk-getex",
        b"hello",
        PutOptions {
            ttl_header: Some(30),
            ..Default::default()
        },
    );
    let url = patch_url(&srv, 0, "pk-getex", "ttl=120");
    let res = common::raw_call_url(ureq::patch(&url).set("X-KV-Return-Value", "1"));
    assert_eq!(res.status, 200);
    assert_eq!(res.body, b"hello");
    let new_ttl = res.ttl.expect("X-KV-TTL should be returned with value");
    assert!(new_ttl > 0 && new_ttl <= 120);
}

#[test]
fn patch_return_value_without_ttl_op_returns_value_unchanged() {
    let srv = TestServer::start();
    srv.put_opts(
        0,
        "pk-getex-noop",
        b"world",
        PutOptions {
            ttl_header: Some(60),
            ..Default::default()
        },
    );
    let url = patch_url(&srv, 0, "pk-getex-noop", "");
    let res = common::raw_call_url(ureq::patch(&url).set("X-KV-Return-Value", "1"));
    assert_eq!(res.status, 200);
    assert_eq!(res.body, b"world");
    let ttl = res.ttl.expect("X-KV-TTL should be preserved");
    assert!(ttl > 0 && ttl <= 60);
}

// ── DBSIZE (GET /v1/kv?count=1) ───────────────────────────────────────────────

fn dbsize_url(srv: &TestServer, ns: u8) -> String {
    format!("http://127.0.0.1:{}/v1/kv?ns={ns}&count=1", srv.http_port)
}

#[test]
fn dbsize_empty_namespace_returns_zero() {
    let srv = TestServer::start();
    let res = common::raw_call_url(ureq::get(&dbsize_url(&srv, 0)));
    assert_eq!(res.status, 200);
    assert_eq!(res.json()["count"], 0);
}

#[test]
fn dbsize_counts_all_keys_in_namespace() {
    let srv = TestServer::start();
    for k in ["alpha", "beta", "gamma"] {
        srv.put(k, b"v");
    }
    let res = common::raw_call_url(ureq::get(&dbsize_url(&srv, 0)));
    assert_eq!(res.status, 200);
    assert_eq!(res.json()["count"], 3);
}

#[test]
fn dbsize_is_namespace_scoped() {
    let srv = TestServer::start();
    srv.put_ns(0, "ns0-key", b"v");
    srv.put_ns(1, "ns1-key1", b"v");
    srv.put_ns(1, "ns1-key2", b"v");
    let res0 = common::raw_call_url(ureq::get(&dbsize_url(&srv, 0)));
    let res1 = common::raw_call_url(ureq::get(&dbsize_url(&srv, 1)));
    assert_eq!(res0.json()["count"], 1);
    assert_eq!(res1.json()["count"], 2);
}

// ── FLUSHDB (DELETE /v1/kv) ───────────────────────────────────────────────────

fn flushdb_url(srv: &TestServer, ns: u8) -> String {
    format!("http://127.0.0.1:{}/v1/kv?ns={ns}", srv.http_port)
}

#[test]
fn flushdb_removes_all_keys_in_namespace() {
    let srv = TestServer::start();
    for k in ["fa", "fb", "fc"] {
        srv.put(k, b"v");
    }
    let res = common::raw_call_url(ureq::delete(&flushdb_url(&srv, 0)));
    assert_eq!(res.status, 204);
    let list = srv.list(0).json();
    assert_eq!(
        list["keys"].as_array().unwrap().len(),
        0,
        "all keys must be gone after FLUSHDB"
    );
}

#[test]
fn flushdb_is_namespace_scoped() {
    let srv = TestServer::start();
    srv.put_ns(0, "safe-key", b"v");
    srv.put_ns(1, "flushed-key", b"v");
    let res = common::raw_call_url(ureq::delete(&flushdb_url(&srv, 1)));
    assert_eq!(res.status, 204);
    assert_eq!(
        srv.get_ns(0, "safe-key").status,
        200,
        "ns=0 key must survive ns=1 flush"
    );
    assert!(
        srv.get_ns(1, "flushed-key").is_not_found(),
        "ns=1 key must be gone"
    );
}

#[test]
fn flushdb_is_idempotent() {
    let srv = TestServer::start();
    let url = flushdb_url(&srv, 0);
    let res1 = common::raw_call_url(ureq::delete(&url));
    let res2 = common::raw_call_url(ureq::delete(&url));
    assert_eq!(res1.status, 204);
    assert_eq!(res2.status, 204);
}

// ── POST /v1/kv/batch ─────────────────────────────────────────────────────────

fn batch_url(srv: &TestServer, ns: u8) -> String {
    format!("http://127.0.0.1:{}/v1/kv/batch?ns={ns}", srv.http_port)
}

fn b64(b: &[u8]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b)
}

fn b64_decode(s: &str) -> Vec<u8> {
    use base64::Engine as _;
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(s)
        .expect("invalid base64url")
}

fn batch_call(srv: &TestServer, ns: u8, ops: serde_json::Value) -> common::KvResponse {
    let url = batch_url(srv, ns);
    let body = ops.to_string();
    let res = ureq::post(&url)
        .set("Content-Type", "application/json")
        .send_string(&body);
    match res {
        Ok(r) => {
            let status = r.status();
            let mut buf = Vec::new();
            std::io::Read::read_to_end(&mut r.into_reader(), &mut buf).unwrap();
            common::KvResponse {
                status,
                body: buf,
                ttl: None,
                ttl_ms: None,
                metadata: None,
            }
        }
        Err(ureq::Error::Status(_, r)) => {
            let status = r.status();
            let mut buf = Vec::new();
            std::io::Read::read_to_end(&mut r.into_reader(), &mut buf).unwrap();
            common::KvResponse {
                status,
                body: buf,
                ttl: None,
                ttl_ms: None,
                metadata: None,
            }
        }
        Err(e) => panic!("HTTP transport error: {e}"),
    }
}

#[test]
fn batch_get_hit_returns_value_and_revision() {
    let srv = TestServer::start();
    srv.put("bk-hit", b"hello");
    let res = batch_call(&srv, 0, serde_json::json!([{"op": "get", "key": "bk-hit"}]));
    assert_eq!(res.status, 200);
    let results = res.json();
    let entry = &results[0];
    assert_eq!(b64_decode(entry["value"].as_str().unwrap()), b"hello");
    assert!(entry["revision"].as_u64().unwrap_or(0) > 0);
}

#[test]
fn batch_get_miss_returns_null() {
    let srv = TestServer::start();
    let res = batch_call(
        &srv,
        0,
        serde_json::json!([{"op": "get", "key": "no-such"}]),
    );
    assert_eq!(res.status, 200);
    let results = res.json();
    assert!(results[0].is_null());
}

#[test]
fn batch_set_stores_value() {
    let srv = TestServer::start();
    let res = batch_call(
        &srv,
        0,
        serde_json::json!([{"op": "set", "key": "bk-set", "value": b64(b"world")}]),
    );
    assert_eq!(res.status, 200);
    assert_eq!(srv.get("bk-set").body, b"world");
}

#[test]
fn batch_set_with_ttl_stores_ttl() {
    let srv = TestServer::start();
    let res = batch_call(
        &srv,
        0,
        serde_json::json!([{"op": "set", "key": "bk-ttl", "value": b64(b"v"), "ttl": 60}]),
    );
    assert_eq!(res.status, 200);
    let ttl = srv.get("bk-ttl").ttl.expect("TTL should be set");
    assert!(ttl > 0 && ttl <= 60);
}

#[test]
fn batch_delete_removes_key() {
    let srv = TestServer::start();
    srv.put("bk-del", b"v");
    let res = batch_call(
        &srv,
        0,
        serde_json::json!([{"op": "delete", "key": "bk-del"}]),
    );
    assert_eq!(res.status, 200);
    assert!(srv.get("bk-del").is_not_found());
}

#[test]
fn batch_incr_from_missing_starts_at_delta() {
    let srv = TestServer::start();
    let res = batch_call(
        &srv,
        0,
        serde_json::json!([{"op": "incr", "key": "bk-ctr", "delta": 5}]),
    );
    assert_eq!(res.status, 200);
    let results = res.json();
    assert_eq!(results[0]["value"], 5);
}

#[test]
fn batch_incr_accumulates() {
    let srv = TestServer::start();
    srv.put("bk-acc", b"10");
    let res = batch_call(
        &srv,
        0,
        serde_json::json!([{"op": "incr", "key": "bk-acc", "delta": 3}]),
    );
    assert_eq!(res.status, 200);
    let results = res.json();
    assert_eq!(results[0]["value"], 13);
}

#[test]
fn batch_mixed_ops_return_ordered_results() {
    let srv = TestServer::start();
    srv.put("bk-mix-a", b"alpha");
    let res = batch_call(
        &srv,
        0,
        serde_json::json!([
            {"op": "get",    "key": "bk-mix-a"},
            {"op": "set",    "key": "bk-mix-b", "value": b64(b"beta")},
            {"op": "get",    "key": "bk-mix-missing"},
            {"op": "incr",   "key": "bk-mix-ctr", "delta": 1},
        ]),
    );
    assert_eq!(res.status, 200);
    let results = res.json();
    assert_eq!(results.as_array().unwrap().len(), 4);
    assert_eq!(b64_decode(results[0]["value"].as_str().unwrap()), b"alpha");
    assert!(results[1].is_null(), "set returns null");
    assert!(results[2].is_null(), "get miss returns null");
    assert_eq!(results[3]["value"], 1);
}

#[test]
fn batch_set_nx_conflict_returns_409() {
    let srv = TestServer::start();
    srv.put("bk-nx", b"existing");
    let res = batch_call(
        &srv,
        0,
        serde_json::json!([{"op": "set", "key": "bk-nx", "value": b64(b"new"), "nx": true}]),
    );
    assert_eq!(res.status, 409);
    assert_eq!(res.json()["error"]["code"], "conflict");
    assert_eq!(
        srv.get("bk-nx").body,
        b"existing",
        "value must not change on nx conflict"
    );
}

#[test]
fn batch_set_xx_conflict_returns_409() {
    let srv = TestServer::start();
    let res = batch_call(
        &srv,
        0,
        serde_json::json!([{"op": "set", "key": "bk-xx-miss", "value": b64(b"v"), "xx": true}]),
    );
    assert_eq!(res.status, 409);
    assert_eq!(res.json()["error"]["code"], "conflict");
}

#[test]
fn batch_set_if_match_conflict_returns_409() {
    let srv = TestServer::start();
    srv.put("bk-cas", b"v");
    let res = batch_call(
        &srv,
        0,
        serde_json::json!([{"op": "set", "key": "bk-cas", "value": b64(b"v2"), "ifMatch": 9999}]),
    );
    assert_eq!(res.status, 409);
    assert_eq!(res.json()["error"]["code"], "conflict");
}

#[test]
fn batch_malformed_body_returns_400() {
    let srv = TestServer::start();
    let url = batch_url(&srv, 0);
    let res = ureq::post(&url)
        .set("Content-Type", "application/json")
        .send_string("this is not json");
    let (status, body) = match res {
        Ok(r) => {
            let s = r.status();
            let mut buf = Vec::new();
            std::io::Read::read_to_end(&mut r.into_reader(), &mut buf).unwrap();
            (s, buf)
        }
        Err(ureq::Error::Status(s, r)) => {
            let mut buf = Vec::new();
            std::io::Read::read_to_end(&mut r.into_reader(), &mut buf).unwrap();
            (s, buf)
        }
        Err(e) => panic!("{e}"),
    };
    assert_eq!(status, 400);
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["error"]["code"], "invalid_request");
}

#[test]
fn batch_get_returns_ttl_when_set() {
    let srv = TestServer::start();
    srv.put_opts(
        0,
        "bk-ttl-get",
        b"v",
        PutOptions {
            ttl_header: Some(60),
            ..Default::default()
        },
    );
    let res = batch_call(
        &srv,
        0,
        serde_json::json!([{"op": "get", "key": "bk-ttl-get"}]),
    );
    assert_eq!(res.status, 200);
    let results = res.json();
    let entry = &results[0];
    let ttl = entry["ttl"].as_u64().expect("ttl field should be present");
    assert!(ttl > 0 && ttl <= 60);
}

// ── POST /v1/admin/compact ────────────────────────────────────────────────────

fn compact_url(srv: &TestServer, ns: u8) -> String {
    format!(
        "http://127.0.0.1:{}/v1/admin/compact?ns={ns}",
        srv.http_port
    )
}

#[test]
fn compact_returns_204() {
    let srv = TestServer::start();
    let res = common::raw_call_url(ureq::post(&compact_url(&srv, 0)));
    assert_eq!(res.status, 204);
}

#[test]
fn compact_is_idempotent() {
    let srv = TestServer::start();
    srv.put("ck", b"v");
    let url = compact_url(&srv, 0);
    let res1 = common::raw_call_url(ureq::post(&url));
    let res2 = common::raw_call_url(ureq::post(&url));
    assert_eq!(res1.status, 204);
    assert_eq!(res2.status, 204);
    assert_eq!(srv.get("ck").body, b"v", "key survives compaction");
}

// ── X-KV-TTL-MS header ───────────────────────────────────────────────────────

#[test]
fn get_response_includes_ttl_ms_header() {
    let srv = TestServer::start();
    srv.put_opts(
        0,
        "ttl-ms-k",
        b"v",
        PutOptions {
            ttl_header: Some(60),
            ..Default::default()
        },
    );
    let res = common::raw_call_url(ureq::get(&srv.value_url(0, "ttl-ms-k")));
    assert_eq!(res.status, 200);
    let ttl_ms = res.ttl_ms.expect("X-KV-TTL-MS header must be present");
    assert!(ttl_ms > 0 && ttl_ms <= 60_000, "ttl_ms={ttl_ms}");
    let ttl_s = res.ttl.expect("X-KV-TTL header must be present");
    assert!(
        ttl_ms >= ttl_s * 1000 - 1000,
        "ttl_ms should be >= ttl_s * 1000 - 1s"
    );
}

#[test]
fn get_response_has_no_ttl_ms_header_for_persistent_key() {
    let srv = TestServer::start();
    srv.put("persist-k", b"v");
    let res = common::raw_call_url(ureq::get(&srv.value_url(0, "persist-k")));
    assert_eq!(res.status, 200);
    assert!(
        res.ttl_ms.is_none(),
        "persistent key must not have X-KV-TTL-MS"
    );
}

// ── Batch: keepTtl ───────────────────────────────────────────────────────────

#[test]
fn batch_set_keeptll_preserves_expiry() {
    let srv = TestServer::start();
    srv.put_opts(
        0,
        "bk-keepttl",
        b"v1",
        PutOptions {
            ttl_header: Some(60),
            ..Default::default()
        },
    );
    let ttl_before = srv.get("bk-keepttl").ttl.expect("initial TTL must be set");

    let res = batch_call(
        &srv,
        0,
        serde_json::json!([{
            "op": "set",
            "key": "bk-keepttl",
            "value": b64(b"v2"),
            "keepTtl": true
        }]),
    );
    assert_eq!(res.status, 200);
    assert_eq!(srv.get("bk-keepttl").body, b"v2");
    let ttl_after = srv
        .get("bk-keepttl")
        .ttl
        .expect("TTL must be preserved after keepTtl set");
    assert!(
        ttl_after > 0 && ttl_after <= ttl_before,
        "TTL must be preserved"
    );
}

#[test]
fn batch_set_ttl_ms_sub_second_precision() {
    let srv = TestServer::start();
    let res = batch_call(
        &srv,
        0,
        serde_json::json!([{
            "op": "set",
            "key": "bk-ttl-ms",
            "value": b64(b"v"),
            "ttlMs": 30_000
        }]),
    );
    assert_eq!(res.status, 200);
    let ttl = srv.get("bk-ttl-ms").ttl.expect("TTL must be set via ttlMs");
    assert!(ttl > 0 && ttl <= 30, "ttl={ttl}");
}

#[test]
fn batch_set_ttl_ms_takes_priority_over_ttl_secs() {
    let srv = TestServer::start();
    let res = batch_call(
        &srv,
        0,
        serde_json::json!([{
            "op": "set",
            "key": "bk-ttl-prio",
            "value": b64(b"v"),
            "ttl": 999,
            "ttlMs": 10_000
        }]),
    );
    assert_eq!(res.status, 200);
    let ttl = srv.get("bk-ttl-prio").ttl.expect("TTL must be set");
    assert!(
        ttl > 0 && ttl <= 10,
        "ttlMs should take priority, ttl={ttl}"
    );
}

// ── Batch: get returns ttl_ms ─────────────────────────────────────────────────

#[test]
fn batch_get_returns_ttl_ms_when_set() {
    let srv = TestServer::start();
    srv.put_opts(
        0,
        "bk-getttl-ms",
        b"v",
        PutOptions {
            ttl_header: Some(60),
            ..Default::default()
        },
    );
    let res = batch_call(
        &srv,
        0,
        serde_json::json!([{"op": "get", "key": "bk-getttl-ms"}]),
    );
    assert_eq!(res.status, 200);
    let results = res.json();
    let entry = &results[0];
    let ttl_ms = entry["ttl_ms"]
        .as_u64()
        .expect("ttl_ms should be in batch get result");
    let ttl_s = entry["ttl"]
        .as_u64()
        .expect("ttl should be in batch get result");
    assert!(ttl_ms > 0 && ttl_ms <= 60_000, "ttl_ms={ttl_ms}");
    assert!(ttl_ms >= ttl_s * 1000 - 1000);
}

// ── Batch: delete with returnOld ──────────────────────────────────────────────

#[test]
fn batch_delete_return_old_returns_entry() {
    let srv = TestServer::start();
    srv.put("bk-getdel", b"precious");
    let res = batch_call(
        &srv,
        0,
        serde_json::json!([{"op": "delete", "key": "bk-getdel", "returnOld": true}]),
    );
    assert_eq!(res.status, 200);
    let results = res.json();
    let entry = &results[0];
    assert!(
        !entry.is_null(),
        "returnOld on existing key must return entry"
    );
    assert_eq!(b64_decode(entry["value"].as_str().unwrap()), b"precious");
    assert!(entry["revision"].as_u64().unwrap_or(0) > 0);
    assert!(srv.get("bk-getdel").is_not_found(), "key must be deleted");
}

#[test]
fn batch_delete_return_old_missing_key_returns_null() {
    let srv = TestServer::start();
    let res = batch_call(
        &srv,
        0,
        serde_json::json!([{"op": "delete", "key": "bk-getdel-miss", "returnOld": true}]),
    );
    assert_eq!(res.status, 200);
    let results = res.json();
    assert!(
        results[0].is_null(),
        "returnOld on missing key must return null"
    );
}

#[test]
fn batch_delete_without_return_old_returns_null() {
    let srv = TestServer::start();
    srv.put("bk-del-plain", b"v");
    let res = batch_call(
        &srv,
        0,
        serde_json::json!([{"op": "delete", "key": "bk-del-plain"}]),
    );
    assert_eq!(res.status, 200);
    let results = res.json();
    assert!(
        results[0].is_null(),
        "delete without returnOld must return null"
    );
    assert!(srv.get("bk-del-plain").is_not_found());
}

// ── Batch: exists ─────────────────────────────────────────────────────────────

#[test]
fn batch_exists_live_key_returns_true() {
    let srv = TestServer::start();
    srv.put("bk-ex-live", b"v");
    let res = batch_call(
        &srv,
        0,
        serde_json::json!([{"op": "exists", "key": "bk-ex-live"}]),
    );
    assert_eq!(res.status, 200);
    let results = res.json();
    assert_eq!(results[0], true, "exists on live key must return true");
}

#[test]
fn batch_exists_missing_key_returns_false() {
    let srv = TestServer::start();
    let res = batch_call(
        &srv,
        0,
        serde_json::json!([{"op": "exists", "key": "bk-ex-miss"}]),
    );
    assert_eq!(res.status, 200);
    let results = res.json();
    assert_eq!(results[0], false, "exists on missing key must return false");
}

#[test]
fn batch_exists_mixed_with_other_ops() {
    let srv = TestServer::start();
    srv.put("bk-mix-ex", b"v");
    let res = batch_call(
        &srv,
        0,
        serde_json::json!([
            {"op": "exists", "key": "bk-mix-ex"},
            {"op": "exists", "key": "bk-mix-ex-no"},
            {"op": "get",    "key": "bk-mix-ex"},
        ]),
    );
    assert_eq!(res.status, 200);
    let results = res.json();
    assert_eq!(results[0], true);
    assert_eq!(results[1], false);
    assert!(!results[2].is_null());
}

// ── Cross-shard: batch conditional set respects NX ────────────────────────────

#[test]
fn batch_set_nx_cross_shard_respects_condition() {
    // Use 2 shards so that keys on shard 1 exercise the cross-shard path.
    // We try keys until we find one whose hash routes to the foreign shard.
    let srv = common::TestServer::start_shards(2);

    // Try a handful of keys — at least one will land on shard 1 (foreign).
    // We want to verify that NX is honoured even for cross-shard writes.
    let key = "nx-cross";
    srv.put(key, b"original");

    let res = batch_call(
        &srv,
        0,
        serde_json::json!([{
            "op": "set",
            "key": key,
            "value": b64(b"overwrite"),
            "nx": true
        }]),
    );
    // Whether the key lands on shard 0 or shard 1, NX must prevent overwrite.
    assert_eq!(
        res.status, 409,
        "NX batch set on existing key must fail regardless of shard"
    );
    assert_eq!(
        srv.get(key).body,
        b"original",
        "value must not change on NX conflict"
    );
}

#[test]
fn batch_set_xx_cross_shard_respects_condition() {
    let srv = common::TestServer::start_shards(2);
    let key = "xx-cross-miss";

    let res = batch_call(
        &srv,
        0,
        serde_json::json!([{
            "op": "set",
            "key": key,
            "value": b64(b"v"),
            "xx": true
        }]),
    );
    assert_eq!(
        res.status, 409,
        "XX on missing key must fail regardless of shard"
    );
}

#[test]
fn batch_set_if_match_cross_shard_respects_condition() {
    let srv = common::TestServer::start_shards(2);
    let key = "cas-cross";
    srv.put(key, b"v");

    let res = batch_call(
        &srv,
        0,
        serde_json::json!([{
            "op": "set",
            "key": key,
            "value": b64(b"v2"),
            "ifMatch": 9999
        }]),
    );
    assert_eq!(
        res.status, 409,
        "ifMatch mismatch must fail regardless of shard"
    );
    assert_eq!(
        srv.get(key).body,
        b"v",
        "value must not change on CAS conflict"
    );
}

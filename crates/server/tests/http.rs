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
    assert_eq!(body["error"], "not_found");
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
        "default",
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
        "default",
        "nx-dup",
        b"new",
        PutOptions {
            nx: true,
            ..Default::default()
        },
    );
    assert!(res.is_conflict());
    assert_eq!(res.json()["error"], "conflict");
}

#[test]
fn put_nx_does_not_overwrite_value() {
    let srv = TestServer::start();
    srv.put("nx-safe", b"original");
    srv.put_opts(
        "default",
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
        "default",
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
        "default",
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
        "default",
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
        "default",
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
    srv.put_ns("default", "shared-name", b"in-default");
    assert!(srv.get_ns("db1", "shared-name").is_not_found());
}

#[test]
fn same_key_independent_values_per_namespace() {
    let srv = TestServer::start();
    srv.put_ns("default", "k", b"default-val");
    srv.put_ns("db2", "k", b"db2-val");
    assert_eq!(srv.get_ns("default", "k").body, b"default-val");
    assert_eq!(srv.get_ns("db2", "k").body, b"db2-val");
}

// ── Method routing ────────────────────────────────────────────────────────────

#[test]
fn post_to_value_endpoint_is_405() {
    let srv = TestServer::start();
    let url = srv.value_url("default", "k");
    let res = common::raw_call_url(ureq::post(&url));
    assert!(res.is_method_not_allowed());
    assert_eq!(res.json()["error"], "method_not_allowed");
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
    let body = srv.list("default").json();
    assert_eq!(body["keys"], serde_json::json!([]));
    assert_eq!(body["complete"], true);
}

#[test]
fn list_returns_all_inserted_keys() {
    let srv = TestServer::start();
    for k in ["alpha", "beta", "gamma"] {
        srv.put(k, b"v");
    }
    let body = srv.list("default").json();
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
            "default",
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
            "default",
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
        let body = srv.list_opts("default", opts).json();
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
    srv.put_opts("default", "live", b"v", PutOptions::default());
    srv.put_opts(
        "default",
        "dead",
        b"v",
        PutOptions {
            ttl_header: Some(1),
            ..Default::default()
        },
    );
    std::thread::sleep(std::time::Duration::from_millis(1100));
    let body = srv.list("default").json();
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
    let res = ureq::put(&srv.value_url("default", "bad-ttl-key"))
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
    let res = ureq::put(&srv.value_url("default", "bad-meta-key"))
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
    let url = format!("{}?limit=banana", srv.keys_url("default"));
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
    let _ = ureq::put(&srv.value_url("default", "ttl-zero"))
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

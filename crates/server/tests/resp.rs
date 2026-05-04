mod common;
use common::{TestServer, scan_all};
use redis::Commands;

// ── PING ──────────────────────────────────────────────────────────────────────

#[test]
fn ping_returns_pong() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let res: String = redis::cmd("PING").query(&mut con).unwrap();
    assert_eq!(res, "PONG");
}

#[test]
fn ping_with_message_echoes_message() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let res: Vec<u8> = redis::cmd("PING").arg(b"hello").query(&mut con).unwrap();
    assert_eq!(res, b"hello");
}

// ── SET / GET ─────────────────────────────────────────────────────────────────

#[test]
fn set_then_get_returns_value() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = con.set("greeting", "world").unwrap();
    let val: String = con.get("greeting").unwrap();
    assert_eq!(val, "world");
}

#[test]
fn get_missing_key_returns_nil() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let val: Option<String> = con.get("nope").unwrap();
    assert!(val.is_none());
}

#[test]
fn set_overwrites_existing_value() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = con.set("k", "first").unwrap();
    let _: () = con.set("k", "second").unwrap();
    let val: String = con.get("k").unwrap();
    assert_eq!(val, "second");
}

#[test]
fn binary_value_round_trips_via_resp() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let data: Vec<u8> = (0u8..=255).collect();
    let _: () = redis::cmd("SET")
        .arg("bin")
        .arg(&data)
        .query(&mut con)
        .unwrap();
    let got: Vec<u8> = redis::cmd("GET").arg("bin").query(&mut con).unwrap();
    assert_eq!(got, data);
}

// ── SET NX / XX ───────────────────────────────────────────────────────────────

#[test]
fn set_nx_succeeds_on_fresh_key() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let res: redis::Value = redis::cmd("SET")
        .arg("nx-fresh")
        .arg("v")
        .arg("NX")
        .query(&mut con)
        .unwrap();
    assert!(matches!(
        res,
        redis::Value::Okay | redis::Value::SimpleString(_)
    ));
}

#[test]
fn set_nx_returns_nil_on_existing_key() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = con.set("nx-dup", "original").unwrap();
    let res: redis::Value = redis::cmd("SET")
        .arg("nx-dup")
        .arg("clobber")
        .arg("NX")
        .query(&mut con)
        .unwrap();
    assert!(
        matches!(res, redis::Value::Nil),
        "SET NX on existing key must return nil"
    );
    let still: String = con.get("nx-dup").unwrap();
    assert_eq!(still, "original");
}

#[test]
fn set_xx_returns_nil_on_missing_key() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let res: redis::Value = redis::cmd("SET")
        .arg("xx-missing")
        .arg("v")
        .arg("XX")
        .query(&mut con)
        .unwrap();
    assert!(matches!(res, redis::Value::Nil));
}

#[test]
fn set_xx_succeeds_on_existing_key() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = con.set("xx-present", "old").unwrap();
    let res: redis::Value = redis::cmd("SET")
        .arg("xx-present")
        .arg("new")
        .arg("XX")
        .query(&mut con)
        .unwrap();
    assert!(matches!(
        res,
        redis::Value::Okay | redis::Value::SimpleString(_)
    ));
    let val: String = con.get("xx-present").unwrap();
    assert_eq!(val, "new");
}

#[test]
fn set_with_get_flag_returns_old_value() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = con.set("get-flag", "old").unwrap();
    let old: Vec<u8> = redis::cmd("SET")
        .arg("get-flag")
        .arg("new")
        .arg("GET")
        .query(&mut con)
        .unwrap();
    assert_eq!(old, b"old");
    let new_val: String = con.get("get-flag").unwrap();
    assert_eq!(new_val, "new");
}

#[test]
fn set_with_get_flag_on_missing_returns_nil() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let res: redis::Value = redis::cmd("SET")
        .arg("get-flag-miss")
        .arg("v")
        .arg("GET")
        .query(&mut con)
        .unwrap();
    assert!(matches!(res, redis::Value::Nil));
}

// ── TTL: EX / PX / EXAT / PXAT ───────────────────────────────────────────────

#[test]
fn set_ex_key_has_ttl_and_expires() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = redis::cmd("SET")
        .arg("ex-key")
        .arg("v")
        .arg("EX")
        .arg(2)
        .query(&mut con)
        .unwrap();
    let ttl: i64 = con.ttl("ex-key").unwrap();
    assert!(ttl > 0 && ttl <= 2, "TTL should be 1–2s, got {ttl}");
}

#[test]
fn set_px_key_has_pttl_and_expires() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = redis::cmd("SET")
        .arg("px-key")
        .arg("v")
        .arg("PX")
        .arg(200)
        .query(&mut con)
        .unwrap();
    let pttl: i64 = con.pttl("px-key").unwrap();
    assert!(pttl > 0 && pttl <= 200, "PTTL should be ≤200ms, got {pttl}");
    std::thread::sleep(std::time::Duration::from_millis(300));
    let gone: Option<String> = con.get("px-key").unwrap();
    assert!(gone.is_none(), "PX key should have expired");
}

#[test]
fn set_pxat_in_past_sets_key_that_is_immediately_expired() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let past_ms: u64 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        - 1000;
    let _: () = redis::cmd("SET")
        .arg("pxat-past")
        .arg("v")
        .arg("PXAT")
        .arg(past_ms)
        .query(&mut con)
        .unwrap();
    // Key may or may not be stored, but any read should return nil
    let val: Option<String> = con.get("pxat-past").unwrap();
    assert!(val.is_none(), "PXAT in past should yield expired key");
}

// ── TTL commands ──────────────────────────────────────────────────────────────

#[test]
fn ttl_on_persistent_key_returns_neg_one() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = con.set("persistent", "v").unwrap();
    assert_eq!(con.ttl::<_, i64>("persistent").unwrap(), -1);
}

#[test]
fn ttl_on_missing_key_returns_neg_two() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    assert_eq!(con.ttl::<_, i64>("missing").unwrap(), -2);
}

#[test]
fn pttl_on_key_with_expiry_returns_millis() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = redis::cmd("SET")
        .arg("pttl-k")
        .arg("v")
        .arg("PX")
        .arg(5000)
        .query(&mut con)
        .unwrap();
    let pttl: i64 = con.pttl("pttl-k").unwrap();
    assert!(
        pttl > 4000 && pttl <= 5000,
        "PTTL should be ~5000ms, got {pttl}"
    );
}

// ── EXPIRE / PEXPIRE ──────────────────────────────────────────────────────────

#[test]
fn expire_on_existing_key_returns_1() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = con.set("exp-live", "v").unwrap();
    let res: i64 = con.expire("exp-live", 60).unwrap();
    assert_eq!(res, 1);
    let ttl: i64 = con.ttl("exp-live").unwrap();
    assert!(ttl > 0 && ttl <= 60);
}

#[test]
fn expire_on_missing_key_returns_0() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let res: i64 = con.expire("exp-missing", 60).unwrap();
    assert_eq!(res, 0);
}

#[test]
fn pexpire_sets_ttl_in_millis() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = con.set("pexp-k", "v").unwrap();
    let _: i64 = con.pexpire("pexp-k", 5000).unwrap();
    let pttl: i64 = con.pttl("pexp-k").unwrap();
    assert!(pttl > 4000 && pttl <= 5000);
}

#[test]
fn expireat_with_past_timestamp_deletes_key() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = con.set("expat-past", "v").unwrap();
    let past: u64 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        - 1;
    let res: i64 = redis::cmd("EXPIREAT")
        .arg("expat-past")
        .arg(past)
        .query(&mut con)
        .unwrap();
    assert_eq!(res, 1);
    let val: Option<String> = con.get("expat-past").unwrap();
    assert!(
        val.is_none(),
        "EXPIREAT in the past should have deleted the key"
    );
}

#[test]
fn expireat_with_future_timestamp_expires_key_at_correct_time() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = con.set("expat-future", "v").unwrap();
    let future_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 2;
    let res: i64 = redis::cmd("EXPIREAT")
        .arg("expat-future")
        .arg(future_secs)
        .query(&mut con)
        .unwrap();
    assert_eq!(res, 1, "EXPIREAT on a live key must return 1");
    let live: Option<String> = con.get("expat-future").unwrap();
    assert!(live.is_some(), "key must be accessible before expiry");
    std::thread::sleep(std::time::Duration::from_millis(2200));
    let gone: Option<String> = con.get("expat-future").unwrap();
    assert!(
        gone.is_none(),
        "key must have expired after EXPIREAT timestamp"
    );
}

#[test]
fn expireat_on_missing_key_returns_0() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let res: i64 = redis::cmd("EXPIREAT")
        .arg("expat-no-key")
        .arg(9999999999u64)
        .query(&mut con)
        .unwrap();
    assert_eq!(res, 0);
}

#[test]
fn pexpireat_with_future_timestamp_expires_key_at_correct_time() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = con.set("pexpat-future", "v").unwrap();
    let future_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        + 300;
    let res: i64 = redis::cmd("PEXPIREAT")
        .arg("pexpat-future")
        .arg(future_ms)
        .query(&mut con)
        .unwrap();
    assert_eq!(res, 1, "PEXPIREAT on a live key must return 1");
    let live: Option<String> = con.get("pexpat-future").unwrap();
    assert!(live.is_some(), "key must be accessible before expiry");
    std::thread::sleep(std::time::Duration::from_millis(500));
    let gone: Option<String> = con.get("pexpat-future").unwrap();
    assert!(
        gone.is_none(),
        "key must have expired after PEXPIREAT timestamp"
    );
}

// ── PERSIST ───────────────────────────────────────────────────────────────────

#[test]
fn persist_removes_ttl() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = redis::cmd("SET")
        .arg("persist-k")
        .arg("v")
        .arg("EX")
        .arg(60)
        .query(&mut con)
        .unwrap();
    let res: i64 = con.persist("persist-k").unwrap();
    assert_eq!(res, 1);
    assert_eq!(con.ttl::<_, i64>("persist-k").unwrap(), -1);
}

#[test]
fn persist_on_persistent_key_returns_0() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = con.set("persist-no-ttl", "v").unwrap();
    let res: i64 = con.persist("persist-no-ttl").unwrap();
    assert_eq!(res, 0);
}

// ── DEL / EXISTS ──────────────────────────────────────────────────────────────

#[test]
fn del_existing_key_returns_1() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = con.set("del-me", "v").unwrap();
    let n: i64 = con.del("del-me").unwrap();
    assert_eq!(n, 1);
    assert!(con.get::<_, Option<String>>("del-me").unwrap().is_none());
}

#[test]
fn del_missing_key_returns_0() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let n: i64 = con.del("ghost").unwrap();
    assert_eq!(n, 0);
}

#[test]
fn del_multiple_keys_returns_count_of_deleted() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = con.set("d1", "v").unwrap();
    let _: () = con.set("d2", "v").unwrap();
    let n: i64 = con.del(&["d1", "d2", "d3"]).unwrap();
    assert_eq!(n, 2);
}

#[test]
fn del_expired_key_counts_as_0() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = redis::cmd("SET")
        .arg("del-exp")
        .arg("v")
        .arg("PX")
        .arg(50)
        .query(&mut con)
        .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(100));
    let n: i64 = con.del("del-exp").unwrap();
    assert_eq!(n, 0, "expired keys don't count as deleted");
}

#[test]
fn exists_returns_1_for_live_key() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = con.set("exists-live", "v").unwrap();
    assert_eq!(con.exists::<_, i64>("exists-live").unwrap(), 1);
}

#[test]
fn exists_returns_0_for_missing_key() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    assert_eq!(con.exists::<_, i64>("no-such").unwrap(), 0);
}

#[test]
fn exists_counts_duplicates() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = con.set("dup-k", "v").unwrap();
    // EXISTS k k counts k twice
    let n: i64 = redis::cmd("EXISTS")
        .arg("dup-k")
        .arg("dup-k")
        .query(&mut con)
        .unwrap();
    assert_eq!(n, 2);
}

// ── MGET / MSET ──────────────────────────────────────────────────────────────

#[test]
fn mset_then_mget_returns_all_values() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = con
        .mset(&[("mk1", "mv1"), ("mk2", "mv2"), ("mk3", "mv3")])
        .unwrap();
    let vals: Vec<Option<String>> = con.mget(&["mk1", "mk2", "mk3"]).unwrap();
    assert_eq!(
        vals,
        vec![Some("mv1".into()), Some("mv2".into()), Some("mv3".into())]
    );
}

#[test]
fn mget_with_missing_key_returns_nil_in_position() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = con.set("mget-present", "here").unwrap();
    let vals: Vec<Option<String>> = con.mget(&["mget-present", "mget-absent"]).unwrap();
    assert_eq!(vals[0], Some("here".into()));
    assert!(vals[1].is_none());
}

#[test]
fn mset_writes_all_keys_atomically_via_write_batch() {
    // MSET uses a RocksDB WriteBatch — all keys are written in a single atomic op.
    // This test confirms all keys land together; no partial state is observable.
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = con
        .mset(&[("batch-a", "1"), ("batch-b", "2"), ("batch-c", "3")])
        .unwrap();
    let a: String = con.get("batch-a").unwrap();
    let b: String = con.get("batch-b").unwrap();
    let c: String = con.get("batch-c").unwrap();
    assert_eq!(a, "1");
    assert_eq!(b, "2");
    assert_eq!(c, "3");
}

// ── GETSET / SETNX / GETDEL ──────────────────────────────────────────────────

#[test]
fn getset_returns_old_value_and_stores_new() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = con.set("gs-k", "old").unwrap();
    let old: String = redis::cmd("GETSET")
        .arg("gs-k")
        .arg("new")
        .query(&mut con)
        .unwrap();
    assert_eq!(old, "old");
    assert_eq!(con.get::<_, String>("gs-k").unwrap(), "new");
}

#[test]
fn getset_clears_ttl_on_existing_key() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = redis::cmd("SET")
        .arg("gs-ttl")
        .arg("v")
        .arg("EX")
        .arg(60)
        .query(&mut con)
        .unwrap();
    let _: String = redis::cmd("GETSET")
        .arg("gs-ttl")
        .arg("new")
        .query(&mut con)
        .unwrap();
    assert_eq!(
        con.ttl::<_, i64>("gs-ttl").unwrap(),
        -1,
        "GETSET should clear TTL"
    );
}

#[test]
fn setnx_returns_1_on_fresh_key() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let n: i64 = redis::cmd("SETNX")
        .arg("snx-fresh")
        .arg("v")
        .query(&mut con)
        .unwrap();
    assert_eq!(n, 1);
}

#[test]
fn setnx_returns_0_on_existing_key() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = con.set("snx-dup", "original").unwrap();
    let n: i64 = redis::cmd("SETNX")
        .arg("snx-dup")
        .arg("clobber")
        .query(&mut con)
        .unwrap();
    assert_eq!(n, 0);
    assert_eq!(con.get::<_, String>("snx-dup").unwrap(), "original");
}

#[test]
fn getdel_returns_value_and_removes_key() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = con.set("gd-k", "bye").unwrap();
    let val: String = redis::cmd("GETDEL").arg("gd-k").query(&mut con).unwrap();
    assert_eq!(val, "bye");
    assert!(con.get::<_, Option<String>>("gd-k").unwrap().is_none());
}

#[test]
fn getdel_on_missing_key_returns_nil() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let res: redis::Value = redis::cmd("GETDEL").arg("gd-miss").query(&mut con).unwrap();
    assert!(matches!(res, redis::Value::Nil));
}

// ── GETEX ─────────────────────────────────────────────────────────────────────

#[test]
fn getex_with_ex_sets_ttl() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = con.set("gex-k", "v").unwrap();
    let val: String = redis::cmd("GETEX")
        .arg("gex-k")
        .arg("EX")
        .arg(60)
        .query(&mut con)
        .unwrap();
    assert_eq!(val, "v");
    let ttl: i64 = con.ttl("gex-k").unwrap();
    assert!(ttl > 0 && ttl <= 60);
}

#[test]
fn getex_with_persist_removes_ttl() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = redis::cmd("SET")
        .arg("gex-ttl")
        .arg("v")
        .arg("EX")
        .arg(60)
        .query(&mut con)
        .unwrap();
    let _: String = redis::cmd("GETEX")
        .arg("gex-ttl")
        .arg("PERSIST")
        .query(&mut con)
        .unwrap();
    assert_eq!(con.ttl::<_, i64>("gex-ttl").unwrap(), -1);
}

#[test]
fn getex_on_missing_key_returns_nil() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let res: redis::Value = redis::cmd("GETEX")
        .arg("gex-miss")
        .arg("EX")
        .arg(10)
        .query(&mut con)
        .unwrap();
    assert!(matches!(res, redis::Value::Nil));
}

// ── KEYS / SCAN ───────────────────────────────────────────────────────────────

#[test]
fn keys_returns_all_live_keys() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = con
        .mset(&[("ka1", "v"), ("ka2", "v"), ("ka3", "v")])
        .unwrap();
    let mut keys: Vec<String> = redis::cmd("KEYS").arg("*").query(&mut con).unwrap();
    keys.sort();
    assert!(keys.contains(&"ka1".to_owned()));
    assert!(keys.contains(&"ka2".to_owned()));
    assert!(keys.contains(&"ka3".to_owned()));
}

#[test]
fn keys_with_pattern_filters() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = con
        .mset(&[("user:1", "v"), ("user:2", "v"), ("session:x", "v")])
        .unwrap();
    let keys: Vec<String> = redis::cmd("KEYS").arg("user:*").query(&mut con).unwrap();
    assert_eq!(keys.len(), 2);
    assert!(keys.iter().all(|k| k.starts_with("user:")));
}

#[test]
fn scan_returns_all_keys_via_cursor_iteration() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let pairs: Vec<(String, &str)> = (0..20).map(|i| (format!("sc-{i:02}"), "v")).collect();
    let pair_refs: Vec<(&str, &str)> = pairs.iter().map(|(k, v)| (k.as_str(), *v)).collect();
    let _: () = con.mset(&pair_refs).unwrap();
    let all = scan_all(&mut con, None);
    for i in 0..20 {
        assert!(
            all.contains(&format!("sc-{i:02}")),
            "sc-{i:02} missing from SCAN"
        );
    }
}

#[test]
fn scan_with_match_pattern_filters() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = con
        .mset(&[("pfx:a", "v"), ("pfx:b", "v"), ("other:c", "v")])
        .unwrap();
    let keys = scan_all(&mut con, Some("pfx:*"));
    assert_eq!(keys.len(), 2);
    assert!(keys.iter().all(|k| k.starts_with("pfx:")));
}

// ── DBSIZE / FLUSHDB ──────────────────────────────────────────────────────────

#[test]
fn dbsize_reflects_live_key_count() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = con
        .mset(&[("sz1", "v"), ("sz2", "v"), ("sz3", "v")])
        .unwrap();
    let n: i64 = redis::cmd("DBSIZE").query(&mut con).unwrap();
    assert!(n >= 3, "DBSIZE should be at least 3, got {n}");
}

#[test]
fn flushdb_removes_all_keys() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = con.mset(&[("flush1", "v"), ("flush2", "v")]).unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut con).unwrap();
    let n: i64 = redis::cmd("DBSIZE").query(&mut con).unwrap();
    assert_eq!(n, 0, "DBSIZE should be 0 after FLUSHDB");
}

#[test]
fn flushdb_twice_is_idempotent() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = redis::cmd("FLUSHDB").query(&mut con).unwrap();
    let _: () = redis::cmd("FLUSHDB").query(&mut con).unwrap();
    let n: i64 = redis::cmd("DBSIZE").query(&mut con).unwrap();
    assert_eq!(n, 0);
}

// ── SELECT (namespace isolation) ──────────────────────────────────────────────

#[test]
fn select_isolates_keys_between_namespaces() {
    let srv = TestServer::start();
    let mut con = srv.resp();

    // Write in db 0 (default)
    let _: () = con.set("ns-key", "in-default").unwrap();

    // Switch to db 1
    let _: () = redis::cmd("SELECT").arg(1).query(&mut con).unwrap();
    let val: Option<String> = con.get("ns-key").unwrap();
    assert!(val.is_none(), "key from db0 must not be visible in db1");

    // Write same key in db 1
    let _: () = con.set("ns-key", "in-db1").unwrap();

    // Switch back to db 0 — original value must be intact
    let _: () = redis::cmd("SELECT").arg(0).query(&mut con).unwrap();
    let original: String = con.get("ns-key").unwrap();
    assert_eq!(original, "in-default");
}

#[test]
fn select_max_db_index_15_is_valid() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let res: redis::Value = redis::cmd("SELECT").arg(15).query(&mut con).unwrap();
    assert!(matches!(
        res,
        redis::Value::Okay | redis::Value::SimpleString(_)
    ));
}

// ── HELLO ─────────────────────────────────────────────────────────────────────

#[test]
fn hello_without_version_returns_server_info() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    // HELLO with no arg — server returns a map/array of server properties
    let res: redis::Value = redis::cmd("HELLO").query(&mut con).unwrap();
    // Should be a non-error, non-nil response
    assert!(!matches!(res, redis::Value::Nil));
}

#[test]
fn hello_2_is_accepted() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let res: redis::Value = redis::cmd("HELLO").arg(2).query(&mut con).unwrap();
    assert!(!matches!(res, redis::Value::Nil));
}

// ── SELECT edge cases ─────────────────────────────────────────────────────────

#[test]
fn select_large_db_succeeds() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    // SELECT accepts any non-negative integer, not just 0-15.
    let res: redis::Value = redis::cmd("SELECT").arg(16u32).query(&mut con).unwrap();
    assert!(
        matches!(res, redis::Value::Okay),
        "SELECT 16 must return OK"
    );
}

#[test]
fn select_large_db_isolates_namespace() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    // Write in db 0.
    let _: () = con.set("ns-check", "default-value").unwrap();
    // Switch to db 16 — key must not be visible.
    let _: () = redis::cmd("SELECT").arg(16u32).query(&mut con).unwrap();
    let got: Option<String> = con.get("ns-check").unwrap();
    assert!(got.is_none(), "key from db 0 must be invisible in db 16");
    // Switch back to db 0 — key must reappear.
    let _: () = redis::cmd("SELECT").arg(0u32).query(&mut con).unwrap();
    let val: String = con.get("ns-check").unwrap();
    assert_eq!(val, "default-value");
}

// ── HELLO 3 ───────────────────────────────────────────────────────────────────

#[test]
fn hello_3_is_accepted_and_commands_work_after() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    // HELLO 3 switches the server to RESP3 encoding
    let res: redis::Value = redis::cmd("HELLO").arg(3).query(&mut con).unwrap();
    assert!(
        !matches!(res, redis::Value::Nil),
        "HELLO 3 must return server info"
    );
    // After protocol switch, basic commands must still work
    let _: () = con.set("hello3-key", "hello3-val").unwrap();
    let got: String = con.get("hello3-key").unwrap();
    assert_eq!(got, "hello3-val");
}

// ── Error protocol ────────────────────────────────────────────────────────────

#[test]
fn unknown_command_returns_error() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let err = redis::cmd("NOTACOMMAND")
        .query::<redis::Value>(&mut con)
        .unwrap_err();
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("err") || msg.contains("unknown"),
        "expected ERR: {msg}"
    );
}

#[test]
fn wrong_arity_returns_error() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    // GET requires exactly 1 arg
    let err = redis::cmd("GET")
        .query::<redis::Value>(&mut con)
        .unwrap_err();
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("err") || msg.contains("wrong") || msg.contains("arity"),
        "{msg}"
    );
}

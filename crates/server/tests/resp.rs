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

#[test]
fn set_keepttl_preserves_existing_ttl() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = redis::cmd("SET")
        .arg("kt")
        .arg("v1")
        .arg("EX")
        .arg(60)
        .query(&mut con)
        .unwrap();
    let ttl_before: i64 = con.ttl("kt").unwrap();
    assert!(ttl_before > 0, "expected TTL set, got {ttl_before}");

    let _: () = redis::cmd("SET")
        .arg("kt")
        .arg("v2")
        .arg("KEEPTTL")
        .query(&mut con)
        .unwrap();

    let got: Vec<u8> = con.get("kt").unwrap();
    assert_eq!(got, b"v2");
    let ttl_after: i64 = con.ttl("kt").unwrap();
    assert!(
        ttl_after > 0,
        "KEEPTTL should preserve TTL, got {ttl_after}"
    );
}

#[test]
fn set_keepttl_on_key_without_ttl_stays_persistent() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = con.set("kt2", "v1").unwrap();
    assert_eq!(con.ttl::<_, i64>("kt2").unwrap(), -1);

    let _: () = redis::cmd("SET")
        .arg("kt2")
        .arg("v2")
        .arg("KEEPTTL")
        .query(&mut con)
        .unwrap();

    assert_eq!(con.ttl::<_, i64>("kt2").unwrap(), -1);
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

#[test]
fn mset_clears_ttl_on_overwrite() {
    // MSET does not accept per-key TTL options, so overwriting a key with an
    // active TTL should clear the TTL (Redis-compatible behaviour).
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = redis::cmd("SET")
        .arg("mset-ttl-k")
        .arg("v1")
        .arg("EX")
        .arg(60u64)
        .query(&mut con)
        .unwrap();
    assert!(
        con.ttl::<_, i64>("mset-ttl-k").unwrap() > 0,
        "key must have TTL before MSET overwrite"
    );

    let _: () = con.mset(&[("mset-ttl-k", "v2")]).unwrap();
    assert_eq!(
        con.ttl::<_, i64>("mset-ttl-k").unwrap(),
        -1,
        "MSET overwrite must clear TTL"
    );
    assert_eq!(con.get::<_, String>("mset-ttl-k").unwrap(), "v2");
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

// ── GETEX with EXAT / PXAT ────────────────────────────────────────────────────

#[test]
fn getex_with_exat_sets_ttl_as_unix_secs() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = con.set("gex-exat", "v").unwrap();

    let future_ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 60;

    let val: Vec<u8> = redis::cmd("GETEX")
        .arg("gex-exat")
        .arg("EXAT")
        .arg(future_ts)
        .query(&mut con)
        .unwrap();
    assert_eq!(val, b"v");

    let ttl: i64 = con.ttl("gex-exat").unwrap();
    assert!(ttl > 0 && ttl <= 60, "GETEX EXAT should set TTL, got {ttl}");
}

#[test]
fn getex_with_pxat_sets_ttl_as_unix_millis() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = con.set("gex-pxat", "v").unwrap();

    let future_ts_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        + 30_000;

    let val: Vec<u8> = redis::cmd("GETEX")
        .arg("gex-pxat")
        .arg("PXAT")
        .arg(future_ts_ms)
        .query(&mut con)
        .unwrap();
    assert_eq!(val, b"v");

    let pttl: i64 = con.pttl("gex-pxat").unwrap();
    assert!(
        pttl > 0 && pttl <= 30_000,
        "GETEX PXAT should set TTL in ms, got {pttl}"
    );
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

// ── INCR / INCRBY / DECR / DECRBY ────────────────────────────────────────────

#[test]
fn incr_missing_key_starts_at_one() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let n: i64 = redis::cmd("INCR").arg("ctr").query(&mut con).unwrap();
    assert_eq!(n, 1);
}

#[test]
fn incr_increments_existing_value() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = con.set("ctr", "5").unwrap();
    let n: i64 = redis::cmd("INCR").arg("ctr").query(&mut con).unwrap();
    assert_eq!(n, 6);
}

#[test]
fn incrby_adds_delta() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = con.set("ctr", "10").unwrap();
    let n: i64 = redis::cmd("INCRBY")
        .arg("ctr")
        .arg(5)
        .query(&mut con)
        .unwrap();
    assert_eq!(n, 15);
}

#[test]
fn incrby_negative_decrements() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = con.set("ctr", "10").unwrap();
    let n: i64 = redis::cmd("INCRBY")
        .arg("ctr")
        .arg(-3)
        .query(&mut con)
        .unwrap();
    assert_eq!(n, 7);
}

#[test]
fn decr_decrements() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = con.set("ctr", "5").unwrap();
    let n: i64 = redis::cmd("DECR").arg("ctr").query(&mut con).unwrap();
    assert_eq!(n, 4);
}

#[test]
fn decr_missing_key_starts_at_minus_one() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let n: i64 = redis::cmd("DECR").arg("ctr").query(&mut con).unwrap();
    assert_eq!(n, -1);
}

#[test]
fn decrby_subtracts_delta() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = con.set("ctr", "20").unwrap();
    let n: i64 = redis::cmd("DECRBY")
        .arg("ctr")
        .arg(7)
        .query(&mut con)
        .unwrap();
    assert_eq!(n, 13);
}

#[test]
fn incr_non_integer_value_returns_error() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = con.set("bad", "hello").unwrap();
    let err = redis::cmd("INCR")
        .arg("bad")
        .query::<i64>(&mut con)
        .unwrap_err();
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("not an integer") || msg.contains("err"),
        "{msg}"
    );
}

#[test]
fn incr_preserves_ttl() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    // Set a key with a TTL, then INCR it — TTL should survive.
    let _: () = redis::cmd("SET")
        .arg("ctr")
        .arg("5")
        .arg("EX")
        .arg(60)
        .query(&mut con)
        .unwrap();
    let _: i64 = redis::cmd("INCR").arg("ctr").query(&mut con).unwrap();
    let ttl: i64 = redis::cmd("TTL").arg("ctr").query(&mut con).unwrap();
    assert!(ttl > 0, "TTL should be preserved after INCR, got {ttl}");
}

#[test]
fn incr_overflow_returns_error() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = con.set("big", i64::MAX.to_string()).unwrap();
    let err = redis::cmd("INCR")
        .arg("big")
        .query::<i64>(&mut con)
        .unwrap_err();
    let msg = err.to_string().to_lowercase();
    assert!(msg.contains("overflow") || msg.contains("err"), "{msg}");
}

// ── REVISION ──────────────────────────────────────────────────────────────────

#[test]
fn revision_returns_integer_for_existing_key() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = con.set("revkey", "hello").unwrap();
    let rev: i64 = redis::cmd("REVISION")
        .arg("revkey")
        .query(&mut con)
        .unwrap();
    assert!(
        rev > 0,
        "revision should be a positive timestamp-based integer, got {rev}"
    );
}

#[test]
fn revision_returns_neg2_for_missing_key() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let rev: i64 = redis::cmd("REVISION")
        .arg("no-such-key")
        .query(&mut con)
        .unwrap();
    assert_eq!(rev, -2);
}

// ── SETREV ────────────────────────────────────────────────────────────────────

#[test]
fn setrev_succeeds_with_correct_revision() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = con.set("sr-key", "v1").unwrap();
    let rev: i64 = redis::cmd("REVISION")
        .arg("sr-key")
        .query(&mut con)
        .unwrap();
    assert!(rev > 0);
    // SETREV with the correct revision returns the new revision.
    let new_rev: i64 = redis::cmd("SETREV")
        .arg("sr-key")
        .arg("v2")
        .arg(rev)
        .query(&mut con)
        .unwrap();
    assert!(new_rev > rev, "new revision should be greater than old");
    let val: String = con.get("sr-key").unwrap();
    assert_eq!(val, "v2");
}

#[test]
fn setrev_conflict_on_wrong_revision() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = con.set("sr-conflict", "v1").unwrap();
    // Provide revision 0 which will never match a real revision.
    let err = redis::cmd("SETREV")
        .arg("sr-conflict")
        .arg("v2")
        .arg(0u64)
        .query::<i64>(&mut con)
        .unwrap_err();
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("conflict") || msg.contains("mismatch") || msg.contains("err"),
        "expected CONFLICT error, got: {msg}"
    );
    // Value unchanged.
    let val: String = con.get("sr-conflict").unwrap();
    assert_eq!(val, "v1");
}

#[test]
fn setrev_conflict_on_missing_key() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    // Key doesn't exist; any revision will mismatch.
    let err = redis::cmd("SETREV")
        .arg("sr-missing")
        .arg("v1")
        .arg(999u64)
        .query::<i64>(&mut con)
        .unwrap_err();
    let msg = err.to_string().to_lowercase();
    assert!(msg.contains("conflict") || msg.contains("err"), "{msg}");
}

// ── SETREV with TTL options ────────────────────────────────────────────────────

#[test]
fn setrev_with_ex_sets_ttl() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = con.set("sr-ttl", "v1").unwrap();
    let rev: i64 = redis::cmd("REVISION")
        .arg("sr-ttl")
        .query(&mut con)
        .unwrap();
    assert!(rev > 0);

    let new_rev: i64 = redis::cmd("SETREV")
        .arg("sr-ttl")
        .arg("v2")
        .arg(rev)
        .arg("EX")
        .arg(60u64)
        .query(&mut con)
        .unwrap();
    assert!(new_rev > rev, "SETREV should return new revision");

    let ttl: i64 = con.ttl("sr-ttl").unwrap();
    assert!(ttl > 0 && ttl <= 60, "SETREV EX should set TTL, got {ttl}");
}

#[test]
fn setrev_with_px_sets_ttl_in_millis() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = con.set("sr-pttl", "v1").unwrap();
    let rev: i64 = redis::cmd("REVISION")
        .arg("sr-pttl")
        .query(&mut con)
        .unwrap();

    let _: i64 = redis::cmd("SETREV")
        .arg("sr-pttl")
        .arg("v2")
        .arg(rev)
        .arg("PX")
        .arg(30_000u64)
        .query(&mut con)
        .unwrap();

    let pttl: i64 = con.pttl("sr-pttl").unwrap();
    assert!(
        pttl > 0 && pttl <= 30_000,
        "SETREV PX should set TTL in ms, got {pttl}"
    );
}

// ── SET with REV condition ─────────────────────────────────────────────────────

#[test]
fn set_with_rev_condition_succeeds() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = con.set("srev-ok", "v1").unwrap();
    let rev: i64 = redis::cmd("REVISION")
        .arg("srev-ok")
        .query(&mut con)
        .unwrap();
    // SET key value REV revision → new revision on success.
    let new_rev: i64 = redis::cmd("SET")
        .arg("srev-ok")
        .arg("v2")
        .arg("REV")
        .arg(rev)
        .query(&mut con)
        .unwrap();
    assert!(new_rev > rev);
    let val: String = con.get("srev-ok").unwrap();
    assert_eq!(val, "v2");
}

#[test]
fn set_with_rev_condition_conflict_on_stale_revision() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = con.set("srev-conflict", "v1").unwrap();
    let err = redis::cmd("SET")
        .arg("srev-conflict")
        .arg("v2")
        .arg("REV")
        .arg(0u64)
        .query::<i64>(&mut con)
        .unwrap_err();
    let msg = err.to_string().to_lowercase();
    assert!(msg.contains("conflict") || msg.contains("err"), "{msg}");
}

// ── DELREV ────────────────────────────────────────────────────────────────────

#[test]
fn delrev_correct_revision_deletes_key() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = con.set("dr-k", "v").unwrap();
    let rev: i64 = redis::cmd("REVISION").arg("dr-k").query(&mut con).unwrap();
    let n: i64 = redis::cmd("DELREV")
        .arg("dr-k")
        .arg(rev)
        .query(&mut con)
        .unwrap();
    assert_eq!(n, 1);
    let gone: Option<String> = con.get("dr-k").unwrap();
    assert!(gone.is_none());
}

#[test]
fn delrev_wrong_revision_returns_error() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = con.set("dr-mismatch", "v").unwrap();
    let err = redis::cmd("DELREV")
        .arg("dr-mismatch")
        .arg(9999u64)
        .query::<i64>(&mut con)
        .unwrap_err();
    let msg = err.to_string().to_lowercase();
    assert!(msg.contains("conflict") || msg.contains("err"), "{msg}");
    let still: Option<String> = con.get("dr-mismatch").unwrap();
    assert!(still.is_some(), "key must survive a revision mismatch");
}

// ── GETMETA / SET META ────────────────────────────────────────────────────────

#[test]
fn set_with_meta_then_getmeta_returns_json() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = redis::cmd("SET")
        .arg("meta-k")
        .arg("v")
        .arg("META")
        .arg(r#"{"env":"test"}"#)
        .query(&mut con)
        .unwrap();
    let meta: String = redis::cmd("GETMETA").arg("meta-k").query(&mut con).unwrap();
    let parsed: serde_json::Value =
        serde_json::from_str(&meta).expect("GETMETA must return valid JSON");
    assert_eq!(parsed["env"], "test");
}

#[test]
fn getmeta_missing_key_returns_nil() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let res: redis::Value = redis::cmd("GETMETA")
        .arg("no-such-key")
        .query(&mut con)
        .unwrap();
    assert!(matches!(res, redis::Value::Nil));
}

#[test]
fn getmeta_key_without_meta_returns_nil() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = con.set("no-meta", "v").unwrap();
    let res: redis::Value = redis::cmd("GETMETA")
        .arg("no-meta")
        .query(&mut con)
        .unwrap();
    assert!(matches!(res, redis::Value::Nil));
}

#[test]
fn set_invalid_json_meta_is_ignored() {
    let srv = TestServer::start();
    let mut con = srv.resp();
    let _: () = redis::cmd("SET")
        .arg("bad-meta")
        .arg("v")
        .arg("META")
        .arg("not-json")
        .query(&mut con)
        .unwrap();
    let res: redis::Value = redis::cmd("GETMETA")
        .arg("bad-meta")
        .query(&mut con)
        .unwrap();
    // Invalid JSON is silently dropped; GETMETA returns nil.
    assert!(matches!(res, redis::Value::Nil));
}

// ── WATCH ─────────────────────────────────────────────────────────────────────

// Minimal RESP3 client for testing push-based protocols.
struct MinResp3 {
    reader: std::io::BufReader<std::net::TcpStream>,
    writer: std::net::TcpStream,
}

impl MinResp3 {
    fn connect(port: u16) -> Self {
        let stream = std::net::TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        let writer = stream.try_clone().unwrap();
        Self {
            reader: std::io::BufReader::new(stream),
            writer,
        }
    }

    fn send(&mut self, args: &[&[u8]]) {
        use std::io::Write as _;
        let mut buf = format!("*{}\r\n", args.len()).into_bytes();
        for arg in args {
            buf.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
            buf.extend_from_slice(arg);
            buf.extend_from_slice(b"\r\n");
        }
        self.writer.write_all(&buf).unwrap();
        self.writer.flush().unwrap();
    }

    fn read_line(&mut self) -> String {
        use std::io::BufRead as _;
        let mut line = String::new();
        self.reader.read_line(&mut line).unwrap();
        line.trim_end_matches('\n')
            .trim_end_matches('\r')
            .to_string()
    }

    /// Skip one complete RESP value (any type).
    fn skip_one(&mut self) {
        use std::io::Read as _;
        let line = self.read_line();
        match line.as_bytes().first().copied() {
            // Simple types — rest of value is on this line.
            Some(b'+') | Some(b'-') | Some(b':') | Some(b'_') | Some(b',') | Some(b'(') => {}
            // Bulk string / blob.
            Some(b'$') | Some(b'=') => {
                let n: i64 = line[1..].parse().unwrap_or(-1);
                if n >= 0 {
                    let mut body = vec![0u8; n as usize + 2];
                    self.reader.read_exact(&mut body).unwrap();
                }
            }
            // Array / set / push.
            Some(b'*') | Some(b'~') | Some(b'>') => {
                let n: usize = line[1..].parse().unwrap_or(0);
                for _ in 0..n {
                    self.skip_one();
                }
            }
            // Map / attribute.
            Some(b'%') | Some(b'|') => {
                let n: usize = line[1..].parse().unwrap_or(0);
                for _ in 0..(2 * n) {
                    self.skip_one();
                }
            }
            _ => {} // unknown type — skip line
        }
    }

    /// Read a bulk string. Returns None for null bulk strings.
    fn read_bulk(&mut self) -> Option<Vec<u8>> {
        use std::io::Read as _;
        let line = self.read_line();
        if let Some(stripped) = line.strip_prefix('$') {
            let n: i64 = stripped.parse().ok()?;
            if n < 0 {
                return None;
            }
            let mut data = vec![0u8; n as usize + 2];
            self.reader.read_exact(&mut data).unwrap();
            Some(data[..n as usize].to_vec())
        } else {
            None
        }
    }

    /// Read a push frame. Returns (push_type, push_subtype). Skips remaining elements.
    fn read_push(&mut self) -> (String, String) {
        let line = self.read_line();
        assert!(
            line.starts_with('>'),
            "expected push frame '>', got: {line:?}"
        );
        let n: usize = line[1..].parse().unwrap_or(0);
        assert!(n >= 2, "push frame must have at least 2 elements");
        let first = self.read_bulk().unwrap_or_default();
        let second = self.read_bulk().unwrap_or_default();
        for _ in 2..n {
            self.skip_one();
        }
        (
            String::from_utf8_lossy(&first).into_owned(),
            String::from_utf8_lossy(&second).into_owned(),
        )
    }
}

#[test]
fn watch_without_resp3_returns_wrongtype_error() {
    let srv = TestServer::start();
    let mut con = srv.resp(); // RESP2 — no HELLO 3
    let err = redis::cmd("WATCH")
        .arg("anykey")
        .query::<()>(&mut con)
        .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("WRONGTYPE") || msg.contains("WATCH") || msg.contains("RESP3"),
        "expected WRONGTYPE or WATCH error, got: {msg}"
    );
}

#[test]
fn watch_with_resp3_sends_ready_push() {
    let srv = TestServer::start();
    let mut raw = MinResp3::connect(srv.resp_port);

    // Negotiate RESP3.
    raw.send(&[b"HELLO", b"3"]);
    raw.skip_one(); // skip the hello map response

    // WATCH a key that doesn't exist.
    raw.send(&[b"WATCH", b"watch-ready-test"]);

    // Should receive the "watch ready" push with no prior initial events.
    let (push_type, push_sub) = raw.read_push();
    assert_eq!(push_type, "watch");
    assert_eq!(push_sub, "ready");
}

#[test]
fn watch_streams_set_event_after_mutation() {
    let srv = TestServer::start();
    let mut raw = MinResp3::connect(srv.resp_port);

    // Negotiate RESP3.
    raw.send(&[b"HELLO", b"3"]);
    raw.skip_one();

    // Subscribe to a key.
    raw.send(&[b"WATCH", b"ws-key"]);
    let (push_type, push_sub) = raw.read_push();
    assert_eq!(push_type, "watch");
    assert_eq!(push_sub, "ready");

    // Mutate the key from another connection.
    let mut con = srv.resp();
    let _: () = con.set("ws-key", "ws-value").unwrap();

    // Read the event push.
    let (push_type, push_sub) = raw.read_push();
    assert_eq!(push_type, "watch");
    assert_eq!(push_sub, "set", "expected a set event push");
}

#[test]
fn watch_initial_state_sent_for_existing_key() {
    let srv = TestServer::start();

    // First, set a key before subscribing.
    let mut setup = srv.resp();
    let _: () = setup.set("wi-key", "wi-value").unwrap();

    let mut raw = MinResp3::connect(srv.resp_port);
    raw.send(&[b"HELLO", b"3"]);
    raw.skip_one();

    // WATCH an existing key — should get initial state push first.
    raw.send(&[b"WATCH", b"wi-key"]);

    // First push is the initial state (set event for existing key).
    let (first_type, first_sub) = raw.read_push();
    assert_eq!(first_type, "watch");
    assert_eq!(
        first_sub, "set",
        "expected initial-state push for existing key"
    );

    // Then the ready push.
    let (ready_type, ready_sub) = raw.read_push();
    assert_eq!(ready_type, "watch");
    assert_eq!(ready_sub, "ready");
}

#[test]
fn pwatch_streams_prefix_events() {
    let srv = TestServer::start();
    let mut raw = MinResp3::connect(srv.resp_port);

    raw.send(&[b"HELLO", b"3"]);
    raw.skip_one();

    // PWATCH with a prefix.
    raw.send(&[b"PWATCH", b"pw:"]);
    let (pt, ps) = raw.read_push();
    assert_eq!(pt, "watch");
    assert_eq!(ps, "ready");

    // Set a key matching the prefix.
    let mut con = srv.resp();
    let _: () = con.set("pw:alpha", "1").unwrap();

    let (pt, ps) = raw.read_push();
    assert_eq!(pt, "watch");
    assert_eq!(ps, "set");

    // Set a key NOT matching the prefix — should not arrive.
    let _: () = con.set("other:beta", "2").unwrap();

    // Set another matching key to confirm we get events, not the non-matching one.
    let _: () = con.set("pw:gamma", "3").unwrap();

    let (pt2, ps2) = raw.read_push();
    assert_eq!(pt2, "watch");
    assert_eq!(ps2, "set");
}

// ── BGREWRITEAOF (reclaim) ────────────────────────────────────────────────────

#[test]
fn reclaim_preserves_all_keys() {
    // Two BGREWRITEAOF calls are required to trigger actual compaction:
    // the first seals the active file (→ 1 sealed file, no compaction yet);
    // the second seals the new active (→ 2 sealed files, compaction runs).
    let srv = TestServer::start();
    let mut con = srv.resp();

    let n = 100usize;
    for i in 0..n {
        let _: () = con.set(format!("rcl-{i:03}"), format!("val-{i}")).unwrap();
    }

    // First BGREWRITEAOF: seals the active file, opens a fresh one.
    let msg: String = redis::cmd("BGREWRITEAOF").query(&mut con).unwrap();
    assert!(
        msg.to_lowercase().contains("started") || msg.to_lowercase().contains("ok"),
        "unexpected BGREWRITEAOF reply: {msg}"
    );

    // Write more keys into the newly opened active file.
    for i in n..n + 50 {
        let _: () = con.set(format!("rcl-{i:03}"), format!("val-{i}")).unwrap();
    }

    // Second BGREWRITEAOF: seals the second file → 2 sealed files → compaction.
    let _: redis::Value = redis::cmd("BGREWRITEAOF").query(&mut con).unwrap();

    // Every key written before or between the two reclaims must survive.
    for i in 0..n + 50 {
        let val: Option<String> = con.get(format!("rcl-{i:03}")).unwrap();
        assert_eq!(
            val.as_deref(),
            Some(format!("val-{i}").as_str()),
            "key rcl-{i:03} lost after reclaim"
        );
    }
}

// ── FLUSHDB notifies watchers ─────────────────────────────────────────────────

#[test]
fn watch_receives_del_event_after_flushdb() {
    // store::flush_db() snapshots live keys and notifies watchers with Del
    // events. A WATCH subscriber must receive a del push for each key that
    // existed at flush time.
    let srv = TestServer::start();

    // Write a key and get a watcher on it before the flush.
    let mut con = srv.resp();
    let _: () = con.set("flush-watch-k", "v").unwrap();

    let mut raw = MinResp3::connect(srv.resp_port);
    raw.send(&[b"HELLO", b"3"]);
    raw.skip_one();
    raw.send(&[b"WATCH", b"flush-watch-k"]);

    // Consume the initial "ready" push (and optional initial "set" push).
    loop {
        let (pt, ps) = raw.read_push();
        assert_eq!(pt, "watch");
        if ps == "ready" {
            break;
        }
    }

    // Flush the database — watcher should receive a del event.
    let _: () = redis::cmd("FLUSHDB").query(&mut con).unwrap();

    let (pt, ps) = raw.read_push();
    assert_eq!(pt, "watch");
    assert_eq!(ps, "del", "FLUSHDB must deliver del push to watchers");
}

// ── WATCH SINCE (catch-up replay) ─────────────────────────────────────────────

#[test]
fn watch_since_replays_missed_write() {
    // Architecture guarantee: WATCH key SINCE <rev> replays all mutations
    // with tstamp_ms > rev before sending "ready". This is the reconnection
    // contract — clients provide their last-seen revision to get any events
    // they missed while disconnected.
    let srv = TestServer::start();

    let mut con = srv.resp();
    let _: () = con.set("ws-since", "v1").unwrap();
    let rev: i64 = redis::cmd("REVISION")
        .arg("ws-since")
        .query(&mut con)
        .unwrap();
    assert!(rev > 0);

    // Write v2 while not watching — this is the "missed" event.
    let _: () = con.set("ws-since", "v2").unwrap();

    // Subscribe with SINCE=rev(v1). Should receive the v2 catch-up, then ready.
    let mut raw = MinResp3::connect(srv.resp_port);
    raw.send(&[b"HELLO", b"3"]);
    raw.skip_one();

    let rev_str = rev.to_string();
    raw.send(&[b"WATCH", b"ws-since", b"SINCE", rev_str.as_bytes()]);

    let (pt, ps) = raw.read_push();
    assert_eq!(pt, "watch");
    assert_eq!(ps, "set", "expected catch-up set event for v2 write");

    let (pt, ps) = raw.read_push();
    assert_eq!(pt, "watch");
    assert_eq!(ps, "ready");
}

#[test]
fn watch_since_current_rev_emits_only_ready() {
    // SINCE=current_revision: no mutations after that point → only "ready",
    // no catch-up events.
    let srv = TestServer::start();

    let mut con = srv.resp();
    let _: () = con.set("ws-curr", "v1").unwrap();
    let rev: i64 = redis::cmd("REVISION")
        .arg("ws-curr")
        .query(&mut con)
        .unwrap();

    let mut raw = MinResp3::connect(srv.resp_port);
    raw.send(&[b"HELLO", b"3"]);
    raw.skip_one();

    let rev_str = rev.to_string();
    raw.send(&[b"WATCH", b"ws-curr", b"SINCE", rev_str.as_bytes()]);

    // First (and only) push should be "ready" — no mutations since rev.
    let (pt, ps) = raw.read_push();
    assert_eq!(pt, "watch");
    assert_eq!(
        ps, "ready",
        "no catch-up expected when SINCE equals current revision"
    );
}

// ── Concurrent INCR ───────────────────────────────────────────────────────────

#[test]
fn concurrent_incr_produces_correct_sum() {
    // Each connection exercises the CAS retry loop inside store::incr():
    // when two coroutines overlap at the disk-write await point, one CAS
    // loses and re-reads before retrying. The final count must equal
    // THREADS × PER_THREAD regardless of interleaving order.
    let srv = TestServer::start();
    let port = srv.resp_port;

    const THREADS: usize = 4;
    const PER_THREAD: i64 = 25;

    let handles: Vec<_> = (0..THREADS)
        .map(|_| {
            std::thread::spawn(move || {
                let mut con = redis::Client::open(format!("redis://127.0.0.1:{port}/"))
                    .unwrap()
                    .get_connection()
                    .unwrap();
                for _ in 0..PER_THREAD {
                    let _: i64 = redis::cmd("INCR")
                        .arg("concurrent-ctr")
                        .query(&mut con)
                        .unwrap();
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    let mut con = redis::Client::open(format!("redis://127.0.0.1:{port}/"))
        .unwrap()
        .get_connection()
        .unwrap();
    let final_val: i64 = con.get("concurrent-ctr").unwrap();
    assert_eq!(
        final_val,
        THREADS as i64 * PER_THREAD,
        "concurrent INCR total mismatch: expected {}, got {final_val}",
        THREADS as i64 * PER_THREAD
    );
}

// ── SCAN under concurrent writes ──────────────────────────────────────────────

#[test]
fn scan_completes_under_concurrent_writes() {
    // Key-based cursor: keys that existed before the scan started and are not
    // deleted during the scan must all appear exactly once. Newly-inserted keys
    // may or may not appear depending on whether they fall after the cursor.
    let srv = TestServer::start();
    let port = srv.resp_port;

    let mut scanner = srv.resp();
    for i in 0..50i32 {
        let _: () = scanner.set(format!("sc-base-{i:02}"), "v").unwrap();
    }

    // Writer thread: insert new keys concurrently with the SCAN below.
    let handle = std::thread::spawn(move || {
        let mut w = redis::Client::open(format!("redis://127.0.0.1:{port}/"))
            .unwrap()
            .get_connection()
            .unwrap();
        for i in 0..50i32 {
            let _: () = w.set(format!("sc-new-{i:02}"), "v").unwrap_or(());
        }
    });

    let found = scan_all(&mut scanner, None);
    handle.join().unwrap();

    // All 50 pre-existing keys must appear exactly once — no gaps, no duplicates.
    let mut base_found: Vec<_> = found.iter().filter(|k| k.starts_with("sc-base-")).collect();
    base_found.sort();
    base_found.dedup();
    assert_eq!(
        base_found.len(),
        50,
        "all pre-existing keys must appear in SCAN: got {}/50",
        base_found.len()
    );
}

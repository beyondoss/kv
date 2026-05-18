//! End-to-end handoff scenarios. Every test boots a real `beyond-kv` binary
//! (or two of them, across a real handoff) and uses real redis/HTTP clients.
//! The harness handles process lifecycle, listener inheritance, and the
//! supervisor.
//!
//! Add new scenarios here — they should be one method call on `Harness` plus
//! assertions.

mod handoff_harness;

use handoff_harness::Harness;

/// **The load-bearing claim:** a key written before a handoff is readable
/// after the handoff, on the same TCP port, served by a different process.
///
/// Exercises:
/// - `Role::ColdStart` with `LISTEN_FDS` inheritance.
/// - One full Hello→Commit protocol on a real `Incumbent::serve` thread.
/// - `ShardStore::seal_all_for_shutdown` writing a real on-disk footer.
/// - `Role::Successor` opening the data dir, reading the footer in O(1),
///   acquiring the (just-released) flock, and serving the inherited
///   listener.
/// - Writes work on the new process too (proves it didn't open read-only).
#[test]
fn data_survives_handoff() {
    let mut h = Harness::new();
    h.cold_start();

    // Write on old.
    let mut conn = h.redis_conn();
    let _: () = redis::cmd("SET")
        .arg("survive-key")
        .arg("survive-value")
        .query(&mut conn)
        .expect("SET pre-handoff");
    let pre: String = redis::cmd("GET")
        .arg("survive-key")
        .query(&mut conn)
        .unwrap();
    assert_eq!(pre, "survive-value");
    drop(conn);

    // Hand off.
    let summary = h.handoff();
    assert!(summary.committed, "handoff must commit: {summary:?}");

    // Read on new — same port, different process.
    let mut conn = h.redis_conn();
    let post: String = redis::cmd("GET")
        .arg("survive-key")
        .query(&mut conn)
        .expect("GET post-handoff");
    assert_eq!(
        post, "survive-value",
        "value written to old must be readable on new"
    );

    // Write on new — proves the successor is fully online, not read-only.
    let _: () = redis::cmd("SET")
        .arg("post-key")
        .arg("post-value")
        .query(&mut conn)
        .expect("SET post-handoff");
    let post_w: String = redis::cmd("GET").arg("post-key").query(&mut conn).unwrap();
    assert_eq!(post_w, "post-value");
}

/// Two handoffs in a row should both commit and preserve data through each.
/// Proves the flock dance is repeatable, not just a one-shot fluke.
#[test]
fn back_to_back_handoffs() {
    let mut h = Harness::new();
    h.cold_start();

    let mut conn = h.redis_conn();
    let _: () = redis::cmd("SET")
        .arg("v1-key")
        .arg("v1-value")
        .query(&mut conn)
        .unwrap();
    drop(conn);

    let s1 = h.handoff();
    assert!(s1.committed, "first handoff: {s1:?}");

    let mut conn = h.redis_conn();
    let _: () = redis::cmd("SET")
        .arg("v2-key")
        .arg("v2-value")
        .query(&mut conn)
        .unwrap();
    drop(conn);

    let s2 = h.handoff();
    assert!(s2.committed, "second handoff: {s2:?}");

    // Both keys must be present on the post-second-handoff process.
    let mut conn = h.redis_conn();
    let v1: String = redis::cmd("GET").arg("v1-key").query(&mut conn).unwrap();
    let v2: String = redis::cmd("GET").arg("v2-key").query(&mut conn).unwrap();
    assert_eq!(v1, "v1-value");
    assert_eq!(v2, "v2-value");
}

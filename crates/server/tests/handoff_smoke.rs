//! End-to-end smoke test: spawn the real `beyond-kv` binary, drive the handoff
//! happy path against its control socket, and assert the process exits cleanly
//! on `Commit`.
//!
//! This exercises:
//!   - `detect_role` → `Role::ColdStart` (no `HANDOFF_ROLE` env set here).
//!   - `DataDirLock::acquire_or_break_stale` on a fresh data dir.
//!   - The incumbent serving the protocol from a dedicated thread.
//!   - `KvHandoff::drain` fanning out to the (single) worker.
//!   - `KvHandoff::seal` → `ShardStore::seal_all_for_shutdown` → flock release.
//!   - Commit causing the handoff thread to flip `shutdown=true`, the accept
//!     loop to exit, and main to fall through to its cleanup path.

use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

/// Kill + reap on drop unless `disarm`ed. Without this, a panic between spawn
/// and the `try_wait` loop drops the `Child` without killing — leaving an
/// orphan whose tempdir gets cleaned out from under it.
struct KillOnDrop(Option<Child>);

impl KillOnDrop {
    fn new(c: Child) -> Self {
        Self(Some(c))
    }
    fn as_mut(&mut self) -> &mut Child {
        self.0.as_mut().expect("disarmed")
    }
    fn disarm(mut self) -> Child {
        self.0.take().expect("disarmed twice")
    }
}

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        if let Some(mut c) = self.0.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

use handoff::frame::{read_message, write_message};
use handoff::protocol::{HandoffId, Message, PROTO_MAX};

const TEST_BINARY: &str = env!("CARGO_BIN_EXE_beyond-kv");

fn ephemeral_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let p = l.local_addr().unwrap().port();
    drop(l);
    p
}

fn wait_for_path(path: &PathBuf, secs: u64) -> bool {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !path.exists() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(50));
    }
    path.exists()
}

#[test]
fn full_handoff_protocol_exits_kv_on_commit() {
    let temp = tempfile::tempdir().unwrap();
    let data_dir = temp.path().join("data");
    let sock_path = temp.path().join("control.sock");

    let resp_port = ephemeral_port();
    let http_addr = format!("127.0.0.1:{}", ephemeral_port());

    let mut child = KillOnDrop::new(
        Command::new(TEST_BINARY)
            .arg("serve")
            .arg("--data-dir")
            .arg(&data_dir)
            .arg("--resp-port")
            .arg(resp_port.to_string())
            .arg("--http-address")
            .arg(&http_addr)
            .arg("--threads")
            .arg("1")
            .arg("--handoff-socket-path")
            .arg(&sock_path)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn beyond-kv"),
    );

    assert!(
        wait_for_path(&sock_path, 10),
        "control socket never appeared"
    );

    let mut stream = UnixStream::connect(&sock_path).expect("connect control socket");

    // Read O's Hello.
    let (_v, hello) = read_message(&mut stream).expect("read Hello");
    match hello {
        Message::Hello { role, .. } => {
            assert!(matches!(role, handoff::protocol::Side::Incumbent));
        }
        other => panic!("expected Hello, got {other:?}"),
    }

    let handoff_id = HandoffId::new();
    write_message(
        &mut stream,
        PROTO_MAX,
        &Message::HelloAck {
            proto_version_chosen: PROTO_MAX,
            handoff_id,
        },
    )
    .unwrap();

    // PrepareHandoff → expect Drained.
    write_message(
        &mut stream,
        PROTO_MAX,
        &Message::PrepareHandoff {
            handoff_id,
            successor_pid: 99_999,
            deadline_ms: 10_000,
            drain_grace_ms: 5_000,
        },
    )
    .unwrap();
    let (_, drained) = read_message(&mut stream).unwrap();
    assert!(matches!(drained, Message::Drained { .. }));

    // SealRequest → expect SealComplete.
    write_message(&mut stream, PROTO_MAX, &Message::SealRequest { handoff_id }).unwrap();
    let (_, sealed) = read_message(&mut stream).unwrap();
    match sealed {
        Message::SealComplete {
            handoff_id: id,
            last_revision_per_shard,
            ..
        } => {
            assert_eq!(id, handoff_id);
            assert_eq!(
                last_revision_per_shard.len(),
                1,
                "single-thread KV should report 1 shard"
            );
        }
        other => panic!("expected SealComplete, got {other:?}"),
    }

    // After SealComplete, the data-dir lock must be released. Verify by
    // acquiring it from the test process.
    let probe = handoff::DataDirLock::acquire(&data_dir)
        .expect("flock should be free after KV sent SealComplete");
    drop(probe);

    // Send Commit → KV should exit shortly.
    write_message(&mut stream, PROTO_MAX, &Message::Commit { handoff_id }).unwrap();
    drop(stream);

    let exit_deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match child.as_mut().try_wait().unwrap() {
            Some(status) => {
                assert!(
                    status.success() || status.code() == Some(0),
                    "KV exited with: {status:?}"
                );
                let _ = child.disarm();
                return;
            }
            None if Instant::now() < exit_deadline => {
                thread::sleep(Duration::from_millis(50));
            }
            None => {
                panic!("KV did not exit within 10s of Commit");
            }
        }
    }
}

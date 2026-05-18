//! Reusable end-to-end test harness for the `handoff` integration.
//!
//! Every test that wants to exercise the real moving parts (process spawning,
//! listener-FD inheritance, the flock dance, data persistence across a binary
//! swap) goes through this module. Each scenario is one method call; the
//! harness owns lifecycle (spawn, wait-ready, reap), the supervisor, and the
//! listener FDs that survive across primitive processes.
//!
//! What gets exercised by going through here:
//!
//! - `handoff::detect_role()` — both `ColdStart { inherited }` and
//!   `Successor(s)` branches on real processes.
//! - `LISTEN_FDS` / `LISTEN_FDNAMES` env-var inheritance via `fork+exec`
//!   `dup2` in `pre_exec`.
//! - `DataDirLock::acquire_or_break_stale` on a real on-disk flock between
//!   old and new processes.
//! - `Incumbent::serve` running inside KV; `Supervisor::perform_handoff`
//!   running here; both sides actually speaking the wire protocol over a
//!   real Unix socket.
//! - `ShardStore::sync_logs` + `seal_all_for_shutdown` triggered by the
//!   protocol, with real fsyncs and real footers.
//! - Data durability via the existing footer-fast-path in `log/recover.rs`
//!   when the successor opens the (now sealed) data directory.
//!
//! Designed to grow: add `kill_kv()`, `kill_successor_mid_handoff()`, traffic
//! generators, log assertions, and so on without breaking existing tests.
//!
//! Cargo integration tests each get their own binary; if one test binary
//! uses only part of the harness, the unused-code warning is suppressed at
//! the top of this module.

#![allow(dead_code)]

use std::io::ErrorKind;
use std::net::{SocketAddr, TcpListener};
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use handoff::supervisor::{SpawnSpec, Supervisor};
use tempfile::TempDir;

/// The compiled `beyond-kv` binary path. Set by cargo for integration tests
/// of the `beyond-kv` package.
const KV_BINARY: &str = env!("CARGO_BIN_EXE_beyond-kv");

/// One in-progress handoff scenario.
///
/// Owns:
/// - A temporary data dir (CoW-safe).
/// - The Unix-domain control socket path.
/// - The two TCP listener FDs that survive across primitive processes.
/// - The currently-tracked KV [`Child`] handle (None when no KV is running
///   or after a committed handoff loses the new-process handle).
/// - A [`Supervisor`] pre-loaded with the listener FDs and journal path.
pub struct Harness {
    binary: PathBuf,
    _temp: TempDir,
    data_dir: PathBuf,
    control_socket: PathBuf,
    journal_path: PathBuf,
    resp_listener: TcpListener,
    http_listener: TcpListener,
    resp_addr: SocketAddr,
    http_addr: SocketAddr,
    threads: usize,
    extra_args: Vec<String>,
    /// `Some` for the very first (cold-start) child. After a committed
    /// handoff this becomes `None` — the successor's `Child` handle was
    /// dropped inside `perform_handoff`. Init reaps it at process exit.
    current: Option<Child>,
    supervisor: Arc<Supervisor>,
}

/// What happened from one `Harness::handoff()` call.
#[derive(Debug)]
pub struct HandoffSummary {
    pub committed: bool,
    pub abort_reason: Option<String>,
    pub handoff_id: handoff::HandoffId,
    pub elapsed: Duration,
}

impl Harness {
    /// Allocate ephemeral ports + temp dir + listeners. Does **not** start
    /// KV yet (call [`cold_start`](Self::cold_start)).
    pub fn new() -> Self {
        let binary = PathBuf::from(KV_BINARY);
        let temp = tempfile::tempdir().expect("tempdir");
        let data_dir = temp.path().join("data");
        let control_socket = temp.path().join("control.sock");
        let journal_path = temp.path().join("handoff-state.bin");

        let resp_listener = TcpListener::bind("127.0.0.1:0").expect("bind resp");
        let http_listener = TcpListener::bind("127.0.0.1:0").expect("bind http");
        let resp_addr = resp_listener.local_addr().unwrap();
        let http_addr = http_listener.local_addr().unwrap();

        let supervisor = Supervisor::new(&control_socket)
            .expect("Supervisor::new")
            .with_listener("resp", resp_listener.as_raw_fd())
            .with_listener("http", http_listener.as_raw_fd())
            .with_journal(journal_path.clone());
        let supervisor = Arc::new(supervisor);

        Self {
            binary,
            _temp: temp,
            data_dir,
            control_socket,
            journal_path,
            resp_listener,
            http_listener,
            resp_addr,
            http_addr,
            threads: 1,
            extra_args: Vec::new(),
            current: None,
            supervisor,
        }
    }

    /// Use `--threads N` for the KV process(es). Affects how keys are sharded
    /// and how many monoio worker threads exist. Set before `cold_start`.
    pub fn with_threads(mut self, n: usize) -> Self {
        assert!(self.current.is_none(), "set threads before cold_start");
        self.threads = n;
        self
    }

    // ── Lifecycle ────────────────────────────────────────────────────────

    /// Spawn the first KV process (no `HANDOFF_ROLE`, so `Role::ColdStart`).
    /// Blocks until KV's control socket appears and its RESP port accepts.
    pub fn cold_start(&mut self) -> &mut Self {
        self.cold_start_with_env(Vec::new())
    }

    /// Like [`cold_start`](Self::cold_start) but with extra env vars passed
    /// to the cold-start child. Used by tests that inject engine-level
    /// fault hooks (e.g. `KV_TEST_FAIL_ONCE_FILE`).
    pub fn cold_start_with_env(&mut self, env: Vec<(String, String)>) -> &mut Self {
        assert!(self.current.is_none(), "kv already running");
        let listener_fds = vec![
            ("resp".to_string(), self.resp_listener.as_raw_fd()),
            ("http".to_string(), self.http_listener.as_raw_fd()),
        ];
        let args = self.kv_args();
        let child =
            spawn_cold_start_with_inherited_and_env(&self.binary, &args, &listener_fds, &env);
        self.current = Some(child);
        self.wait_ready();
        self
    }

    /// Drive a full happy-path handoff: spawn successor, run Hello → Commit.
    /// Reaps the old child if the handoff commits. Blocks until the successor
    /// is reachable on the same port.
    pub fn handoff(&mut self) -> HandoffSummary {
        self.handoff_with_env(Vec::new())
    }

    /// Like [`handoff`](Self::handoff) but with extra env vars passed to the
    /// successor process. Used by tests that inject faults via env-var hooks
    /// (e.g. `KV_TEST_PANIC_BEFORE_READY=1`).
    pub fn handoff_with_env(&mut self, env: Vec<(String, String)>) -> HandoffSummary {
        let started = Instant::now();
        let args = self.kv_args();
        let spec = SpawnSpec {
            binary: self.binary.clone(),
            args,
            env,
            deadline: Duration::from_secs(15),
            drain_grace: Duration::from_secs(5),
        };
        let mut outcome = self
            .supervisor
            .perform_handoff(spec)
            .expect("perform_handoff");

        if outcome.committed {
            if let Some(mut old) = self.current.take() {
                let _ = old.wait();
            }
            // The supervisor handed us the successor `Child`; track it for
            // lifecycle cleanup so test exit doesn't leak a process.
            self.current = outcome.child.take();
            // Successor recreates the control socket on its own startup.
            self.wait_ready();
        }
        // On abort, the old child is still alive and serving (we kept the
        // handle in `self.current`). The successor is already reaped by
        // `perform_handoff`'s `ChildGuard`.

        HandoffSummary {
            committed: outcome.committed,
            abort_reason: outcome.abort_reason,
            handoff_id: outcome.handoff_id,
            elapsed: started.elapsed(),
        }
    }

    /// Block until the control socket exists and RESP accepts.
    pub fn wait_ready(&self) {
        assert!(
            wait_for_path(&self.control_socket, Duration::from_secs(10)),
            "control socket {:?} never appeared",
            self.control_socket
        );
        wait_for_tcp(self.resp_addr, Duration::from_secs(10));
    }

    /// Kill the currently-tracked child (best-effort). For lifecycle hygiene
    /// at the end of a test that doesn't run a happy-path handoff.
    pub fn kill_current(&mut self) {
        if let Some(mut c) = self.current.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }

    // ── Inspection ───────────────────────────────────────────────────────

    pub fn resp_addr(&self) -> SocketAddr {
        self.resp_addr
    }

    pub fn http_addr(&self) -> SocketAddr {
        self.http_addr
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub fn control_socket(&self) -> &Path {
        &self.control_socket
    }

    pub fn journal_path(&self) -> &Path {
        &self.journal_path
    }

    /// Clone of the harness's supervisor — for tests that need to drive
    /// multiple `perform_handoff` calls from different threads (e.g. to
    /// exercise the in-flight mutex on `Supervisor`).
    pub fn supervisor(&self) -> Arc<Supervisor> {
        Arc::clone(&self.supervisor)
    }

    /// Build a `SpawnSpec` matching the harness's defaults — used by tests
    /// that call `perform_handoff` directly instead of through `handoff()`.
    pub fn make_spawn_spec(&self) -> SpawnSpec {
        SpawnSpec {
            binary: self.binary.clone(),
            args: self.kv_args(),
            env: Vec::new(),
            deadline: Duration::from_secs(15),
            drain_grace: Duration::from_secs(5),
        }
    }

    /// PID of the *cold-start* child if it is still around. After a commit,
    /// returns `None` (we lose the successor's `Child` handle).
    pub fn current_pid(&self) -> Option<u32> {
        self.current.as_ref().map(|c| c.id())
    }

    // ── Clients ──────────────────────────────────────────────────────────

    /// Fresh redis client pointing at the harness's RESP port. Resilient to
    /// the brief gap between old's last `accept()` and new's first
    /// `accept()` — connect retries until ready.
    pub fn redis_conn(&self) -> redis::Connection {
        let client = redis::Client::open(format!("redis://{}/", self.resp_addr))
            .expect("redis::Client::open");
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match client.get_connection() {
                Ok(c) => return c,
                Err(_) if Instant::now() < deadline => {
                    thread::sleep(Duration::from_millis(25));
                }
                Err(e) => panic!("redis connect to {}: {e}", self.resp_addr),
            }
        }
    }

    /// HTTP base URL (`http://127.0.0.1:N`).
    pub fn http_url(&self) -> String {
        format!("http://{}", self.http_addr)
    }

    // ── Internals ────────────────────────────────────────────────────────

    fn kv_args(&self) -> Vec<String> {
        let mut v = vec![
            "serve".into(),
            "--data-dir".into(),
            self.data_dir.to_str().unwrap().into(),
            "--resp-port".into(),
            self.resp_addr.port().to_string(),
            "--http-address".into(),
            self.http_addr.to_string(),
            "--threads".into(),
            self.threads.to_string(),
            "--handoff-socket-path".into(),
            self.control_socket.to_str().unwrap().into(),
        ];
        v.extend(self.extra_args.iter().cloned());
        v
    }

    /// Append additional CLI args to every KV spawn (both cold-start and
    /// handoff). Use for tuning reclaim cadence, memory budgets, etc.
    pub fn with_extra_args(mut self, args: Vec<String>) -> Self {
        assert!(self.current.is_none(), "set extra args before cold_start");
        self.extra_args = args;
        self
    }

    // ── Fault-injection helpers ──────────────────────────────────────────

    /// SIGKILL the current child (simulates a hard crash). The flock is
    /// released by the kernel; the pidfile remains as a stale hint. Use
    /// [`cold_start_after_crash`](Self::cold_start_after_crash) to verify
    /// the stale-break path then reclaims the lock.
    pub fn sigkill_current(&mut self) {
        if let Some(mut c) = self.current.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }

    /// Cold-start again on the same data dir + listeners. Required to
    /// exercise the `acquire_or_break_stale` path after a `sigkill_current`.
    pub fn cold_start_after_crash(&mut self) -> &mut Self {
        assert!(self.current.is_none(), "kill current child first");
        let listener_fds = vec![
            ("resp".to_string(), self.resp_listener.as_raw_fd()),
            ("http".to_string(), self.http_listener.as_raw_fd()),
        ];
        let args = self.kv_args();
        let child = spawn_cold_start_with_inherited(&self.binary, &args, &listener_fds);
        self.current = Some(child);
        self.wait_ready();
        self
    }

    /// Try to start a second KV process pointed at the same data dir, on a
    /// *different* set of ephemeral ports and a *different* handoff socket.
    /// No FD inheritance, no supervisor coordination — just a plain process
    /// that should refuse to start because the data-dir lock is held.
    ///
    /// Returns the second child so the caller can inspect its exit status.
    pub fn try_spawn_competitor(&self) -> Child {
        let extra_resp = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let extra_http = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let resp_port = extra_resp.local_addr().unwrap().port();
        let http_addr = extra_http.local_addr().unwrap().to_string();
        // Free the ports — the competitor will bind them itself.
        drop(extra_resp);
        drop(extra_http);
        let other_socket = self._temp.path().join("competitor-control.sock");

        let mut cmd = Command::new(&self.binary);
        cmd.args([
            "serve",
            "--data-dir",
            self.data_dir.to_str().unwrap(),
            "--resp-port",
            &resp_port.to_string(),
            "--http-address",
            &http_addr,
            "--threads",
            "1",
            "--handoff-socket-path",
            other_socket.to_str().unwrap(),
        ]);
        cmd.stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        cmd.spawn().expect("spawn competitor")
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.kill_current();
    }
}

// ── Free helpers ─────────────────────────────────────────────────────────

/// Cold-start spawn that mirrors the production supervisor's FD inheritance:
/// `dup2` each listener FD into FD 3..3+N in the child via `pre_exec`,
/// clearing `FD_CLOEXEC` so the FDs survive `execve`.
pub fn spawn_cold_start_with_inherited(
    binary: &Path,
    args: &[String],
    listener_fds: &[(String, RawFd)],
) -> Child {
    spawn_cold_start_with_inherited_and_env(binary, args, listener_fds, &[])
}

/// Like [`spawn_cold_start_with_inherited`] but with extra env vars merged
/// into the child's environment. Used by tests that need to flip engine-
/// or main-level fault hooks (e.g. `KV_TEST_FAIL_ONCE_FILE`).
pub fn spawn_cold_start_with_inherited_and_env(
    binary: &Path,
    args: &[String],
    listener_fds: &[(String, RawFd)],
    extra_env: &[(String, String)],
) -> Child {
    let mut cmd = Command::new(binary);
    cmd.args(args);
    let names: Vec<String> = listener_fds.iter().map(|(n, _)| n.clone()).collect();
    cmd.env("LISTEN_FDS", listener_fds.len().to_string());
    cmd.env("LISTEN_FDNAMES", names.join(":"));
    for (k, v) in extra_env {
        cmd.env(k, v);
    }
    // Route stdio to /dev/null. If we inherited the test runner's stdout, the
    // orphaned successor (whose `Child` handle the supervisor drops) would
    // keep the pipe write-end open after the test exits, hanging `tail`/CI.
    cmd.stdin(Stdio::null()).stdout(Stdio::null());
    if std::env::var("KV_TEST_LOGS").is_ok() {
        cmd.stderr(Stdio::inherit());
    } else {
        cmd.stderr(Stdio::null());
    }

    let sources: Vec<RawFd> = listener_fds.iter().map(|(_, f)| *f).collect();
    // SAFETY: `pre_exec` runs in the forked child before `execve`. Only
    // async-signal-safe libc calls; no allocations.
    unsafe {
        cmd.pre_exec(move || {
            for (i, src) in sources.iter().enumerate() {
                let dst = 3 + i as RawFd;
                if *src == dst {
                    if libc::fcntl(*src, libc::F_SETFD, 0) == -1 {
                        return Err(std::io::Error::last_os_error());
                    }
                } else if libc::dup2(*src, dst) == -1 {
                    return Err(std::io::Error::last_os_error());
                }
            }
            Ok(())
        });
    }
    cmd.spawn().expect("spawn beyond-kv (cold start)")
}

/// Wait for `path` to exist, polling at 25 ms.
pub fn wait_for_path(path: &Path, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while !path.exists() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(25));
    }
    path.exists()
}

/// Wait until a TCP connection to `addr` succeeds.
pub fn wait_for_tcp(addr: SocketAddr, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        match std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(250)) {
            Ok(_) => return,
            Err(e) if Instant::now() < deadline => {
                let _ = e;
                thread::sleep(Duration::from_millis(25));
            }
            Err(e) if e.kind() == ErrorKind::TimedOut => continue,
            Err(e) => panic!("wait_for_tcp({addr}): {e}"),
        }
    }
}

// ─── Traffic generator ───────────────────────────────────────────────────

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// One acked write the [`Writer`] has produced.
#[derive(Debug, Clone)]
pub struct AckedWrite {
    pub key: String,
    pub value: String,
}

/// Stats collected by [`Writer::stop`].
#[derive(Debug)]
pub struct WriterResult {
    /// Every (key, value) pair that the KV server returned `OK` for.
    /// Post-handoff, `GET key` MUST return `value` for every entry here.
    pub acked: Vec<AckedWrite>,
    /// Count of SET attempts that failed (connection drop, error response).
    pub errors: u64,
    /// Time the writer was active.
    pub elapsed: Duration,
}

/// Background writer thread. Connects to a redis endpoint and continuously
/// SETs unique `k-<N>=v-<N>` pairs as fast as it can, recording each ack.
/// Reconnects automatically when the connection drops (which it will during
/// the brief window between O's last accept and N's first accept).
pub struct Writer {
    handle: Option<std::thread::JoinHandle<WriterResult>>,
    stop: Arc<AtomicBool>,
    acked_count: Arc<AtomicU64>,
}

impl Writer {
    /// Start a writer hammering `addr` with sequential SETs.
    pub fn start(addr: SocketAddr) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let acked_count = Arc::new(AtomicU64::new(0));
        let acked_snapshot = Arc::new(Mutex::new(Vec::<AckedWrite>::new()));
        let stop_for_thread = Arc::clone(&stop);
        let count_for_thread = Arc::clone(&acked_count);
        let acked_for_thread = Arc::clone(&acked_snapshot);

        let handle = std::thread::Builder::new()
            .name("kv-handoff-writer".into())
            .spawn(move || {
                let started = Instant::now();
                let client =
                    redis::Client::open(format!("redis://{addr}/")).expect("redis::Client::open");
                let mut conn: Option<redis::Connection> = None;
                let mut errors = 0u64;
                let mut seq = 0u64;

                while !stop_for_thread.load(Ordering::Relaxed) {
                    if conn.is_none() {
                        conn = client.get_connection().ok();
                        if conn.is_none() {
                            errors += 1;
                            thread::sleep(Duration::from_millis(5));
                            continue;
                        }
                    }
                    let key = format!("k-{seq}");
                    let value = format!("v-{seq}");
                    let c = conn.as_mut().unwrap();
                    match redis::cmd("SET").arg(&key).arg(&value).query::<()>(c) {
                        Ok(()) => {
                            acked_for_thread.lock().unwrap().push(AckedWrite {
                                key: key.clone(),
                                value,
                            });
                            count_for_thread.fetch_add(1, Ordering::Relaxed);
                            seq += 1;
                        }
                        Err(_e) => {
                            errors += 1;
                            // Force reconnect on next iteration.
                            conn = None;
                        }
                    }
                }
                let acked = acked_for_thread.lock().unwrap().clone();
                WriterResult {
                    acked,
                    errors,
                    elapsed: started.elapsed(),
                }
            })
            .expect("spawn writer thread");

        Self {
            handle: Some(handle),
            stop,
            acked_count,
        }
    }

    /// Approximate number of acked writes so far (cheap, atomic).
    pub fn acked_count(&self) -> u64 {
        self.acked_count.load(Ordering::Relaxed)
    }

    /// Signal the writer to stop and collect its results.
    pub fn stop(mut self) -> WriterResult {
        self.stop.store(true, Ordering::SeqCst);
        self.handle
            .take()
            .expect("handle")
            .join()
            .expect("writer panic")
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

// ─── HTTP helpers ────────────────────────────────────────────────────────

/// Blocking HTTP PUT to `http://<addr>/v1/kv/<key>`. Returns the status code.
pub fn http_put(addr: SocketAddr, key: &str, value: &str) -> u16 {
    let url = format!("http://{addr}/v1/kv/{key}");
    match ureq::put(&url).send_string(value) {
        Ok(resp) => resp.status(),
        Err(ureq::Error::Status(code, _)) => code,
        Err(e) => panic!("http_put({url}): {e}"),
    }
}

/// Blocking HTTP GET; returns `Some(body)` for 200, `None` for 404.
pub fn http_get(addr: SocketAddr, key: &str) -> Option<String> {
    let url = format!("http://{addr}/v1/kv/{key}");
    let agent = ureq::AgentBuilder::new()
        .timeout(Duration::from_secs(5))
        .build();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match agent.get(&url).call() {
            Ok(resp) => return Some(resp.into_string().expect("body")),
            Err(ureq::Error::Status(404, _)) => return None,
            Err(ureq::Error::Transport(_)) if Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(25));
            }
            Err(e) => panic!("http_get({url}): {e}"),
        }
    }
}

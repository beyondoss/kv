//! Forced file-descriptor exhaustion (EMFILE).
//!
//! This lives in its OWN integration-test binary on purpose: it lowers the
//! process-global `RLIMIT_NOFILE`, which would poison any sibling test sharing
//! the process. As the sole test here, the clamp affects only this process.
//!
//! It proves the descriptor-exhaustion gap we characterized degrades gracefully:
//! opening a new namespace under EMFILE fails with a clean `Io` error (no panic,
//! no corruption), an already-open namespace keeps serving reads, and the store
//! recovers as soon as descriptors are freed.

use beyond_kv_engine::error::EngineError;
use beyond_kv_engine::store::ShardStore;
use beyond_kv_engine::types::SetOptions;
use bytes::Bytes;
use tempfile::TempDir;

fn open_fd_count() -> usize {
    std::fs::read_dir("/proc/self/fd")
        .map(|d| d.count())
        .unwrap_or(0)
}

/// Set the `RLIMIT_NOFILE` soft limit (clamped to the hard limit); return the
/// previous soft limit so the caller can restore it.
fn set_nofile_soft(soft: u64) -> u64 {
    unsafe {
        let mut rl = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        assert_eq!(
            libc::getrlimit(libc::RLIMIT_NOFILE, &mut rl),
            0,
            "getrlimit failed"
        );
        let old = rl.rlim_cur;
        rl.rlim_cur = soft.min(rl.rlim_max);
        assert_eq!(
            libc::setrlimit(libc::RLIMIT_NOFILE, &rl),
            0,
            "setrlimit failed"
        );
        old
    }
}

#[test]
fn emfile_on_namespace_open_degrades_gracefully_and_recovers() {
    let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
        .enable_timer()
        .build()
        .expect("monoio runtime");
    rt.block_on(async {
        let dir = TempDir::new().unwrap();
        let s = ShardStore::open(dir.path(), 4 << 20).await.unwrap();

        // A namespace we rely on after the clamp; seed a value to read back.
        s.set(
            "keeper",
            b"k",
            Bytes::from_static(b"v0"),
            SetOptions::default(),
        )
        .await
        .unwrap();

        // Clamp the soft fd limit to just above current usage. All runtime + store
        // infra descriptors are already allocated, so only NEW file opens (new
        // namespaces / log files) can now hit EMFILE.
        let cur = open_fd_count();
        let old = set_nofile_soft((cur + 8) as u64);

        // Open fresh namespaces until one fails for lack of descriptors.
        let mut hit: Option<EngineError> = None;
        for i in 0..256 {
            match s
                .set(
                    &format!("new{i}"),
                    b"k",
                    Bytes::from_static(b"v"),
                    SetOptions::default(),
                )
                .await
            {
                Ok(()) => {}
                Err(e) => {
                    hit = Some(e);
                    break;
                }
            }
        }
        let err = hit.expect("expected an fd-exhaustion error while opening new namespaces");
        assert!(
            matches!(err, EngineError::Io { .. }),
            "EMFILE must surface as a clean Io error, got {err:?}"
        );

        // Graceful degradation: an already-open namespace still serves reads
        // (the read path needs no new descriptor). No panic, no corruption.
        let got = s
            .get("keeper", b"k")
            .await
            .expect("get must not error under EMFILE");
        assert_eq!(
            got.map(|e| e.value),
            Some(Bytes::from_static(b"v0")),
            "existing namespace remains readable while descriptors are exhausted"
        );

        // Recovery: once descriptors are available again, opening namespaces works.
        set_nofile_soft(old);
        s.set(
            "after_recovery",
            b"k",
            Bytes::from_static(b"v"),
            SetOptions::default(),
        )
        .await
        .expect("opening a namespace must succeed once fds are freed");
        assert!(
            s.get("after_recovery", b"k").await.unwrap().is_some(),
            "store recovers cleanly after fd exhaustion clears"
        );
    });
}

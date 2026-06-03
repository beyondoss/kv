//! Write-amplification measurement: value separation vs the inline baseline
//! (pre-value-separation behavior), using the engine's real `compaction_bytes`
//! counter (bytes relocated by compaction = the GlideFS S3 re-upload cost).
//!
//! Run: cargo test -p beyond-kv-engine --test writeamp -- --nocapture

use beyond_kv_engine::log::NamespaceLog;
use beyond_kv_engine::log::config::LogConfig;
use bytes::Bytes;
use tempfile::TempDir;

fn key(i: usize) -> Bytes {
    Bytes::from(format!("k{i:05}"))
}

/// Churn `n` 32 KiB values across `rounds` reclaims; return cumulative
/// compaction bytes after each round. `threshold = usize::MAX` ⇒ values stay
/// inline (the pre-value-separation baseline); a small threshold ⇒ separated.
async fn sweep(threshold: usize, rounds: usize) -> Vec<u64> {
    let dir = TempDir::new().unwrap();
    let cfg = LogConfig {
        rotate_threshold: 64 * 1024,
        fanout: 4,
        value_sep_threshold: threshold,
    };
    let log = NamespaceLog::open(dir.path().to_path_buf(), cfg)
        .await
        .unwrap();
    let n = 60usize;
    let v0 = vec![0xCDu8; 32 * 1024];
    for i in 0..n {
        log.put_full(key(i), &v0, &[], None).await.unwrap();
    }
    log.reclaim().await.unwrap();
    log.compaction_bytes.set(0); // measure only the churn phase
    let mut series = Vec::with_capacity(rounds);
    for r in 0..rounds {
        let vr = vec![r as u8; 32 * 1024]; // new content each round
        for i in 0..n {
            log.put_full(key(i), &vr, &[], None).await.unwrap();
        }
        log.reclaim().await.unwrap();
        series.push(log.compaction_bytes.get());
    }
    series
}

#[test]
fn writeamp_sweep_csv() {
    let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
        .enable_timer()
        .build()
        .unwrap();
    rt.block_on(async {
        let rounds = 15usize;
        let inline = sweep(usize::MAX, rounds).await; // baseline: values inline
        let vsep = sweep(4096, rounds).await; // value separation on
        println!("WRITEAMP_CSV_START");
        println!("round,inline_mib,valuesep_mib");
        for r in 0..rounds {
            println!(
                "{},{:.4},{:.4}",
                r + 1,
                inline[r] as f64 / 1048576.0,
                vsep[r] as f64 / 1048576.0
            );
        }
        println!("WRITEAMP_CSV_END");
        let (ti, tv) = (inline[rounds - 1], vsep[rounds - 1]);
        println!(
            "TOTAL inline={:.2} MiB  valuesep={:.4} MiB  ratio={:.0}x",
            ti as f64 / 1048576.0,
            tv as f64 / 1048576.0,
            ti as f64 / tv.max(1) as f64
        );
    });
}

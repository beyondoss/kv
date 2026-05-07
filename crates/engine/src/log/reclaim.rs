use std::path::PathBuf;
use std::rc::Rc;

use bytes::Bytes;
use futures_util::future::join_all;
use rustc_hash::FxHashMap;
use tracing::{info, warn};

use crate::error::{EngineError, Result};
use crate::log::file::{
    BufGuard, FooterEntry, LogFile, data_filename, footer_entry_from_index, reclaim_tmp_filename,
};
use crate::log::index::IndexEntry;

#[derive(Debug, Clone, Copy)]
pub struct ReclaimReport {
    pub live_keys: u64,
    pub live_bytes: u64,
    pub dead_files_dropped: u32,
    /// Files whose unlink failed after compaction; disk space is not freed until
    /// a subsequent reclaim or manual cleanup.
    pub dead_files_leaked: u32,
    pub new_file_id: u16,
}

/// Read every live entry from `sealed_files` and write them into a single new
/// sealed file at `dir/data-{next_file_id}.log.tmp`, finalize with a footer +
/// fsync, then atomically rename to `dir/data-{next_file_id}.log`. Returns the
/// updated index entries; caller applies them after this future completes so
/// no `NsIndex` borrow is held across await points. Old sealed files are
/// unlinked (failures are logged but do not abort the operation).
pub async fn reclaim_namespace(
    dir: PathBuf,
    sealed_files: &[Rc<LogFile>],
    next_file_id: u16,
    live: &[(Bytes, IndexEntry, Option<u64>)],
) -> Result<(ReclaimReport, Vec<(Bytes, IndexEntry, Option<u64>)>)> {
    let tmp_path = dir.join(reclaim_tmp_filename(next_file_id));
    match monoio::fs::remove_file(&tmp_path).await {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => warn!(
            path = %tmp_path.display(),
            error = %e,
            "failed to remove stale reclaim tmp; proceeding — open will truncate it"
        ),
    }

    let new_file = LogFile::open_rw(tmp_path.clone(), next_file_id).await?;
    // Always start from a clean slate: if the prior remove failed and a stale
    // .tmp exists, open_rw inherits its content without this truncate.
    new_file.truncate_to(0).await?;

    // Build an owned-Rc map so read futures can capture file handles without borrowing.
    let file_map: FxHashMap<u16, Rc<LogFile>> = sealed_files
        .iter()
        .map(|f| (f.file_id, Rc::clone(f)))
        .collect();

    // Submit all reads concurrently; io_uring sees them as a batch.
    let read_futures: Vec<_> = live
        .iter()
        .map(|(_, old_entry, _)| {
            let file = file_map.get(&old_entry.file_id).cloned();
            let offset = old_entry.record_offset;
            let size = old_entry.record_size as usize;
            async move {
                match file {
                    None => Err(EngineError::BadRecord {
                        offset,
                        reason: "reclaim: file_id not in sealed snapshot",
                    }),
                    Some(f) => f.read_exact(offset, size).await,
                }
            }
        })
        .collect();
    let read_results: Vec<Result<BufGuard>> = join_all(read_futures).await;

    // Write sequentially (preserves deterministic record order in the new file).
    let mut footer: Vec<FooterEntry> = Vec::with_capacity(live.len());
    let mut new_entries: Vec<(Bytes, IndexEntry, Option<u64>)> = Vec::with_capacity(live.len());
    let mut live_bytes: u64 = 0;

    for ((key, old_entry, ttl), bytes_res) in live.iter().zip(read_results) {
        let bytes = bytes_res?.into_inner();
        let (new_offset, _) = new_file.append(bytes).await?;
        live_bytes += old_entry.record_size as u64;
        let new_entry = IndexEntry::new(
            next_file_id,
            new_offset,
            old_entry.record_size,
            old_entry.tstamp_ms,
        );
        new_entries.push((key.clone(), new_entry, *ttl));
        footer.push(footer_entry_from_index(key.clone(), &new_entry, *ttl));
    }

    new_file.write_footer(&footer).await?;
    drop(new_file); // close fd before rename (not strictly required, but clean)

    let final_path = dir.join(data_filename(next_file_id));
    monoio::fs::rename(&tmp_path, &final_path).await?;

    let live_keys = new_entries.len() as u64;

    // Unlink all old sealed files concurrently via io_uring.
    let unlink_futures: Vec<_> = sealed_files
        .iter()
        .map(|f| monoio::fs::remove_file(f.path.clone()))
        .collect();
    let unlink_results = join_all(unlink_futures).await;
    let mut dead_files_dropped = 0u32;
    let mut dead_files_leaked = 0u32;
    for (f, res) in sealed_files.iter().zip(unlink_results) {
        match res {
            Ok(()) => dead_files_dropped += 1,
            Err(e) => {
                dead_files_leaked += 1;
                warn!(
                    path = %f.path.display(),
                    error = %e,
                    "failed to unlink old sealed file after reclaim; disk space not freed"
                );
            }
        }
    }

    let report = ReclaimReport {
        live_keys,
        live_bytes,
        dead_files_dropped,
        dead_files_leaked,
        new_file_id: next_file_id,
    };
    info!(?report, "reclaim complete");
    Ok((report, new_entries))
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use bytes::Bytes;
    use tempfile::TempDir;

    use super::*;
    use crate::log::file::{LogFile, data_filename};
    use crate::log::index::IndexEntry;
    use crate::log::record::{self, flags as rflags};

    fn run<F: std::future::Future>(f: F) -> F::Output {
        monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_timer()
            .build()
            .expect("monoio runtime")
            .block_on(f)
    }

    /// Write a full record into `file`, returning the `IndexEntry` pointing at it.
    async fn write_record(
        file: &LogFile,
        key: &[u8],
        value: &[u8],
        tstamp_ms: u64,
        ttl: Option<u64>,
    ) -> IndexEntry {
        let flags = if ttl.is_some() { 0 } else { rflags::NO_EXPIRY };
        let expires_at_ms = ttl.unwrap_or(0);
        let mut buf = Vec::new();
        record::encode_into(&mut buf, tstamp_ms, flags, expires_at_ms, key, value, &[]).unwrap();
        let record_size = buf.len() as u32;
        let (offset, _) = file.append(buf).await.unwrap();
        IndexEntry::new(file.file_id, offset, record_size, tstamp_ms)
    }

    #[test]
    fn reclaim_compacts_sealed_files_into_one() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        run(async move {
            let path0 = dir.join(data_filename(0));
            let file0 = Rc::new(LogFile::open_rw(path0, 0).await.unwrap());

            let e_a = write_record(&file0, b"alpha", b"1", 100, None).await;
            let e_b = write_record(&file0, b"beta", b"2", 200, None).await;

            let live: Vec<(Bytes, IndexEntry, Option<u64>)> = vec![
                (Bytes::from("alpha"), e_a, None),
                (Bytes::from("beta"), e_b, None),
            ];

            let (report, new_entries) = reclaim_namespace(dir.clone(), &[file0], 1, &live)
                .await
                .unwrap();

            assert_eq!(report.live_keys, 2);
            assert_eq!(report.dead_files_dropped, 1);
            assert_eq!(report.new_file_id, 1);
            assert_eq!(new_entries.len(), 2);

            // New file must exist; old file must be gone.
            assert!(
                dir.join(data_filename(1)).exists(),
                "compacted file must exist"
            );
            assert!(
                !dir.join(data_filename(0)).exists(),
                "old file must be unlinked"
            );
        });
    }

    #[test]
    fn reclaim_on_empty_live_set_produces_empty_sealed_file() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        run(async move {
            let path0 = dir.join(data_filename(0));
            let file0 = Rc::new(LogFile::open_rw(path0, 0).await.unwrap());
            // Write a record that is no longer live.
            write_record(&file0, b"stale", b"v", 1, None).await;

            let live: Vec<(Bytes, IndexEntry, Option<u64>)> = vec![];
            let (report, new_entries) = reclaim_namespace(dir.clone(), &[file0], 1, &live)
                .await
                .unwrap();

            assert_eq!(report.live_keys, 0);
            assert_eq!(new_entries.len(), 0);
            assert!(dir.join(data_filename(1)).exists());
        });
    }

    #[test]
    fn reclaim_preserves_ttl_in_new_entries() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        run(async move {
            let path0 = dir.join(data_filename(0));
            let file0 = Rc::new(LogFile::open_rw(path0, 0).await.unwrap());
            let expires_at = 9_999_999u64;
            let entry = write_record(&file0, b"ttlkey", b"v", 1, Some(expires_at)).await;
            let live = vec![(Bytes::from("ttlkey"), entry, Some(expires_at))];

            let (_report, new_entries) = reclaim_namespace(dir, &[file0], 1, &live).await.unwrap();

            assert_eq!(new_entries.len(), 1);
            assert_eq!(new_entries[0].2, Some(expires_at), "TTL must be preserved");
        });
    }

    #[test]
    fn reclaim_excludes_keys_absent_from_live_set() {
        // The caller (NamespaceLog::reclaim) filters expired keys before building
        // the live set. This test documents that interface contract: keys not in
        // `live` are absent from the compacted output.
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        run(async move {
            let path0 = dir.join(data_filename(0));
            let file0 = Rc::new(LogFile::open_rw(path0, 0).await.unwrap());

            write_record(&file0, b"expired", b"old", 1, Some(1)).await;
            let live_entry = write_record(&file0, b"live", b"keep", 2, None).await;

            let live = vec![(Bytes::from("live"), live_entry, None)];
            let (report, new_entries) = reclaim_namespace(dir, &[file0], 1, &live).await.unwrap();

            assert_eq!(report.live_keys, 1, "only the live key must survive");
            assert_eq!(new_entries.len(), 1);
            assert_eq!(new_entries[0].0, Bytes::from("live"));
        });
    }

    #[test]
    fn stale_tmp_is_cleared_before_reclaim() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        run(async move {
            let path0 = dir.join(data_filename(0));
            let file0 = Rc::new(LogFile::open_rw(path0, 0).await.unwrap());
            let entry = write_record(&file0, b"k", b"v", 1, None).await;

            // Plant a stale .tmp from a previous interrupted reclaim.
            let stale_tmp = dir.join(crate::log::file::reclaim_tmp_filename(1));
            std::fs::write(&stale_tmp, b"garbage").unwrap();
            assert!(stale_tmp.exists());

            let live = vec![(Bytes::from("k"), entry, None)];
            // Must succeed despite the stale .tmp.
            let (report, _) = reclaim_namespace(dir.clone(), &[file0], 1, &live)
                .await
                .unwrap();
            assert_eq!(report.live_keys, 1);
            assert!(
                !stale_tmp.exists(),
                "stale .tmp must be replaced by the real file"
            );
            assert!(dir.join(data_filename(1)).exists());
        });
    }
}

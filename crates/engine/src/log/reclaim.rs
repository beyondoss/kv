use std::path::PathBuf;
use std::rc::Rc;

use bytes::Bytes;
use futures_util::future::join_all;
use rustc_hash::FxHashMap;
use tracing::{info, warn};

use crate::error::{EngineError, Result};
use crate::log::file::{
    FooterEntry, LogFile, data_filename, footer_entry_from_index, reclaim_tmp_filename,
};
use crate::log::index::IndexEntry;

#[derive(Debug, Clone, Copy)]
pub struct ReclaimReport {
    pub live_keys: u64,
    pub live_bytes: u64,
    pub dead_files_dropped: u32,
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
    let read_results: Vec<Result<Vec<u8>>> = join_all(read_futures).await;

    // Write sequentially (preserves deterministic record order in the new file).
    let mut footer: Vec<FooterEntry> = Vec::with_capacity(live.len());
    let mut new_entries: Vec<(Bytes, IndexEntry, Option<u64>)> = Vec::with_capacity(live.len());
    let mut live_bytes: u64 = 0;

    for ((key, old_entry, ttl), bytes_res) in live.iter().zip(read_results) {
        let bytes = bytes_res?;
        let new_offset = new_file.append(bytes).await?;
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
    for (f, res) in sealed_files.iter().zip(unlink_results) {
        match res {
            Ok(()) => dead_files_dropped += 1,
            Err(e) => warn!(
                path = %f.path.display(),
                error = %e,
                "failed to unlink old sealed file after reclaim; disk space not freed"
            ),
        }
    }

    let report = ReclaimReport {
        live_keys,
        live_bytes,
        dead_files_dropped,
        new_file_id: next_file_id,
    };
    info!(?report, "reclaim complete");
    Ok((report, new_entries))
}

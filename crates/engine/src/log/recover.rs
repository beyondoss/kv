use std::path::PathBuf;

use bytes::Bytes;
use tracing::warn;

use crate::error::{EngineError, Result};
use crate::log::file::{FooterEntry, LogFile, list_data_files};
use crate::log::index::{IndexEntry, NsIndex};
use crate::log::record::{HEADER_LEN, flags as rflags, parse_header, verify_crc};

/// Result of opening a namespace directory.
pub struct OpenedFiles {
    /// Sealed (immutable) files, ordered by file_id ascending.
    pub sealed: Vec<LogFile>,
    /// Active file (writable). Always present after open — created if absent.
    pub active: LogFile,
    /// Populated index for the namespace.
    pub index: NsIndex,
}

/// Open all log files in `dir`, recovering the in-memory index.
///
/// Strategy:
/// 1. List `data-*.log` in file_id order. The highest is active, the rest are sealed.
/// 2. For each sealed file, try to load its footer. If absent or corrupt, scan the
///    file's records and rebuild the index from those.
/// 3. For the active file, scan records from offset 0 to EOF, validating CRC.
///    Truncate at first invalid CRC. Apply tombstones, full records, and TTL-update
///    records (the last only if the key currently exists in the index — orphan
///    TTL-updates are ignored).
pub async fn open_namespace(dir: PathBuf) -> Result<OpenedFiles> {
    std::fs::create_dir_all(&dir)?;
    let mut data_files = list_data_files(&dir)?;

    let mut index = NsIndex::new();
    let mut sealed: Vec<LogFile> = Vec::new();

    if data_files.is_empty() {
        // Fresh namespace — create active file id 0.
        let path = dir.join(crate::log::file::data_filename(0));
        let active = LogFile::open_rw(path, 0).await?;
        return Ok(OpenedFiles {
            sealed,
            active,
            index,
        });
    }

    let (active_id, active_path) = data_files.pop().ok_or(EngineError::BadRecord {
        offset: 0,
        reason: "data file list unexpectedly empty",
    })?;

    for (file_id, path) in data_files {
        let file = LogFile::open_ro(path, file_id).await?;
        match file.read_footer().await? {
            Some(entries) => {
                apply_footer_entries(&mut index, file_id, &entries);
            }
            None => {
                warn!(
                    file_id,
                    "sealed file footer missing or corrupt; rebuilding from records \
                     — data loss possible if records are also damaged"
                );
                rebuild_from_records(&file, file_id, &mut index).await?;
            }
        }
        sealed.push(file);
    }

    // Check if the highest-id file was cleanly sealed on shutdown (footer present).
    // If so, load it as a sealed file and open a fresh empty active file.
    let highest = LogFile::open_ro(active_path.clone(), active_id).await?;
    let active = match highest.read_footer().await? {
        Some(entries) => {
            apply_footer_entries(&mut index, active_id, &entries);
            sealed.push(highest);
            let next_id = active_id.checked_add(1).ok_or(EngineError::BadRecord {
                offset: 0,
                reason: "file_id overflow on clean-shutdown recovery",
            })?;
            if next_id >= u16::MAX - 100 {
                warn!(
                    file_id = next_id,
                    remaining = u16::MAX - next_id,
                    "file_id nearing u16::MAX; compact sealed files to reclaim IDs"
                );
            }
            let new_path = active_path
                .parent()
                .ok_or(EngineError::BadRecord {
                    offset: 0,
                    reason: "namespace data_dir has no parent; cannot compute next-file path",
                })?
                .join(crate::log::file::data_filename(next_id));
            LogFile::open_rw(new_path, next_id).await?
        }
        None => {
            drop(highest);
            let active = LogFile::open_rw(active_path, active_id).await?;
            replay_active(&active, active_id, &mut index).await?;
            active
        }
    };

    Ok(OpenedFiles {
        sealed,
        active,
        index,
    })
}

fn apply_footer_entries(index: &mut NsIndex, file_id: u16, entries: &[FooterEntry]) {
    for e in entries {
        let entry = IndexEntry::new(file_id, e.record_offset, e.record_size, e.tstamp_ms);
        index.insert(e.key.clone(), entry, e.expires_at_ms);
    }
}

/// Scan a file's records from the start, populating the index. Used as a
/// fallback when a sealed file's footer is missing/corrupt.
async fn rebuild_from_records(file: &LogFile, file_id: u16, index: &mut NsIndex) -> Result<()> {
    let total = file.size().await?;
    let mut offset = 0u64;
    while offset < total {
        let header_buf = match file.read_at(offset, HEADER_LEN).await {
            Ok(b) if b.len() < HEADER_LEN => break,
            Ok(b) => b,
            Err(e) => {
                warn!(file_id, offset, error = %e, "I/O error reading sealed file header; stopping scan at this offset");
                break;
            }
        };
        let hdr = match parse_header(&header_buf, offset) {
            Ok(h) => h,
            Err(_) => break,
        };
        let body_size = hdr.body_len();
        let body = match file.read_at(offset + HEADER_LEN as u64, body_size).await {
            Ok(b) if b.len() < body_size => break,
            Ok(b) => b,
            Err(e) => {
                warn!(file_id, offset, error = %e, "I/O error reading sealed file body; stopping scan at this offset");
                break;
            }
        };
        if verify_crc(&hdr, &header_buf, &body, offset).is_err() {
            break;
        }
        apply_record(index, file_id, offset, &hdr, &body);
        offset += hdr.record_len() as u64;
    }
    Ok(())
}

/// Replay the active file from offset 0 to EOF. On bad CRC, truncate at the
/// last good boundary.
async fn replay_active(file: &LogFile, file_id: u16, index: &mut NsIndex) -> Result<()> {
    let total = file.size().await?;
    let mut offset = 0u64;
    let mut last_good = 0u64;
    while offset < total {
        let header_buf = match file.read_at(offset, HEADER_LEN).await {
            Ok(b) if b.len() < HEADER_LEN => break,
            Ok(b) => b,
            Err(e) => {
                warn!(file_id, offset, error = %e, "I/O error reading active file header during replay; truncating at last good boundary");
                break;
            }
        };
        let hdr = match parse_header(&header_buf, offset) {
            Ok(h) => h,
            Err(_) => break,
        };
        let body_size = hdr.body_len();
        let body = match file.read_at(offset + HEADER_LEN as u64, body_size).await {
            Ok(b) if b.len() < body_size => break,
            Ok(b) => b,
            Err(e) => {
                warn!(file_id, offset, error = %e, "I/O error reading active file body during replay; truncating at last good boundary");
                break;
            }
        };
        if verify_crc(&hdr, &header_buf, &body, offset).is_err() {
            warn!(offset, "bad CRC during active replay; truncating");
            break;
        }
        apply_record(index, file_id, offset, &hdr, &body);
        let len = hdr.record_len() as u64;
        offset += len;
        last_good = offset;
    }

    if last_good < total {
        warn!(
            truncate_to = last_good,
            was = total,
            "truncating active file at last good boundary"
        );
        file.truncate_to(last_good).await?;
    }
    Ok(())
}

fn apply_record(
    index: &mut NsIndex,
    file_id: u16,
    offset: u64,
    hdr: &crate::log::record::RecordHeader,
    body: &[u8],
) {
    if body.len() < hdr.key_size as usize {
        warn!(
            offset,
            key_size = hdr.key_size,
            body_len = body.len(),
            "body shorter than declared key_size; skipping record"
        );
        return;
    }
    let key = &body[..hdr.key_size as usize];

    if hdr.flags & rflags::TOMBSTONE != 0 {
        index.remove(key);
        return;
    }

    if hdr.flags & rflags::TTL_UPDATE != 0 {
        // Orphan TTL-updates (key not currently in index) are ignored — see plan.
        if index.get(key).is_some() {
            let key_bytes = Bytes::copy_from_slice(key);
            let ttl = if hdr.flags & rflags::NO_EXPIRY != 0 {
                None
            } else {
                Some(hdr.expires_at_ms)
            };
            index.set_ttl(&key_bytes, ttl);
        }
        return;
    }

    // Full record.
    let record_size = match u32::try_from(hdr.record_len()) {
        Ok(n) => n,
        Err(_) => return, // record > 4 GiB is invalid; skip silently
    };
    let entry = IndexEntry::new(file_id, offset, record_size, hdr.tstamp_ms);
    let ttl = if hdr.flags & rflags::NO_EXPIRY != 0 {
        None
    } else {
        Some(hdr.expires_at_ms)
    };
    index.insert(Bytes::copy_from_slice(key), entry, ttl);
}

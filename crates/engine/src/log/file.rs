use std::cell::Cell;
use std::os::unix::prelude::OpenOptionsExt;
use std::path::{Path, PathBuf};

use monoio::fs::{File, OpenOptions};

use tracing::warn;

use crate::error::{EngineError, Result};
use crate::log::index::IndexEntry;

/// Magic at the very end of every sealed file. Lets recovery distinguish
/// "sealed cleanly" from "active or crashed mid-seal" without scanning.
pub const FOOTER_MAGIC: u64 = 0x4259_4F4E_445F_4B56; // "BYOND_KV"
/// Footer trailer size: footer_body_len (8) + footer_crc (8) + magic (8).
pub const FOOTER_TRAILER_LEN: u64 = 24;

/// One footer entry per live key when the file was sealed.
///
/// Wire layout (little-endian):
///   [key_size: u32][record_offset: u64][record_size: u32]
///   [expires_at_ms: u64 (0 if absent)][has_expiry: u8]
///   [key bytes]
#[derive(Debug, Clone)]
pub struct FooterEntry {
    pub key: bytes::Bytes,
    pub record_offset: u64,
    pub record_size: u32,
    pub expires_at_ms: Option<u64>,
}

impl FooterEntry {
    fn encoded_size(&self) -> usize {
        4 + 8 + 4 + 8 + 1 + self.key.len()
    }

    fn encode_into(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&(self.key.len() as u32).to_le_bytes());
        buf.extend_from_slice(&self.record_offset.to_le_bytes());
        buf.extend_from_slice(&self.record_size.to_le_bytes());
        let (has_expiry, ms) = match self.expires_at_ms {
            Some(ms) => (1u8, ms),
            None => (0u8, 0u64),
        };
        buf.extend_from_slice(&ms.to_le_bytes());
        buf.push(has_expiry);
        buf.extend_from_slice(&self.key);
    }

    fn parse(buf: &[u8]) -> Option<(Self, usize)> {
        if buf.len() < 25 {
            return None;
        }
        let key_size = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        let record_offset = u64::from_le_bytes([buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10], buf[11]]);
        let record_size = u32::from_le_bytes([buf[12], buf[13], buf[14], buf[15]]);
        let expires_ms = u64::from_le_bytes([buf[16], buf[17], buf[18], buf[19], buf[20], buf[21], buf[22], buf[23]]);
        let has_expiry = buf[24];
        let total = 25 + key_size;
        if buf.len() < total {
            return None;
        }
        let key = bytes::Bytes::copy_from_slice(&buf[25..total]);
        Some((
            Self {
                key,
                record_offset,
                record_size,
                expires_at_ms: if has_expiry != 0 { Some(expires_ms) } else { None },
            },
            total,
        ))
    }
}

pub fn data_filename(file_id: u16) -> String {
    format!("data-{:010}.log", file_id)
}

pub fn reclaim_tmp_filename(file_id: u16) -> String {
    format!("data-{:010}.log.tmp", file_id)
}

/// An open log file. Used for both active (writable) and sealed (read-only)
/// files; the only difference is whether `append` is called.
///
/// Concurrency: methods take `&self`. The underlying `monoio::fs::File`
/// supports concurrent `read_at`/`write_at` to non-overlapping ranges (each
/// future submits its own io_uring SQE). `write_offset` is a `Cell<u64>` —
/// safe under single-thread (`!Sync`) access; sufficient since each shard runs
/// on its own monoio runtime.
pub struct LogFile {
    pub file_id: u16,
    pub path: PathBuf,
    file: File,
    write_offset: Cell<u64>,
    poisoned: Cell<bool>,
}

impl LogFile {
    pub async fn open_rw(path: PathBuf, file_id: u16) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .mode(0o600)
            .open(&path)
            .await?;
        let metadata = file.metadata().await?;
        let len = metadata.len();
        Ok(Self {
            file_id,
            path,
            file,
            write_offset: Cell::new(len),
            poisoned: Cell::new(false),
        })
    }

    pub async fn open_ro(path: PathBuf, file_id: u16) -> Result<Self> {
        let file = OpenOptions::new().read(true).open(&path).await?;
        let metadata = file.metadata().await?;
        let len = metadata.len();
        Ok(Self {
            file_id,
            path,
            file,
            write_offset: Cell::new(len),
            poisoned: Cell::new(false),
        })
    }

    pub async fn size(&self) -> Result<u64> {
        let metadata = self.file.metadata().await?;
        Ok(metadata.len())
    }

    pub fn write_offset(&self) -> u64 {
        self.write_offset.get()
    }

    /// Append a buffer at an offset reserved atomically *before* awaiting the
    /// kernel write. Concurrent appenders see distinct offsets; their writes
    /// run as parallel io_uring SQEs.
    pub async fn append(&self, buf: Vec<u8>) -> Result<u64> {
        if self.poisoned.get() {
            return Err(EngineError::Io {
                source: std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "log file poisoned after prior write error",
                ),
            });
        }
        let len = buf.len() as u64;
        let offset = self.write_offset.get();
        self.write_offset.set(offset + len);
        let (res, _buf) = self.file.write_all_at(buf, offset).await;
        if let Err(e) = res {
            self.poisoned.set(true);
            return Err(EngineError::Io { source: e });
        }
        Ok(offset)
    }

    pub async fn sync(&self) -> Result<()> {
        self.file.sync_all().await?;
        Ok(())
    }

    pub async fn read_exact(&self, offset: u64, size: usize) -> Result<Vec<u8>> {
        let buf = vec![0u8; size];
        let (res, buf) = self.file.read_exact_at(buf, offset).await;
        res?;
        Ok(buf)
    }

    pub async fn read_at(&self, offset: u64, size: usize) -> Result<Vec<u8>> {
        let buf = vec![0u8; size];
        let (res, mut buf) = self.file.read_at(buf, offset).await;
        let n = res?;
        buf.truncate(n);
        Ok(buf)
    }

    /// Write the sealed-file footer at the current offset and fsync.
    pub async fn write_footer(&self, entries: &[FooterEntry]) -> Result<()> {
        let body_size: usize = entries.iter().map(|e| e.encoded_size()).sum();
        let mut body = Vec::with_capacity(body_size);
        for e in entries {
            e.encode_into(&mut body);
        }
        let crc = crc_fast::checksum(crc_fast::CrcAlgorithm::Crc64Nvme, &body);
        let mut trailer = Vec::with_capacity(FOOTER_TRAILER_LEN as usize);
        trailer.extend_from_slice(&(body.len() as u64).to_le_bytes());
        trailer.extend_from_slice(&crc.to_le_bytes());
        trailer.extend_from_slice(&FOOTER_MAGIC.to_le_bytes());

        let _body_offset = self.append(body).await?;
        let _trailer_offset = self.append(trailer).await?;
        self.sync().await?;
        Ok(())
    }

    /// Look for a footer at end-of-file. Returns None if magic doesn't match
    /// (file was active or crashed mid-seal). Returns Err on read failures or
    /// CRC mismatch.
    pub async fn read_footer(&self) -> Result<Option<Vec<FooterEntry>>> {
        let total_size = self.size().await?;
        if total_size < FOOTER_TRAILER_LEN {
            return Ok(None);
        }

        let trailer = self
            .read_exact(total_size - FOOTER_TRAILER_LEN, FOOTER_TRAILER_LEN as usize)
            .await?;
        let body_len = u64::from_le_bytes([trailer[0], trailer[1], trailer[2], trailer[3], trailer[4], trailer[5], trailer[6], trailer[7]]);
        let stored_crc = u64::from_le_bytes([trailer[8], trailer[9], trailer[10], trailer[11], trailer[12], trailer[13], trailer[14], trailer[15]]);
        let magic = u64::from_le_bytes([trailer[16], trailer[17], trailer[18], trailer[19], trailer[20], trailer[21], trailer[22], trailer[23]]);
        if magic != FOOTER_MAGIC {
            return Ok(None);
        }
        if body_len > total_size - FOOTER_TRAILER_LEN {
            return Err(EngineError::BadRecord {
                offset: total_size - FOOTER_TRAILER_LEN,
                reason: "footer body length out of range",
            });
        }
        let body_offset = total_size - FOOTER_TRAILER_LEN - body_len;
        let body = self.read_exact(body_offset, body_len as usize).await?;
        let actual_crc = crc_fast::checksum(crc_fast::CrcAlgorithm::Crc64Nvme, &body);
        if actual_crc != stored_crc {
            return Err(EngineError::CrcMismatch { offset: body_offset });
        }

        let mut entries: Vec<FooterEntry> = Vec::new();
        let mut cursor = 0usize;
        while cursor < body.len() {
            let (entry, used) = FooterEntry::parse(&body[cursor..]).ok_or(
                EngineError::BadRecord {
                    offset: body_offset + cursor as u64,
                    reason: "malformed footer entry",
                },
            )?;
            entries.push(entry);
            cursor += used;
        }
        Ok(Some(entries))
    }

    /// Truncate the on-disk file to `size`. monoio's File doesn't expose
    /// `set_len`, so we re-open via std briefly. Safe because we have exclusive
    /// access to the path during recovery.
    pub async fn truncate_to(&self, size: u64) -> Result<()> {
        let path = self.path.clone();
        let std_file = std::fs::OpenOptions::new().write(true).open(&path)?;
        std_file.set_len(size)?;
        self.write_offset.set(size);
        Ok(())
    }
}

pub(crate) fn footer_entry_from_index(
    key: bytes::Bytes,
    entry: &IndexEntry,
    expires_at_ms: Option<u64>,
) -> FooterEntry {
    FooterEntry {
        key,
        record_offset: entry.record_offset,
        record_size: entry.record_size,
        expires_at_ms,
    }
}

/// List all `data-*.log` files in `dir`, sorted ascending by file_id.
pub fn list_data_files(dir: &Path) -> Result<Vec<(u16, PathBuf)>> {
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut out: Vec<(u16, PathBuf)> = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            warn!(path = %path.display(), "skipping data directory entry with non-UTF-8 filename");
            continue;
        };
        let Some(rest) = name.strip_prefix("data-") else { continue };
        let Some(num) = rest.strip_suffix(".log") else { continue };
        let Ok(file_id_u32) = num.parse::<u32>() else { continue };
        if file_id_u32 > u16::MAX as u32 {
            continue;
        }
        out.push((file_id_u32 as u16, path));
    }
    out.sort_by_key(|(id, _)| *id);
    Ok(out)
}

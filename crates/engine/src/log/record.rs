use crate::error::{EngineError, Result};
use crc_fast::{CrcAlgorithm, checksum};

fn u64_le(buf: &[u8], off: usize, at: u64) -> Result<u64> {
    buf.get(off..off + 8)
        .and_then(|s| s.try_into().ok())
        .map(u64::from_le_bytes)
        .ok_or(EngineError::BadRecord {
            offset: at,
            reason: "truncated u64 field",
        })
}
fn u32_le(buf: &[u8], off: usize, at: u64) -> Result<u32> {
    buf.get(off..off + 4)
        .and_then(|s| s.try_into().ok())
        .map(u32::from_le_bytes)
        .ok_or(EngineError::BadRecord {
            offset: at,
            reason: "truncated u32 field",
        })
}

/// Algorithm used for record + footer CRCs.
const CRC_ALG: CrcAlgorithm = CrcAlgorithm::Crc64Nvme;

/// Record bit flags.
pub mod flags {
    pub const TOMBSTONE: u8 = 0b0000_0001;
    pub const NO_EXPIRY: u8 = 0b0000_0010;
    pub const TTL_UPDATE: u8 = 0b0000_0100;
    /// Value-separated: the record's value field is a 16-byte content hash, not
    /// the value itself. The value lives in the content-addressed blob store
    /// (`value_store`). Set for values >= `LogConfig::value_sep_threshold`.
    pub const VALUE_SEP: u8 = 0b0000_1000;
}

/// Fixed header bytes preceding every record.
/// Layout (little-endian):
///   0..8   crc64
///   8..16  tstamp_ms
///   16..17 flags
///   17..25 expires_at_ms
///   25..29 key_size
///   29..33 val_size
///   33..37 meta_size
pub const HEADER_LEN: usize = 37;

#[derive(Debug, Clone, Copy)]
pub struct RecordHeader {
    pub crc: u64,
    pub tstamp_ms: u64,
    pub flags: u8,
    pub expires_at_ms: u64,
    pub key_size: u32,
    pub val_size: u32,
    pub meta_size: u32,
}

impl RecordHeader {
    pub fn body_len(&self) -> usize {
        self.key_size as usize + self.val_size as usize + self.meta_size as usize
    }

    pub fn record_len(&self) -> usize {
        HEADER_LEN + self.body_len()
    }
}

/// Parse a record header from a byte slice (does NOT verify CRC against body).
pub fn parse_header(buf: &[u8], offset: u64) -> Result<RecordHeader> {
    if buf.len() < HEADER_LEN {
        return Err(EngineError::BadRecord {
            offset,
            reason: "short header",
        });
    }
    let crc = u64_le(buf, 0, offset)?;
    let tstamp_ms = u64_le(buf, 8, offset)?;
    let flags = buf[16];
    let expires_at_ms = u64_le(buf, 17, offset)?;
    let key_size = u32_le(buf, 25, offset)?;
    let val_size = u32_le(buf, 29, offset)?;
    let meta_size = u32_le(buf, 33, offset)?;
    Ok(RecordHeader {
        crc,
        tstamp_ms,
        flags,
        expires_at_ms,
        key_size,
        val_size,
        meta_size,
    })
}

/// Verify a header's CRC against the bytes that follow it.
pub fn verify_crc(
    header: &RecordHeader,
    header_bytes: &[u8],
    body: &[u8],
    offset: u64,
) -> Result<()> {
    debug_assert_eq!(header_bytes.len(), HEADER_LEN);
    debug_assert_eq!(body.len(), header.body_len());
    // CRC covers everything after the CRC field itself: tstamp_ms..end of body.
    let mut digest = crc_fast::Digest::new(CRC_ALG);
    digest.update(&header_bytes[8..]);
    digest.update(body);
    let actual = digest.finalize();
    if actual != header.crc {
        return Err(EngineError::CrcMismatch { offset });
    }
    Ok(())
}

/// Encode a full record into a buffer.
///
/// Caller passes the timestamp; we don't read the clock here so encode is deterministic
/// and testable.
pub fn encode_into(
    buf: &mut Vec<u8>,
    tstamp_ms: u64,
    flags: u8,
    expires_at_ms: u64,
    key: &[u8],
    value: &[u8],
    metadata: &[u8],
) -> Result<()> {
    // Total record must fit in u32 to be addressable via IndexEntry.record_size.
    let record_len = HEADER_LEN
        .checked_add(key.len())
        .and_then(|n| n.checked_add(value.len()))
        .and_then(|n| n.checked_add(metadata.len()))
        .filter(|&n| n <= u32::MAX as usize)
        .ok_or(EngineError::BadRecord {
            offset: 0,
            reason: "record exceeds 4 GiB limit",
        })?;
    let _ = record_len;
    let key_size = key.len() as u32;
    let val_size = value.len() as u32;
    let meta_size = metadata.len() as u32;

    let start = buf.len();
    buf.resize(start + HEADER_LEN, 0);
    // CRC placeholder; filled in below.
    buf[start + 8..start + 16].copy_from_slice(&tstamp_ms.to_le_bytes());
    buf[start + 16] = flags;
    buf[start + 17..start + 25].copy_from_slice(&expires_at_ms.to_le_bytes());
    buf[start + 25..start + 29].copy_from_slice(&key_size.to_le_bytes());
    buf[start + 29..start + 33].copy_from_slice(&val_size.to_le_bytes());
    buf[start + 33..start + 37].copy_from_slice(&meta_size.to_le_bytes());
    buf.extend_from_slice(key);
    buf.extend_from_slice(value);
    buf.extend_from_slice(metadata);

    // Compute CRC over everything from start+8 to end.
    let crc = checksum(CRC_ALG, &buf[start + 8..]);
    buf[start..start + 8].copy_from_slice(&crc.to_le_bytes());
    Ok(())
}

/// Encode a record into a fresh buffer.
pub fn encode(
    tstamp_ms: u64,
    flags: u8,
    expires_at_ms: u64,
    key: &[u8],
    value: &[u8],
    metadata: &[u8],
) -> Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(HEADER_LEN + key.len() + value.len() + metadata.len());
    encode_into(
        &mut buf,
        tstamp_ms,
        flags,
        expires_at_ms,
        key,
        value,
        metadata,
    )?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let buf = encode(123, 0, 999, b"key", b"value", b"meta").unwrap();
        let hdr = parse_header(&buf, 0).unwrap();
        assert_eq!(hdr.tstamp_ms, 123);
        assert_eq!(hdr.flags, 0);
        assert_eq!(hdr.expires_at_ms, 999);
        assert_eq!(hdr.key_size, 3);
        assert_eq!(hdr.val_size, 5);
        assert_eq!(hdr.meta_size, 4);
        assert_eq!(hdr.record_len(), buf.len());
        let body = &buf[HEADER_LEN..];
        verify_crc(&hdr, &buf[..HEADER_LEN], body, 0).unwrap();
        assert_eq!(&body[..3], b"key");
        assert_eq!(&body[3..8], b"value");
        assert_eq!(&body[8..12], b"meta");
    }

    #[test]
    fn crc_mismatch_detected() {
        let mut buf = encode(1, 0, 0, b"k", b"v", b"").unwrap();
        // Flip a bit in the value.
        let val_offset = HEADER_LEN + 1;
        buf[val_offset] ^= 1;
        let hdr = parse_header(&buf, 0).unwrap();
        let body = &buf[HEADER_LEN..];
        let err = verify_crc(&hdr, &buf[..HEADER_LEN], body, 0).unwrap_err();
        assert!(matches!(err, EngineError::CrcMismatch { offset: 0 }));
    }

    #[test]
    fn tombstone_is_zero_body() {
        let buf = encode(1, flags::TOMBSTONE, 0, b"k", b"", b"").unwrap();
        let hdr = parse_header(&buf, 0).unwrap();
        assert_eq!(hdr.flags & flags::TOMBSTONE, flags::TOMBSTONE);
        assert_eq!(hdr.val_size, 0);
        assert_eq!(hdr.meta_size, 0);
    }

    #[test]
    fn ttl_update_carries_no_value() {
        let buf = encode(1, flags::TTL_UPDATE, 5000, b"k", b"", b"").unwrap();
        let hdr = parse_header(&buf, 0).unwrap();
        assert_eq!(hdr.flags & flags::TTL_UPDATE, flags::TTL_UPDATE);
        assert_eq!(hdr.expires_at_ms, 5000);
        assert_eq!(hdr.val_size, 0);
    }
}

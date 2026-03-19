use std::error::Error;
use std::fmt::{Display, Formatter};

/// Fixed header size for a frame: [u64 offset][u32 len][u32 crc]
pub const FRAME_HEADER_SIZE: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecodedFrame<'a> {
    pub offset: u64,
    pub payload: &'a [u8],
    pub frame_len: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrameDecodeError {
    CrcMismatch {
        offset: u64,
        expected_crc: u32,
        computed_crc: u32,
    },
}

impl Display for FrameDecodeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CrcMismatch {
                offset,
                expected_crc,
                computed_crc,
            } => write!(
                f,
                "frame CRC mismatch at offset {} (expected {}, got {})",
                offset, expected_crc, computed_crc
            ),
        }
    }
}

impl Error for FrameDecodeError {}

#[cfg(test)]
pub fn encode_frame(offset: u64, payload: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(FRAME_HEADER_SIZE + payload.len());
    append_encoded_frame(&mut frame, offset, payload);
    frame
}

pub fn append_encoded_frame(out: &mut Vec<u8>, offset: u64, payload: &[u8]) {
    let len = payload.len() as u32;
    let crc = crc32fast::hash(payload);
    out.extend_from_slice(&offset.to_le_bytes());
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(payload);
}

/// Decode exactly one WAL frame from the start of `buf`.
///
/// Return model
/// - `Ok(Some(frame))` when a full frame is present and its CRC matches.
/// - `Ok(None)` when `buf` ends in a partial header or partial payload, so the caller should wait
///   for more bytes.
/// - `Err(FrameDecodeError::CrcMismatch)` when a full frame is present but corrupted.
///
/// This three-way result lets stream readers distinguish incomplete trailing bytes from true data
/// corruption without guessing from buffer length alone.
pub fn decode_next_frame(buf: &[u8]) -> Result<Option<DecodedFrame<'_>>, FrameDecodeError> {
    if buf.len() < FRAME_HEADER_SIZE {
        return Ok(None);
    }

    let offset = u64::from_le_bytes(buf[..8].try_into().unwrap());
    let payload_len = u32::from_le_bytes(buf[8..12].try_into().unwrap()) as usize;
    let crc = u32::from_le_bytes(buf[12..16].try_into().unwrap());
    let frame_len = FRAME_HEADER_SIZE + payload_len;
    if frame_len > buf.len() {
        return Ok(None);
    }

    let payload = &buf[FRAME_HEADER_SIZE..frame_len];
    let computed_crc = crc32fast::hash(payload);
    if computed_crc != crc {
        return Err(FrameDecodeError::CrcMismatch {
            offset,
            expected_crc: crc,
            computed_crc,
        });
    }

    Ok(Some(DecodedFrame {
        offset,
        payload,
        frame_len,
    }))
}

/// Scan the buffer and return the largest prefix length that ends exactly on a full frame.
/// Validates CRC for each full frame. On CRC mismatch, stops and returns the last
/// known-safe boundary (defensive).
///
/// This helper is used before exporting or replaying bytes from files that may end with a partially
/// flushed frame. It trims the buffer to the longest prefix that is both frame-aligned and CRC-clean.
pub fn scan_safe_frame_boundary(buf: &[u8]) -> usize {
    let mut idx = 0usize;
    while idx < buf.len() {
        match decode_next_frame(&buf[idx..]) {
            Ok(Some(frame)) => idx += frame.frame_len,
            Ok(None) | Err(_) => break,
        }
    }
    idx
}

/// Extract first and last offsets inside a complete-frames prefix. Assumes `buf`
/// ends on a valid frame boundary (or will ignore trailing partial data).
///
/// This is a lightweight way to recover a file or segment's inclusive offset range without fully
/// decoding the embedded `StreamMessage` payloads.
pub fn extract_offsets(buf: &[u8]) -> (Option<u64>, Option<u64>) {
    let mut idx = 0usize;
    let mut first: Option<u64> = None;
    let mut last: Option<u64> = None;

    while idx < buf.len() {
        match decode_next_frame(&buf[idx..]) {
            Ok(Some(frame)) => {
                if first.is_none() {
                    first = Some(frame.offset);
                }
                last = Some(frame.offset);
                idx += frame.frame_len;
            }
            Ok(None) | Err(_) => break,
        }
    }

    (first, last)
}

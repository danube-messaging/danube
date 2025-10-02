use crc32fast;

/// Fixed header size for a frame: [u64 offset][u32 len][u32 crc]
pub const FRAME_HEADER_SIZE: usize = 16;

/// Scan the buffer and return the largest prefix length that ends exactly on a full frame.
/// Validates CRC for each full frame. On CRC mismatch, stops and returns the last
/// known-safe boundary (defensive).
pub fn scan_safe_frame_boundary_with_crc(buf: &[u8]) -> usize {
    let mut idx = 0usize;
    while idx + FRAME_HEADER_SIZE <= buf.len() {
        let len = u32::from_le_bytes(buf[idx + 8..idx + 12].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(buf[idx + 12..idx + 16].try_into().unwrap());
        let next = idx + FRAME_HEADER_SIZE + len;
        if next > buf.len() {
            break;
        }
        let payload = &buf[idx + FRAME_HEADER_SIZE..next];
        let computed = crc32fast::hash(payload);
        if computed != crc {
            break;
        }
        idx = next;
    }
    idx
}

/// Extract first and last offsets inside a complete-frames prefix. Assumes `buf`
/// ends on a valid frame boundary (or will ignore trailing partial data).
pub fn extract_offsets_in_prefix(buf: &[u8]) -> (Option<u64>, Option<u64>) {
    let mut idx = 0usize;
    let mut first: Option<u64> = None;
    let mut last: Option<u64> = None;
    while idx + FRAME_HEADER_SIZE <= buf.len() {
        let off = u64::from_le_bytes(buf[idx..idx + 8].try_into().unwrap());
        let len = u32::from_le_bytes(buf[idx + 8..idx + 12].try_into().unwrap()) as usize;
        if first.is_none() {
            first = Some(off);
        }
        last = Some(off);
        let next = idx + FRAME_HEADER_SIZE + len;
        if next > buf.len() {
            break;
        }
        idx = next;
    }
    (first, last)
}

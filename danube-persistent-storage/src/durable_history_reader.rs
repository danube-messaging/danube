use danube_core::message::StreamMessage;
use danube_core::storage::{PersistentStorageError, TopicStream};

use crate::durable_store::{segment_object_path, DurableRangeReader, DurableStore};
use crate::frames::{decode_next_frame, FrameDecodeError};
use crate::persistent_metrics::{
    DURABLE_HISTORY_SEGMENTS_READ_TOTAL, DURABLE_HISTORY_READER_ERRORS_TOTAL, DURABLE_HISTORY_READ_BYTES_TOTAL,
};
use crate::metadata::StorageMetadata;
use metrics::counter;
use std::collections::VecDeque;
use std::sync::Arc;
use tracing::error;

/// DurableHistoryReader reads historical messages for a topic from durable segments
/// that contain raw WAL frames exported from the hot log.
///
/// Frame format per record (little-endian):
/// - `u64 offset`
/// - `u32 len`
/// - `u32 crc`
/// - `len` bytes (bincode-serialized `StreamMessage`)
#[derive(Clone, Debug)]
pub struct DurableHistoryReader {
    durable_store: Arc<dyn DurableStore>,
    metadata: StorageMetadata,
    topic_path: String,
}

impl DurableHistoryReader {
    /// Create a new DurableHistoryReader for a given topic path.
    ///
    /// `topic_path` should be the logical topic identifier used in metadata
    /// (e.g., "ns/topic").
    pub fn new(
        durable_store: Arc<dyn DurableStore>,
        metadata: StorageMetadata,
        topic_path: String,
    ) -> Self {
        Self {
            durable_store,
            metadata,
            topic_path,
        }
    }

    /// Return the topic path associated with this DurableHistoryReader.
    pub fn topic_path(&self) -> &str {
        &self.topic_path
    }

    /// Read messages in the inclusive range `[start, end_inclusive]` from durable segments.
    /// If `end_inclusive` is None, read all available segments starting at `start`.
    ///
    /// Segments are discovered via metadata segment descriptors and filtered by
    /// `[start_offset, end_offset]` overlap. Each selected segment is decoded
    /// as a sequence of raw WAL frames. Frames outside the requested range
    /// are filtered out. The resulting stream yields messages ordered by offset.
    ///
    /// Functional flow
    /// - Load and sort all segment descriptors that overlap the requested offset range.
    /// - Open each durable object lazily, using the sparse offset index to seek near `start` when
    ///   available.
    /// - Read the object in chunks, parsing only complete frames and carrying any trailing partial
    ///   frame bytes into the next read.
    /// - Buffer decoded messages temporarily so the stream can yield one item at a time while the
    ///   parser works in chunk-sized batches.
    ///
    /// This reader is intentionally sequential across segment objects: it preserves on-disk segment
    /// order and avoids overlapping reads from multiple durable objects at once.
    pub async fn read_range(
        &self,
        start: u64,
        end_inclusive: Option<u64>,
    ) -> Result<TopicStream, PersistentStorageError> {
        // Fetch descriptors and keep only those overlapping the requested range.
        let mut descriptors = self
            .metadata
            .get_segment_descriptors(self.topic_path())
            .await?;
        // Keep only overlapping descriptors and ensure ascending order by start_offset.
        descriptors.retain(|d| d.end_offset >= start);
        if let Some(end) = end_inclusive {
            descriptors.retain(|d| d.start_offset <= end);
        }
        descriptors.sort_by_key(|d| d.start_offset);

        // Build a stream that iterates objects and yields messages lazily
        let durable_store = self.durable_store.clone();
        let topic_path = self.topic_path.clone();
        let chunk_size: usize = 4 * 1024 * 1024; // 4 MiB
        let mut idx = 0usize;

        // State for current object
        let mut cur_reader: Option<DurableRangeReader> = None;
        let mut carry: Vec<u8> = Vec::new();
        let mut emit_buf: VecDeque<StreamMessage> = VecDeque::new();

        let s = async_stream::try_stream! {
            loop {
                // Emit buffered messages first
                if let Some(msg) = emit_buf.pop_front() {
                    yield msg;
                    continue;
                }

                // If no current reader, open next object
                if cur_reader.is_none() {
                    if idx >= descriptors.len() {
                        break;
                    }
                    let desc = &descriptors[idx];
                    idx += 1;
                    let key = segment_object_path(&topic_path, &desc.segment_id);
                    // Use sparse index if present to jump near the requested start offset.
                    let start_byte = match &desc.offset_index {
                        Some(index) if !index.is_empty() => {
                            find_start_byte(index, start)
                        }
                        _ => 0u64,
                    };
                    let provider = durable_store.provider().to_string();
                    let reader = durable_store.open_segment_reader(&key, start_byte).await?;
                    counter!(DURABLE_HISTORY_SEGMENTS_READ_TOTAL.name, "topic"=> topic_path.clone(), "provider"=> provider.clone()).increment(1);
                    cur_reader = Some(reader);
                    carry.clear();
                }

                // Read next chunk from current reader
                let reader = cur_reader.as_mut().unwrap();
                let provider = durable_store.provider().to_string();
                let chunk = reader.read_chunk(chunk_size).await?;
                if !chunk.is_empty() {
                    counter!(DURABLE_HISTORY_READ_BYTES_TOTAL.name, "topic"=> topic_path.clone(), "provider"=> provider.clone()).increment(chunk.len() as u64);
                }
                if chunk.is_empty() {
                    // End of current object
                    cur_reader = None;
                    continue;
                }
                carry.extend_from_slice(&chunk);

                // Parse complete frames from carry, leave remainder in carry
                let mut parsed: Vec<(u64, StreamMessage)> = Vec::new();
                if let Err(e) = parse_frames_from_carry(&mut carry, &mut parsed, &provider) {
                    // Increment error counter with a coarse reason derived below
                    let reason = if e.to_string().contains("CRC mismatch") { "crc" } else { "decode" };
                    counter!(DURABLE_HISTORY_READER_ERRORS_TOTAL.name, "provider"=> provider.clone(), "reason"=> reason).increment(1);
                    Err(e)?;
                }
                // Filter by requested offset range and push into emit buffer (reverse for pop())
                for (_off, msg) in parsed
                    .into_iter()
                    .filter(|(off, _)| *off >= start && end_inclusive.map(|e| *off <= e).unwrap_or(true))
                {
                    emit_buf.push_back(msg);
                }
            }
        };

        Ok(Box::pin(s))
    }
}

/// Parse a cloud object composed of concatenated raw WAL frames and return
/// a vector of `(offset, StreamMessage)` pairs. Stops gracefully at the first
/// partial frame at the end of the object.
///
/// The caller reuses `carry` across chunk reads. This helper consumes only the prefix made of full
/// valid frames, appends decoded messages into `out`, and leaves any trailing partial frame bytes in
/// `carry` so the next object read can complete them.
fn parse_frames_from_carry(
    carry: &mut Vec<u8>,
    out: &mut Vec<(u64, StreamMessage)>,
    _provider: &str,
) -> Result<(), PersistentStorageError> {
    let mut idx = 0usize;
    while idx < carry.len() {
        let frame = match decode_next_frame(&carry[idx..]) {
            Ok(Some(frame)) => frame,
            Ok(None) => break,
            Err(FrameDecodeError::CrcMismatch {
                offset,
                expected_crc,
                computed_crc,
            }) => {
                error!(
                    target = "durable_history_reader",
                    at_byte = idx,
                    offset,
                    expected_crc,
                    computed_crc,
                    "CRC mismatch while decoding durable segment"
                );
                return Err(PersistentStorageError::Other(format!(
                    "durable_history_reader: CRC mismatch at offset {} (expected {}, got {})",
                    offset, expected_crc, computed_crc
                )));
            }
        };
        let mut msg: StreamMessage =
            bincode::serde::decode_from_slice(frame.payload, bincode::config::standard())
                .map(|(v, _)| v)
                .map_err(|e| {
                    PersistentStorageError::Other(format!(
                        "durable_history_reader: bincode decode failed: {}",
                        e
                    ))
                })?;
        msg.msg_id.topic_offset = frame.offset;
        out.push((frame.offset, msg));
        idx += frame.frame_len;
    }
    // Remove consumed prefix, retain leftover for next chunk
    if idx > 0 {
        carry.drain(0..idx);
    }
    Ok(())
}

/// Find the byte position from a sparse `(offset, byte_pos)` index for a target `start_offset`.
/// Returns the `byte_pos` of the greatest entry with `offset <= start_offset`, or 0 if none.
///
/// This is a lower-cost seek hint, not an exact frame lookup. The caller still scans forward from
/// the returned byte position until it reaches the first requested message offset.
fn find_start_byte(index: &[(u64, u64)], start_offset: u64) -> u64 {
    if index.is_empty() {
        return 0;
    }
    let mut lo = 0usize;
    let mut hi = index.len();
    while lo < hi {
        let mid = (lo + hi) / 2;
        if index[mid].0 <= start_offset {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    if lo == 0 {
        0
    } else {
        index[lo - 1].1
    }
}

use danube_core::message::StreamMessage;
use danube_core::storage::{PersistentStorageError, TopicStream};

use crate::etcd_metadata::EtcdMetadata;
use crate::{CloudRangeReader, CloudStore};

/// CloudReader reads historical messages for a topic from cloud objects
/// that contain raw WAL frames as uploaded by the Uploader.
///
/// Frame format per record (little-endian):
/// - `u64 offset`
/// - `u32 len`
/// - `u32 crc`
/// - `len` bytes (bincode-serialized `StreamMessage`)
#[derive(Clone, Debug)]
pub struct CloudReader {
    cloud: CloudStore,
    etcd: EtcdMetadata,
    topic_path: String,
}

impl CloudReader {
    /// Create a new CloudReader for a given topic path.
    ///
    /// `topic_path` should be the logical topic identifier used in metadata
    /// (e.g., "ns/topic").
    pub fn new(cloud: CloudStore, etcd: EtcdMetadata, topic_path: String) -> Self {
        Self {
            cloud,
            etcd,
            topic_path,
        }
    }

    /// Return the topic path associated with this CloudReader.
    pub fn topic_path(&self) -> &str {
        &self.topic_path
    }

    /// Expose the underlying Etcd metadata helper (primarily for tests).
    pub fn etcd(&self) -> &EtcdMetadata {
        &self.etcd
    }

    /// Read messages in the inclusive range `[start, end_inclusive]` from cloud objects.
    /// If `end_inclusive` is None, read all available objects starting at `start`.
    ///
    /// Objects are discovered via ETCD object descriptors and filtered by
    /// `[start_offset, end_offset]` overlap. Each selected object is decoded
    /// as a sequence of raw WAL frames. Frames outside the requested range
    /// are filtered out. The resulting stream yields messages ordered by offset.
    pub async fn read_range(
        &self,
        start: u64,
        end_inclusive: Option<u64>,
    ) -> Result<TopicStream, PersistentStorageError> {
        // Fetch descriptors and keep only those overlapping the requested range.
        let mut descriptors = self.etcd.get_object_descriptors(self.topic_path()).await?;
        // Keep only overlapping descriptors and ensure ascending order by start_offset.
        descriptors.retain(|d| d.end_offset >= start);
        if let Some(end) = end_inclusive {
            descriptors.retain(|d| d.start_offset <= end);
        }
        descriptors.sort_by_key(|d| d.start_offset);

        // Build a stream that iterates objects and yields messages lazily
        let cloud = self.cloud.clone();
        let topic_path = self.topic_path.clone();
        let chunk_size: usize = 4 * 1024 * 1024; // 4 MiB
        let mut idx = 0usize;

        // State for current object
        let mut cur_reader: Option<CloudRangeReader> = None;
        let mut carry: Vec<u8> = Vec::new();
        let mut emit_buf: Vec<StreamMessage> = Vec::new();

        let s = async_stream::try_stream! {
            loop {
                // Emit buffered messages first
                if let Some(msg) = emit_buf.pop() {
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
                    let key = format!("storage/topics/{}/objects/{}", topic_path, desc.object_id);
                    // Use sparse index if present to jump near the requested start offset.
                    let start_byte = match &desc.offset_index {
                        Some(index) if !index.is_empty() => {
                            find_start_byte(index, start)
                        }
                        _ => 0u64,
                    };
                    let reader = cloud.open_ranged_reader(&key, start_byte).await?;
                    cur_reader = Some(reader);
                    carry.clear();
                }

                // Read next chunk from current reader
                let reader = cur_reader.as_mut().unwrap();
                let chunk = reader.read_chunk(chunk_size).await?;
                if chunk.is_empty() {
                    // End of current object
                    cur_reader = None;
                    continue;
                }
                carry.extend_from_slice(&chunk);

                // Parse complete frames from carry, leave remainder in carry
                let mut parsed: Vec<(u64, StreamMessage)> = Vec::new();
                parse_frames_from_carry(&mut carry, &mut parsed)?;
                // Filter by requested offset range and push into emit buffer (reverse for pop())
                for (_off, msg) in parsed.into_iter().filter(|(off, _)| *off >= start && end_inclusive.map(|e| *off <= e).unwrap_or(true)) {
                    emit_buf.insert(0, msg);
                }
            }
        };

        Ok(Box::pin(s))
    }
}

/// Parse a cloud object composed of concatenated raw WAL frames and return
/// a vector of `(offset, StreamMessage)` pairs. Stops gracefully at the first
/// partial frame at the end of the object.
fn parse_frames_from_carry(
    carry: &mut Vec<u8>,
    out: &mut Vec<(u64, StreamMessage)>,
) -> Result<(), PersistentStorageError> {
    let mut idx = 0usize;
    while idx + 16 <= carry.len() {
        let off = u64::from_le_bytes(carry[idx..idx + 8].try_into().unwrap());
        let len = u32::from_le_bytes(carry[idx + 8..idx + 12].try_into().unwrap()) as usize;
        let _crc = u32::from_le_bytes(carry[idx + 12..idx + 16].try_into().unwrap());
        let next = idx + 16 + len;
        if next > carry.len() {
            break;
        }
        let rec = &carry[idx + 16..next];
        let msg: StreamMessage = bincode::deserialize(rec).map_err(|e| {
            PersistentStorageError::Other(format!("cloud_reader: bincode decode failed: {}", e))
        })?;
        out.push((off, msg));
        idx = next;
    }
    // Remove consumed prefix, retain leftover for next chunk
    if idx > 0 {
        carry.drain(0..idx);
    }
    Ok(())
}

/// Find the byte position from a sparse `(offset, byte_pos)` index for a target `start_offset`.
/// Returns the `byte_pos` of the greatest entry with `offset <= start_offset`, or 0 if none.
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

//! Segment reader — reads .dnb1 segment files from object storage and decodes
//! them into `StreamMessage` values.
//!
//! Uses `danube_persistent_storage::frames::decode_next_frame` for WAL frame
//! parsing and bincode for message deserialization.
//!
//! ## Corruption handling
//!
//! If a CRC mismatch is detected mid-segment, the reader returns all
//! successfully decoded messages plus the corrupt offset via `DecodeResult`.
//! The caller can flush partial data and skip past the corrupt segment.

use crate::storage::StorageHandle;
use danube_core::message::StreamMessage;
use danube_persistent_storage::frames::{decode_next_frame, FrameDecodeError};
use object_store::ObjectStore;
use tracing::{debug, warn};

/// Read a .dnb1 segment from object storage and decode all frames into messages.
///
/// The segment path follows the Danube convention:
/// `storage/topics/{namespace}/{topic}/segments/{segment_id}`
pub async fn read_segment(
    storage: &StorageHandle,
    topic_path: &str,
    segment_id: &str,
) -> anyhow::Result<DecodeResult> {
    let relative = format!(
        "storage/topics/{}/segments/{}",
        topic_path, segment_id
    );
    let path = storage.path(&relative);

    debug!(path = %path, "reading segment from object store");

    // Fetch the entire segment into memory
    let result = storage.store.get(&path).await?;
    let data = result.bytes().await?;

    debug!(
        path = %path,
        size = data.len(),
        "fetched segment, decoding frames"
    );

    Ok(decode_frames(&data))
}

/// A decoded message with its WAL offset.
pub struct DecodedMessage {
    /// WAL offset of this message within the topic.
    pub offset: u64,
    /// The deserialized StreamMessage.
    pub message: StreamMessage,
}

/// Result of decoding a segment's WAL frames.
pub struct DecodeResult {
    /// Successfully decoded messages (may be partial if corruption was found).
    pub messages: Vec<DecodedMessage>,
    /// If `Some`, a CRC mismatch was encountered at this offset.
    /// Messages before this offset are valid; the rest of the segment is corrupt.
    /// The worker should flush partial data and skip past this segment.
    pub corrupt_at_offset: Option<u64>,
}

/// Decode all WAL frames from raw .dnb1 bytes into messages.
///
/// On CRC mismatch, returns all successfully decoded messages up to the
/// corruption point and sets `corrupt_at_offset` so the caller can skip
/// past the corrupt segment.
fn decode_frames(data: &[u8]) -> DecodeResult {
    let mut messages = Vec::new();
    let mut corrupt_at_offset = None;
    let mut cursor = 0;

    loop {
        if cursor >= data.len() {
            break;
        }

        let buf = &data[cursor..];
        match decode_next_frame(buf) {
            Ok(Some(frame)) => {
                // Deserialize the message from the frame payload
                match deserialize_message(frame.payload) {
                    Ok(msg) => {
                        messages.push(DecodedMessage {
                            offset: frame.offset,
                            message: msg,
                        });
                    }
                    Err(e) => {
                        warn!(
                            offset = frame.offset,
                            error = %e,
                            "skipping frame: deserialization failed"
                        );
                    }
                }
                cursor += frame.frame_len;
            }
            Ok(None) => {
                // Not enough data for another frame — we're done
                break;
            }
            Err(FrameDecodeError::CrcMismatch {
                offset,
                expected_crc,
                computed_crc,
            }) => {
                warn!(
                    offset,
                    expected_crc,
                    computed_crc,
                    "CRC mismatch in segment — partial data will be flushed, \
                     segment will be skipped"
                );
                corrupt_at_offset = Some(offset);
                break;
            }
        }
    }

    debug!(
        count = messages.len(),
        corrupt = corrupt_at_offset.is_some(),
        "decoded messages from segment"
    );

    DecodeResult {
        messages,
        corrupt_at_offset,
    }
}

/// Deserialize a StreamMessage from bincode-encoded bytes.
///
/// This is the same encoding used by `danube-persistent-storage` for WAL frames.
fn deserialize_message(bytes: &[u8]) -> anyhow::Result<StreamMessage> {
    let config = bincode::config::standard();
    let (msg, _): (StreamMessage, _) = bincode::serde::decode_from_slice(bytes, config)
        .map_err(|e| anyhow::anyhow!("bincode deserialize failed: {}", e))?;
    Ok(msg)
}

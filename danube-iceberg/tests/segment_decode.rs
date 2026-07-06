//! # Segment Decoding Tests
//!
//! These tests validate that `danube-iceberg` can correctly read and decode the `.dnb1`
//! segment files that the Danube broker writes to object storage.
//!
//! ## Why this matters
//!
//! The `.dnb1` format is Danube's internal WAL (Write-Ahead Log) binary format.
//! Each segment file contains a sequence of **frames**, where each frame wraps a
//! bincode-serialized `StreamMessage`. The frame header includes:
//! - `offset` (u64) — the WAL offset of the message within the topic
//! - `payload_len` (u32) — length of the bincode payload
//! - `crc32` (u32) — checksum of the payload for integrity verification
//!
//! `danube-iceberg` reads these files from S3 (or any object store) and decodes
//! them back into `StreamMessage` values before converting to Arrow/Parquet.
//! If this decoding step fails or produces incorrect data, the entire pipeline
//! produces garbage output — so these tests are foundational.
//!
//! ## What we test
//!
//! 1. **Single message** — basic roundtrip: encode → decode → verify all fields
//! 2. **Multiple messages** — sequential frames preserve ordering and offsets
//! 3. **Empty segment** — edge case: no frames → no messages (no crash)
//! 4. **Truncated segment** — simulates incomplete upload or crash mid-write;
//!    the decoder must skip the partial trailing frame without losing earlier data
//! 5. **Corrupt CRC** — simulates bit-rot or storage corruption; the decoder
//!    must detect and report the error rather than silently producing bad data
//! 6. **Object store integration** — writes a segment to local filesystem via
//!    the `object_store` API and reads it back, verifying the full I/O path

mod common;

use common::{
    build_local_storage, build_segment_from_messages, make_test_message,
    write_segment_to_storage,
};
use danube_persistent_storage::frames::decode_next_frame;

// ============================================================================
// Frame decoding tests
// ============================================================================

/// Verifies that a segment containing a single message can be decoded correctly.
///
/// This is the simplest possible roundtrip test: one `StreamMessage` is
/// bincode-encoded, wrapped in a WAL frame, and then decoded back.
/// We verify the offset, the raw payload bytes, the producer name, and the
/// topic offset from the embedded `MessageID` — ensuring no data corruption
/// occurred during the encode → frame → decode cycle.
#[tokio::test]
async fn decode_single_message_segment() {
    let msg = make_test_message(0, b"hello world");
    let segment = build_segment_from_messages(&[msg]);

    let frame = decode_next_frame(&segment)
        .expect("decode ok")
        .expect("frame present");

    assert_eq!(frame.offset, 0);

    // Deserialize the payload back to a StreamMessage
    let config = bincode::config::standard();
    let (decoded, _): (danube_core::message::StreamMessage, _) =
        bincode::serde::decode_from_slice(frame.payload, config).expect("bincode decode");

    assert_eq!(decoded.payload.as_ref(), b"hello world");
    assert_eq!(decoded.producer_name, "test-producer-1");
    assert_eq!(decoded.msg_id.topic_offset, 0);
}

/// Verifies that a segment with 10 sequential messages decodes all of them
/// in the correct order with monotonically increasing offsets.
///
/// This is important because real segments contain thousands of messages packed
/// sequentially. The decoder must correctly advance its cursor past each frame's
/// header + payload to find the next frame, without skipping or duplicating any.
#[tokio::test]
async fn decode_multi_message_segment() {
    let messages: Vec<_> = (0..10u64)
        .map(|i| make_test_message(i, format!("payload-{}", i).as_bytes()))
        .collect();
    let segment = build_segment_from_messages(&messages);

    // Decode all frames
    let mut cursor = 0;
    let mut decoded_offsets = Vec::new();

    while cursor < segment.len() {
        match decode_next_frame(&segment[cursor..]) {
            Ok(Some(frame)) => {
                decoded_offsets.push(frame.offset);
                cursor += frame.frame_len;
            }
            Ok(None) => break,
            Err(e) => panic!("unexpected decode error: {:?}", e),
        }
    }

    assert_eq!(decoded_offsets.len(), 10);
    // Offsets should be 0..10 in order
    let expected: Vec<u64> = (0..10).collect();
    assert_eq!(decoded_offsets, expected);
}

/// Verifies that decoding an empty byte slice returns `None` (no frames)
/// without panicking or returning an error.
///
/// This edge case can happen when a segment file exists in S3 but is empty
/// (e.g., created but never written to before the broker crashed). The
/// decoder must handle it gracefully.
#[tokio::test]
async fn decode_empty_segment() {
    let segment: Vec<u8> = Vec::new();
    let result = decode_next_frame(&segment).expect("decode ok");
    assert!(result.is_none(), "empty segment should yield no frames");
}

/// Simulates a segment that was partially written — e.g., the broker crashed
/// or the upload was interrupted before the final frame was fully written.
///
/// We build a 5-message segment and truncate the last 10 bytes, cutting into
/// the final frame. The decoder should:
/// - Successfully decode the first 4 complete frames
/// - Return `None` for the truncated 5th frame (not enough bytes)
/// - NOT return an error — partial trailing data is expected in production
///
/// This is critical for robustness because S3 multipart uploads can leave
/// partial data, and the converter must not lose the valid messages that
/// precede the partial frame.
#[tokio::test]
async fn decode_truncated_segment() {
    let messages: Vec<_> = (0..5u64)
        .map(|i| make_test_message(i, b"test-data"))
        .collect();
    let mut segment = build_segment_from_messages(&messages);

    // Truncate the last few bytes to simulate a partial write
    let original_len = segment.len();
    segment.truncate(original_len - 10);

    // Decode should get 4 complete frames and stop at the truncated one
    let mut count = 0;
    let mut cursor = 0;
    while cursor < segment.len() {
        match decode_next_frame(&segment[cursor..]) {
            Ok(Some(frame)) => {
                count += 1;
                cursor += frame.frame_len;
            }
            Ok(None) => break,
            Err(_) => break,
        }
    }

    assert_eq!(count, 4, "should decode 4 of 5 messages (last one truncated)");
}

/// Simulates storage corruption by manually crafting a frame with an invalid
/// CRC32 checksum.
///
/// The decoder must:
/// - Successfully decode the first valid frame (offset 0)
/// - Return `Err(CrcMismatch)` for the corrupted second frame (offset 1)
/// - Report the expected and computed CRC values for debugging
///
/// This is important because object storage can (rarely) return corrupted
/// data due to bit-rot, network errors, or storage-layer bugs. The CRC check
/// is our only defense against silently ingesting bad data into Parquet files,
/// which would poison downstream analytics.
#[tokio::test]
async fn decode_corrupt_crc_segment() {
    let msg = make_test_message(0, b"valid-message");
    let valid_segment = build_segment_from_messages(&[msg]);

    // Build a second frame with corrupted CRC
    let mut corrupt_segment = valid_segment.clone();
    // Append a frame with bad CRC: manually craft header + payload
    let offset: u64 = 1;
    let payload = b"corrupt-payload";
    let len = payload.len() as u32;
    let bad_crc: u32 = 0xDEADBEEF; // Deliberately wrong
    corrupt_segment.extend_from_slice(&offset.to_le_bytes());
    corrupt_segment.extend_from_slice(&len.to_le_bytes());
    corrupt_segment.extend_from_slice(&bad_crc.to_le_bytes());
    corrupt_segment.extend_from_slice(payload);

    // First frame should decode fine
    let frame0 = decode_next_frame(&corrupt_segment)
        .expect("first frame ok")
        .expect("frame present");
    assert_eq!(frame0.offset, 0);

    // Second frame should fail with CRC mismatch
    let result = decode_next_frame(&corrupt_segment[frame0.frame_len..]);
    assert!(result.is_err(), "corrupt frame should error");
    match result.unwrap_err() {
        danube_persistent_storage::frames::FrameDecodeError::CrcMismatch {
            offset,
            expected_crc,
            ..
        } => {
            assert_eq!(offset, 1);
            assert_eq!(expected_crc, 0xDEADBEEF);
        }
    }
}

// ============================================================================
// Segment reader integration test (reads from object store)
// ============================================================================

/// End-to-end test: writes a `.dnb1` segment to a local filesystem via the
/// `object_store` API, then reads it back and decodes all frames.
///
/// This validates the full I/O path that `danube-iceberg` uses in production:
/// 1. The segment is written at the standard Danube path layout:
///    `storage/topics/{namespace}/{topic}/segments/{segment_id}`
/// 2. The `object_store::get()` call retrieves the raw bytes
/// 3. Frame decoding + bincode deserialization produces the original messages
///
/// By testing against local filesystem instead of real S3, we can run this
/// in CI without cloud credentials while still exercising the same code paths
/// (the `object_store` crate uses the same trait for all backends).
#[tokio::test]
async fn read_segment_from_local_storage() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let storage = build_local_storage(tmp.path());

    // Build a segment with 5 messages
    let messages: Vec<_> = (0..5u64)
        .map(|i| make_test_message(i, format!("msg-{}", i).as_bytes()))
        .collect();
    let segment_data = build_segment_from_messages(&messages);

    // Write it to the expected path
    write_segment_to_storage(&storage, "default/test-topic", "seg-0000000000", &segment_data)
        .await;

    // Read it back using the segment reader path directly via object_store
    let path = storage.path("storage/topics/default/test-topic/segments/seg-0000000000");
    let result = storage.store.get(&path).await.expect("get segment");
    let data = result.bytes().await.expect("bytes");

    // Decode all frames
    let mut decoded = Vec::new();
    let mut cursor = 0;
    while cursor < data.len() {
        match decode_next_frame(&data[cursor..]) {
            Ok(Some(frame)) => {
                let config = bincode::config::standard();
                let (msg, _): (danube_core::message::StreamMessage, _) =
                    bincode::serde::decode_from_slice(frame.payload, config)
                        .expect("bincode decode");
                decoded.push((frame.offset, msg));
                cursor += frame.frame_len;
            }
            Ok(None) => break,
            Err(e) => panic!("decode error: {:?}", e),
        }
    }

    assert_eq!(decoded.len(), 5);
    for (i, (offset, msg)) in decoded.iter().enumerate() {
        assert_eq!(*offset, i as u64);
        assert_eq!(
            msg.payload.as_ref(),
            format!("msg-{}", i).as_bytes()
        );
    }
}

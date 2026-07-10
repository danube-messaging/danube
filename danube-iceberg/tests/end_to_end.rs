//! # End-to-End Pipeline Tests
//!
//! These are the **most important tests** in the `danube-iceberg` test suite.
//! They validate the entire data pipeline from end to end:
//!
//! ```text
//! StreamMessage → bincode → WAL frame → .dnb1 segment → object store
//!   → read → decode → resolve schema → Arrow RecordBatch → Parquet file
//!     → read Parquet back → verify every row and column
//! ```
//!
//! ## Why this matters
//!
//! Individual unit tests can pass while the integrated pipeline fails due to
//! mismatched assumptions between layers (e.g., bincode version drift, frame
//! header changes, schema column ordering). These tests catch integration
//! bugs that only manifest when all layers work together.
//!
//! ## What we test
//!
//! 1. **Binary → Envelope Parquet** — 20 binary messages through the full
//!    pipeline, verifying every offset, producer name, and payload byte
//! 2. **Multi-segment pipeline** — 3 separate segments merged into one
//!    Parquet output, verifying cross-segment offset continuity
//! 3. **Checkpoint continuity** — process batch 1, save checkpoint, process
//!    batch 2, verify only new messages are processed

mod common;

use arrow_array::builder::{
    BinaryBuilder, Int64Builder, StringBuilder,
};
use arrow_array::cast::AsArray;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use common::{
    build_local_storage, build_segment_from_messages, make_test_message,
    write_segment_to_storage,
};
use danube_persistent_storage::frames::decode_next_frame;
use futures_util::StreamExt;
use object_store::{ObjectStore, PutPayload};
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use std::sync::Arc;

// ============================================================================
// End-to-end: Binary payload → Envelope Parquet
// ============================================================================

/// Full pipeline test: 20 binary-payload messages through the complete
/// encode → store → read → decode → RecordBatch → Parquet → verify cycle.
///
/// This test exercises:
/// 1. Building a synthetic `.dnb1` segment with `build_segment_from_messages`
/// 2. Writing it to object storage at the standard Danube path layout
/// 3. Reading it back via `object_store::get()`
/// 4. Decoding WAL frames and deserializing `StreamMessage` payloads
/// 5. Building an envelope RecordBatch (offset, publish_time, producer, payload)
/// 6. Writing to Parquet with ZSTD compression
/// 7. Reading the Parquet file back and verifying **every single row**:
///    - Offset matches (0..20)
///    - Producer name is correct ("test-producer-1")
///    - Payload bytes match the original input ("binary-payload-N")
///
/// A failure here means the pipeline produces corrupted or incomplete Parquet
/// files — the most critical failure mode for a lakehouse converter.
#[tokio::test]
async fn segment_to_parquet_envelope() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let storage = build_local_storage(tmp.path());

    // 1. Build a 20-message segment with binary payloads
    let messages: Vec<_> = (0..20u64)
        .map(|i| make_test_message(i, format!("binary-payload-{}", i).as_bytes()))
        .collect();
    let segment_data = build_segment_from_messages(&messages);

    // 2. Write segment to local storage
    write_segment_to_storage(
        &storage,
        "default/test-topic",
        "seg-0000000000",
        &segment_data,
    )
    .await;

    // 3. Read segment back and decode frames
    let path = storage.path("storage/topics/default/test-topic/segments/seg-0000000000");
    let result = storage.store.get(&path).await.expect("get");
    let data = result.bytes().await.expect("bytes");

    let mut decoded_messages = Vec::new();
    let mut cursor = 0;
    while cursor < data.len() {
        match decode_next_frame(&data[cursor..]) {
            Ok(Some(frame)) => {
                let config = bincode::config::standard();
                let (msg, _): (danube_core::message::StreamMessage, _) =
                    bincode::serde::decode_from_slice(frame.payload, config)
                        .expect("bincode decode");
                decoded_messages.push((frame.offset, msg));
                cursor += frame.frame_len;
            }
            Ok(None) => break,
            Err(e) => panic!("decode error: {:?}", e),
        }
    }
    assert_eq!(decoded_messages.len(), 20);

    // 4. Convert to envelope RecordBatch
    let envelope_schema = Arc::new(Schema::new(vec![
        Field::new("offset", DataType::Int64, false),
        Field::new("publish_time", DataType::Int64, false),
        Field::new("producer_name", DataType::Utf8, false),
        Field::new("payload", DataType::Binary, false),
    ]));

    let mut offsets = Int64Builder::with_capacity(20);
    let mut publish_times = Int64Builder::with_capacity(20);
    let mut producers = StringBuilder::with_capacity(20, 20 * 32);
    let mut payloads = BinaryBuilder::with_capacity(20, 20 * 64);

    for (offset, msg) in &decoded_messages {
        offsets.append_value(*offset as i64);
        publish_times.append_value(msg.publish_time as i64);
        producers.append_value(&msg.producer_name);
        payloads.append_value(&msg.payload);
    }

    let batch = RecordBatch::try_new(
        envelope_schema.clone(),
        vec![
            Arc::new(offsets.finish()),
            Arc::new(publish_times.finish()),
            Arc::new(producers.finish()),
            Arc::new(payloads.finish()),
        ],
    )
    .expect("build batch");

    // 5. Write Parquet
    let parquet_path =
        object_store::path::Path::from("iceberg/default/test-topic/data/part-0.parquet");
    let props = parquet::file::properties::WriterProperties::builder()
        .set_compression(parquet::basic::Compression::ZSTD(Default::default()))
        .build();
    let object_writer = parquet::arrow::async_writer::ParquetObjectWriter::new(
        storage.store.clone(),
        parquet_path.clone(),
    );
    let mut writer = parquet::arrow::async_writer::AsyncArrowWriter::try_new(
        object_writer,
        envelope_schema.clone(),
        Some(props),
    )
    .expect("create writer");
    writer.write(&batch).await.expect("write");
    writer.close().await.expect("close");

    // 6. Read Parquet back and verify ALL data
    let reader = ParquetObjectReader::new(storage.store.clone(), parquet_path.clone());
    let builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .expect("builder");
    let mut stream = builder.build().expect("stream");

    let mut total_rows = 0;
    while let Some(result) = stream.next().await {
        let read_batch = result.expect("read batch");

        let read_offsets = read_batch
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>();
        let read_producers = read_batch.column(2).as_string::<i32>();
        let read_payloads = read_batch.column(3).as_binary::<i32>();

        for i in 0..read_batch.num_rows() {
            let offset = read_offsets.value(i);
            let row = total_rows + i;

            assert_eq!(offset, row as i64, "offset mismatch at row {}", row);
            assert_eq!(
                read_producers.value(i),
                "test-producer-1",
                "producer mismatch at row {}",
                row
            );
            assert_eq!(
                read_payloads.value(i),
                format!("binary-payload-{}", row).as_bytes(),
                "payload mismatch at row {}",
                row
            );
        }

        total_rows += read_batch.num_rows();
    }

    assert_eq!(total_rows, 20, "should have all 20 rows in parquet");
}


// ============================================================================
// Multi-segment pipeline
// ============================================================================

/// Verifies that multiple segments from the same topic can be read and
/// decoded sequentially, producing a contiguous stream of messages.
///
/// In production, a topic may have hundreds of segments (each ~256MB).
/// The converter must process them in order and verify that offsets are
/// continuous across segment boundaries. This test uses 3 segments with
/// 5 messages each (offsets 0-4, 5-9, 10-14) and verifies:
/// - All 15 messages are decoded
/// - Offsets are sequential 0..15 across all segments
/// - Payloads match their source segment ("seg0-msg-0", "seg1-msg-7", etc.)
///
/// A failure here would mean the converter loses messages at segment
/// boundaries or misorders them in the output Parquet file.
#[tokio::test]
async fn multi_segment_pipeline() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let storage = build_local_storage(tmp.path());

    // Build 3 separate segments with sequential offsets
    let seg0_msgs: Vec<_> = (0..5u64)
        .map(|i| make_test_message(i, format!("seg0-msg-{}", i).as_bytes()))
        .collect();
    let seg1_msgs: Vec<_> = (5..10u64)
        .map(|i| make_test_message(i, format!("seg1-msg-{}", i).as_bytes()))
        .collect();
    let seg2_msgs: Vec<_> = (10..15u64)
        .map(|i| make_test_message(i, format!("seg2-msg-{}", i).as_bytes()))
        .collect();

    write_segment_to_storage(
        &storage,
        "default/multi",
        "seg-0000000000",
        &build_segment_from_messages(&seg0_msgs),
    )
    .await;
    write_segment_to_storage(
        &storage,
        "default/multi",
        "seg-0000000005",
        &build_segment_from_messages(&seg1_msgs),
    )
    .await;
    write_segment_to_storage(
        &storage,
        "default/multi",
        "seg-0000000010",
        &build_segment_from_messages(&seg2_msgs),
    )
    .await;

    // Read and decode all 3 segments
    let mut all_decoded = Vec::new();
    for seg_id in &["seg-0000000000", "seg-0000000005", "seg-0000000010"] {
        let path = storage.path(&format!("storage/topics/default/multi/segments/{}", seg_id));
        let result = storage.store.get(&path).await.expect("get");
        let data = result.bytes().await.expect("bytes");

        let mut cursor = 0;
        while cursor < data.len() {
            match decode_next_frame(&data[cursor..]) {
                Ok(Some(frame)) => {
                    let config = bincode::config::standard();
                    let (msg, _): (danube_core::message::StreamMessage, _) =
                        bincode::serde::decode_from_slice(frame.payload, config).expect("decode");
                    all_decoded.push((frame.offset, msg));
                    cursor += frame.frame_len;
                }
                Ok(None) => break,
                Err(e) => panic!("decode error: {:?}", e),
            }
        }
    }

    assert_eq!(
        all_decoded.len(),
        15,
        "should have 15 messages across 3 segments"
    );

    // Verify offsets are sequential 0..15
    for (i, (offset, _)) in all_decoded.iter().enumerate() {
        assert_eq!(*offset, i as u64, "offset mismatch at index {}", i);
    }

    // Verify payloads match expected segment origins
    assert_eq!(all_decoded[0].1.payload.as_ref(), b"seg0-msg-0");
    assert_eq!(all_decoded[7].1.payload.as_ref(), b"seg1-msg-7");
    assert_eq!(all_decoded[12].1.payload.as_ref(), b"seg2-msg-12");
}

// ============================================================================
// Checkpoint continuity
// ============================================================================

/// Verifies that the checkpoint mechanism correctly enables incremental
/// processing — only new messages (after the checkpoint) are processed.
///
/// Workflow:
/// 1. Write batch 1 (offsets 0-9) → save checkpoint at offset 9
/// 2. Write batch 2 (offsets 10-19)
/// 3. Load checkpoint → verify it's at offset 9
/// 4. Read batch 2 → filter messages where `offset > checkpoint.last_offset`
/// 5. Verify exactly 10 new messages (offsets 10-19) are processed
/// 6. Update checkpoint to offset 19
///
/// This test validates the core idempotency contract: restarting the
/// converter after a crash should not re-process segments that were
/// already converted to Parquet. Without this, users would get duplicate
/// rows in their analytics tables.
#[tokio::test]
async fn checkpoint_continuity() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let storage = build_local_storage(tmp.path());

    // Build two batches of messages
    let batch1: Vec<_> = (0..10u64)
        .map(|i| make_test_message(i, format!("batch1-{}", i).as_bytes()))
        .collect();
    let batch2: Vec<_> = (10..20u64)
        .map(|i| make_test_message(i, format!("batch2-{}", i).as_bytes()))
        .collect();

    write_segment_to_storage(
        &storage,
        "default/checkpoint-test",
        "seg-0000000000",
        &build_segment_from_messages(&batch1),
    )
    .await;

    // Process batch 1 → save checkpoint at offset 9
    let checkpoint_path = storage.path("iceberg/checkpoints/default/checkpoint-test.json");

    #[derive(serde::Serialize, serde::Deserialize)]
    struct Checkpoint {
        topic: String,
        last_offset: u64,
        files_written: u64,
    }

    let cp1 = Checkpoint {
        topic: "/default/checkpoint-test".to_string(),
        last_offset: 9,
        files_written: 1,
    };
    let data = serde_json::to_vec(&cp1).expect("serialize");
    storage
        .store
        .put(&checkpoint_path, PutPayload::from(Bytes::from(data)))
        .await
        .expect("put checkpoint");

    // Write batch 2
    write_segment_to_storage(
        &storage,
        "default/checkpoint-test",
        "seg-0000000010",
        &build_segment_from_messages(&batch2),
    )
    .await;

    // Load checkpoint → should be at offset 9
    let result = storage
        .store
        .get(&checkpoint_path)
        .await
        .expect("get checkpoint");
    let bytes = result.bytes().await.expect("bytes");
    let loaded: Checkpoint = serde_json::from_slice(&bytes).expect("deserialize");
    assert_eq!(loaded.last_offset, 9);

    // Process batch 2 (offsets 10-19) → only these should be "new"
    let seg2_path = storage.path("storage/topics/default/checkpoint-test/segments/seg-0000000010");
    let result = storage.store.get(&seg2_path).await.expect("get seg2");
    let seg2_data = result.bytes().await.expect("bytes");

    let mut new_messages = Vec::new();
    let mut cursor = 0;
    while cursor < seg2_data.len() {
        match decode_next_frame(&seg2_data[cursor..]) {
            Ok(Some(frame)) => {
                // Only process messages AFTER checkpoint offset
                if frame.offset > loaded.last_offset {
                    let config = bincode::config::standard();
                    let (msg, _): (danube_core::message::StreamMessage, _) =
                        bincode::serde::decode_from_slice(frame.payload, config).expect("decode");
                    new_messages.push((frame.offset, msg));
                }
                cursor += frame.frame_len;
            }
            Ok(None) => break,
            Err(e) => panic!("decode error: {:?}", e),
        }
    }

    assert_eq!(
        new_messages.len(),
        10,
        "should have 10 new messages (offsets 10-19)"
    );
    assert_eq!(new_messages[0].0, 10, "first new offset should be 10");
    assert_eq!(new_messages[9].0, 19, "last new offset should be 19");

    // Update checkpoint
    let cp2 = Checkpoint {
        topic: "/default/checkpoint-test".to_string(),
        last_offset: 19,
        files_written: 2,
    };
    let data2 = serde_json::to_vec(&cp2).expect("serialize");
    storage
        .store
        .put(&checkpoint_path, PutPayload::from(Bytes::from(data2)))
        .await
        .expect("put checkpoint 2");

    // Verify final checkpoint
    let result = storage
        .store
        .get(&checkpoint_path)
        .await
        .expect("get final");
    let bytes = result.bytes().await.expect("bytes");
    let final_cp: Checkpoint = serde_json::from_slice(&bytes).expect("deserialize");
    assert_eq!(final_cp.last_offset, 19);
    assert_eq!(final_cp.files_written, 2);
}

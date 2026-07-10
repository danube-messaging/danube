//! # Checkpoint Persistence Tests
//!
//! These tests validate the checkpoint system that tracks how far `danube-iceberg`
//! has progressed in converting each topic's segments to Parquet.
//!
//! ## Why this matters
//!
//! Checkpoints are essential for **exactly-once delivery** semantics. Without them,
//! restarting `danube-iceberg` would re-process all segments from the beginning,
//! producing duplicate Parquet files. Checkpoints are stored as JSON files in
//! object storage (not on local disk), so they survive container restarts without
//! requiring persistent volumes — critical for Kubernetes deployments.
//!
//! ## What we test
//!
//! 1. **Save/load roundtrip** — checkpoint data survives serialization to S3
//! 2. **Fresh start** — no checkpoint file → starts from offset 0
//! 3. **Advance logic** — offset, file count, and row count update correctly
//! 4. **Overwrite** — writing a new checkpoint replaces the old one atomically

mod common;

use bytes::Bytes;
use common::build_local_storage;
use object_store::{ObjectStore, PutPayload};

/// Checkpoint data for testing (mirrors the checkpoint module).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct Checkpoint {
    topic: String,
    last_offset: u64,
    files_written: u64,
    total_rows: u64,
    updated_at: String,
}

impl Checkpoint {
    fn new(topic: &str) -> Self {
        Self {
            topic: topic.to_string(),
            last_offset: 0,
            files_written: 0,
            total_rows: 0,
            updated_at: "2026-01-01T00:00:00Z".to_string(),
        }
    }

    fn advance(&mut self, last_offset: u64, rows_written: usize) {
        self.last_offset = last_offset;
        self.files_written += 1;
        self.total_rows += rows_written as u64;
        self.updated_at = chrono::Utc::now().to_rfc3339();
    }
}

fn checkpoint_path(
    storage: &common::TestStorageHandle,
    namespace: &str,
    topic: &str,
) -> object_store::path::Path {
    let relative = format!("iceberg/checkpoints/{}/{}.json", namespace, topic);
    storage.path(&relative)
}

/// Verifies the full checkpoint lifecycle: create → advance twice → save to
/// object store → load back → verify all fields match.
///
/// This is the core checkpoint test. It ensures that:
/// - `last_offset` reflects the most recent advance (200, not 100)
/// - `files_written` accumulates correctly (2 advances = 2 files)
/// - `total_rows` sums across advances (500 + 300 = 800)
/// - JSON serialization/deserialization preserves all fields
///
/// A failure here would mean the converter can't resume from where it left
/// off after a restart — it would either skip data or duplicate it.
#[tokio::test]
async fn checkpoint_save_load_roundtrip() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let storage = build_local_storage(tmp.path());

    let mut cp = Checkpoint::new("/default/sensor-data");
    cp.advance(100, 500);
    cp.advance(200, 300);

    // Save
    let path = checkpoint_path(&storage, "default", "sensor-data");
    let data = serde_json::to_vec_pretty(&cp).expect("serialize");
    let payload = PutPayload::from(Bytes::from(data));
    storage.store.put(&path, payload).await.expect("put");

    // Load
    let result = storage.store.get(&path).await.expect("get");
    let bytes = result.bytes().await.expect("bytes");
    let loaded: Checkpoint = serde_json::from_slice(&bytes).expect("deserialize");

    assert_eq!(loaded.topic, "/default/sensor-data");
    assert_eq!(loaded.last_offset, 200);
    assert_eq!(loaded.files_written, 2);
    assert_eq!(loaded.total_rows, 800);
}

/// Verifies that loading a checkpoint for a topic that has never been
/// processed returns a "not found" error from the object store, and that
/// a freshly constructed checkpoint starts at offset 0.
///
/// This simulates the first time `danube-iceberg` runs for a new topic —
/// there's no checkpoint file yet, so the converter must start processing
/// from the beginning. The worker handles this by catching the NotFound
/// error and creating a fresh checkpoint.
#[tokio::test]
async fn checkpoint_fresh_start() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let storage = build_local_storage(tmp.path());

    let path = checkpoint_path(&storage, "default", "nonexistent");

    // Attempt to load — should get NotFound
    let result = storage.store.get(&path).await;
    assert!(result.is_err(), "should not find checkpoint");

    // Verify a fresh checkpoint starts at offset 0
    let fresh = Checkpoint::new("/default/nonexistent");
    assert_eq!(fresh.last_offset, 0);
    assert_eq!(fresh.files_written, 0);
    assert_eq!(fresh.total_rows, 0);
}

/// Verifies the `advance()` method correctly updates checkpoint state
/// across multiple calls.
///
/// Each call to `advance(offset, rows)` should:
/// - Set `last_offset` to the new value (not accumulate)
/// - Increment `files_written` by 1
/// - Add `rows` to `total_rows` (accumulate)
///
/// This logic is simple but critical — a bug in the accumulation would
/// cause the converter to skip segments or over-count its output.
#[tokio::test]
async fn checkpoint_advance() {
    let mut cp = Checkpoint::new("/default/events");

    assert_eq!(cp.last_offset, 0);
    assert_eq!(cp.files_written, 0);
    assert_eq!(cp.total_rows, 0);

    cp.advance(100, 500);
    assert_eq!(cp.last_offset, 100);
    assert_eq!(cp.files_written, 1);
    assert_eq!(cp.total_rows, 500);

    cp.advance(250, 200);
    assert_eq!(cp.last_offset, 250);
    assert_eq!(cp.files_written, 2);
    assert_eq!(cp.total_rows, 700);
}

/// Verifies that writing a new checkpoint to the same path overwrites the
/// previous one, and loading returns the latest version.
///
/// Object store `put()` is atomic — the old file is fully replaced by the
/// new one. This test ensures we don't accidentally append or merge
/// checkpoints. After two saves, only the second checkpoint's data should
/// be visible.
///
/// This matters because the checkpoint file is the converter's only
/// persistent state — if it gets corrupted or contains stale data, the
/// converter will either re-process segments (duplicates) or skip them
/// (data loss).
#[tokio::test]
async fn checkpoint_overwrite() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let storage = build_local_storage(tmp.path());
    let path = checkpoint_path(&storage, "default", "overwrite-test");

    // Write first checkpoint
    let mut cp1 = Checkpoint::new("/default/overwrite-test");
    cp1.advance(50, 100);
    let data1 = serde_json::to_vec_pretty(&cp1).expect("serialize");
    storage
        .store
        .put(&path, PutPayload::from(Bytes::from(data1)))
        .await
        .expect("put 1");

    // Overwrite with second checkpoint
    let mut cp2 = Checkpoint::new("/default/overwrite-test");
    cp2.advance(50, 100);
    cp2.advance(150, 200);
    let data2 = serde_json::to_vec_pretty(&cp2).expect("serialize");
    storage
        .store
        .put(&path, PutPayload::from(Bytes::from(data2)))
        .await
        .expect("put 2");

    // Load should return the latest
    let result = storage.store.get(&path).await.expect("get");
    let bytes = result.bytes().await.expect("bytes");
    let loaded: Checkpoint = serde_json::from_slice(&bytes).expect("deserialize");

    assert_eq!(loaded.last_offset, 150, "should have latest offset");
    assert_eq!(loaded.files_written, 2, "should have latest file count");
    assert_eq!(loaded.total_rows, 300, "should have latest row count");
}

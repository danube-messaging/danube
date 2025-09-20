use danube_core::message::{MessageID, StreamMessage};
use danube_persistent_storage::wal::{Wal, WalConfig};
use serde::Deserialize;
use std::fs;
use tokio::time::{sleep, Duration};

// This test module validates WAL rotation and checkpoint behavior.
// Why: To ensure durability and recovery, the WAL must rotate by size/time and write checkpoints
// capturing the latest committed offset and active file information.
// Expected: After configured thresholds are crossed, a rotated wal.<n>.log exists (n >= 1)
// and wal.ckpt reflects last_offset and the current rotated file path/seq.

fn make_msg(topic: &str, idx: u64, payload: &[u8]) -> StreamMessage {
    StreamMessage {
        request_id: idx,
        msg_id: MessageID {
            producer_id: 1,
            topic_name: topic.to_string(),
            broker_addr: "127.0.0.1:6650".to_string(),
            segment_id: 0,
            segment_offset: idx,
        },
        payload: payload.to_vec(),
        publish_time: 0,
        producer_name: "test-producer".to_string(),
        subscription_name: None,
        attributes: Default::default(),
    }
}

#[derive(Debug, Deserialize)]
struct WalCheckpoint {
    last_offset: u64,
    file_seq: u64,
    file_path: String,
}

/// Test size-based rotation and checkpoint updates.
/// Rationale: With tiny batch size and small rotate_max_bytes, the third append should force a
/// rotation. The checkpoint should update with last_offset=2 and point to a rotated wal.<n>.log.
#[tokio::test]
async fn test_rotation_and_checkpoint() {
    let tmp = tempfile::tempdir().unwrap();

    // Configure WAL to flush each append and rotate after a tiny number of bytes
    let wal = Wal::with_config(WalConfig {
        dir: Some(tmp.path().to_path_buf()),
        file_name: Some("wal.log".to_string()),
        cache_capacity: Some(16),
        fsync_interval_ms: Some(1),
        max_batch_bytes: Some(1),
        // Each frame ~ (8+4+4+payload) bytes; with payload 4 bytes => ~20B. Rotate after ~32B -> on 3rd append.
        rotate_max_bytes: Some(32),
        rotate_max_seconds: None,
    })
    .await
    .expect("create wal");

    // Append a few messages to trigger rotation
    for i in 0..3u64 {
        let msg = make_msg("/default/rotation", i, &[1, 2, 3, 4]);
        let _ = wal.append(&msg).await.expect("append");
    }

    // Expect rotated file exists (wal.<n>.log)
    let rotated = tmp.path().join("wal.1.log");
    assert!(
        rotated.exists() || tmp.path().join("wal.2.log").exists(),
        "rotated wal file should exist"
    );

    // Expect checkpoint exists and contains expected fields
    let ckpt_path = tmp.path().join("wal.ckpt");
    assert!(
        ckpt_path.exists(),
        "checkpoint file should exist: {:?}",
        ckpt_path
    );

    let bytes = fs::read(&ckpt_path).expect("read checkpoint");
    let ckpt: WalCheckpoint = serde_json::from_slice(&bytes).expect("parse checkpoint json");

    // Offsets start at 0; after 3 appends, last_offset should be 2
    assert_eq!(ckpt.last_offset, 2);
    // Rotated file sequence should be >= 1 and file path must be a rotated file name
    assert!(
        ckpt.file_seq >= 1,
        "file_seq should be >= 1, got {}",
        ckpt.file_seq
    );
    let fname = std::path::Path::new(&ckpt.file_path)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("");
    assert!(
        fname.starts_with("wal.") && fname.ends_with(".log"),
        "file_path must be a rotated file, got {}",
        ckpt.file_path
    );
    let seq_str = fname.trim_start_matches("wal.").trim_end_matches(".log");
    let seq_num: u64 = seq_str.parse().unwrap_or(0);
    assert!(
        seq_num >= 1,
        "rotated wal filename should have seq >= 1, got {} ({})",
        seq_num,
        ckpt.file_path
    );
}

/// Test time-based rotation and checkpoint updates.
/// Rationale: With rotate_max_seconds set to 1s, waiting just over 1s between appends should
/// force rotation even if size threshold isn't met.
/// Expected: Checkpoint exists and references a rotated wal.<n>.log with file_seq>=1.
#[tokio::test]
async fn test_time_based_rotation_and_checkpoint() {
    let tmp = tempfile::tempdir().unwrap();

    // Configure WAL to rotate after 1 second regardless of size; force frequent flushes
    let wal = Wal::with_config(WalConfig {
        dir: Some(tmp.path().to_path_buf()),
        file_name: Some("wal.log".to_string()),
        cache_capacity: Some(16),
        fsync_interval_ms: Some(1),
        max_batch_bytes: Some(1),
        rotate_max_bytes: None,
        rotate_max_seconds: Some(1),
    })
    .await
    .expect("create wal");

    // Append one message, wait to cross the rotation boundary, then append another
    let m0 = make_msg("/default/rotation_time", 0, b"abcd");
    wal.append(&m0).await.expect("append m0");
    sleep(Duration::from_millis(1100)).await;
    let m1 = make_msg("/default/rotation_time", 1, b"efgh");
    wal.append(&m1).await.expect("append m1");

    // Check that a rotated wal file exists (wal.<n>.log, n >= 1)
    let ckpt_path = tmp.path().join("wal.ckpt");
    assert!(
        ckpt_path.exists(),
        "checkpoint file should exist: {:?}",
        ckpt_path
    );
    let bytes = fs::read(&ckpt_path).expect("read checkpoint");
    let ckpt: WalCheckpoint = serde_json::from_slice(&bytes).expect("parse checkpoint json");
    assert!(
        ckpt.file_seq >= 1,
        "file_seq should be >= 1, got {}",
        ckpt.file_seq
    );
    let fname = std::path::Path::new(&ckpt.file_path)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("");
    assert!(
        fname.starts_with("wal.") && fname.ends_with(".log"),
        "file_path must be a rotated file, got {}",
        ckpt.file_path
    );
}

use std::path::PathBuf;
use std::sync::Arc;

use danube_core::storage::{PersistentStorage, StartPosition};
use danube_metadata_store::{MemoryStore, MetadataStorage};
use danube_persistent_storage::checkpoint::{CheckpointStore, WalCheckpoint};
use danube_persistent_storage::wal::deleter::{Deleter, DeleterConfig};
use danube_persistent_storage::{
    wal::WalConfig, BackendConfig, LocalBackend, UploaderBaseConfig, WalStorageFactory,
};
use std::time::Duration;
use tokio_stream::StreamExt;

// Reuse shared test utilities
mod common;
use common::{make_test_message, wait_for_condition};

// Helper: create a small file with provided bytes
async fn touch_file(dir: &PathBuf, name: &str, bytes: &[u8]) -> PathBuf {
    let p = dir.join(name);
    tokio::fs::write(&p, bytes).await.expect("write file");
    p
}

/// Test: Retention end-to-end (time-based) deletes eligible files and advances start_offset
///
/// Purpose
/// - Validate that, end-to-end, the deleter removes eligible rotated files on disk and
///   advances `WalCheckpoint.start_offset` accordingly without scanning.
///
/// Flow
/// - Create temp WAL dir and `CheckpointStore`.
/// - Create rotated files seq=1 (first_offset=0) and seq=2 (first_offset=100); active seq=3 (first_offset=200).
/// - Set uploader watermark `last_read_file_seq=2` so only seq=1 is strictly older (eligible).
/// - Start deleter with `retention_time_minutes = 0` to trigger time-based retention and run immediately.
///
/// Expected
/// - File `wal.1.log` is removed from disk.
/// - `rotated_files` retains only seq=2 and `start_offset` advances to 100 (the first_offset of seq=2).
#[tokio::test]
async fn test_retention_time_based_end_to_end() {
    let tmp = tempfile::TempDir::new().expect("temp wal root");
    let root = tmp.path().to_path_buf();
    let wal_ckpt = root.join("wal.ckpt");
    let up_ckpt = root.join("uploader.ckpt");
    let store = std::sync::Arc::new(CheckpointStore::new(wal_ckpt, up_ckpt));

    // Files
    let f1 = touch_file(&root, "wal.1.log", b"a").await; // seq 1
    let f2 = touch_file(&root, "wal.2.log", b"bb").await; // seq 2
    let active = touch_file(&root, "wal.3.log", b"ccc").await; // seq 3

    // Initial WAL checkpoint
    store
        .update_wal(&WalCheckpoint {
            start_offset: 0,
            last_offset: 0,
            file_seq: 3,
            file_path: active.to_string_lossy().into_owned(),
            rotated_files: vec![(1, f1.clone(), 0), (2, f2.clone(), 100)],
            active_file_name: Some("wal.3.log".into()),
            last_rotation_at: None,
            active_file_first_offset: Some(200),
        })
        .await
        .expect("update wal");

    // Uploader watermark: seq < 2 is safe to delete; seq 2 is not.
    store
        .update_uploader(&danube_persistent_storage::checkpoint::UploaderCheckpoint {
            last_committed_offset: 999,
            last_read_file_seq: 2,
            last_read_byte_position: 0,
            last_object_id: None,
            updated_at: 0,
        })
        .await
        .expect("update up");

    // Start deleter: time-based retention 0 minutes, runs one cycle immediately
    let cfg = DeleterConfig {
        check_interval_minutes: 5,
        retention_time_minutes: Some(0),
        retention_size_mb: None,
    };
    let deleter = std::sync::Arc::new(Deleter::new("ns/topic".into(), store.clone(), cfg));
    let handle = deleter.clone().start();

    // Wait until rotated_files length == 1 and file wal.1.log removed
    let ok = wait_for_condition(
        || {
            let store = store.clone();
            let f1 = f1.clone();
            async move {
                if let Some(wal) = store.get_wal().await {
                    let len_ok = wal.rotated_files.len() == 1
                        && wal.rotated_files[0].0 == 2
                        && wal.start_offset == 100;
                    let file_ok = tokio::fs::metadata(&f1).await.is_err();
                    len_ok && file_ok
                } else {
                    false
                }
            }
        },
        5_000,
    )
    .await;
    assert!(ok, "retention did not prune as expected within timeout");

    // Active file should not be deleted
    assert!(
        tokio::fs::metadata(&active).await.is_ok(),
        "active WAL file must not be deleted"
    );

    // Stop background task
    handle.abort();
}

/// Test: Retention end-to-end for multiple topics runs independently
///
/// Purpose
/// - Validate that per-topic deleters operate independently and prune only their own topic files.
///
/// Flow
/// - Create two distinct temp roots (simulating two topics) with their own CheckpointStore and files.
/// - Topic A has uploade watermark allowing deletion of seq=1; Topic B allows deletion of none.
/// - Start one deleter per topic (time-based retention 0 minutes) and wait for outcomes.
///
/// Expected
/// - Topic A: seq 1 file removed; rotated_files retains only seq 2 and start_offset advanced.
/// - Topic B: no deletions; rotated_files unchanged.
#[tokio::test]
async fn test_retention_multi_topic_independence() {
    // Topic A
    let tmp_a = tempfile::TempDir::new().expect("tmp A");
    let root_a = tmp_a.path().to_path_buf();
    let store_a = std::sync::Arc::new(CheckpointStore::new(
        root_a.join("wal.ckpt"),
        root_a.join("uploader.ckpt"),
    ));
    let a1 = touch_file(&root_a, "wal.1.log", b"a").await;
    let a2 = touch_file(&root_a, "wal.2.log", b"bb").await;
    let a3 = touch_file(&root_a, "wal.3.log", b"ccc").await;
    store_a
        .update_wal(&WalCheckpoint {
            start_offset: 0,
            last_offset: 0,
            file_seq: 3,
            file_path: a3.to_string_lossy().into_owned(),
            rotated_files: vec![(1, a1.clone(), 0), (2, a2.clone(), 50)],
            active_file_name: Some("wal.3.log".into()),
            last_rotation_at: None,
            active_file_first_offset: Some(100),
        })
        .await
        .expect("wal a");
    store_a
        .update_uploader(&danube_persistent_storage::checkpoint::UploaderCheckpoint {
            last_committed_offset: 999,
            last_read_file_seq: 2,
            last_read_byte_position: 0,
            last_object_id: None,
            updated_at: 0,
        })
        .await
        .expect("up a");

    // Topic B
    let tmp_b = tempfile::TempDir::new().expect("tmp B");
    let root_b = tmp_b.path().to_path_buf();
    let store_b = std::sync::Arc::new(CheckpointStore::new(
        root_b.join("wal.ckpt"),
        root_b.join("uploader.ckpt"),
    ));
    let b1 = touch_file(&root_b, "wal.10.log", b"xx").await;
    let b2 = touch_file(&root_b, "wal.11.log", b"yyy").await;
    let b3 = touch_file(&root_b, "wal.12.log", b"zzzz").await;
    store_b
        .update_wal(&WalCheckpoint {
            start_offset: 0,
            last_offset: 0,
            file_seq: 12,
            file_path: b3.to_string_lossy().into_owned(),
            rotated_files: vec![(10, b1.clone(), 5), (11, b2.clone(), 15)],
            active_file_name: Some("wal.12.log".into()),
            last_rotation_at: None,
            active_file_first_offset: Some(25),
        })
        .await
        .expect("wal b");
    // B watermark prevents deletion (strictly less than 11 only)
    store_b
        .update_uploader(&danube_persistent_storage::checkpoint::UploaderCheckpoint {
            last_committed_offset: 999,
            last_read_file_seq: 11, // seq < 11 eligible -> only 10
            last_read_byte_position: 0,
            last_object_id: None,
            updated_at: 0,
        })
        .await
        .expect("up b");

    let cfg = DeleterConfig {
        check_interval_minutes: 5,
        retention_time_minutes: Some(0),
        retention_size_mb: None,
    };
    let del_a = std::sync::Arc::new(Deleter::new("ns/a".into(), store_a.clone(), cfg.clone()));
    let del_b = std::sync::Arc::new(Deleter::new("ns/b".into(), store_b.clone(), cfg));
    let ha = del_a.clone().start();
    let hb = del_b.clone().start();

    // Wait for A: seq 10 removed, seq 11 remains with start_offset 15
    let a_ok = wait_for_condition(
        || {
            let s = store_a.clone();
            let a1 = a1.clone();
            async move {
                if let Some(w) = s.get_wal().await {
                    let len_ok = w.rotated_files.len() == 1
                        && w.rotated_files[0].0 == 2
                        && w.start_offset == 50; // NOTE: seq numbers for A are 1 and 2
                    let file_ok = tokio::fs::metadata(&a1).await.is_err();
                    len_ok && file_ok
                } else {
                    false
                }
            }
        },
        5_000,
    )
    .await;
    assert!(a_ok, "topic A did not prune as expected");

    // Wait for B: only seq 10 eligible; ensure it is deleted and seq 11 remains with start_offset 15
    let b_ok = wait_for_condition(
        || {
            let s = store_b.clone();
            let b1 = b1.clone();
            async move {
                if let Some(w) = s.get_wal().await {
                    let len_ok = w.rotated_files.len() == 1
                        && w.rotated_files[0].0 == 11
                        && w.start_offset == 15;
                    let file_ok = tokio::fs::metadata(&b1).await.is_err();
                    len_ok && file_ok
                } else {
                    false
                }
            }
        },
        5_000,
    )
    .await;
    assert!(b_ok, "topic B did not prune as expected");

    ha.abort();
    hb.abort();
}

/// Test: StatefulReader works after retention (public-API flow)
///
/// Purpose
/// - Validate that readers continue to work end-to-end after retention may prune local rotated files.
/// - Ensure cloud→WAL handoff is seamless and message ordering is preserved.
///
/// Flow
/// - Build a factory with small rotation threshold and size-based retention (0MB) for determinism.
/// - Produce a batch of messages to trigger at least one rotation.
/// - Wait until the uploader commits an object (using `count_cloud_objects`).
/// - Sleep briefly to give the deleter time to run a cycle (no internal calls/hooks).
/// - Create readers using only public PersistentStorage APIs:
///   - From offset 0 (historical + live via handoff)
///   - From a mid offset (tail)
///
/// Expected
/// - Reader from 0 returns all messages in order with correct payloads.
/// - Reader from the mid offset returns the tail (mid..N-1) in order.
#[tokio::test]
async fn test_stateful_reader_after_retention() {
    // Build a custom factory with rotation and aggressive retention
    let unique_dir = {
        let mut d = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        d.push(format!("danube-retention-reader-{}", nanos));
        d
    };

    let wal_cfg = WalConfig {
        dir: Some(unique_dir),
        // Make rotation more aggressive to ensure multiple rotated files
        rotate_max_bytes: Some(64),
        ..Default::default()
    };
    let backend = BackendConfig::Local {
        backend: LocalBackend::Memory,
        root: "integration-test".to_string(),
    };
    // Use size-based retention to avoid mtime edge-cases for files created moments ago.
    // Setting retention_size_mb to 0 forces deletion of all eligible rotated files once the
    // uploader watermark has advanced beyond them.
    let deleter_cfg = DeleterConfig {
        check_interval_minutes: 5,
        retention_time_minutes: None,
        retention_size_mb: Some(0),
    };
    let memory_store: Arc<MemoryStore> = Arc::new(MemoryStore::new().await.expect("mem"));
    let factory = WalStorageFactory::new(
        wal_cfg.clone(),
        backend.clone(),
        MetadataStorage::InMemory((*memory_store).clone()),
        "/danube".to_string(),
        UploaderBaseConfig {
            interval_seconds: 1,
            ..Default::default()
        },
        deleter_cfg.clone(),
    );

    let topic = "integration/retention-reader";
    let storage = factory.for_topic(topic).await.expect("storage");

    // Produce enough messages to trigger at least one rotation
    let total_msgs = 120u64;
    for i in 0..total_msgs {
        let msg = make_test_message(i, 1, topic, i, &format!("msg-{}", i));
        storage.append_message(topic, msg).await.expect("append");
    }

    // Wait for uploader to upload at least one object (cloud handoff available)
    let uploaded = wait_for_condition(
        || {
            let ms = memory_store.clone();
            let topic_path = topic.to_string();
            async move { common::count_cloud_objects(&ms, &topic_path).await > 0 }
        },
        15000,
    )
    .await;
    assert!(uploaded, "uploader should upload at least one object");

    // Allow a bit of time for the deleter to run a cycle (size-based retention is 0MB)
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Reader from 0 should read all messages in order without gaps
    let mut reader0 = storage
        .create_reader(topic, StartPosition::Offset(0))
        .await
        .expect("reader0");
    let mut read0 = Vec::new();
    for _ in 0..total_msgs {
        if let Some(m) = reader0.next().await {
            read0.push(m.expect("m"));
        }
    }
    assert_eq!(read0.len(), total_msgs as usize);
    for (i, m) in read0.iter().enumerate() {
        assert_eq!(m.msg_id.segment_offset, i as u64);
        assert_eq!(m.payload, format!("msg-{}", i).as_bytes());
    }

    // Reader from a mid offset should read the remaining tail
    let mid = total_msgs / 2;
    let mut reader_mid = storage
        .create_reader(topic, StartPosition::Offset(mid))
        .await
        .expect("reader_mid");
    let mut read_mid = Vec::new();
    for _ in mid..total_msgs {
        if let Some(m) = reader_mid.next().await {
            read_mid.push(m.expect("m"));
        }
    }
    assert_eq!(read_mid.first().unwrap().msg_id.segment_offset, mid);
    assert_eq!(
        read_mid.last().unwrap().msg_id.segment_offset,
        total_msgs - 1
    );
}

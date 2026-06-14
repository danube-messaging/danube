/// Integration tests for the Warm Tier (Valkey) read path.
///
/// These tests validate that `TieredStorage::create_reader()` reads from the
/// Valkey warm tier when local WAL files have been deleted by the retention
/// deleter but data is still available in Valkey.
///
/// ## Scenario
///
/// This happens in production when:
/// - Local retention is aggressive (e.g., `time_minutes: 5`, `size_mb: 100`)
/// - Valkey `max_cached_closed_segments` keeps more history than local disk
/// - A slow consumer or new subscriber starts from an offset that has been
///   deleted from the local WAL but is still in Valkey
///
/// ## Requirements
///
/// These tests require a running Valkey/Redis instance at `redis://127.0.0.1:6379`.
/// They are gated with `#[ignore]` and run in the Tiered Storage E2E workflow.
///
/// To run locally:
/// ```bash
/// docker run -d --name valkey-test -p 6379:6379 valkey/valkey:latest
/// cargo test -p danube-persistent-storage --test warm_tier_read -- --ignored --nocapture
/// docker rm -f valkey-test
/// ```
mod common;

use common::{make_test_message, wait_for_condition};
use danube_core::metadata::{MemoryStore, MetadataStore};
use danube_core::storage::StartPosition;
use danube_persistent_storage::valkey::config::WriteBufferConfig;
use danube_persistent_storage::wal::WalConfig;
use danube_persistent_storage::{
    ObjectStoreConfig, RetentionConfig, StorageFactory, StorageFactoryConfig,
};
use futures::TryStreamExt;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use tokio_stream::StreamExt;

const VALKEY_ENDPOINT: &str = "redis://127.0.0.1:6379";

/// Helper: flush all keys from Valkey (clean state for each test).
async fn flush_valkey() {
    let client = redis::Client::open(VALKEY_ENDPOINT).expect("open redis");
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .expect("connect");
    let _: () = redis::cmd("FLUSHALL")
        .query_async(&mut conn)
        .await
        .expect("flushall");
}

/// Helper: create a factory with WAL + Cloud Storage + Valkey + aggressive retention.
///
/// This mimics a production setup where:
/// - WAL rotates aggressively (small files)
/// - Segments are exported to cloud (in-memory filesystem)
/// - Valkey write buffer is enabled (double-write)
/// - Retention deleter removes WAL files once safely in cloud
async fn create_warm_read_factory() -> (StorageFactory, Arc<MemoryStore>) {
    let memory_store = Arc::new(MemoryStore::new().await.expect("create memory store"));
    let unique_dir = {
        let mut d = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        d.push(format!("danube-warm-read-test-{}", nanos));
        d
    };

    let wal_cfg = WalConfig {
        dir: Some(unique_dir),
        flush_interval_ms: Some(50),
        flush_max_batch_bytes: Some(1),
        // Aggressive rotation: 64-byte files trigger fast rotation
        rotate_max_bytes: Some(64),
        ..Default::default()
    };

    let durable_root = {
        let mut d = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        d.push(format!("danube-warm-read-durable-{}", nanos));
        d
    };

    let wb_config = WriteBufferConfig {
        endpoints: vec![VALKEY_ENDPOINT.to_string()],
        wait_replicas: 0,
        wait_timeout_ms: 100,
        ..Default::default()
    };

    let store_arc: Arc<dyn MetadataStore> = memory_store.clone();
    let factory = StorageFactory::new(
        StorageFactoryConfig::object_store(
            wal_cfg,
            "/danube",
            ObjectStoreConfig::filesystem_for_tests(durable_root.to_string_lossy().to_string()),
            Some(RetentionConfig {
                check_interval_minutes: 5,
                // Size-based retention: 0MB forces deletion of all eligible rotated files
                time_minutes: None,
                size_mb: Some(0),
            }),
        )
        .with_write_buffer(wb_config)
        .with_segment_export_interval_seconds(1),
        store_arc,
    );

    (factory, memory_store)
}

// ============================================================
// Test: Warm tier read after local WAL retention deletes files
// ============================================================

/// What this test validates
///
/// When local WAL files are deleted by the retention deleter (because segments
/// were exported to cloud storage), and the requested read offset falls in a
/// range that:
/// - Is no longer on local disk (deleted by retention)
/// - Is still available in Valkey (warm tier)
/// - Has also been exported to cloud (cold tier)
///
/// The `TieredStorage::create_reader()` should stitch the tiers and deliver
/// all messages in order: Cold → Warm → Hot.
///
/// ## Flow
/// 1. Create factory with WAL + Cloud + Valkey + aggressive retention (0MB)
/// 2. Write 120 messages → double-written to WAL + Valkey
/// 3. Wait for segment export to cloud (at least 1 object)
/// 4. Wait for retention deleter to remove old WAL files
/// 5. Create reader from offset 0 → should get data from Cold/Warm/Hot tiers
/// 6. Verify all 120 messages are readable in order
#[tokio::test]
#[ignore = "requires running Valkey (docker run -p 6379:6379 valkey/valkey:latest)"]
async fn warm_tier_read_after_wal_retention() {
    flush_valkey().await;
    let (factory, memory_store) = create_warm_read_factory().await;

    let topic = "/default/warm-read-retention";
    let topic_path = "default/warm-read-retention";
    let storage = factory.for_topic(topic).await.expect("create storage");

    let total_msgs = 120u64;

    // Write messages — each one is double-written to WAL + Valkey
    for i in 0..total_msgs {
        let offset = storage
            .append_message(
                topic,
                make_test_message(i, 1, topic, i, &format!("warm-{}", i)),
            )
            .await
            .expect("append");
        assert_eq!(offset, i);
    }

    // Wait for at least one segment to be exported to cloud
    let uploaded = wait_for_condition(
        || {
            let ms = memory_store.clone();
            async move { common::count_cloud_objects(&ms, topic_path).await > 0 }
        },
        15_000,
    )
    .await;
    assert!(uploaded, "segment export to cloud should complete");

    // Allow time for the retention deleter to remove old WAL files
    // (size-based retention: 0MB forces deletion of all eligible rotated files)
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Create reader from offset 0 — this should stitch across tiers:
    // Cold (exported segments) → Warm (Valkey) → Hot (current WAL)
    let reader = storage
        .create_reader(topic, StartPosition::Offset(0))
        .await
        .expect("create reader from offset 0");

    let fut = reader.take(total_msgs as usize).try_collect::<Vec<_>>();
    let msgs = timeout(Duration::from_secs(10), fut)
        .await
        .expect("reader timed out")
        .expect("try_collect");

    // Verify all messages in order
    assert_eq!(
        msgs.len(),
        total_msgs as usize,
        "should receive all {} messages across tiers",
        total_msgs
    );
    for (i, m) in msgs.iter().enumerate() {
        assert_eq!(
            m.msg_id.topic_offset, i as u64,
            "offset mismatch at position {}",
            i
        );
        assert_eq!(
            m.payload.as_ref(),
            format!("warm-{}", i).as_bytes(),
            "payload mismatch at offset {}",
            i
        );
    }
}

// ============================================================
// Test: Mid-offset read hits warm tier for gap between hot and cold
// ============================================================

/// What this test validates
///
/// A consumer starting from a mid-range offset (where local WAL files have
/// been deleted but data is in Valkey) still receives all messages from that
/// offset onward.
///
/// This simulates a slow consumer that falls behind and its read position
/// is in the "warm gap" — data deleted from local disk but retained in Valkey.
#[tokio::test]
#[ignore = "requires running Valkey (docker run -p 6379:6379 valkey/valkey:latest)"]
async fn warm_tier_mid_offset_read() {
    flush_valkey().await;
    let (factory, memory_store) = create_warm_read_factory().await;

    let topic = "/default/warm-mid-offset";
    let topic_path = "default/warm-mid-offset";
    let storage = factory.for_topic(topic).await.expect("create storage");

    let total_msgs = 120u64;

    for i in 0..total_msgs {
        storage
            .append_message(
                topic,
                make_test_message(i, 1, topic, i, &format!("mid-{}", i)),
            )
            .await
            .expect("append");
    }

    // Wait for cloud export + retention deletion
    let uploaded = wait_for_condition(
        || {
            let ms = memory_store.clone();
            async move { common::count_cloud_objects(&ms, topic_path).await > 0 }
        },
        15_000,
    )
    .await;
    assert!(uploaded, "segment export should complete");
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Read from mid-point — should get data from warm/hot tiers
    let mid = total_msgs / 2;
    let expected_count = (total_msgs - mid) as usize;

    let reader = storage
        .create_reader(topic, StartPosition::Offset(mid))
        .await
        .expect("create reader from mid");

    let fut = reader.take(expected_count).try_collect::<Vec<_>>();
    let msgs = timeout(Duration::from_secs(10), fut)
        .await
        .expect("reader timed out")
        .expect("try_collect");

    assert_eq!(msgs.len(), expected_count);
    assert_eq!(msgs.first().unwrap().msg_id.topic_offset, mid);
    assert_eq!(msgs.last().unwrap().msg_id.topic_offset, total_msgs - 1);

    // Verify ordering and payloads
    for (idx, m) in msgs.iter().enumerate() {
        let expected_offset = mid + idx as u64;
        assert_eq!(m.msg_id.topic_offset, expected_offset);
        assert_eq!(
            m.payload.as_ref(),
            format!("mid-{}", expected_offset).as_bytes()
        );
    }
}

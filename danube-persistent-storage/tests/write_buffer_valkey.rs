/// Integration tests for the Valkey write buffer feature.
///
/// These tests require a running Valkey/Redis instance at `redis://127.0.0.1:6379`.
/// They are gated with `#[ignore = "requires running Valkey (docker run -p 6379:6379 valkey/valkey:latest)"]` so they don't run in the default test suite.
///
/// To run locally:
/// ```bash
/// docker run -d --name valkey-test -p 6379:6379 valkey/valkey:latest
/// cargo test -p danube-persistent-storage --test write_buffer_valkey -- --ignored --nocapture
/// docker rm -f valkey-test
/// ```
mod common;

use common::make_test_message;
use danube_core::metadata::{MemoryStore, MetadataStore};
use danube_core::storage::StartPosition;
use danube_persistent_storage::valkey::config::WriteBufferConfig;
use danube_persistent_storage::valkey::ValkeyClient;
use danube_persistent_storage::wal::WalConfig;
use danube_persistent_storage::{StorageFactory, StorageFactoryConfig};
use std::sync::Arc;
use tokio_stream::StreamExt;

const VALKEY_ENDPOINT: &str = "redis://127.0.0.1:6379";

/// Helper: create a StorageFactory with write_buffer enabled.
async fn create_write_buffer_factory() -> (StorageFactory, Arc<MemoryStore>) {
    let memory_store = Arc::new(MemoryStore::new().await.expect("create memory store"));
    let unique_dir = {
        let mut d = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        d.push(format!("danube-wb-test-{}", nanos));
        d
    };

    let wal_cfg = WalConfig {
        dir: Some(unique_dir),
        flush_interval_ms: Some(5_000),
        ..Default::default()
    };
    let wb_config = WriteBufferConfig {
        endpoints: vec![VALKEY_ENDPOINT.to_string()],
        wait_replicas: 0, // standalone Valkey has no replicas
        wait_timeout_ms: 100,
        ..Default::default()
    };

    let store_arc: Arc<dyn MetadataStore> = memory_store.clone();
    let factory = StorageFactory::new(
        StorageFactoryConfig::local(wal_cfg, "/danube", None).with_write_buffer(wb_config),
        store_arc,
    );

    (factory, memory_store)
}

/// Helper: create a ValkeyClient for direct verification.
async fn connect_valkey() -> ValkeyClient {
    let config = WriteBufferConfig {
        endpoints: vec![VALKEY_ENDPOINT.to_string()],
        ..Default::default()
    };
    ValkeyClient::connect(&config)
        .await
        .expect("connect to Valkey")
}

/// Helper: flush all keys from Valkey (clean state for each test).
async fn flush_valkey() {
    let config = WriteBufferConfig {
        endpoints: vec![VALKEY_ENDPOINT.to_string()],
        ..Default::default()
    };
    let client = redis::Client::open(VALKEY_ENDPOINT).expect("open redis");
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .expect("connect");
    let _: () = redis::cmd("FLUSHALL")
        .query_async(&mut conn)
        .await
        .expect("flushall");
    // Give Valkey a moment to settle
    let _ = config;
}

// ============================================================
// Test: Messages written via BufferedStorage appear in Valkey
// ============================================================

#[tokio::test]
#[ignore = "requires running Valkey (docker run -p 6379:6379 valkey/valkey:latest)"]
async fn write_buffer_append_and_verify_valkey() {
    flush_valkey().await;
    let (factory, _store) = create_write_buffer_factory().await;
    let valkey = connect_valkey().await;

    let topic = "/default/wb-append-test";
    let storage = factory.for_topic(topic).await.expect("create storage");

    // Write 3 messages
    for i in 0..3u64 {
        let offset = storage
            .append_message(
                topic,
                make_test_message(i, 1, topic, i, &format!("msg-{}", i)),
            )
            .await
            .expect("append");
        assert_eq!(offset, i);
    }

    // Verify Valkey has the data
    let active_key = "/topics/default/wb-append-test/active_segment";
    let entries = valkey.hgetall(active_key).await.expect("hgetall");
    assert_eq!(
        entries.len(),
        3,
        "expected 3 entries in Valkey active_segment, got {}",
        entries.len()
    );

    // Verify the fields are the string offsets "0", "1", "2"
    let mut fields: Vec<String> = entries.into_iter().map(|(f, _)| f).collect();
    fields.sort();
    assert_eq!(fields, vec!["0", "1", "2"]);
}

// ============================================================
// Test: Batch append pipelines correctly to Valkey
// ============================================================

#[tokio::test]
#[ignore = "requires running Valkey (docker run -p 6379:6379 valkey/valkey:latest)"]
async fn write_buffer_batch_append() {
    flush_valkey().await;
    let (factory, _store) = create_write_buffer_factory().await;
    let valkey = connect_valkey().await;

    let topic = "/default/wb-batch-test";
    let storage = factory.for_topic(topic).await.expect("create storage");

    // Write a batch of 5 messages
    let messages: Vec<_> = (0..5u64)
        .map(|i| make_test_message(i, 1, topic, i, &format!("batch-{}", i)))
        .collect();
    let (first, last) = storage
        .append_batch(topic, &messages)
        .await
        .expect("append_batch");
    assert_eq!(first, 0);
    assert_eq!(last, 4);

    // Verify Valkey
    let active_key = "/topics/default/wb-batch-test/active_segment";
    let entries = valkey.hgetall(active_key).await.expect("hgetall");
    assert_eq!(entries.len(), 5, "expected 5 entries in batch");
}

// ============================================================
// Test: Reads still come from local WAL (not Valkey)
// ============================================================

#[tokio::test]
#[ignore = "requires running Valkey (docker run -p 6379:6379 valkey/valkey:latest)"]
async fn write_buffer_read_delegates_to_wal() {
    flush_valkey().await;
    let (factory, _store) = create_write_buffer_factory().await;

    let topic = "/default/wb-read-test";
    let storage = factory.for_topic(topic).await.expect("create storage");

    // Write messages
    for i in 0..3u64 {
        storage
            .append_message(
                topic,
                make_test_message(i, 1, topic, i, &format!("read-{}", i)),
            )
            .await
            .expect("append");
    }

    // Read from WAL via create_reader
    let mut reader = storage
        .create_reader(topic, StartPosition::Offset(0))
        .await
        .expect("create reader");

    for expected in 0..3u64 {
        let msg = reader
            .next()
            .await
            .expect("reader item")
            .expect("reader result");
        assert_eq!(
            msg.payload.as_ref(),
            format!("read-{}", expected).as_bytes()
        );
        assert_eq!(msg.msg_id.topic_offset, expected);
    }
}

// ============================================================
// Test: Offset continuity — both WAL and Valkey agree
// ============================================================

#[tokio::test]
#[ignore = "requires running Valkey (docker run -p 6379:6379 valkey/valkey:latest)"]
async fn write_buffer_offset_continuity() {
    flush_valkey().await;
    let (factory, _store) = create_write_buffer_factory().await;
    let valkey = connect_valkey().await;

    let topic = "/default/wb-continuity-test";
    let storage = factory.for_topic(topic).await.expect("create storage");

    // Write 10 messages
    for i in 0..10u64 {
        let offset = storage
            .append_message(
                topic,
                make_test_message(i, 1, topic, i, &format!("cont-{}", i)),
            )
            .await
            .expect("append");
        assert_eq!(offset, i, "offset should be sequential");
    }

    // Verify WAL offset
    assert_eq!(storage.current_offset(), 10);

    // Verify Valkey has all 10 entries with correct offsets
    let active_key = "/topics/default/wb-continuity-test/active_segment";
    let entries = valkey.hgetall(active_key).await.expect("hgetall");
    assert_eq!(entries.len(), 10);

    let mut offsets: Vec<u64> = entries
        .iter()
        .filter_map(|(f, _)| f.parse::<u64>().ok())
        .collect();
    offsets.sort();
    let expected: Vec<u64> = (0..10).collect();
    assert_eq!(offsets, expected);
}

// ============================================================
// Test: Factory without write_buffer returns plain WalStorage
// ============================================================

#[tokio::test]
#[ignore = "requires running Valkey (docker run -p 6379:6379 valkey/valkey:latest)"]
async fn factory_without_write_buffer_skips_valkey() {
    flush_valkey().await;
    let memory_store = Arc::new(MemoryStore::new().await.expect("create memory store"));
    let unique_dir = {
        let mut d = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        d.push(format!("danube-no-wb-test-{}", nanos));
        d
    };

    let store_arc: Arc<dyn MetadataStore> = memory_store.clone();
    let factory = StorageFactory::new(
        StorageFactoryConfig::local(
            WalConfig {
                dir: Some(unique_dir),
                flush_interval_ms: Some(5_000),
                ..Default::default()
            },
            "/danube",
            None,
        ),
        // No .with_write_buffer() — plain config
        store_arc,
    );

    let topic = "/default/no-wb-test";
    let storage = factory.for_topic(topic).await.expect("create storage");

    // Write a message — should succeed without Valkey
    let offset = storage
        .append_message(topic, make_test_message(0, 1, topic, 0, "no-valkey"))
        .await
        .expect("append without write buffer");
    assert_eq!(offset, 0);

    // Verify Valkey has NO data for this topic
    let valkey = connect_valkey().await;
    let active_key = "/topics/default/no-wb-test/active_segment";
    let entries = valkey.hgetall(active_key).await.expect("hgetall");
    assert!(
        entries.is_empty(),
        "no entries should be in Valkey when write_buffer is disabled"
    );
}

// ============================================================
// Test: ValkeyClient basic connectivity and CRUD operations
// ============================================================

#[tokio::test]
#[ignore = "requires running Valkey (docker run -p 6379:6379 valkey/valkey:latest)"]
async fn valkey_client_crud_operations() {
    flush_valkey().await;
    let client = connect_valkey().await;

    // HSET + HGETALL
    client
        .hset("test-hash", "field1", b"value1")
        .await
        .expect("hset");
    client
        .hset("test-hash", "field2", b"value2")
        .await
        .expect("hset");

    let entries = client.hgetall("test-hash").await.expect("hgetall");
    assert_eq!(entries.len(), 2);

    // EXISTS
    assert!(client.exists("test-hash").await.expect("exists"));
    assert!(!client.exists("nonexistent").await.expect("exists"));

    // DEL
    client.del("test-hash").await.expect("del");
    assert!(!client.exists("test-hash").await.expect("exists after del"));

    // RPUSH + LLEN + LPOP
    client.rpush("test-list", "a").await.expect("rpush");
    client.rpush("test-list", "b").await.expect("rpush");
    client.rpush("test-list", "c").await.expect("rpush");

    let len = client.llen("test-list").await.expect("llen");
    assert_eq!(len, 3);

    let popped = client.lpop("test-list").await.expect("lpop");
    assert_eq!(popped, Some("a".to_string()));

    let len = client.llen("test-list").await.expect("llen after pop");
    assert_eq!(len, 2);

    // Cleanup
    client.del("test-list").await.expect("cleanup");
}

// ============================================================
// Test: Crash recovery — WAL destroyed, messages replayed from Valkey
// ============================================================

#[tokio::test]
#[ignore = "requires running Valkey (docker run -p 6379:6379 valkey/valkey:latest)"]
async fn write_buffer_crash_recovery_replay() {
    flush_valkey().await;

    let message_count = 5u64;
    let topic = "/default/wb-crash-recovery";

    // Use a unique temp directory so we can delete it to simulate crash.
    let wal_dir = {
        let mut d = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        d.push(format!("danube-recovery-test-{}", nanos));
        d
    };

    // Phase 1: Write messages via BufferedStorage (double-write: WAL + Valkey)
    {
        let memory_store = Arc::new(MemoryStore::new().await.expect("create memory store"));
        let store_arc: Arc<dyn MetadataStore> = memory_store.clone();

        let wal_cfg = WalConfig {
            dir: Some(wal_dir.clone()),
            flush_interval_ms: Some(5_000),
            ..Default::default()
        };
        let wb_config = WriteBufferConfig {
            endpoints: vec![VALKEY_ENDPOINT.to_string()],
            wait_replicas: 0,
            wait_timeout_ms: 100,
            ..Default::default()
        };

        let factory = StorageFactory::new(
            StorageFactoryConfig::local(wal_cfg, "/danube", None).with_write_buffer(wb_config),
            store_arc,
        );

        let storage = factory.for_topic(topic).await.expect("create storage");

        for i in 0..message_count {
            let offset = storage
                .append_message(
                    topic,
                    make_test_message(i, 1, topic, i, &format!("recovery-{}", i)),
                )
                .await
                .expect("append");
            assert_eq!(offset, i);
        }

        // Verify Valkey has the data
        let valkey = connect_valkey().await;
        let active_key = "/topics/default/wb-crash-recovery/active_segment";
        let entries = valkey.hgetall(active_key).await.expect("hgetall");
        assert_eq!(
            entries.len(),
            message_count as usize,
            "Valkey should have all {} messages before crash",
            message_count
        );
    }
    // Factory/storage dropped — simulates broker shutdown

    // Phase 2: Delete WAL directory to simulate disk loss / crash
    assert!(wal_dir.exists(), "WAL dir should exist before deletion");
    std::fs::remove_dir_all(&wal_dir).expect("delete WAL dir");
    assert!(!wal_dir.exists(), "WAL dir should be gone after deletion");

    // Phase 3: Create a NEW factory with same config — recovery should replay from Valkey
    let memory_store2 = Arc::new(MemoryStore::new().await.expect("create memory store 2"));
    let store_arc2: Arc<dyn MetadataStore> = memory_store2.clone();

    let wal_cfg2 = WalConfig {
        dir: Some(wal_dir.clone()),
        flush_interval_ms: Some(5_000),
        ..Default::default()
    };
    let wb_config2 = WriteBufferConfig {
        endpoints: vec![VALKEY_ENDPOINT.to_string()],
        wait_replicas: 0,
        wait_timeout_ms: 100,
        ..Default::default()
    };

    let factory2 = StorageFactory::new(
        StorageFactoryConfig::local(wal_cfg2, "/danube", None).with_write_buffer(wb_config2),
        store_arc2,
    );

    // This should trigger ValkeyWriteBuffer recovery + WAL replay
    let storage2 = factory2.for_topic(topic).await.expect("recovery for_topic");

    // Phase 4: Verify WAL offset is correct (messages were replayed)
    assert_eq!(
        storage2.current_offset(),
        message_count,
        "WAL should be at offset {} after replay",
        message_count
    );

    // Phase 5: Read all messages from the recovered WAL and verify payloads
    let mut reader = storage2
        .create_reader(topic, StartPosition::Offset(0))
        .await
        .expect("create reader on recovered WAL");

    for expected_offset in 0..message_count {
        let msg = reader
            .next()
            .await
            .expect("reader item")
            .expect("reader result");
        assert_eq!(
            msg.msg_id.topic_offset, expected_offset,
            "message offset mismatch"
        );
        assert_eq!(
            msg.payload.as_ref(),
            format!("recovery-{}", expected_offset).as_bytes(),
            "message payload mismatch at offset {}",
            expected_offset
        );
    }

    // Cleanup WAL dir
    let _ = std::fs::remove_dir_all(&wal_dir);
}

//! Tests: Multi-topic isolation via WalStorageFactory and per-topic components
//!
//! - Validate that WalStorageFactory produces per-topic WALs under <wal_root>/<ns>/<topic>/
//!   and that readers only see messages for their own topic.
//! - Validate that per-topic uploaders (started manually here for control) write to disjoint
//!   cloud namespaces and descriptors in metadata.

use std::time::Duration;

use danube_core::message::{MessageID, StreamMessage};
use danube_metadata_store::{MemoryStore, MetadataStorage, MetadataStore};
use danube_core::storage::PersistentStorage;
use danube_persistent_storage::wal::WalConfig;
use danube_persistent_storage::{BackendConfig, LocalBackend, Uploader, UploaderConfig, WalStorageFactory};
use tokio_stream::StreamExt;

fn make_msg(topic: &str, i: u64) -> StreamMessage {
    StreamMessage {
        request_id: i,
        msg_id: MessageID {
            producer_id: 1,
            topic_name: topic.to_string(),
            broker_addr: "127.0.0.1:6650".to_string(),
            segment_id: 0,
            segment_offset: i,
        },
        payload: format!("msg-{}", i).into_bytes(),
        publish_time: 0,
        producer_name: "test-producer".to_string(),
        subscription_name: None,
        attributes: Default::default(),
    }
}

#[tokio::test]
async fn test_factory_multi_topic_wal_isolation() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_root = tmp.path().to_path_buf();

    // Build BackendConfig (memory) and Metadata store; factory constructs Cloud/Etcd internally
    let backend = BackendConfig::Local { backend: LocalBackend::Memory, root: "mem-prefix".to_string() };
    let meta = MemoryStore::new().await.expect("memory meta");
    let metadata_store = MetadataStorage::InMemory(meta);

    let factory = WalStorageFactory::new_with_backend(
        WalConfig { dir: Some(wal_root.clone()), ..Default::default() },
        backend,
        metadata_store,
        "/danube",
    );

    let topic_a = "/default/topic-a";
    let topic_b = "/default/topic-b";

    let storage_a = factory.for_topic(topic_a).await.expect("create storage_a");
    let storage_b = factory.for_topic(topic_b).await.expect("create storage_b");

    // Append messages to each topic
    for i in 0..3u64 {
        storage_a
            .append_message(topic_a, make_msg(topic_a, i))
            .await
            .expect("append a");
        storage_b
            .append_message(topic_b, make_msg(topic_b, i))
            .await
            .expect("append b");
    }

    // Verify per-topic directories exist: <wal_root>/default/topic-a and topic-b
    let dir_a = wal_root.join("default").join("topic-a");
    let dir_b = wal_root.join("default").join("topic-b");
    assert!(tokio::fs::metadata(&dir_a).await.is_ok(), "topic-a dir should exist");
    assert!(tokio::fs::metadata(&dir_b).await.is_ok(), "topic-b dir should exist");

    // Readers should see only their own topic messages
    let mut ra = storage_a
        .create_reader(topic_a, danube_core::storage::StartPosition::Offset(0))
        .await
        .expect("reader a");
    let mut rb = storage_b
        .create_reader(topic_b, danube_core::storage::StartPosition::Offset(0))
        .await
        .expect("reader b");

    // Collect first 3 messages from each and compare payloads
    let mut got_a = Vec::new();
    let mut got_b = Vec::new();
    for _ in 0..3 {
        got_a.push(ra.next().await.unwrap().unwrap().payload);
        got_b.push(rb.next().await.unwrap().unwrap().payload);
    }
    assert_eq!(got_a, vec![b"msg-0".to_vec(), b"msg-1".to_vec(), b"msg-2".to_vec()]);
    assert_eq!(got_b, vec![b"msg-0".to_vec(), b"msg-1".to_vec(), b"msg-2".to_vec()]);
}

#[tokio::test]
async fn test_multi_topic_uploader_isolation() {
    // This test validates per-topic uploader isolation by starting two uploaders manually
    // over two independent per-topic WALs under the same root. We use memory cloud and
    // in-memory metadata to assert separate object namespaces and descriptors.

    let tmp = tempfile::tempdir().unwrap();
    let wal_root = tmp.path().to_path_buf();

    // Build two per-topic WALs under <wal_root>/default/topic-a and topic-b
    let wal_a = danube_persistent_storage::wal::Wal::with_config(WalConfig {
        dir: Some(wal_root.join("default").join("topic-a")),
        ..Default::default()
    })
    .await
    .expect("wal a");
    let wal_b = danube_persistent_storage::wal::Wal::with_config(WalConfig {
        dir: Some(wal_root.join("default").join("topic-b")),
        ..Default::default()
    })
    .await
    .expect("wal b");

    let topic_a = "default/topic-a"; // uploader expects topic_path without leading '/'
    let topic_b = "default/topic-b";

    // Append messages
    for i in 0..2u64 {
        wal_a.append(&make_msg(topic_a, i)).await.expect("append a");
        wal_b.append(&make_msg(topic_b, i)).await.expect("append b");
    }

    // Cloud: memory; Metadata: memory
    let cloud = danube_persistent_storage::CloudStore::new(BackendConfig::Local {
        backend: LocalBackend::Memory,
        root: "mem-prefix".to_string(),
    })
    .expect("cloud");
    let mem = MemoryStore::new().await.expect("meta mem");
    let meta = danube_persistent_storage::EtcdMetadata::new(MetadataStorage::InMemory(mem.clone()), "/danube".to_string());

    // Start two uploaders with short intervals
    let up_a = Uploader::new(
        UploaderConfig { interval_seconds: 1, max_batch_bytes: 8 * 1024 * 1024, topic_path: topic_a.to_string(), root_prefix: "/danube".to_string() },
        wal_a.clone(),
        cloud.clone(),
        meta.clone(),
    )
    .expect("up a");
    let up_b = Uploader::new(
        UploaderConfig { interval_seconds: 1, max_batch_bytes: 8 * 1024 * 1024, topic_path: topic_b.to_string(), root_prefix: "/danube".to_string() },
        wal_b.clone(),
        cloud.clone(),
        meta.clone(),
    )
    .expect("up b");

    let h_a = std::sync::Arc::new(up_a).start();
    let h_b = std::sync::Arc::new(up_b).start();

    // Wait for uploaders to tick
    tokio::time::sleep(Duration::from_millis(1200)).await;

    h_a.abort();
    h_b.abort();

    // Validate descriptors exist for each topic under distinct prefixes
    let prefix_a = "/danube/storage/topics/default/topic-a/objects";
    let prefix_b = "/danube/storage/topics/default/topic-b/objects";

    let mut children_a = mem.get_childrens(prefix_a).await.expect("children a");
    let mut children_b = mem.get_childrens(prefix_b).await.expect("children b");

    // Filter pointers and directories
    children_a.retain(|c| c != "cur" && !c.ends_with('/'));
    children_b.retain(|c| c != "cur" && !c.ends_with('/'));

    assert!(!children_a.is_empty(), "topic-a should have object descriptors");
    assert!(!children_b.is_empty(), "topic-b should have object descriptors");
}

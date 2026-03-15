//! Tests: Multi-topic isolation via WalStorageFactory and per-topic components
//!
//! - Validate that WalStorageFactory produces per-topic WALs under <wal_root>/<ns>/<topic>/
//!   and that readers only see messages for their own topic.
//! - Validate that per-topic uploaders (started manually here for control) write to disjoint
//!   cloud namespaces and descriptors in metadata.

use std::sync::Arc;
use std::time::Duration;

use danube_core::message::{MessageID, StreamMessage};
use danube_core::metadata::{MemoryStore, MetadataStore};
use danube_core::storage::PersistentStorage;
use danube_persistent_storage::wal::WalConfig;
use danube_persistent_storage::{BackendConfig, LocalBackend, StorageFactory, StorageFactoryConfig};
use tokio_stream::StreamExt;

fn make_msg(topic: &str, i: u64) -> StreamMessage {
    StreamMessage {
        request_id: i,
        msg_id: MessageID {
            producer_id: 1,
            topic_name: topic.to_string(),
            broker_addr: "127.0.0.1:6650".to_string(),
            topic_offset: i,
        },
        payload: format!("msg-{}", i).into_bytes().into(),
        publish_time: 0,
        producer_name: "test-producer".to_string(),
        subscription_name: None,
        attributes: Default::default(),
        schema_id: None,
        schema_version: None,
    }
}

#[tokio::test]
async fn test_factory_multi_topic_wal_isolation() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_root = tmp.path().to_path_buf();

    // Build BackendConfig (memory) and Metadata store; factory constructs Cloud/StorageMetadata internally
    let backend = BackendConfig::Local {
        backend: LocalBackend::Memory,
        root: "mem-prefix".to_string(),
    };
    let meta = MemoryStore::new().await.expect("memory meta");
    let metadata_store: std::sync::Arc<dyn MetadataStore> = std::sync::Arc::new(meta);

    let factory = StorageFactory::new(
        StorageFactoryConfig::cloud_native(
            WalConfig {
                dir: Some(wal_root.clone()),
                fsync_interval_ms: Some(50),
                fsync_max_batch_bytes: Some(1),
                ..Default::default()
            },
            "/danube",
            backend,
            None,
        )
        .with_uploader_interval_seconds(1),
        metadata_store,
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
    assert!(
        tokio::fs::metadata(&dir_a).await.is_ok(),
        "topic-a dir should exist"
    );
    assert!(
        tokio::fs::metadata(&dir_b).await.is_ok(),
        "topic-b dir should exist"
    );

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
    assert_eq!(
        got_a,
        vec![b"msg-0".to_vec(), b"msg-1".to_vec(), b"msg-2".to_vec()]
    );
    assert_eq!(
        got_b,
        vec![b"msg-0".to_vec(), b"msg-1".to_vec(), b"msg-2".to_vec()]
    );
}

#[tokio::test]
async fn test_multi_topic_uploader_isolation() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_root = tmp.path().to_path_buf();
    let backend = BackendConfig::Local {
        backend: LocalBackend::Memory,
        root: "mem-prefix".to_string(),
    };
    let mem = Arc::new(MemoryStore::new().await.expect("meta mem"));
    let metadata_store: Arc<dyn MetadataStore> = mem.clone();
    let factory = StorageFactory::new(
        StorageFactoryConfig::cloud_native(
            WalConfig {
                dir: Some(wal_root.clone()),
                fsync_interval_ms: Some(50),
                fsync_max_batch_bytes: Some(1),
                ..Default::default()
            },
            "/danube",
            backend,
            None,
        )
        .with_uploader_interval_seconds(1),
        metadata_store,
    );

    let topic_a = "/default/topic-a";
    let topic_b = "/default/topic-b";
    let storage_a = factory.for_topic(topic_a).await.expect("storage a");
    let storage_b = factory.for_topic(topic_b).await.expect("storage b");

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

    // Wait deterministically for descriptors to appear
    for _ in 0..50 {
        let a = mem
            .get_childrens("/danube/storage/topics/default/topic-a/segments")
            .await
            .unwrap_or_default();
        let b = mem
            .get_childrens("/danube/storage/topics/default/topic-b/segments")
            .await
            .unwrap_or_default();
        let a_has = a.iter().any(|c| c != "cur" && !c.ends_with('/'));
        let b_has = b.iter().any(|c| c != "cur" && !c.ends_with('/'));
        if a_has && b_has {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let prefix_a = "/danube/storage/topics/default/topic-a/segments";
    let prefix_b = "/danube/storage/topics/default/topic-b/segments";

    let mut children_a = mem.get_childrens(prefix_a).await.expect("children a");
    let mut children_b = mem.get_childrens(prefix_b).await.expect("children b");

    // Filter pointers and directories
    children_a.retain(|c| c != "cur" && !c.ends_with('/'));
    children_b.retain(|c| c != "cur" && !c.ends_with('/'));

    assert!(
        !children_a.is_empty(),
        "topic-a should have segment descriptors"
    );
    assert!(
        !children_b.is_empty(),
        "topic-b should have segment descriptors"
    );
}

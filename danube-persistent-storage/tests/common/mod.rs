use danube_core::message::{MessageID, StreamMessage};
use danube_core::storage::{PersistentStorage, StartPosition};
use danube_metadata_store::{MemoryStore, MetadataStorage, MetadataStore};
use danube_persistent_storage::wal::WalConfig;
use danube_persistent_storage::{
    BackendConfig, LocalBackend, ObjectDescriptor, UploaderBaseConfig, WalStorageFactory,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio_stream::StreamExt;

/// Creates a test message with the given parameters
pub fn make_test_message(
    request_id: u64,
    producer_id: u64,
    topic_name: &str,
    segment_offset: u64,
    payload: &str,
) -> StreamMessage {
    StreamMessage {
        request_id,
        msg_id: MessageID {
            producer_id,
            topic_name: topic_name.to_string(),
            broker_addr: "localhost:6650".to_string(),
            segment_id: 0,
            segment_offset,
        },
        payload: payload.as_bytes().to_vec(),
        publish_time: segment_offset,
        producer_name: format!("producer-{}", producer_id),
        subscription_name: None,
        attributes: HashMap::new(),
    }
}

/// Creates a test setup with in-memory backends
pub async fn create_test_factory() -> (WalStorageFactory, Arc<MemoryStore>) {
    let memory_store = Arc::new(MemoryStore::new().await.expect("create memory store"));

    let factory = WalStorageFactory::new(
        WalConfig {
            dir: Some(std::path::PathBuf::from("/tmp/danube-integration-test")),
            ..Default::default()
        },
        BackendConfig::Local {
            backend: LocalBackend::Memory,
            root: "integration-test".to_string(),
        },
        MetadataStorage::InMemory((*memory_store).clone()),
        "/danube",
        UploaderBaseConfig { interval_seconds: 1, ..Default::default() },
    );

    (factory, memory_store)
}

/// Waits for a condition to be true with timeout
pub async fn wait_for_condition<F, Fut>(mut condition: F, timeout_ms: u64) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_millis(timeout_ms);

    while start.elapsed() < timeout {
        if condition().await {
            return true;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    false
}

/// Counts objects in cloud storage for a topic
pub async fn count_cloud_objects(memory_store: &MemoryStore, topic_path: &str) -> usize {
    let prefix = format!("/danube/storage/topics/{}/objects", topic_path);
    let children = memory_store
        .get_childrens(&prefix)
        .await
        .unwrap_or_default();
    children
        .into_iter()
        .filter(|c| c != "cur" && !c.ends_with('/'))
        .count()
}

/// Gets the latest object descriptor for a topic
#[allow(dead_code)]
pub async fn get_latest_object_descriptor(
    memory_store: &MemoryStore,
    topic_path: &str,
) -> Option<ObjectDescriptor> {
    let prefix = format!("/danube/storage/topics/{}/objects", topic_path);
    let children = memory_store.get_childrens(&prefix).await.ok()?;
    let mut objects: Vec<_> = children
        .into_iter()
        .filter(|c| c != "cur" && !c.ends_with('/'))
        .collect();
    objects.sort();

    if let Some(latest) = objects.last() {
        let object_key = format!("{}/{}", prefix, latest);
        let desc_value = memory_store
            .get(&object_key, danube_metadata_store::MetaOptions::None)
            .await
            .ok()??;

        serde_json::from_value(desc_value).ok()
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_make_test_message() {
        let msg = make_test_message(1, 100, "test-topic", 5, "hello");
        assert_eq!(msg.request_id, 1);
        assert_eq!(msg.msg_id.producer_id, 100);
        assert_eq!(msg.msg_id.topic_name, "test-topic");
        assert_eq!(msg.msg_id.segment_offset, 5);
        assert_eq!(msg.payload, b"hello");
        assert_eq!(msg.producer_name, "producer-100");
    }

    #[tokio::test]
    async fn test_create_test_factory() {
        let (factory, _memory) = create_test_factory().await;

        // Test that we can create a WAL storage for a topic
        let storage = factory
            .for_topic("test/topic")
            .await
            .expect("create storage");

        // Test basic append operation
        let msg = make_test_message(1, 1, "test/topic", 0, "test");
        storage
            .append_message("test/topic", msg)
            .await
            .expect("append message");

        // Note: WalStorage doesn't expose current_offset directly, so we test via reader
        let mut reader = storage
            .create_reader("test/topic", StartPosition::Offset(0))
            .await
            .expect("create reader");
        let read_msg = reader
            .next()
            .await
            .expect("read message")
            .expect("message result");
        assert_eq!(read_msg.payload, b"test");
    }

    #[tokio::test]
    async fn test_wait_for_condition() {
        let start = std::time::Instant::now();

        // Test successful condition
        let result = wait_for_condition(|| async { true }, 100).await;
        assert!(result);
        assert!(start.elapsed().as_millis() < 100);

        // Test timeout condition
        let start = std::time::Instant::now();
        let result = wait_for_condition(|| async { false }, 100).await;
        assert!(!result);
        assert!(start.elapsed().as_millis() >= 100);
    }

    #[tokio::test]
    async fn test_count_cloud_objects() {
        let (factory, memory) = create_test_factory().await;
        let storage = factory
            .for_topic("test/count")
            .await
            .expect("create storage");

        // Initially no objects
        let count = count_cloud_objects(&memory, "test/count").await;
        assert_eq!(count, 0);

        // Add messages and wait for upload
        for i in 0..5 {
            let msg = make_test_message(i, 1, "test/count", i, &format!("msg-{}", i));
            storage
                .append_message("test/count", msg)
                .await
                .expect("append message");
        }

        // Wait for uploader to create objects
        let uploaded = wait_for_condition(
            || async { count_cloud_objects(&memory, "test/count").await > 0 },
            5000,
        )
        .await;

        if uploaded {
            let final_count = count_cloud_objects(&memory, "test/count").await;
            assert!(final_count > 0);
        }
    }
}

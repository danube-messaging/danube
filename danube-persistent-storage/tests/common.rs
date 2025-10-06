use danube_core::message::{MessageID, StreamMessage};
use danube_metadata_store::{MemoryStore, MetadataStorage, MetadataStore};
use danube_persistent_storage::wal::WalConfig;
use danube_persistent_storage::{
    BackendConfig, LocalBackend, ObjectDescriptor, UploaderBaseConfig, WalStorageFactory,
};
use std::collections::HashMap;
use std::sync::Arc;

/// Creates a test message with the given parameters
#[allow(dead_code)]
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

    // Use a unique WAL directory per test run to avoid interference with previous runs.
    let unique_dir = {
        let mut d = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        d.push(format!("danube-integration-test-{}", nanos));
        d
    };

    let factory = WalStorageFactory::new(
        WalConfig {
            dir: Some(unique_dir),
            fsync_interval_ms: Some(200),
            ..Default::default()
        },
        BackendConfig::Local {
            backend: LocalBackend::Memory,
            root: "integration-test".to_string(),
        },
        MetadataStorage::InMemory((*memory_store).clone()),
        "/danube",
        UploaderBaseConfig {
            interval_seconds: 1,
            ..Default::default()
        },
    );

    (factory, memory_store)
}

/// Waits for a condition to be true with timeout
#[allow(dead_code)]
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
#[allow(dead_code)]
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
        // Some MetadataStore implementations may return fully-qualified keys.
        let object_key = if latest.contains('/') {
            latest.clone()
        } else {
            format!("{}/{}", prefix, latest)
        };
        let desc_value = memory_store
            .get(&object_key, danube_metadata_store::MetaOptions::None)
            .await
            .ok()??;

        serde_json::from_value(desc_value).ok()
    } else {
        None
    }
}

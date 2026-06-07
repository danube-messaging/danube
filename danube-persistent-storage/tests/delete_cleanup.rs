use danube_core::message::{MessageID, StreamMessage};
use danube_core::metadata::{MemoryStore, MetaOptions, MetadataStore};
use danube_persistent_storage::wal::WalConfig;
use danube_persistent_storage::{
    ObjectStoreConfig, SegmentDescriptor, StorageFactory, StorageFactoryConfig,
};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

fn make_message(topic_name: &str, topic_offset: u64) -> StreamMessage {
    StreamMessage {
        request_id: topic_offset,
        msg_id: MessageID {
            producer_id: 1,
            topic_name: topic_name.to_string(),
            broker_addr: "localhost:6650".to_string(),
            topic_offset,
        },
        payload: format!("msg-{}", topic_offset).into_bytes().into(),
        publish_time: topic_offset,
        producer_name: "producer-1".to_string(),
        subscription_name: None,
        attributes: HashMap::new(),
        schema_id: None,
        schema_version: None,
            routing_key: None,
    }
}

fn build_factory(
    cloud_native: bool,
    wal_dir: PathBuf,
    durable_root: &Path,
    metadata_store: Arc<dyn MetadataStore>,
) -> StorageFactory {
    let wal = WalConfig {
        dir: Some(wal_dir),
        flush_interval_ms: Some(5_000),
        ..Default::default()
    };

    if cloud_native {
        StorageFactory::new(
            StorageFactoryConfig::object_store(
                wal,
                "/danube",
                ObjectStoreConfig::filesystem_for_tests(durable_root.to_string_lossy().to_string()),
                None,
            )
            .with_segment_export_interval_seconds(1),
            metadata_store,
        )
    } else {
        StorageFactory::new(
            StorageFactoryConfig::shared_fs(
                wal,
                "/danube",
                durable_root.to_string_lossy().to_string(),
                None,
            ),
            metadata_store,
        )
    }
}

async fn run_delete_cleanup(cloud_native: bool, topic: &str) {
    let wal_dir = tempfile::tempdir().expect("wal dir");
    let durable_root = tempfile::tempdir().expect("durable root");
    let memory_store = Arc::new(MemoryStore::new().await.expect("memory store"));
    let metadata_store: Arc<dyn MetadataStore> = memory_store.clone();

    let factory = build_factory(
        cloud_native,
        wal_dir.path().to_path_buf(),
        durable_root.path(),
        metadata_store,
    );

    let storage = factory.for_topic(topic).await.expect("create storage");
    for offset in 0..3u64 {
        let assigned = storage
            .append_message(topic, make_message(topic, offset))
            .await
            .expect("append message");
        assert_eq!(assigned, offset);
    }
    factory.seal(topic, 41).await.expect("seal topic");

    let topic_path = topic.trim_start_matches('/');
    let segment_prefix = format!("/danube/storage/topics/{}/segments", topic_path);
    let segment_children = memory_store
        .get_childrens(&segment_prefix)
        .await
        .expect("list segment descriptors before delete");
    let descriptor_keys: Vec<_> = segment_children
        .into_iter()
        .filter(|child| {
            !child.ends_with('/')
                && child.rsplit('/').next() != Some("cur")
        })
        .map(|child| {
            if child.contains('/') {
                child
            } else {
                format!("{}/{}", segment_prefix, child)
            }
        })
        .collect();
    assert!(
        !descriptor_keys.is_empty(),
        "topic should have durable segment descriptors before delete"
    );

    let mut durable_paths = Vec::new();
    for key in &descriptor_keys {
        let desc_value = memory_store
            .get(key, MetaOptions::None)
            .await
            .expect("get descriptor")
            .expect("descriptor exists");
        let desc: SegmentDescriptor = serde_json::from_value(desc_value).expect("parse descriptor");
        let durable_path = durable_root
            .path()
            .join("storage")
            .join("topics")
            .join(topic_path)
            .join("segments")
            .join(&desc.segment_id);
        assert!(
            tokio::fs::try_exists(&durable_path)
                .await
                .expect("check durable segment exists"),
            "durable segment should exist before delete"
        );
        durable_paths.push(durable_path);
    }

    factory
        .delete_storage_metadata(topic)
        .await
        .expect("delete storage metadata");

    let remaining_children = memory_store
        .get_childrens(&segment_prefix)
        .await
        .unwrap_or_default();
    let remaining_descriptors: Vec<_> = remaining_children
        .into_iter()
        .filter(|child| {
            !child.ends_with('/')
                && child.rsplit('/').next() != Some("cur")
        })
        .collect();
    assert!(
        remaining_descriptors.is_empty(),
        "segment descriptors should be removed after delete"
    );

    let state_key = format!("/danube/storage/topics/{}/state", topic_path);
    let state = memory_store
        .get(&state_key, MetaOptions::None)
        .await
        .expect("read state key after delete");
    assert!(state.is_none(), "sealed state should be removed after delete");

    for durable_path in durable_paths {
        assert!(
            !tokio::fs::try_exists(&durable_path)
                .await
                .expect("check durable segment removal"),
            "durable segment should be removed after delete"
        );
    }

    let topic_wal_dir = wal_dir.path().join(topic_path);
    assert!(
        !tokio::fs::try_exists(&topic_wal_dir)
            .await
            .expect("check wal dir removal"),
        "topic wal directory should be removed after delete"
    );
}

#[tokio::test]
async fn shared_fs_delete_removes_catalog_and_durable_segments() {
    run_delete_cleanup(false, "/default/shared-fs-delete").await;
}

#[tokio::test]
async fn cloud_native_delete_removes_catalog_and_durable_segments() {
    run_delete_cleanup(true, "/default/cloud-native-delete").await;
}

use danube_core::message::{MessageID, StreamMessage};
use danube_core::metadata::{MemoryStore, MetadataStore};
use danube_core::storage::{PersistentStorage, StartPosition};
use danube_persistent_storage::wal::WalConfig;
use danube_persistent_storage::{
    BackendConfig, LocalBackend, StorageFactory, StorageFactoryConfig,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio_stream::StreamExt;

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
    }
}

#[tokio::test]
async fn shared_fs_seal_takeover_replay_continuity() {
    let wal_owner_1 = tempfile::tempdir().expect("owner1 wal dir");
    let wal_owner_2 = tempfile::tempdir().expect("owner2 wal dir");
    let durable_root = tempfile::tempdir().expect("shared durable root");
    let memory_store = Arc::new(MemoryStore::new().await.expect("memory store"));
    let metadata_store: Arc<dyn MetadataStore> = memory_store.clone();

    let topic = "/default/shared-fs-smoke";
    let backend = BackendConfig::Local {
        backend: LocalBackend::Fs,
        root: durable_root.path().to_string_lossy().to_string(),
    };

    let factory_1 = StorageFactory::new(
        StorageFactoryConfig::shared_fs(
            WalConfig {
                dir: Some(wal_owner_1.path().to_path_buf()),
                fsync_interval_ms: Some(5_000),
                ..Default::default()
            },
            "/danube",
            backend.clone(),
            None,
        ),
        metadata_store.clone(),
    );

    let storage_1 = factory_1.for_topic(topic).await.expect("create owner1 storage");
    for offset in 0..3u64 {
        let assigned = storage_1
            .append_message(topic, make_message(topic, offset))
            .await
            .expect("append owner1 message");
        assert_eq!(assigned, offset);
    }

    let seal_info = factory_1.seal(topic, 11).await.expect("seal owner1");
    assert_eq!(seal_info.last_committed_offset, 2);

    let segment_prefix = "/danube/storage/topics/default/shared-fs-smoke/segments";
    let segment_children = memory_store
        .get_childrens(segment_prefix)
        .await
        .expect("list shared-fs segment descriptors");
    let segments: Vec<_> = segment_children
        .into_iter()
        .filter(|child| child != "cur" && !child.ends_with('/'))
        .collect();
    assert!(
        !segments.is_empty(),
        "shared_fs seal should export durable segment descriptors"
    );

    let owner1_topic_dir = wal_owner_1.path().join("default").join("shared-fs-smoke");
    assert!(
        !tokio::fs::try_exists(&owner1_topic_dir)
            .await
            .expect("check owner1 wal dir"),
        "shared_fs seal should remove owner1 local wal directory"
    );

    let factory_2 = StorageFactory::new(
        StorageFactoryConfig::shared_fs(
            WalConfig {
                dir: Some(wal_owner_2.path().to_path_buf()),
                fsync_interval_ms: Some(5_000),
                ..Default::default()
            },
            "/danube",
            backend,
            None,
        ),
        metadata_store,
    );

    let storage_2 = factory_2.for_topic(topic).await.expect("create owner2 storage");
    let assigned = storage_2
        .append_message(topic, make_message(topic, 3))
        .await
        .expect("append owner2 message");
    assert_eq!(assigned, 3);

    let mut reader = storage_2
        .create_reader(topic, StartPosition::Offset(0))
        .await
        .expect("create owner2 reader");
    for expected in 0..4u64 {
        let msg = reader
            .next()
            .await
            .expect("reader item")
            .expect("reader result");
        assert_eq!(msg.payload.as_ref(), format!("msg-{}", expected).as_bytes());
        assert_eq!(msg.msg_id.topic_offset, expected);
    }
}

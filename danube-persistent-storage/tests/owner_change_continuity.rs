use danube_core::message::{MessageID, StreamMessage};
use danube_core::metadata::{MemoryStore, MetadataStore};
use danube_core::storage::{PersistentStorage, StartPosition};
use danube_persistent_storage::wal::WalConfig;
use danube_persistent_storage::{
    BackendConfig, LocalBackend, StorageFactory, StorageFactoryConfig,
};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::time::{timeout, Duration};
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

fn build_factory(
    cloud_native: bool,
    wal_dir: PathBuf,
    durable_root: &Path,
    metadata_store: Arc<dyn MetadataStore>,
) -> StorageFactory {
    let backend = BackendConfig::Local {
        backend: LocalBackend::Fs,
        root: durable_root.to_string_lossy().to_string(),
    };
    let wal = WalConfig {
        dir: Some(wal_dir),
        fsync_interval_ms: Some(5_000),
        ..Default::default()
    };

    if cloud_native {
        StorageFactory::new(
            StorageFactoryConfig::cloud_native(wal, "/danube", backend, None)
                .with_uploader_interval_seconds(1),
            metadata_store,
        )
    } else {
        StorageFactory::new(
            StorageFactoryConfig::shared_fs(wal, "/danube", backend, None),
            metadata_store,
        )
    }
}

async fn run_multi_owner_continuity(cloud_native: bool, topic: &str) {
    let wal_owner_1 = tempfile::tempdir().expect("owner1 wal dir");
    let wal_owner_2 = tempfile::tempdir().expect("owner2 wal dir");
    let wal_owner_3 = tempfile::tempdir().expect("owner3 wal dir");
    let durable_root = tempfile::tempdir().expect("durable root");
    let memory_store = Arc::new(MemoryStore::new().await.expect("memory store"));
    let metadata_store: Arc<dyn MetadataStore> = memory_store.clone();

    let factory_1 = build_factory(
        cloud_native,
        wal_owner_1.path().to_path_buf(),
        durable_root.path(),
        metadata_store.clone(),
    );
    let storage_1 = factory_1.for_topic(topic).await.expect("create owner1 storage");
    for offset in 0..2u64 {
        let assigned = storage_1
            .append_message(topic, make_message(topic, offset))
            .await
            .expect("append owner1 message");
        assert_eq!(assigned, offset);
    }
    let seal_1 = factory_1.seal(topic, 31).await.expect("seal owner1");
    assert_eq!(seal_1.last_committed_offset, 1);

    let factory_2 = build_factory(
        cloud_native,
        wal_owner_2.path().to_path_buf(),
        durable_root.path(),
        metadata_store.clone(),
    );
    let storage_2 = factory_2.for_topic(topic).await.expect("create owner2 storage");
    for offset in 2..4u64 {
        let assigned = storage_2
            .append_message(topic, make_message(topic, offset))
            .await
            .expect("append owner2 message");
        assert_eq!(assigned, offset);
    }
    let seal_2 = factory_2.seal(topic, 32).await.expect("seal owner2");
    assert_eq!(seal_2.last_committed_offset, 3);

    let factory_3 = build_factory(
        cloud_native,
        wal_owner_3.path().to_path_buf(),
        durable_root.path(),
        metadata_store,
    );
    let storage_3 = factory_3.for_topic(topic).await.expect("create owner3 storage");
    let mut reader = storage_3
        .create_reader(topic, StartPosition::Offset(0))
        .await
        .expect("create owner3 reader");

    let assigned = storage_3
        .append_message(topic, make_message(topic, 4))
        .await
        .expect("append owner3 message");
    assert_eq!(assigned, 4);

    let mut seen_offsets = Vec::new();
    for expected in 0..5u64 {
        let msg = timeout(Duration::from_secs(5), reader.next())
            .await
            .expect("timeout reading continuity item")
            .expect("continuity reader item")
            .expect("continuity reader result");
        assert_eq!(msg.payload.as_ref(), format!("msg-{}", expected).as_bytes());
        seen_offsets.push(msg.msg_id.topic_offset);
    }

    assert_eq!(seen_offsets, vec![0, 1, 2, 3, 4]);
}

#[tokio::test]
async fn shared_fs_multiple_owner_changes_preserve_offsets_without_duplicates() {
    run_multi_owner_continuity(false, "/default/shared-fs-continuity").await;
}

#[tokio::test]
async fn cloud_native_multiple_owner_changes_preserve_offsets_without_duplicates() {
    run_multi_owner_continuity(true, "/default/cloud-native-continuity").await;
}

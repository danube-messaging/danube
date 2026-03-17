use danube_core::message::{MessageID, StreamMessage};
use danube_core::metadata::{MemoryStore, MetadataStore};
use danube_core::storage::{PersistentStorage, StartPosition};
use danube_persistent_storage::checkpoint::{CheckpointStore, WalCheckpoint};
use danube_persistent_storage::wal::WalConfig;
use danube_persistent_storage::{
    ObjectStoreConfig, SegmentDescriptor, StorageFactory, StorageFactoryConfig, StorageMetadata,
};
use std::collections::HashMap;
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

#[tokio::test]
async fn cloud_native_seal_takeover_replay_and_live_continuity() {
    let wal_owner_1 = tempfile::tempdir().expect("owner1 wal dir");
    let wal_owner_2 = tempfile::tempdir().expect("owner2 wal dir");
    let durable_root = tempfile::tempdir().expect("cloud durable root");
    let memory_store = Arc::new(MemoryStore::new().await.expect("memory store"));
    let metadata_store: Arc<dyn MetadataStore> = memory_store.clone();

    let topic = "/default/cloud-native-smoke";
    let object_store =
        ObjectStoreConfig::filesystem_for_tests(durable_root.path().to_string_lossy().to_string());

    let factory_1 = StorageFactory::new(
        StorageFactoryConfig::object_store(
            WalConfig {
                dir: Some(wal_owner_1.path().to_path_buf()),
                fsync_interval_ms: Some(5_000),
                ..Default::default()
            },
            "/danube",
            object_store.clone(),
            None,
        )
        .with_segment_export_interval_seconds(1),
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

    let seal_info = factory_1.seal(topic, 21).await.expect("seal owner1");
    assert_eq!(seal_info.last_committed_offset, 2);

    let segment_prefix = "/danube/storage/topics/default/cloud-native-smoke/segments";
    let segment_children = memory_store
        .get_childrens(segment_prefix)
        .await
        .expect("list cloud-native segment descriptors");
    let segments: Vec<_> = segment_children
        .into_iter()
        .filter(|child| child != "cur" && !child.ends_with('/'))
        .collect();
    assert!(
        !segments.is_empty(),
        "cloud_native seal should leave durable segment descriptors for takeover"
    );

    let owner1_topic_dir = wal_owner_1.path().join("default").join("cloud-native-smoke");
    assert!(
        !tokio::fs::try_exists(&owner1_topic_dir)
            .await
            .expect("check owner1 wal dir"),
        "cloud_native seal should remove owner1 local wal directory"
    );

    let factory_2 = StorageFactory::new(
        StorageFactoryConfig::object_store(
            WalConfig {
                dir: Some(wal_owner_2.path().to_path_buf()),
                fsync_interval_ms: Some(5_000),
                ..Default::default()
            },
            "/danube",
            object_store,
            None,
        )
        .with_segment_export_interval_seconds(1),
        metadata_store,
    );

    let storage_2 = factory_2.for_topic(topic).await.expect("create owner2 storage");
    let mut reader = storage_2
        .create_reader(topic, StartPosition::Offset(0))
        .await
        .expect("create owner2 reader");

    for expected in 0..3u64 {
        let msg = timeout(Duration::from_secs(5), reader.next())
            .await
            .expect("timeout reading historical item")
            .expect("historical reader item")
            .expect("historical reader result");
        assert_eq!(msg.payload.as_ref(), format!("msg-{}", expected).as_bytes());
        assert_eq!(msg.msg_id.topic_offset, expected);
    }

    let assigned = storage_2
        .append_message(topic, make_message(topic, 3))
        .await
        .expect("append owner2 live message");
    assert_eq!(assigned, 3);

    let live = timeout(Duration::from_secs(5), reader.next())
        .await
        .expect("timeout reading live item")
        .expect("live reader item")
        .expect("live reader result");
    assert_eq!(live.payload.as_ref(), b"msg-3");
    assert_eq!(live.msg_id.topic_offset, 3);
}

#[tokio::test]
async fn cloud_native_recovers_from_catalog_when_checkpoint_points_to_missing_local_files() {
    let wal_root = tempfile::tempdir().expect("wal root");
    let durable_root = tempfile::tempdir().expect("cloud durable root");
    let memory_store = Arc::new(MemoryStore::new().await.expect("memory store"));
    let metadata_store: Arc<dyn MetadataStore> = memory_store.clone();

    let topic = "/default/cloud-native-recovery";
    let topic_path = "default/cloud-native-recovery";
    let object_store =
        ObjectStoreConfig::filesystem_for_tests(durable_root.path().to_string_lossy().to_string());

    let topic_dir = wal_root.path().join("default").join("cloud-native-recovery");
    tokio::fs::create_dir_all(&topic_dir)
        .await
        .expect("create topic wal dir");

    let ckpt_store = Arc::new(CheckpointStore::new(topic_dir.join("wal.ckpt")));
    ckpt_store
        .update_wal(&WalCheckpoint {
            start_offset: 0,
            last_offset: 2,
            file_seq: 0,
            file_path: topic_dir.join("wal.0.log").to_string_lossy().to_string(),
            rotated_files: Vec::new(),
            active_file_name: Some("wal.0.log".to_string()),
            last_rotation_at: None,
            active_file_first_offset: Some(0),
        })
        .await
        .expect("seed stale wal checkpoint");

    let storage_metadata = StorageMetadata::new(metadata_store.clone(), "/danube".to_string());
    let segment = SegmentDescriptor {
        segment_id: "data-0-test.dnb1".to_string(),
        start_offset: 0,
        end_offset: 2,
        size: 123,
        etag: None,
        created_at: 1,
        completed: true,
        offset_index: None,
    };
    storage_metadata
        .put_segment_descriptor(topic_path, "00000000000000000000", &segment)
        .await
        .expect("seed segment descriptor");
    storage_metadata
        .put_current_segment(topic_path, "00000000000000000000")
        .await
        .expect("seed current segment descriptor");

    let factory = StorageFactory::new(
        StorageFactoryConfig::object_store(
            WalConfig {
                dir: Some(wal_root.path().to_path_buf()),
                fsync_interval_ms: Some(5_000),
                ..Default::default()
            },
            "/danube",
            object_store,
            None,
        )
        .with_segment_export_interval_seconds(1),
        metadata_store,
    );

    let storage = factory.for_topic(topic).await.expect("create storage");
    let assigned = storage
        .append_message(topic, make_message(topic, 0))
        .await
        .expect("append after catalog recovery");
    assert_eq!(assigned, 3);
}

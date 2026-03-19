use danube_core::message::{MessageID, StreamMessage};
use danube_core::metadata::{MemoryStore, MetadataStore};
use danube_core::storage::{PersistentStorage, StartPosition};
use danube_persistent_storage::checkpoint::{CheckpointStore, WalCheckpoint};
use danube_persistent_storage::wal::WalConfig;
use danube_persistent_storage::{SegmentDescriptor, StorageFactory, StorageFactoryConfig, StorageMetadata};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::time::{sleep, timeout, Duration};
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
    let durable_root_path = durable_root.path().to_string_lossy().to_string();

    let factory_1 = StorageFactory::new(
        StorageFactoryConfig::shared_fs(
            WalConfig {
                dir: Some(wal_owner_1.path().to_path_buf()),
                fsync_interval_ms: Some(5_000),
                ..Default::default()
            },
            "/danube",
            durable_root_path.clone(),
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
            durable_root_path,
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

#[tokio::test]
async fn shared_fs_periodically_exports_segments_during_active_ownership() {
    let wal_root = tempfile::tempdir().expect("wal root");
    let durable_root = tempfile::tempdir().expect("shared durable root");
    let memory_store = Arc::new(MemoryStore::new().await.expect("memory store"));
    let metadata_store: Arc<dyn MetadataStore> = memory_store.clone();

    let topic = "/default/shared-fs-active-export";
    let factory = StorageFactory::new(
        StorageFactoryConfig::shared_fs(
            WalConfig {
                dir: Some(wal_root.path().to_path_buf()),
                fsync_interval_ms: Some(5_000),
                ..Default::default()
            },
            "/danube",
            durable_root.path().to_string_lossy().to_string(),
            None,
        )
        .with_segment_export_interval_seconds(1),
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

    let segment_prefix = "/danube/storage/topics/default/shared-fs-active-export/segments";
    let segments = timeout(Duration::from_secs(5), async {
        loop {
            let segment_children = memory_store
                .get_childrens(segment_prefix)
                .await
                .expect("list shared-fs segment descriptors");
            let segments: Vec<_> = segment_children
                .into_iter()
                .filter(|child| child != "cur" && !child.ends_with('/'))
                .collect();
            if !segments.is_empty() {
                break segments;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("timed out waiting for active shared-fs export");

    assert!(
        !segments.is_empty(),
        "shared_fs should export durable segment descriptors before seal"
    );
}

#[tokio::test]
async fn shared_fs_requires_wal_dir_for_durable_wal_state() {
    let durable_root = tempfile::tempdir().expect("shared durable root");
    let memory_store = Arc::new(MemoryStore::new().await.expect("memory store"));
    let metadata_store: Arc<dyn MetadataStore> = memory_store;

    let factory = StorageFactory::new(
        StorageFactoryConfig::shared_fs(
            WalConfig {
                dir: None,
                ..Default::default()
            },
            "/danube",
            durable_root.path().to_string_lossy().to_string(),
            None,
        ),
        metadata_store,
    );

    let err = factory
        .for_topic("/default/shared-fs-missing-wal-dir")
        .await
        .expect_err("shared_fs should require wal.dir in export-later mode");

    assert!(
        err.to_string().contains("requires wal.dir"),
        "unexpected error: {}",
        err
    );
}

#[tokio::test]
async fn shared_fs_recovers_from_catalog_when_checkpoint_points_to_missing_local_files() {
    let wal_root = tempfile::tempdir().expect("wal root");
    let durable_root = tempfile::tempdir().expect("shared durable root");
    let memory_store = Arc::new(MemoryStore::new().await.expect("memory store"));
    let metadata_store: Arc<dyn MetadataStore> = memory_store.clone();

    let topic = "/default/shared-fs-recovery";
    let topic_path = "default/shared-fs-recovery";

    let topic_dir = wal_root.path().join("default").join("shared-fs-recovery");
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
        StorageFactoryConfig::shared_fs(
            WalConfig {
                dir: Some(wal_root.path().to_path_buf()),
                fsync_interval_ms: Some(5_000),
                ..Default::default()
            },
            "/danube",
            durable_root.path().to_string_lossy().to_string(),
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

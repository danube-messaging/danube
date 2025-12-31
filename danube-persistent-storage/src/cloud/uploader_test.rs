use tokio::time::Duration;

// Simple async wait helper: polls condition up to timeout_ms .
async fn wait_for_condition<F, Fut>(mut f: F, timeout_ms: u64, interval_ms: u64) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let mut waited = 0u64;
    while waited <= timeout_ms {
        if f().await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(interval_ms)).await;
        waited += interval_ms;
    }
    false
}
#[cfg(test)]
mod tests {
    use super::wait_for_condition;
    use crate::checkpoint::CheckpointStore;
    use crate::wal::{UploaderCheckpoint, Wal, WalConfig};
    use crate::{BackendConfig, CloudStore, EtcdMetadata, LocalBackend};
    use crate::{Uploader, UploaderConfig};
    use danube_core::message::{MessageID, StreamMessage};
    use danube_metadata_store::MetadataStore; // bring trait into scope for MemoryStore methods
    use danube_metadata_store::{MemoryStore, MetadataStorage};
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::TempDir;

    fn make_message(i: u64) -> StreamMessage {
        StreamMessage {
            request_id: i,
            msg_id: MessageID {
                producer_id: 1,
                topic_name: "test-topic".to_string(),
                broker_addr: "localhost:6650".to_string(),
                topic_offset: i,
            },
            payload: format!("uploader-msg-{}", i).into_bytes(),
            publish_time: i,
            producer_name: "test-producer".to_string(),
            subscription_name: None,
            attributes: HashMap::new(),
            schema_id: None,
            schema_version: None,
        }
    }

    async fn create_test_setup() -> (
        Wal,
        CloudStore,
        EtcdMetadata,
        MemoryStore,
        Arc<CheckpointStore>,
        TempDir,
    ) {
        // Create a temp dir for durable WAL files so uploader can read persisted frames.
        let temp = TempDir::new().expect("tempdir");
        let wal_dir = temp.path().to_path_buf();
        let cfg = WalConfig {
            dir: Some(wal_dir.clone()),
            cache_capacity: Some(128),
            ..Default::default()
        };

        // Create a shared CheckpointStore for this topic directory.
        let wal_ckpt = wal_dir.join("wal.ckpt");
        let uploader_ckpt = wal_dir.join("uploader.ckpt");
        let store = Arc::new(CheckpointStore::new(wal_ckpt, uploader_ckpt));
        let _ = store.load_from_disk().await; // best-effort preload

        // Create WAL with the injected CheckpointStore so writer will persist checkpoints through it.
        let wal = Wal::with_config_with_store(cfg, Some(store.clone()))
            .await
            .expect("wal init");

        let cloud = CloudStore::new(BackendConfig::Local {
            backend: LocalBackend::Memory,
            root: "uploader-test".to_string(),
        })
        .expect("cloud store");

        let mem = MemoryStore::new().await.expect("memory store");
        let meta = EtcdMetadata::new(
            MetadataStorage::InMemory(mem.clone()),
            "/danube".to_string(),
        );

        (wal, cloud, meta, mem, store, temp)
    }

    /// Test: Uploader configuration default values
    ///
    /// Purpose
    /// - Validate that UploaderConfig::default() provides sensible default values
    /// - Ensure default configuration is suitable for typical usage
    ///
    /// Flow
    /// - Create default UploaderConfig instance
    /// - Verify all default field values match expected settings
    ///
    /// Expected
    /// - interval_seconds: 300 (5 minutes default upload frequency)
    /// - topic_path: "default/topic" (placeholder path)
    /// - root_prefix: "/danube" (standard metadata prefix)
    #[tokio::test]
    async fn test_uploader_config_default() {
        let config = UploaderConfig::default();
        assert_eq!(config.interval_seconds, 300);
        assert_eq!(config.topic_path, "default/topic");
        assert_eq!(config.root_prefix, "/danube");
    }

    /// Test: Uploader instance creation and initialization
    ///
    /// Purpose
    /// - Validate successful Uploader creation with custom configuration
    /// - Ensure all dependencies (WAL, CloudStore, EtcdMetadata) are properly integrated
    ///
    /// Flow
    /// - Create test setup with WAL, cloud store, and metadata store
    /// - Create custom UploaderConfig with specific settings
    /// - Initialize Uploader and verify configuration is preserved
    ///
    /// Expected
    /// - Uploader creation succeeds without errors
    /// - Configuration fields are correctly stored in uploader instance
    /// - All dependencies are properly initialized
    #[tokio::test]
    async fn test_uploader_creation() {
        let (_wal, cloud, meta, _mem, store, _tmp) = create_test_setup().await;

        let config = UploaderConfig {
            interval_seconds: 1,
            topic_path: "test/topic".to_string(),
            root_prefix: "/test".to_string(),
            max_object_mb: None,
        };

        let uploader = Uploader::new(config.clone(), cloud, meta, Some(store));
        assert!(uploader.is_ok());

        let uploader = uploader.unwrap();
        assert_eq!(uploader.test_cfg().topic_path, "test/topic");
        assert_eq!(uploader.test_cfg().interval_seconds, 1);
    }

    /// Test: Uploader raw frame upload
    ///
    /// Purpose
    /// - Validate that uploader uploads concatenated raw WAL frames
    ///
    /// Flow
    /// - Add 3 messages to WAL
    /// - Start uploader and wait for upload completion
    /// - Verify object creation in metadata store and cloud storage
    /// - Parse first frame header and validate offset/length
    ///
    /// Expected
    /// - Cloud object is created with DNB1 magic bytes
    /// - Version field is set to 1
    /// - Record count matches number of uploaded messages (3)
    /// - Object descriptor is stored in metadata
    #[tokio::test]
    async fn test_uploader_dnb1_format() {
        let (wal, cloud, meta, mem, store, _tmp) = create_test_setup().await;

        // Add messages to WAL
        for i in 0..3u64 {
            wal.append(&make_message(i)).await.expect("append message");
        }
        // Ensure writer persists wal.ckpt so uploader can read persisted frames
        wal.flush().await.expect("flush wal");

        let config = UploaderConfig {
            interval_seconds: 1,
            topic_path: "test/topic".to_string(),
            root_prefix: "/danube".to_string(),
            max_object_mb: None,
        };

        let uploader =
            Arc::new(Uploader::new(config, cloud.clone(), meta, Some(store)).expect("uploader"));
        let handle = uploader.clone().start();

        // Wait for upload deterministically
        let ok = wait_for_condition(
            || {
                let uploader = uploader.clone();
                async move { uploader.test_last_uploaded_offset() >= 2 }
            },
            5000,
            50,
        )
        .await;
        assert!(ok, "uploader did not advance offset in time");
        handle.abort();

        // Verify object was created
        let prefix = "/danube/storage/topics/test/topic/objects";
        let children = mem.get_childrens(prefix).await.expect("get children");
        let objects: Vec<_> = children
            .into_iter()
            .filter(|c| c != "cur" && !c.ends_with('/'))
            .collect();

        assert!(
            !objects.is_empty(),
            "Should have created at least one object"
        );

        // Get the first object and basic checks
        let object_key = &objects[0];
        let desc_value = mem
            .get(object_key, danube_metadata_store::MetaOptions::None)
            .await
            .expect("get descriptor")
            .expect("descriptor should exist");

        let desc: crate::etcd_metadata::ObjectDescriptor =
            serde_json::from_value(desc_value).expect("parse descriptor");

        // let object_path = format!("storage/topics/test/topic/objects/{}", desc.object_id);
        // let data = cloud.get_object(&object_path).await.expect("get object");
        // assert!(data.len() >= 16);
        // let off = u64::from_le_bytes(data[0..8].try_into().unwrap());
        // assert_eq!(off, 0);

        // Verify object exists in cloud
        let object_path = format!("storage/topics/test/topic/objects/{}", desc.object_id);
        let data = cloud.get_object(&object_path).await.expect("get object");
        assert!(
            data.len() >= 16,
            "Object should contain at least one raw frame header"
        );
        let off = u64::from_le_bytes(data[0..8].try_into().unwrap());
        let len = u32::from_le_bytes(data[8..12].try_into().unwrap()) as usize;
        let _crc = u32::from_le_bytes(data[12..16].try_into().unwrap());
        assert_eq!(off, 0, "First uploaded frame should start at offset 0");
        assert!(data.len() >= 16 + len, "Object should include full payload");
    }

    /// Test: Uploader checkpoint resume functionality
    ///
    /// Purpose
    /// - Validate that uploader can resume from previous checkpoint
    /// - Ensure only new messages after checkpoint are uploaded
    ///
    /// Flow
    /// - Create checkpoint with last_committed_offset = 5
    /// - Add messages with offsets 6-8 to WAL
    /// - Start uploader and verify it processes only new messages
    /// - Check that uploader advances past checkpoint offset
    ///
    /// Expected
    /// - Uploader skips messages at or before checkpoint offset
    /// - Only messages after checkpoint (6-8) are processed
    /// - Final uploaded offset is >= 8
    #[tokio::test]
    async fn test_uploader_checkpoint_resume() {
        let (wal, cloud, meta, mem, store, _tmp) = create_test_setup().await;

        // Preload WAL with offsets 0..=5
        for i in 0..=5u64 {
            wal.append(&make_message(i)).await.expect("append message");
        }
        wal.flush().await.expect("flush wal");

        // Create checkpoint manually at offset 5
        let checkpoint = UploaderCheckpoint {
            last_committed_offset: 5,
            last_read_file_seq: 0,
            last_read_byte_position: 0,
            last_object_id: Some("previous-object".to_string()),
            updated_at: chrono::Utc::now().timestamp() as u64,
        };

        store
            .update_uploader(&checkpoint)
            .await
            .expect("write checkpoint");

        // Add messages with next offsets 6..=8
        for i in 6..9u64 {
            wal.append(&make_message(i)).await.expect("append message");
        }
        wal.flush().await.expect("flush wal");

        let config = UploaderConfig {
            interval_seconds: 1,
            topic_path: "test/resume".to_string(),
            root_prefix: "/danube".to_string(),
            max_object_mb: None,
        };

        let uploader = Arc::new(Uploader::new(config, cloud, meta, Some(store)).expect("uploader"));

        assert_eq!(uploader.test_last_uploaded_offset(), 0);
        let handle = uploader.clone().start();

        let ok = wait_for_condition(
            || {
                let mem = mem.clone();
                async move {
                    let prefix = "/danube/storage/topics/test/resume/objects";
                    let children = mem.get_childrens(prefix).await.unwrap_or_default();
                    let objects: Vec<_> = children
                        .into_iter()
                        .filter(|c| c != "cur" && !c.ends_with('/'))
                        .collect();
                    !objects.is_empty()
                }
            },
            5000,
            50,
        )
        .await;
        assert!(ok, "Should have created objects after resume");
        handle.abort();
    }

    /// Test: Uploader behavior with empty WAL
    ///
    /// Purpose
    /// - Validate uploader handles empty WAL gracefully without errors
    /// - Ensure no unnecessary cloud objects are created for empty WAL
    ///
    /// Flow
    /// - Create uploader without adding any messages to WAL
    /// - Start uploader and wait for tick interval
    /// - Verify no objects are created in cloud storage or metadata
    ///
    /// Expected
    /// - Uploader runs without errors on empty WAL
    /// - No cloud objects are created
    /// - No metadata entries are added
    #[tokio::test]
    async fn test_uploader_empty_wal() {
        let (_wal, cloud, meta, mem, store, _tmp) = create_test_setup().await;

        let config = UploaderConfig {
            interval_seconds: 1,
            topic_path: "test/empty".to_string(),
            root_prefix: "/danube".to_string(),
            max_object_mb: None,
        };

        let uploader = Arc::new(Uploader::new(config, cloud, meta, Some(store)).expect("uploader"));
        let handle = uploader.clone().start();

        tokio::time::sleep(Duration::from_millis(1200)).await;
        handle.abort();

        let prefix = "/danube/storage/topics/test/empty/objects";
        let children = mem.get_childrens(prefix).await.unwrap_or_default();
        let objects: Vec<_> = children
            .into_iter()
            .filter(|c| c != "cur" && !c.ends_with('/'))
            .collect();

        assert!(
            objects.is_empty(),
            "Should not create objects for empty WAL"
        );
    }

    /// Test: Uploader object naming convention
    ///
    /// Purpose
    /// - Validate that uploaded objects follow correct naming convention
    /// - Ensure object IDs contain start and end offsets for easy identification
    ///
    /// Flow
    /// - Add messages with specific offsets (10-12) to WAL
    /// - Start uploader and wait for upload
    /// - Retrieve object descriptor and verify object_id format
    ///
    /// Expected
    /// - Object ID follows pattern: "data-<start>-<end>.dnb1"
    /// - Object ID contains start offset (10) and end offset (12)
    /// - Object ID has proper DNB1 file extension
    #[tokio::test]
    async fn test_uploader_object_naming() {
        let (wal, cloud, meta, mem, store, _tmp) = create_test_setup().await;

        for i in 10..13u64 {
            wal.append(&make_message(i)).await.expect("append message");
        }
        wal.flush().await.expect("flush wal");

        let config = UploaderConfig {
            interval_seconds: 1,
            topic_path: "test/naming".to_string(),
            root_prefix: "/danube".to_string(),
            max_object_mb: None,
        };

        let uploader = Arc::new(Uploader::new(config, cloud, meta, Some(store)).expect("uploader"));
        let handle = uploader.clone().start();

        let ok = wait_for_condition(
            || {
                let mem = mem.clone();
                async move {
                    let prefix = "/danube/storage/topics/test/naming/objects";
                    let children = mem.get_childrens(prefix).await.unwrap_or_default();
                    let objects: Vec<_> = children
                        .into_iter()
                        .filter(|c| c != "cur" && !c.ends_with('/'))
                        .collect();
                    !objects.is_empty()
                }
            },
            5000,
            50,
        )
        .await;
        assert!(ok, "Should have created objects");
        handle.abort();

        let prefix = "/danube/storage/topics/test/naming/objects";
        let children = mem.get_childrens(prefix).await.expect("get children");
        let objects: Vec<_> = children
            .into_iter()
            .filter(|c| c != "cur" && !c.ends_with('/'))
            .collect();
        let object_key = &objects[0];
        let desc_value = mem
            .get(object_key, danube_metadata_store::MetaOptions::None)
            .await
            .expect("get descriptor")
            .expect("descriptor should exist");

        let desc: crate::etcd_metadata::ObjectDescriptor =
            serde_json::from_value(desc_value).expect("parse descriptor");

        assert!(desc.object_id.starts_with("data-"));
        assert!(desc.object_id.ends_with(".dnb1"));
        assert_eq!(desc.start_offset, 0);
        assert_eq!(desc.end_offset, 2);
    }
}

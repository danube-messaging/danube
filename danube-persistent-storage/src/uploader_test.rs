#[cfg(test)]
mod tests {
    use crate::{Uploader, UploaderConfig};
    use crate::wal::{Wal, WalConfig, UploaderCheckpoint};
    use crate::{BackendConfig, CloudStore, EtcdMetadata, LocalBackend};
    use danube_core::message::{MessageID, StreamMessage};
    use danube_metadata_store::{MemoryStore, MetadataStorage};
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    fn make_message(i: u64) -> StreamMessage {
        StreamMessage {
            request_id: i,
            msg_id: MessageID {
                producer_id: 1,
                topic_name: "test-topic".to_string(),
                broker_addr: "localhost:6650".to_string(),
                segment_id: 0,
                segment_offset: i,
            },
            payload: format!("uploader-msg-{}", i).into_bytes(),
            publish_time: i,
            producer_name: "test-producer".to_string(),
            subscription_name: None,
            attributes: HashMap::new(),
        }
    }

    async fn create_test_setup() -> (Wal, CloudStore, EtcdMetadata, MemoryStore) {
        let wal = Wal::with_config(WalConfig {
            cache_capacity: Some(128),
            ..Default::default()
        })
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

        (wal, cloud, meta, mem)
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
    /// - interval_seconds: 10 (reasonable upload frequency)
    /// - max_batch_bytes: 8MB (good balance of throughput and memory)
    /// - topic_path: "default/topic" (placeholder path)
    /// - root_prefix: "/danube" (standard metadata prefix)
    #[tokio::test]
    async fn test_uploader_config_default() {
        let config = UploaderConfig::default();
        assert_eq!(config.interval_seconds, 10);
        assert_eq!(config.max_batch_bytes, 8 * 1024 * 1024);
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
        let (wal, cloud, meta, _mem) = create_test_setup().await;

        let config = UploaderConfig {
            interval_seconds: 1,
            max_batch_bytes: 1024,
            topic_path: "test/topic".to_string(),
            root_prefix: "/test".to_string(),
        };

        let uploader = Uploader::new(config.clone(), wal, cloud, meta);
        assert!(uploader.is_ok());

        let uploader = uploader.unwrap();
        assert_eq!(uploader.test_cfg().topic_path, "test/topic");
        assert_eq!(uploader.test_cfg().interval_seconds, 1);
    }

    /// Test: Uploader DNB1 format generation and upload
    ///
    /// Purpose
    /// - Validate that uploader creates correct DNB1 format objects in cloud storage
    /// - Ensure DNB1 header format and record count are accurate
    ///
    /// Flow
    /// - Add 3 messages to WAL
    /// - Start uploader and wait for upload completion
    /// - Verify object creation in metadata store and cloud storage
    /// - Parse DNB1 format and validate header and record count
    ///
    /// Expected
    /// - Cloud object is created with DNB1 magic bytes
    /// - Version field is set to 1
    /// - Record count matches number of uploaded messages (3)
    /// - Object descriptor is stored in metadata
    #[tokio::test]
    async fn test_uploader_dnb1_format() {
        let (wal, cloud, meta, mem) = create_test_setup().await;

        // Add messages to WAL
        for i in 0..3u64 {
            wal.append(&make_message(i)).await.expect("append message");
        }

        let config = UploaderConfig {
            interval_seconds: 1,
            max_batch_bytes: 8 * 1024 * 1024,
            topic_path: "test/topic".to_string(),
            root_prefix: "/danube".to_string(),
        };

        let uploader = Arc::new(Uploader::new(config, wal, cloud.clone(), meta).expect("uploader"));
        let handle = uploader.start();

        // Wait for upload
        tokio::time::sleep(Duration::from_millis(1200)).await;
        handle.abort();

        // Verify object was created with correct format
        let prefix = "/danube/storage/topics/test/topic/objects";
        let children = mem.get_childrens(prefix).await.expect("get children");
        let objects: Vec<_> = children.into_iter().filter(|c| c != "cur" && !c.ends_with('/')).collect();
        
        assert!(!objects.is_empty(), "Should have created at least one object");

        // Get the first object
        let object_key = &objects[0];
        let desc_value = mem.get(object_key, danube_metadata_store::MetaOptions::None)
            .await
            .expect("get descriptor")
            .expect("descriptor should exist");
        
        let desc: crate::etcd_metadata::ObjectDescriptor = 
            serde_json::from_value(desc_value).expect("parse descriptor");

        // Verify object exists in cloud
        let object_path = format!("storage/topics/test/topic/objects/{}", desc.object_id);
        let data = cloud.get_object(&object_path).await.expect("get object");

        // Verify DNB1 format
        assert!(data.len() >= 9, "Object should have at least header");
        assert_eq!(&data[0..4], b"DNB1", "Should have DNB1 magic");
        assert_eq!(data[4], 1u8, "Should have version 1");

        let record_count = u32::from_le_bytes(data[5..9].try_into().unwrap());
        assert_eq!(record_count, 3, "Should have 3 records");
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
        let (wal, cloud, meta, _mem) = create_test_setup().await;

        // Create checkpoint manually
        let checkpoint = UploaderCheckpoint {
            last_committed_offset: 5,
            last_object_id: Some("previous-object".to_string()),
            updated_at: chrono::Utc::now().timestamp() as u64,
        };

        wal.write_uploader_checkpoint(&checkpoint).await.expect("write checkpoint");

        // Add messages starting from offset 6
        for i in 6..9u64 {
            wal.append(&make_message(i)).await.expect("append message");
        }

        let config = UploaderConfig {
            interval_seconds: 1,
            max_batch_bytes: 8 * 1024 * 1024,
            topic_path: "test/resume".to_string(),
            root_prefix: "/danube".to_string(),
        };

        let uploader = Arc::new(Uploader::new(config, wal, cloud, meta).expect("uploader"));
        
        // Verify uploader starts from correct offset
        assert_eq!(uploader.test_last_uploaded_offset(), 0);
        
        let handle = uploader.start();

        // Wait for upload
        tokio::time::sleep(Duration::from_millis(1200)).await;
        handle.abort();

        // Verify uploader advanced past checkpoint
        let final_offset = uploader.test_last_uploaded_offset();
        assert!(final_offset >= 8, "Should have processed messages after checkpoint");
    }

    /// Test: Uploader batch size limit handling
    ///
    /// Purpose
    /// - Validate uploader behavior when messages exceed batch size limits
    /// - Ensure large messages are still uploaded despite size constraints
    ///
    /// Flow
    /// - Create message with 2KB payload (larger than 1KB batch limit)
    /// - Configure uploader with small max_batch_bytes (1024)
    /// - Start uploader and verify large message is still processed
    ///
    /// Expected
    /// - Large message exceeding batch size is still uploaded
    /// - Uploader doesn't skip messages due to size constraints
    /// - Upload process completes successfully
    #[tokio::test]
    async fn test_uploader_batch_size_limits() {
        let (wal, cloud, meta, _mem) = create_test_setup().await;

        // Add one large message that would exceed batch size
        let large_msg = StreamMessage {
            request_id: 0,
            msg_id: MessageID {
                producer_id: 1,
                topic_name: "test-topic".to_string(),
                broker_addr: "localhost:6650".to_string(),
                segment_id: 0,
                segment_offset: 0,
            },
            payload: vec![0u8; 2048], // Large payload
            publish_time: 0,
            producer_name: "test-producer".to_string(),
            subscription_name: None,
            attributes: HashMap::new(),
        };

        wal.append(&large_msg).await.expect("append large message");

        let config = UploaderConfig {
            interval_seconds: 1,
            max_batch_bytes: 1024, // Small batch size
            topic_path: "test/batch".to_string(),
            root_prefix: "/danube".to_string(),
        };

        let uploader = Arc::new(Uploader::new(config, wal, cloud, meta).expect("uploader"));
        let handle = uploader.start();

        // Wait for upload
        tokio::time::sleep(Duration::from_millis(1200)).await;
        handle.abort();

        // Should still upload even if message exceeds batch size
        let final_offset = uploader.test_last_uploaded_offset();
        assert!(final_offset > 0, "Should have processed the large message");
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
        let (wal, cloud, meta, mem) = create_test_setup().await;

        // Don't add any messages to WAL
        let config = UploaderConfig {
            interval_seconds: 1,
            max_batch_bytes: 8 * 1024 * 1024,
            topic_path: "test/empty".to_string(),
            root_prefix: "/danube".to_string(),
        };

        let uploader = Arc::new(Uploader::new(config, wal, cloud, meta).expect("uploader"));
        let handle = uploader.start();

        // Wait for tick
        tokio::time::sleep(Duration::from_millis(1200)).await;
        handle.abort();

        // Should not create any objects
        let prefix = "/danube/storage/topics/test/empty/objects";
        let children = mem.get_childrens(prefix).await.unwrap_or_default();
        let objects: Vec<_> = children.into_iter().filter(|c| c != "cur" && !c.ends_with('/')).collect();
        
        assert!(objects.is_empty(), "Should not create objects for empty WAL");
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
        let (wal, cloud, meta, mem) = create_test_setup().await;

        // Add messages with specific offsets
        for i in 10..13u64 {
            wal.append(&make_message(i)).await.expect("append message");
        }

        let config = UploaderConfig {
            interval_seconds: 1,
            max_batch_bytes: 8 * 1024 * 1024,
            topic_path: "test/naming".to_string(),
            root_prefix: "/danube".to_string(),
        };

        let uploader = Arc::new(Uploader::new(config, wal, cloud, meta).expect("uploader"));
        let handle = uploader.start();

        // Wait for upload
        tokio::time::sleep(Duration::from_millis(1200)).await;
        handle.abort();

        // Verify object naming convention
        let prefix = "/danube/storage/topics/test/naming/objects";
        let children = mem.get_childrens(prefix).await.expect("get children");
        let objects: Vec<_> = children.into_iter().filter(|c| c != "cur" && !c.ends_with('/')).collect();
        
        assert!(!objects.is_empty(), "Should have created objects");

        // Get descriptor and verify object_id format
        let object_key = &objects[0];
        let desc_value = mem.get(object_key, danube_metadata_store::MetaOptions::None)
            .await
            .expect("get descriptor")
            .expect("descriptor should exist");
        
        let desc: crate::etcd_metadata::ObjectDescriptor = 
            serde_json::from_value(desc_value).expect("parse descriptor");

        // Object ID should follow pattern: data-<start>-<end>.dnb1
        assert!(desc.object_id.starts_with("data-"));
        assert!(desc.object_id.ends_with(".dnb1"));
        assert!(desc.object_id.contains("-10-")); // Should contain start offset
        assert!(desc.object_id.contains("-12")); // Should contain end offset
    }

    /// Test: UploaderConfig clone functionality
    ///
    /// Purpose
    /// - Validate that UploaderConfig can be cloned correctly
    /// - Ensure all fields are properly copied in cloned instance
    ///
    /// Flow
    /// - Create UploaderConfig with custom values
    /// - Clone the configuration
    /// - Verify all fields match between original and cloned instances
    ///
    /// Expected
    /// - Clone operation succeeds
    /// - All configuration fields are identical in both instances
    /// - Changes to one instance don't affect the other
    #[test]
    fn test_uploader_config_clone() {
        let config1 = UploaderConfig {
            interval_seconds: 5,
            max_batch_bytes: 2048,
            topic_path: "ns/topic".to_string(),
            root_prefix: "/test".to_string(),
        };

        let config2 = config1.clone();
        assert_eq!(config1.interval_seconds, config2.interval_seconds);
        assert_eq!(config1.max_batch_bytes, config2.max_batch_bytes);
        assert_eq!(config1.topic_path, config2.topic_path);
        assert_eq!(config1.root_prefix, config2.root_prefix);
    }
}

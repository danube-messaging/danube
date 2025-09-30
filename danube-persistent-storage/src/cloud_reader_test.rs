#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use crate::checkpoint::CheckpointStore;
    use crate::wal::{Wal, WalConfig};
    use crate::{
        BackendConfig, CloudReader, CloudStore, EtcdMetadata, LocalBackend, Uploader,
        UploaderConfig,
    };
    use danube_core::message::{MessageID, StreamMessage};
    use danube_metadata_store::{MemoryStore, MetadataStorage, MetadataStore};
    use futures::TryStreamExt;
    use tempfile::TempDir;

    fn make_msg(i: u64, topic: &str) -> StreamMessage {
        StreamMessage {
            request_id: i,
            msg_id: MessageID {
                producer_id: 1,
                topic_name: topic.to_string(),
                broker_addr: "127.0.0.1:8080".to_string(),
                segment_id: 0,
                segment_offset: i,
            },
            payload: format!("cloud-{}", i).into_bytes(),
            publish_time: 0,
            producer_name: "producer-cloud".to_string(),
            subscription_name: None,
            attributes: HashMap::new(),
        }
    }

    /// Test: CloudReader range reads from memory backend
    ///
    /// Purpose
    /// - Validate CloudReader can read historical messages from cloud objects (memory backend)
    /// - Ensure DNB1 object format parsing works correctly for message reconstruction
    /// - Verify range filtering and message ordering from cloud storage
    ///
    /// Flow
    /// - Create WAL and append 3 messages (offsets 0-2)
    /// - Set up uploader to write messages to cloud storage in DNB1 format
    /// - Use CloudReader to read range [0, 2] and verify message content
    ///
    /// Expected
    /// - All messages are uploaded to cloud storage successfully
    /// - CloudReader can parse DNB1 format and reconstruct original messages
    /// - Messages are returned in correct offset order with proper content
    #[tokio::test]
    #[ignore]
    async fn test_cloud_reader_range_reads_memory() {
        let topic_path = "ns/topic-cloud";

        // WAL durable config: use temp dir + shared CheckpointStore for persisted reads
        let tmp = TempDir::new().expect("temp wal dir");
        let wal_dir = tmp.path().to_path_buf();
        let cfg = WalConfig {
            dir: Some(wal_dir.clone()),
            cache_capacity: Some(128),
            ..Default::default()
        };
        let wal_ckpt = wal_dir.join("wal.ckpt");
        let uploader_ckpt = wal_dir.join("uploader.ckpt");
        let store = std::sync::Arc::new(CheckpointStore::new(wal_ckpt, uploader_ckpt));
        let _ = store.load_from_disk().await;
        let wal = Wal::with_config_with_store(cfg, Some(store.clone()))
            .await
            .expect("wal init");

        for i in 0..3u64 {
            let m = make_msg(i, topic_path);
            wal.append(&m).await.expect("append");
        }
        // Ensure checkpoint is written so uploader can read persisted frames
        wal.flush().await.expect("flush wal");

        // CloudStore memory
        let cloud = CloudStore::new(BackendConfig::Local {
            backend: LocalBackend::Memory,
            root: "mem-cloud".to_string(),
        })
        .expect("cloud store mem");

        // Metadata store
        let mem = MemoryStore::new().await.expect("memory meta store");
        let meta = EtcdMetadata::new(
            MetadataStorage::InMemory(mem.clone()),
            "/danube".to_string(),
        );

        // Uploader
        let up_cfg = UploaderConfig {
            interval_seconds: 1,
            max_batch_bytes: 8 * 1024 * 1024,
            topic_path: topic_path.to_string(),
            root_prefix: "/danube".to_string(),
        };
        let uploader = Arc::new(
            Uploader::new(up_cfg, cloud.clone(), meta.clone(), Some(store)).expect("uploader"),
        );
        let handle = uploader.clone().start();

        // Wait for an object to appear deterministically
        async fn wait_for_objects(
            mem: danube_metadata_store::MemoryStore,
            prefix: &str,
            timeout_ms: u64,
            interval_ms: u64,
        ) -> bool {
            let mut waited = 0u64;
            while waited <= timeout_ms {
                let children = mem.get_childrens(prefix).await.unwrap_or_default();
                let objects: Vec<_> = children
                    .into_iter()
                    .filter(|c| c != "cur" && !c.ends_with('/'))
                    .collect();
                if !objects.is_empty() {
                    return true;
                }
                tokio::time::sleep(Duration::from_millis(interval_ms)).await;
                waited += interval_ms;
            }
            false
        }
        let ok = wait_for_objects(
            mem.clone(),
            "/danube/storage/topics/ns/topic-cloud/objects",
            5000,
            50,
        )
        .await;
        assert!(ok, "uploader did not create objects in time");
        handle.abort();

        // CloudReader
        let reader = CloudReader::new(cloud.clone(), meta.clone(), topic_path.to_string());
        let stream = reader.read_range(0, Some(2)).await.expect("cloud read");
        let msgs: Vec<StreamMessage> = stream.try_collect::<Vec<_>>().await.expect("try_collect");

        assert_eq!(msgs.len(), 3, "should read 3 messages from cloud objects");
        for (i, m) in msgs.into_iter().enumerate() {
            assert_eq!(m.payload, format!("cloud-{}", i).into_bytes());
        }
    }
}

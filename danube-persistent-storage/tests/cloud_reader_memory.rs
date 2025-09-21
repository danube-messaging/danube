//! Test: cloud_reader_range_reads_memory
//!
//! Purpose
//! - Validate CloudReader can read historical messages from cloud objects (opendal `memory`).
//! - Ensures DNB1 object framing is parsed into full `StreamMessage`s with correct ordering.
//!
//! Flow
//! - Append 3 messages into WAL (offsets 0..2), run one uploader tick to write a DNB1 object.
//! - Use CloudReader to read range [0, 2] and verify payloads and count.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use danube_core::message::{MessageID, StreamMessage};
use danube_metadata_store::{MemoryStore, MetadataStorage};
use danube_persistent_storage::wal::{Wal, WalConfig};
use danube_persistent_storage::{
    BackendConfig, CloudReader, CloudStore, EtcdMetadata, LocalBackend, Uploader, UploaderConfig,
};
use futures::TryStreamExt;

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

#[tokio::test]
async fn cloud_reader_range_reads_memory() {
    let topic_path = "tenant/ns/topic-cloud";

    // WAL minimal config
    let wal = Wal::with_config(WalConfig {
        cache_capacity: Some(128),
        ..Default::default()
    })
    .await
    .expect("wal init");

    for i in 0..3u64 {
        let m = make_msg(i, topic_path);
        wal.append(&m).await.expect("append");
    }

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
        Uploader::new(up_cfg, wal.clone(), cloud.clone(), meta.clone()).expect("uploader"),
    );
    let handle = uploader.clone().start();

    // Wait for upload
    tokio::time::sleep(Duration::from_millis(1200)).await;
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

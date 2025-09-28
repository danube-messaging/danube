//! Test: chaining_stream_handoff_memory
//!
//! Purpose
//! - Validate that a reader started behind WAL retention window (simulated via uploaded range)
//!   will first consume from cloud objects and then seamlessly handoff to WAL tail without
//!   gaps or duplicates.
//!
//! Flow
//! - Append 3 messages (0..2) into WAL, run uploader to persist a DNB1 object [0..2].
//! - Append 3 more messages (3..5) into WAL (not uploaded).
//! - Construct WalStorage.with_cloud(...) and create a reader from offset 0.
//! - Verify received messages are exactly 0..5 in order.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

use danube_core::message::{MessageID, StreamMessage};
use danube_core::storage::{PersistentStorage, StartPosition};
use danube_metadata_store::{MemoryStore, MetadataStorage, MetadataStore};
use danube_persistent_storage::wal::{Wal, WalConfig};
use danube_persistent_storage::{
    BackendConfig, CloudStore, EtcdMetadata, LocalBackend, Uploader, UploaderConfig, WalStorage,
};
use futures::{StreamExt, TryStreamExt};

fn make_msg(i: u64, topic: &str, tag: &str) -> StreamMessage {
    StreamMessage {
        request_id: i,
        msg_id: MessageID {
            producer_id: 1,
            topic_name: topic.to_string(),
            broker_addr: "127.0.0.1:8080".to_string(),
            segment_id: 0,
            segment_offset: i,
        },
        payload: format!("{}-{}", tag, i).into_bytes(),
        publish_time: 0,
        producer_name: "producer".to_string(),
        subscription_name: None,
        attributes: HashMap::new(),
    }
}

#[tokio::test]
async fn chaining_stream_handoff_memory() {
    let topic_path = "ns/topic-handoff";

    // WAL minimal config
    let wal = Wal::with_config(WalConfig {
        cache_capacity: Some(128),
        ..Default::default()
    })
    .await
    .expect("wal init");

    // Append 0..2 and upload
    for i in 0..3u64 {
        let m = make_msg(i, topic_path, "cloud");
        wal.append(&m).await.expect("append pre-upload");
    }

    let cloud = CloudStore::new(BackendConfig::Local {
        backend: LocalBackend::Memory,
        root: "mem-handoff".to_string(),
    })
    .expect("cloud store mem");

    let mem = MemoryStore::new().await.expect("memory meta store");
    let meta = EtcdMetadata::new(
        MetadataStorage::InMemory(mem.clone()),
        "/danube".to_string(),
    );

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
    tokio::time::sleep(Duration::from_millis(1200)).await;
    handle.abort();

    // Assert that at least one descriptor exists to avoid racing with the uploader
    // Use prefix without trailing slash to match MemoryStore::get_childrens filtering logic
    let prefix = format!("/danube/storage/topics/{}/objects", topic_path);
    let mut found = false;
    for _ in 0..25 {
        // up to ~2.5s at 100ms per attempt
        let desc_keys = mem
            .get_childrens(&prefix)
            .await
            .expect("list descriptor keys");
        if desc_keys.iter().any(|k| k.starts_with(&prefix)) {
            found = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(found, "no object descriptors found under {}", prefix);

    // Append 3..5 after upload (these exist only in WAL)
    for i in 3..6u64 {
        let m = make_msg(i, topic_path, "wal");
        wal.append(&m).await.expect("append post-upload");
    }

    // Build WalStorage with cloud capabilities and read from offset 0
    let storage = WalStorage::from_wal(wal.clone()).with_cloud(
        cloud.clone(),
        meta.clone(),
        topic_path.to_string(),
    );
    let stream = storage
        .create_reader(topic_path, StartPosition::Offset(0))
        .await
        .expect("create reader");

    // Limit to the first 6 messages to avoid hanging on the infinite WAL tail stream
    let fut = stream.take(6).try_collect::<Vec<_>>();
    let msgs: Vec<StreamMessage> = timeout(Duration::from_secs(5), fut)
        .await
        .expect("stream timed out waiting for 6 messages")
        .expect("try_collect");

    // Expect exactly 6 messages (0..5) in order
    assert_eq!(msgs.len(), 6, "should receive 6 messages across cloud+wal");
    for (i, m) in msgs.into_iter().enumerate() {
        let payload_str = String::from_utf8_lossy(&m.payload);
        // First three should be produced by CloudReader, last three by WAL tail.
        if i < 3 {
            assert!(
                payload_str.starts_with("cloud-"),
                "expected cloud payload at {}, got {}",
                i,
                payload_str
            );
        } else {
            assert!(
                payload_str.starts_with("wal-"),
                "expected wal payload at {}, got {}",
                i,
                payload_str
            );
        }
    }
}

// Example: WAL wiring with ReliableDispatch
// Purpose: Demonstrates how to configure a durable WAL via WalConfig, wrap it in WalStorage,
// and construct ReliableDispatch::new_with_persistent() so producers/consumers use the WAL-backed
// hot path.
// What it shows:
// - Creating a WAL with file-backed durability (batched fsync, CRC frames)
// - Wiring WAL into ReliableDispatch alongside a TopicCache built from StorageConfig::InMemory
// - Creating a stream at Latest and verifying that only messages appended after subscription arrive
// Expected behavior:
// - After create_stream_latest(), messages appended are delivered in order over the WAL-backed path
// - You can swap Latest for an offset-based reader in production code as needed

use danube_core::dispatch_strategy::{ReliableOptions, RetentionPolicy};
use danube_core::message::{MessageID, StreamMessage};
use danube_core::storage::{CacheConfig, StorageConfig};
use danube_persistent_storage::wal::{Wal, WalConfig};
use danube_persistent_storage::WalStorage;
use danube_reliable_dispatch::create_message_storage;
use danube_reliable_dispatch::{ReliableDispatch, TopicCache};
use tokio_stream::StreamExt;

fn make_msg(
    topic: &str,
    producer_id: u64,
    segment_id: u64,
    segment_offset: u64,
    payload: &[u8],
) -> StreamMessage {
    StreamMessage {
        request_id: producer_id,
        msg_id: MessageID {
            producer_id,
            topic_name: topic.to_string(),
            broker_addr: "127.0.0.1:6650".to_string(),
            segment_id,
            segment_offset,
        },
        payload: payload.to_vec(),
        publish_time: 0,
        producer_name: "example-producer".to_string(),
        subscription_name: None,
        attributes: Default::default(),
    }
}

#[tokio::main]
async fn main() {
    // Configure a WAL with durability knobs and optional file backing
    let tmp = tempfile::tempdir().expect("tempdir");
    let wal = Wal::with_config(WalConfig {
        dir: Some(tmp.path().to_path_buf()),
        file_name: Some("wal.log".to_string()),
        cache_capacity: Some(1024),
        fsync_interval_ms: Some(5),
        max_batch_bytes: Some(8 * 1024),
        ..Default::default()
    })
    .await
    .expect("create wal");

    let wal_storage = WalStorage::from_wal(wal);

    // ReliableDispatch wiring with pre-configured WAL
    let topic_name = "/default/wal-example";
    let reliable_options = ReliableOptions::new(1, RetentionPolicy::RetainUntilAck, 60);

    // Build a TopicCache using the public factory and in-memory storage config
    let storage_cfg = StorageConfig::InMemory {
        cache: CacheConfig {
            max_capacity: 100,
            time_to_idle: 10,
        },
    };
    let topic_cache: TopicCache = create_message_storage(&storage_cfg).await;

    let dispatch = ReliableDispatch::new_with_persistent(
        topic_name,
        reliable_options,
        topic_cache,
        wal_storage,
    );

    // Create a stream starting at Latest (will yield messages appended after this point)
    let mut stream = dispatch
        .create_stream_latest()
        .await
        .expect("create stream latest");

    // Append a few messages
    let m1 = make_msg(topic_name, 1, 0, 0, b"hello");
    let m2 = make_msg(topic_name, 1, 0, 1, b"world");
    dispatch.store_message(m1.clone()).await.expect("store m1");
    dispatch.store_message(m2.clone()).await.expect("store m2");

    // Read them back from the stream
    if let Some(Ok(msg)) = stream.next().await {
        println!("got payload: {}", String::from_utf8_lossy(&msg.payload));
    }
    if let Some(Ok(msg)) = stream.next().await {
        println!("got payload: {}", String::from_utf8_lossy(&msg.payload));
    }

    // If you need replay from a specific offset, use the TopicStore API via dispatch-provided methods when available.
}

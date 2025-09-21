#[cfg(test)]
use danube_core::{
    dispatch_strategy::{ReliableOptions, RetentionPolicy},
    message::{MessageID, StreamMessage},
};
#[cfg(test)]
use std::collections::HashMap;
#[cfg(test)]
use std::time::{SystemTime, UNIX_EPOCH};
#[cfg(test)]
use tokio_stream::StreamExt;

#[cfg(test)]
use crate::ReliableDispatch;
#[cfg(test)]
use danube_core::storage::StartPosition;

#[cfg(test)]
use danube_persistent_storage::{
    wal::{Wal, WalConfig},
    WalStorage,
};

// Legacy segment-based tests removed as Phase D eliminates StorageBackend and Segment paths.
// Below are WAL-first tests.

/// E2E (WAL): Verify replay-from-offset and tailing
/// Why: Consumers may start from an older offset; the system should replay historical entries
/// (from WAL cache/file) and then seamlessly continue with live messages without duplicates.
/// Expected: Starting from offset 3 returns payloads [3], [4], then subsequent appends appear.
#[tokio::test]
async fn test_wal_replay_from_offset() {
    let topic_name = "/default/wal-replay";
    let reliable_options = ReliableOptions::new(1, RetentionPolicy::RetainUntilAck, 60);

    // Use an explicit WAL for deterministic behavior
    let tmp = tempfile::tempdir().unwrap();
    let wal = Wal::with_config(WalConfig {
        dir: Some(tmp.path().to_path_buf()),
        file_name: Some("wal.log".to_string()),
        cache_capacity: Some(1024),
        fsync_interval_ms: Some(1),
        max_batch_bytes: Some(1),
        ..Default::default()
    })
    .await
    .expect("create wal");
    let wal_storage = WalStorage::from_wal(wal);
    let dispatch = ReliableDispatch::new_with_persistent(topic_name, reliable_options, wal_storage);

    // Append 5 messages before creating reader
    for i in 0..5u8 {
        let msg = create_test_message(topic_name, 0, i as u64, vec![i]);
        dispatch.store_message(msg).await.unwrap();
    }

    // Create reader from offset 3 => expect two cached messages [3], [4]
    let mut stream = dispatch
        .topic_store
        .create_reader(StartPosition::Offset(3))
        .await
        .expect("stream created");

    let r1 = stream.next().await.expect("first cached").expect("ok");
    let r2 = stream.next().await.expect("second cached").expect("ok");
    assert_eq!(r1.payload, vec![3]);
    assert_eq!(r2.payload, vec![4]);

    // Then append a live message and verify it appears next without duplication
    let live = create_test_message(topic_name, 0, 5, b"live".to_vec());
    dispatch.store_message(live.clone()).await.unwrap();
    let r3 = stream.next().await.expect("live item").expect("ok");
    assert_eq!(r3.payload, b"live");
}

/// E2E (WAL): Verify tailing from Latest
/// Why: A consumer starting at Latest should only see messages appended after the subscription
/// starts, with stable latency, backed by the WAL broadcast path.
/// Expected: Messages appended after `create_stream_latest()` are delivered in order.
#[tokio::test]
async fn test_wal_stream_latest() {
    let topic_name = "/default/wal-latest";
    let reliable_options = ReliableOptions::new(1, RetentionPolicy::RetainUntilAck, 60);

    // Use an explicit WAL for deterministic behavior
    let tmp = tempfile::tempdir().unwrap();
    let wal = Wal::with_config(WalConfig {
        dir: Some(tmp.path().to_path_buf()),
        file_name: Some("wal.log".to_string()),
        cache_capacity: Some(1024),
        fsync_interval_ms: Some(1),
        max_batch_bytes: Some(1),
        ..Default::default()
    })
    .await
    .expect("create wal");
    let wal_storage = WalStorage::from_wal(wal);
    let dispatch = ReliableDispatch::new_with_persistent(topic_name, reliable_options, wal_storage);

    // Start stream at Latest
    let mut stream = dispatch
        .create_stream_latest()
        .await
        .expect("stream should be created");

    // Append messages after stream subscription
    let m1 = create_test_message(topic_name, 0, 0, b"m1".to_vec());
    let m2 = create_test_message(topic_name, 0, 1, b"m2".to_vec());
    dispatch.store_message(m1.clone()).await.unwrap();
    dispatch.store_message(m2.clone()).await.unwrap();

    // Collect 2 messages
    let r1 = stream.next().await.expect("first item").expect("ok");
    let r2 = stream.next().await.expect("second item").expect("ok");
    assert_eq!(r1.payload, b"m1");
    assert_eq!(r2.payload, b"m2");
}

#[cfg(test)]
fn create_test_message(
    topic_name: &str,
    segment_id: u64,
    segment_offset: u64,
    payload: Vec<u8>,
) -> StreamMessage {
    StreamMessage {
        request_id: 1,
        msg_id: MessageID {
            producer_id: 1,
            topic_name: topic_name.to_string(),
            broker_addr: "localhost:6650".to_string(),
            segment_id,
            segment_offset,
        },
        payload,
        publish_time: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs(),
        producer_name: "test-producer".to_string(),
        subscription_name: Some("test-subscription".to_string()),
        attributes: HashMap::new(),
    }
}

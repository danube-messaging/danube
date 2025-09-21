#[cfg(test)]
use danube_core::{
    dispatch_strategy::{ReliableOptions, RetentionPolicy},
    message::{MessageID, StreamMessage},
    storage::StartPosition,
};
#[cfg(test)]
use std::collections::HashMap;
#[cfg(test)]
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(test)]
use crate::topic_storage::TopicStore;
#[cfg(test)]
use danube_persistent_storage::wal::{Wal, WalConfig};
#[cfg(test)]
use danube_persistent_storage::WalStorage;
#[cfg(test)]
use tokio_stream::StreamExt;

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

/// WAL-only: store and read messages from a TopicStore wired to WalStorage
#[tokio::test]
async fn test_topic_store_wal_store_and_read() {
    let topic_name = "/default/test_topic_wal";
    let reliable_options = ReliableOptions::new(1, RetentionPolicy::RetainUntilAck, 60);

    let tmp = tempfile::tempdir().unwrap();
    let wal = Wal::with_config(WalConfig {
        dir: Some(tmp.path().to_path_buf()),
        file_name: Some("wal.log".to_string()),
        cache_capacity: Some(256),
        ..Default::default()
    })
    .await
    .expect("create wal");
    let wal_storage = WalStorage::from_wal(wal);

    let topic_store = TopicStore::new(topic_name, reliable_options, wal_storage.clone());

    // Store messages
    topic_store
        .store_message(create_test_message(topic_name, 0, 0, b"a".to_vec()))
        .await
        .unwrap();
    topic_store
        .store_message(create_test_message(topic_name, 0, 1, b"b".to_vec()))
        .await
        .unwrap();
    topic_store
        .store_message(create_test_message(topic_name, 0, 2, b"c".to_vec()))
        .await
        .unwrap();

    // Read from offset 1
    let mut stream = topic_store
        .create_reader(StartPosition::Offset(1))
        .await
        .expect("reader");

    let m1 = stream.next().await.expect("msg1").expect("ok");
    let m2 = stream.next().await.expect("msg2").expect("ok");
    assert_eq!(m1.payload, b"b");
    assert_eq!(m2.payload, b"c");
}

/// WAL-only: tail from Latest and receive only post-subscription messages
#[tokio::test]
async fn test_topic_store_wal_latest_tailing() {
    let topic_name = "/default/test_topic_latest";
    let reliable_options = ReliableOptions::new(1, RetentionPolicy::RetainUntilAck, 60);

    let tmp = tempfile::tempdir().unwrap();
    let wal = Wal::with_config(WalConfig {
        dir: Some(tmp.path().to_path_buf()),
        file_name: Some("wal.log".to_string()),
        cache_capacity: Some(256),
        fsync_interval_ms: Some(1),
        ..Default::default()
    })
    .await
    .expect("create wal");
    let wal_storage = WalStorage::from_wal(wal);

    let topic_store = TopicStore::new(topic_name, reliable_options, wal_storage.clone());

    // Start reader at Latest
    let mut stream = topic_store
        .create_reader(StartPosition::Latest)
        .await
        .expect("reader latest");

    // Append two messages, expect to receive them in order
    topic_store
        .store_message(create_test_message(topic_name, 0, 0, b"m1".to_vec()))
        .await
        .unwrap();
    topic_store
        .store_message(create_test_message(topic_name, 0, 1, b"m2".to_vec()))
        .await
        .unwrap();

    let r1 = stream.next().await.expect("first").expect("ok");
    let r2 = stream.next().await.expect("second").expect("ok");
    assert_eq!(r1.payload, b"m1");
    assert_eq!(r2.payload, b"m2");
}

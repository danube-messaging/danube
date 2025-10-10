//! Test: TopicStore WAL store/read and latest tailing
//!
//! Purpose
//! - Validate the broker's TopicStore facade over WalStorage supports:
//!   - Appending messages and reading back from a specific offset
//!   - Tailing from StartPosition::Latest to only receive post-subscription messages
//!
//! Notes
//! - These are unit tests colocated with the crate to access pub(crate) TopicStore
//! - Uses in-memory WAL (temp dir) and PersistentStorage trait methods

use std::collections::HashMap;

use danube_core::message::{MessageID, StreamMessage};
use danube_core::storage::StartPosition;
use danube_persistent_storage::wal::{Wal, WalConfig};
use danube_persistent_storage::WalStorage;
use futures::StreamExt;

use crate::topic::TopicStore;

fn make_msg(i: u64, topic: &str) -> StreamMessage {
    StreamMessage {
        request_id: i,
        msg_id: MessageID {
            producer_id: 1,
            topic_name: topic.to_string(),
            broker_addr: "127.0.0.1:8080".to_string(),
            topic_offset: i,
        },
        payload: format!("wal-hello-{}", i).into_bytes(),
        publish_time: 0,
        producer_name: "producer-wal".to_string(),
        subscription_name: None,
        attributes: HashMap::new(),
    }
}

#[tokio::test]
async fn topic_store_wal_store_and_read_from_offset() {
    // Temp WAL (file-backed in temp dir)
    let wal = Wal::with_config(WalConfig::default())
        .await
        .expect("create wal");
    let wal_storage = WalStorage::from_wal(wal);

    let topic = "/default/topic_store_offset";
    let ts = TopicStore::new(topic.to_string(), wal_storage);

    // Store messages offsets 0..2
    ts.store_message(make_msg(0, topic)).await.unwrap();
    ts.store_message(make_msg(1, topic)).await.unwrap();
    ts.store_message(make_msg(2, topic)).await.unwrap();

    // Read from offset 1, expect messages 1 and 2
    let mut stream = ts
        .create_reader(StartPosition::Offset(1))
        .await
        .expect("reader");

    let m1 = stream.next().await.expect("msg1").expect("ok");
    let m2 = stream.next().await.expect("msg2").expect("ok");

    assert_eq!(m1.payload, b"wal-hello-1");
    assert_eq!(m2.payload, b"wal-hello-2");
}

#[tokio::test]
async fn topic_store_wal_latest_tailing() {
    // Temp WAL (file-backed in temp dir)
    let wal = Wal::with_config(WalConfig::default())
        .await
        .expect("create wal");
    let wal_storage = WalStorage::from_wal(wal);

    let topic = "/default/topic_store_latest";
    let ts = TopicStore::new(topic.to_string(), wal_storage);

    // Tail from Latest; append afterwards
    let mut stream = ts
        .create_reader(StartPosition::Latest)
        .await
        .expect("reader");

    // Append after subscribing
    ts.store_message(make_msg(10, topic)).await.unwrap();
    ts.store_message(make_msg(11, topic)).await.unwrap();

    let m1 = stream.next().await.expect("msg1").expect("ok");
    let m2 = stream.next().await.expect("msg2").expect("ok");

    assert_eq!(m1.payload, b"wal-hello-10");
    assert_eq!(m2.payload, b"wal-hello-11");
}

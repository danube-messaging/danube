//! Test: UnifiedMultipleDispatcher reliable round-robin with strict ack-gating and non-reliable round-robin
//!
//! Purpose
//! - Reliable: validate strict ack-gating across the subscription and round-robin target selection
//!   among multiple consumers. Only one in-flight at a time, next delivered after ack.
//! - Non-reliable: validate round-robin fan-out across multiple healthy consumers without acks.
//!
//! Notes
//! - Uses WalStorage + TopicStore for reliable mode and dispatcher notifier to kick the loop.
//! - Health gating is implicitly preserved via Consumer::get_status().

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use danube_core::message::{MessageID, StreamMessage};
use danube_persistent_storage::wal::{Wal, WalConfig};
use danube_persistent_storage::WalStorage;
use tokio::sync::{mpsc, Mutex};
use tokio::time::timeout;

use crate::consumer::Consumer;
use crate::dispatcher::subscription_engine::{SubscriptionEngine, TopicStoreLike};
use crate::dispatcher::UnifiedMultipleDispatcher;
use crate::topic::TopicStore;

fn make_msg(req_id: u64, seg_off: u64, topic: &str) -> StreamMessage {
    StreamMessage {
        request_id: req_id,
        msg_id: MessageID {
            producer_id: 1,
            topic_name: topic.to_string(),
            broker_addr: "127.0.0.1:8080".to_string(),
            segment_id: 0,
            segment_offset: seg_off,
        },
        payload: format!("unified-multi-{}", req_id).into_bytes(),
        publish_time: 0,
        producer_name: "producer-test".to_string(),
        subscription_name: None,
        attributes: HashMap::new(),
    }
}

#[tokio::test]
async fn reliable_multiple_round_robin_ack_gating() {
    // Arrange WAL + TopicStore + engine + dispatcher
    let wal = Wal::with_config(WalConfig::default())
        .await
        .expect("create wal");
    let wal_storage = WalStorage::from_wal(wal);

    let topic = "/default/unified_multiple_reliable";
    let ts = TopicStore::new(topic.to_string(), wal_storage);

    let engine = SubscriptionEngine::new(
        "sub-shared".to_string(),
        Arc::new(ts.clone()) as Arc<dyn TopicStoreLike>,
    );
    let dispatcher = UnifiedMultipleDispatcher::new_reliable(engine);
    let notifier = dispatcher.get_notifier();

    // Two consumers capture messages
    let (tx1, mut rx1) = mpsc::channel::<StreamMessage>(8);
    let status1 = Arc::new(Mutex::new(true));
    let c1 = Consumer::new(1, "c1", 1, topic, tx1, status1);
    dispatcher.add_consumer(c1).await.expect("add c1");

    let (tx2, mut rx2) = mpsc::channel::<StreamMessage>(8);
    let status2 = Arc::new(Mutex::new(true));
    let c2 = Consumer::new(2, "c2", 1, topic, tx2, status2);
    dispatcher.add_consumer(c2).await.expect("add c2");

    // Wait for reliable dispatcher readiness (stream initialized)
    dispatcher.ready().await;

    // Append first message and notify -> expect delivery to one of the consumers
    ts.store_message(make_msg(500, 0, topic)).await.unwrap();
    notifier.notify_one();

    // Expect first delivery to either c1 or c2
    let first_delivered = timeout(Duration::from_secs(2), async {
        tokio::select! {
            Some(m) = rx1.recv() => Ok::<(u64, StreamMessage), ()>((1, m)),
            Some(m) = rx2.recv() => Ok::<(u64, StreamMessage), ()>((2, m)),
        }
    })
    .await
    .expect("first delivery")
    .expect("some");

    // Ack the first delivery
    let (first_consumer_id, first_msg) = first_delivered;
    dispatcher
        .ack_message(first_msg.request_id, first_msg.msg_id.clone())
        .await
        .expect("ack first");

    // Append second message and notify -> expect other consumer to receive next (round-robin)
    ts.store_message(make_msg(501, 1, topic)).await.unwrap();
    notifier.notify_one();

    let second_delivered = timeout(Duration::from_secs(2), async {
        tokio::select! {
            Some(m) = rx1.recv() => Ok::<(u64, StreamMessage), ()>((1, m)),
            Some(m) = rx2.recv() => Ok::<(u64, StreamMessage), ()>((2, m)),
        }
    })
    .await
    .expect("second delivery")
    .expect("some");

    let (second_consumer_id, second_msg) = second_delivered;
    assert_ne!(
        second_consumer_id, first_consumer_id,
        "round-robin target changed"
    );
    assert_eq!(
        second_msg.request_id, 501,
        "next message delivered after ack"
    );
}

#[tokio::test]
async fn non_reliable_multiple_round_robin() {
    // Arrange non-reliable dispatcher
    let dispatcher = UnifiedMultipleDispatcher::new_non_reliable();

    let topic = "/default/unified_multiple_non_reliable";

    // Two consumers capture messages
    let (tx1, mut rx1) = mpsc::channel::<StreamMessage>(8);
    let status1 = Arc::new(Mutex::new(true));
    let c1 = Consumer::new(11, "c1", 1, topic, tx1, status1);
    dispatcher.add_consumer(c1).await.expect("add c1");

    let (tx2, mut rx2) = mpsc::channel::<StreamMessage>(8);
    let status2 = Arc::new(Mutex::new(true));
    let c2 = Consumer::new(12, "c2", 1, topic, tx2, status2);
    dispatcher.add_consumer(c2).await.expect("add c2");

    // Dispatch 4 messages -> expect alternating delivery
    for i in 0..4u64 {
        dispatcher
            .dispatch_message(make_msg(700 + i, i, topic))
            .await
            .expect("dispatch");
    }

    // Collect 4 deliveries
    let mut got = Vec::new();
    for _ in 0..4 {
        let m = timeout(Duration::from_secs(2), async {
            tokio::select! {
                Some(m) = rx1.recv() => Ok::<(u64, StreamMessage), ()>((1, m)),
                Some(m) = rx2.recv() => Ok::<(u64, StreamMessage), ()>((2, m)),
            }
        })
        .await
        .expect("timely recv")
        .expect("some");
        got.push(m.0);
    }

    // We expect both consumer ids to appear ~alternating
    // Exact sequence: [1,2,1,2] given fresh rr state; allow for minimal variability
    assert_eq!(got.len(), 4);
    assert!(got.iter().filter(|&&id| id == 1).count() >= 2);
    assert!(got.iter().filter(|&&id| id == 2).count() >= 2);
}

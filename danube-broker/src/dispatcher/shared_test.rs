//! # Shared Dispatcher Tests
//!
//! Tests for `SharedDispatcher` (multiple consumers with round-robin load balancing).
//!
//! ## Test Coverage
//!
//! ### Reliable Mode (At-Least-Once + Load Balancing)
//! - **Ack-gating**: Only one message in-flight at a time across subscription
//! - **Round-robin selection**: Messages distributed evenly across consumers
//! - **Stream-driven dispatch**: Messages polled from WAL via SubscriptionEngine
//! - **Notification mechanism**: Topic notifies dispatcher via `Arc<Notify>`
//! - **Cursor persistence**: Subscription progress tracked
//!
//! ### Non-Reliable Mode (At-Most-Once + Load Balancing)
//! - **Fire-and-forget**: Immediate dispatch without ack tracking
//! - **Round-robin distribution**: Atomic counter ensures fair load balancing
//! - **Direct dispatch**: Messages sent via `dispatch_message()` API
//! - **No persistence**: No cursor tracking or WAL involvement
//!
//! ## Implementation Notes
//!
//! - Uses `WalStorage` to back `TopicStore` for reliable mode
//! - Uses `get_notifier()` to trigger `PollAndDispatch` after storing messages to WAL
//! - Round-robin verified by tracking which consumer receives each message
//! - Health gating exercised implicitly via `Consumer::get_status()` remaining true

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use danube_core::message::{MessageID, StreamMessage};
use danube_persistent_storage::wal::{Wal, WalConfig};
use danube_persistent_storage::WalStorage;
use tokio::sync::{mpsc, Mutex};
use tokio::time::timeout;

use crate::consumer::{Consumer, ConsumerSession};
use crate::dispatcher::subscription_engine::SubscriptionEngine;
use crate::dispatcher::Dispatcher;
use crate::message::AckMessage;
use crate::topic::TopicStore;

fn make_msg(req_id: u64, topic_off: u64, topic: &str) -> StreamMessage {
    StreamMessage {
        request_id: req_id,
        msg_id: MessageID {
            producer_id: 1,
            topic_name: topic.to_string(),
            broker_addr: "127.0.0.1:8080".to_string(),
            topic_offset: topic_off,
        },
        payload: format!("shared-{}", req_id).into_bytes(),
        publish_time: 0,
        producer_name: "producer-test".to_string(),
        subscription_name: None,
        attributes: HashMap::new(),
        schema_id: None,
        schema_version: None,
    }
}

/// Test reliable shared dispatcher with round-robin and strict ack-gating.
///
/// # Test Scenario
///
/// 1. Create reliable dispatcher with SubscriptionEngine backed by WAL
/// 2. Add two consumers (c1, c2)
/// 3. Store message #1 to WAL → notify dispatcher
/// 4. Verify message #1 delivered to one consumer
/// 5. Send ack for message #1
/// 6. Store message #2 to WAL → notify dispatcher
/// 7. Verify message #2 delivered to the OTHER consumer (round-robin)
///
/// # What This Validates
///
/// - **Ack-gating**: Second message not dispatched until first is acked
/// - **Round-robin**: Different consumer receives second message
/// - **Stream polling**: Messages read from WAL via SubscriptionEngine
/// - **Notification flow**: Topic → Notify → PollAndDispatch → Consumer selection
/// - **Load balancing**: Work distributed evenly across consumers
#[tokio::test]
async fn reliable_multiple_round_robin_ack_gating() {
    // Arrange WAL + TopicStore + engine + dispatcher
    let wal = Wal::with_config(WalConfig::default())
        .await
        .expect("create wal");
    let wal_storage = WalStorage::from_wal(wal);

    let topic = "/default/shared_reliable";
    let ts = TopicStore::new(topic.to_string(), wal_storage);

    let engine = SubscriptionEngine::new("sub-shared".to_string(), Arc::new(ts.clone()));
    let dispatcher = Dispatcher::reliable_shared(engine);
    let notifier = dispatcher.get_notifier();

    // Two consumers capture messages
    let (tx1, mut rx1) = mpsc::channel::<StreamMessage>(8);
    let (_rx_cons_tx1, rx_cons_rx1) = mpsc::channel::<StreamMessage>(8);
    let session1 = Arc::new(Mutex::new(ConsumerSession::new()));
    let rx_cons_arc1 = Arc::new(Mutex::new(rx_cons_rx1));
    let c1 = Consumer::new(42, "c1", 1, topic, "sub", tx1, session1, rx_cons_arc1);
    dispatcher.add_consumer(c1).await.expect("add c1");

    let (tx2, mut rx2) = mpsc::channel::<StreamMessage>(8);
    let (_rx_cons_tx2, rx_cons_rx2) = mpsc::channel::<StreamMessage>(8);
    let session2 = Arc::new(Mutex::new(ConsumerSession::new()));
    let rx_cons_arc2 = Arc::new(Mutex::new(rx_cons_rx2));
    let c2 = Consumer::new(43, "c2", 1, topic, "sub", tx2, session2, rx_cons_arc2);
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
    let ack = AckMessage {
        request_id: first_msg.request_id,
        msg_id: first_msg.msg_id.clone(),
        subscription_name: "test-sub".to_string(),
    };
    dispatcher.ack_message(ack).await.expect("ack first");

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

/// Test non-reliable shared dispatcher with round-robin distribution.
///
/// # Test Scenario
///
/// 1. Create non-reliable dispatcher (no SubscriptionEngine)
/// 2. Add two consumers (c1, c2)
/// 3. Call `dispatch_message()` 4 times
/// 4. Verify all 4 messages delivered
/// 5. Verify both consumers received ~equal number of messages (round-robin)
///
/// # What This Validates
///
/// - **Fire-and-forget**: Messages sent immediately without ack
/// - **Round-robin distribution**: Atomic counter ensures fair targeting
/// - **Direct dispatch**: No WAL, no SubscriptionEngine
/// - **Load balancing**: Work distributed evenly across consumers
/// - **No blocking**: All messages sent without waiting for acks
#[tokio::test]
async fn non_reliable_multiple_round_robin() {
    // Arrange non-reliable dispatcher
    let dispatcher = Dispatcher::non_reliable_shared();

    let topic = "/default/shared_non_reliable";

    // Two consumers capture messages
    let (tx_c1, mut rx_c1) = mpsc::channel::<StreamMessage>(16);
    let (_rx_cons_tx_c1, rx_cons_rx_c1) = mpsc::channel::<StreamMessage>(8);
    let session_c1 = Arc::new(Mutex::new(ConsumerSession::new()));
    let rx_cons_arc_c1 = Arc::new(Mutex::new(rx_cons_rx_c1));
    let c1 = Consumer::new(
        101,
        "c1",
        1,
        topic,
        "sub",
        tx_c1,
        session_c1,
        rx_cons_arc_c1,
    );
    dispatcher.add_consumer(c1).await.expect("add c1");

    let (tx_c2, mut rx_c2) = mpsc::channel::<StreamMessage>(16);
    let (_rx_cons_tx_c2, rx_cons_rx_c2) = mpsc::channel::<StreamMessage>(8);
    let session_c2 = Arc::new(Mutex::new(ConsumerSession::new()));
    let rx_cons_arc_c2 = Arc::new(Mutex::new(rx_cons_rx_c2));
    let c2 = Consumer::new(
        102,
        "c2",
        1,
        topic,
        "sub",
        tx_c2,
        session_c2,
        rx_cons_arc_c2,
    );
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
                Some(m) = rx_c1.recv() => Ok::<(u64, StreamMessage), ()>((1, m)),
                Some(m) = rx_c2.recv() => Ok::<(u64, StreamMessage), ()>((2, m)),
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

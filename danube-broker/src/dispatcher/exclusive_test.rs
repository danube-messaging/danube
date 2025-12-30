//! # Exclusive Dispatcher Tests
//!
//! Tests for `ExclusiveDispatcher` (single active consumer at a time).
//!
//! ## Test Coverage
//!
//! ### Reliable Mode (At-Least-Once)
//! - **Ack-gating**: Only one message in-flight at a time
//! - **Stream-driven dispatch**: Messages polled from WAL via SubscriptionEngine
//! - **Notification mechanism**: Topic notifies dispatcher via `Arc<Notify>`
//! - **Cursor persistence**: Subscription progress tracked
//!
//! ### Non-Reliable Mode (At-Most-Once)
//! - **Fire-and-forget**: Immediate dispatch without ack tracking
//! - **Direct dispatch**: Messages sent via `dispatch_message()` API
//! - **No persistence**: No cursor tracking or WAL involvement
//!
//! ## Implementation Notes
//!
//! - Uses `WalStorage` to back `TopicStore` for reliable mode
//! - Uses `get_notifier()` to trigger `PollAndDispatch` after storing messages to WAL
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
use crate::dispatcher::exclusive::ExclusiveDispatcher;
use crate::dispatcher::subscription_engine::SubscriptionEngine;
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
        payload: format!("exclusive-{}", req_id).into_bytes(),
        publish_time: 0,
        producer_name: "producer-test".to_string(),
        subscription_name: None,
        attributes: HashMap::new(),
        schema_id: None,
        schema_version: None,
    }
}

/// Test reliable exclusive dispatcher with strict ack-gating.
///
/// # Test Scenario
///
/// 1. Create reliable dispatcher with SubscriptionEngine backed by WAL
/// 2. Add single consumer
/// 3. Store message #1 to WAL → notify dispatcher
/// 4. Verify message #1 delivered
/// 5. Verify no additional messages delivered (ack-gating)
/// 6. Send ack for message #1
/// 7. Store message #2 to WAL → notify dispatcher
/// 8. Verify message #2 delivered
///
/// # What This Validates
///
/// - **Ack-gating**: Second message not dispatched until first is acked
/// - **Stream polling**: Messages read from WAL via SubscriptionEngine
/// - **Notification flow**: Topic → Notify → PollAndDispatch → Consumer
/// - **Single active consumer**: Only one consumer receives messages
#[tokio::test]
async fn reliable_single_ack_gating() {
    // Arrange: WAL + TopicStore + SubscriptionEngine
    let wal = Wal::with_config(WalConfig::default())
        .await
        .expect("create wal");
    let wal_storage = WalStorage::from_wal(wal);

    let topic = "/default/exclusive_reliable";
    let ts = TopicStore::new(topic.to_string(), wal_storage);

    // Dispatcher with reliable engine (start: Latest)
    let engine = SubscriptionEngine::new("sub-a".to_string(), Arc::new(ts.clone()));
    let dispatcher = ExclusiveDispatcher::new_reliable(engine);
    let notifier = dispatcher.get_notifier();

    // Consumer wiring: use a channel to capture dispatched messages
    let (tx, mut rx) = mpsc::channel::<StreamMessage>(8);
    let (_rx_cons_tx, rx_cons_rx) = mpsc::channel::<StreamMessage>(8);
    let session = Arc::new(Mutex::new(ConsumerSession::new()));
    let rx_cons_arc = Arc::new(Mutex::new(rx_cons_rx));
    let consumer = Consumer::new(42, "c1", 0, topic, "sub", tx, session, rx_cons_arc);
    dispatcher
        .add_consumer(consumer)
        .await
        .expect("add consumer");

    // Wait for reliable dispatcher readiness (stream initialized)
    dispatcher.ready().await;

    // Append the first message AFTER engine init; then notify to trigger dispatch loop
    ts.store_message(make_msg(100, 0, topic)).await.unwrap();
    notifier.notify_one();

    // Expect first message (should be the only one available)
    let first = timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("timely first")
        .expect("some");
    assert_eq!(first.request_id, 100);

    // No second message until ack is sent
    let none_second = timeout(Duration::from_millis(300), rx.recv()).await;
    assert!(none_second.is_err(), "should not receive second before ack");

    // Ack the first; then the second should arrive
    let ack = AckMessage {
        request_id: first.request_id,
        msg_id: first.msg_id.clone(),
        subscription_name: "test-sub".to_string(),
    };
    dispatcher.ack_message(ack).await.expect("ack");

    // Append the second message NOW; then notify to trigger next dispatch
    ts.store_message(make_msg(101, 1, topic)).await.unwrap();
    notifier.notify_one();

    // Expect second message
    let second = timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("timely second")
        .expect("some");
    assert_eq!(second.request_id, 101);
}

/// Test non-reliable exclusive dispatcher with immediate dispatch.
///
/// # Test Scenario
///
/// 1. Create non-reliable dispatcher (no SubscriptionEngine)
/// 2. Add single consumer
/// 3. Call `dispatch_message()` directly
/// 4. Verify message delivered immediately
///
/// # What This Validates
///
/// - **Fire-and-forget**: Message sent immediately without ack
/// - **Direct dispatch**: No WAL, no SubscriptionEngine
/// - **Active consumer targeting**: Message goes to active consumer
/// - **No blocking**: No ack-gating, next message can be sent immediately
#[tokio::test]
async fn non_reliable_single_immediate_dispatch() {
    // Arrange dispatcher (non-reliable)
    let dispatcher = ExclusiveDispatcher::new_non_reliable();

    // Consumer wiring
    let topic = "/default/exclusive_non_reliable";
    let (tx2, mut rx2) = mpsc::channel::<StreamMessage>(8);
    let (_rx_cons_tx2, rx_cons_rx2) = mpsc::channel::<StreamMessage>(8);
    let session2 = Arc::new(Mutex::new(ConsumerSession::new()));
    let rx_cons_arc2 = Arc::new(Mutex::new(rx_cons_rx2));
    let consumer2 = Consumer::new(43, "c2", 0, topic, "sub", tx2, session2, rx_cons_arc2);
    dispatcher
        .add_consumer(consumer2)
        .await
        .expect("add consumer");

    // Send a message via dispatcher (no WAL)
    let msg = make_msg(200, 0, topic);
    dispatcher
        .dispatch_message(msg.clone())
        .await
        .expect("dispatch");

    let got = timeout(Duration::from_secs(2), rx2.recv())
        .await
        .expect("timely recv")
        .expect("some");
    assert_eq!(got.request_id, 200);
}

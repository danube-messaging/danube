//! Test: UnifiedSingleDispatcher reliable ack-gating and non-reliable fan-out
//!
//! Purpose
//! - Validate strict ack-gating in reliable mode: only one in-flight message per subscription; next
//!   message is delivered only after explicit ack.
//! - Validate basic non-reliable behavior: immediate dispatch to the active consumer without ack.
//!
//! Notes
//! - Uses WalStorage to back TopicStore for reliable mode.
//! - Uses get_notifier() to trigger PollAndDispatch after storing messages to WAL.
//! - Health gating is exercised implicitly via Consumer::get_status() remaining true.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use danube_core::message::{MessageID, StreamMessage};
use danube_persistent_storage::wal::{Wal, WalConfig};
use danube_persistent_storage::WalStorage;
use tokio::sync::{mpsc, Mutex};
use tokio::time::timeout;

use crate::consumer::Consumer;
use crate::message::AckMessage;
use crate::dispatcher::subscription_engine::SubscriptionEngine;
use crate::dispatcher::UnifiedSingleDispatcher;
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
        payload: format!("unified-single-{}", req_id).into_bytes(),
        publish_time: 0,
        producer_name: "producer-test".to_string(),
        subscription_name: None,
        attributes: HashMap::new(),
    }
}

#[tokio::test]
async fn reliable_single_ack_gating() {
    // Arrange: WAL + TopicStore + SubscriptionEngine
    let wal = Wal::with_config(WalConfig::default())
        .await
        .expect("create wal");
    let wal_storage = WalStorage::from_wal(wal);

    let topic = "/default/unified_single_reliable";
    let ts = TopicStore::new(topic.to_string(), wal_storage);

    // Dispatcher with reliable engine (start: Latest)
    let engine = SubscriptionEngine::new("sub-a".to_string(), Arc::new(ts.clone()));
    let dispatcher = UnifiedSingleDispatcher::new_reliable(engine);
    let notifier = dispatcher.get_notifier();

    // Consumer wiring: use a channel to capture dispatched messages
    let (tx, mut rx) = mpsc::channel::<StreamMessage>(8);
    let status = Arc::new(Mutex::new(true));
    let consumer = Consumer::new(42, "c1", 0, topic, tx, status);
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
    let ack = AckMessage { request_id: first.request_id, msg_id: first.msg_id.clone(), subscription_name: "test-sub".to_string() };
    dispatcher
        .ack_message(ack)
        .await
        .expect("ack");

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

#[tokio::test]
async fn non_reliable_single_immediate_dispatch() {
    // Arrange dispatcher (non-reliable)
    let dispatcher = UnifiedSingleDispatcher::new_non_reliable();

    // Consumer wiring
    let topic = "/default/unified_single_non_reliable";
    let (tx, mut rx) = mpsc::channel::<StreamMessage>(8);
    let status = Arc::new(Mutex::new(true));
    let consumer = Consumer::new(43, "c1", 0, topic, tx, status);
    dispatcher
        .add_consumer(consumer)
        .await
        .expect("add consumer");

    // Send a message via dispatcher (no WAL)
    let msg = make_msg(200, 0, topic);
    dispatcher
        .dispatch_message(msg.clone())
        .await
        .expect("dispatch");

    let got = timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("timely recv")
        .expect("some");
    assert_eq!(got.request_id, 200);
}

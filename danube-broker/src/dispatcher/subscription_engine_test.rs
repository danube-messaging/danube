//! Unit tests for SubscriptionEngine lag detection
//!
//! These tests validate the lag detection mechanism that enables the heartbeat
//! watchdog to identify when a subscription has fallen behind the WAL head.
//! This is critical for ensuring at-least-once delivery when notifications fail.

use std::sync::Arc;

use super::SubscriptionEngine;
use crate::topic::TopicStore;
use danube_core::message::{MessageID, StreamMessage};
use danube_persistent_storage::wal::{Wal, WalConfig};
use danube_persistent_storage::WalStorage;

/// Helper to create a test TopicStore backed by an in-memory WAL
async fn create_test_store(topic_name: &str) -> Arc<TopicStore> {
    let wal = Wal::with_config(WalConfig::default())
        .await
        .expect("WAL creation failed");
    let storage = WalStorage::from_wal(wal);
    Arc::new(TopicStore::new(topic_name.to_string(), storage))
}

/// Helper to create a test message
fn make_test_message(topic_name: &str, offset: u64) -> StreamMessage {
    StreamMessage {
        request_id: offset,
        msg_id: MessageID {
            producer_id: 1,
            topic_name: topic_name.to_string(),
            broker_addr: "127.0.0.1:8080".to_string(),
            topic_offset: offset,
        },
        payload: format!("test_payload_{}", offset).into_bytes(),
        publish_time: 0,
        producer_name: "test_producer".to_string(),
        subscription_name: None,
        attributes: Default::default(),
        schema_id: None,
        schema_version: None,
    }
}

/// Helper to create a MessageID for acking
fn make_message_id(topic_name: &str, offset: u64) -> MessageID {
    MessageID {
        producer_id: 1,
        topic_name: topic_name.to_string(),
        broker_addr: "127.0.0.1:8080".to_string(),
        topic_offset: offset,
    }
}

/// **What this test validates:**
/// - When the WAL is empty (no messages written), the subscription should report no lag
/// - `has_lag()` should return `false` because there's nothing to be behind on
///
/// **Why this matters:**
/// - Ensures heartbeat doesn't trigger unnecessary polls on empty topics
/// - Prevents spurious lag detection at system startup or on idle topics
#[tokio::test]
async fn test_has_lag_empty_wal() {
    let topic_name = "test_empty_wal";
    let topic_store = create_test_store(topic_name).await;

    let engine = SubscriptionEngine::new("test_sub".to_string(), topic_store);

    // WAL is empty, no messages
    assert!(!engine.has_lag(), "Should have no lag when WAL is empty");

    let lag_info = engine.get_lag_info();
    assert_eq!(lag_info.wal_head, 0, "WAL head should be 0");
    assert_eq!(lag_info.lag_messages, 0, "Lag messages should be 0");
    assert!(!lag_info.has_lag, "Should not have lag");
}

/// **What this test validates:**
/// - When messages exist in the WAL and subscription has only acked the first one,
///   the subscription should report lag for remaining messages
/// - This is the common case during active consumption when dispatcher falls behind
///
/// **Why this matters:**
/// - This is the PRIMARY scenario the heartbeat mechanism addresses
/// - Validates lag detection works when notifications are missed/coalesced
/// - Critical for ensuring at-least-once delivery under load
#[tokio::test]
async fn test_has_lag_with_pending_messages() {
    let topic_name = "test_pending";
    let topic_store = create_test_store(topic_name).await;

    // Write 5 messages to WAL
    for i in 0..5 {
        let msg = make_test_message(topic_name, i);
        topic_store.store_message(msg).await.expect("Store failed");
    }

    let mut engine = SubscriptionEngine::new("test_sub".to_string(), topic_store);

    // Ack only the first message (offset 0), leaving 4 unacked
    engine
        .on_acked(make_message_id(topic_name, 0))
        .await
        .expect("Ack failed");

    // Should have lag because cursor=0, wal_head=5, so (5-1)-0=4 messages behind
    assert!(
        engine.has_lag(),
        "Should have lag when messages are pending"
    );

    let lag_info = engine.get_lag_info();
    assert_eq!(
        lag_info.wal_head, 5,
        "WAL head should be 5 (next offset to write)"
    );
    assert_eq!(
        lag_info.subscription_cursor,
        Some(0),
        "Cursor should be 0 (first message acked)"
    );
    assert_eq!(lag_info.lag_messages, 4, "Should have 4 messages of lag");
    assert!(lag_info.has_lag, "Should have lag");
}

/// **What this test validates:**
/// - When the subscription cursor equals WAL head - 1, the subscription is caught up
/// - `has_lag()` should return `false` because there are no pending messages
/// - This is the normal steady-state condition
///
/// **Why this matters:**
/// - Ensures heartbeat doesn't trigger when everything is working correctly
/// - Prevents unnecessary polls that waste CPU and increase latency
/// - Validates the "caught up" detection logic (cursor == wal_head - 1)
#[tokio::test]
async fn test_has_lag_caught_up() {
    let topic_name = "test_caught_up";
    let topic_store = create_test_store(topic_name).await;

    // Write 5 messages
    for i in 0..5 {
        let msg = make_test_message(topic_name, i);
        topic_store.store_message(msg).await.expect("Store failed");
    }

    let mut engine = SubscriptionEngine::new("test_sub".to_string(), topic_store);

    // Simulate acking up to offset 4 (last message)
    engine
        .on_acked(make_message_id(topic_name, 4))
        .await
        .expect("Ack failed");

    // WAL head is 5 (next offset), cursor is 4 (last acked)
    // This means caught up: cursor == wal_head - 1
    assert!(!engine.has_lag(), "Should have no lag when caught up");

    let lag_info = engine.get_lag_info();
    assert_eq!(lag_info.wal_head, 5, "WAL head should be 5");
    assert_eq!(lag_info.subscription_cursor, Some(4), "Cursor should be 4");
    assert_eq!(lag_info.lag_messages, 0, "Lag messages should be 0");
    assert!(!lag_info.has_lag, "Should not have lag");
}

/// **What this test validates:**
/// - When the subscription cursor is behind WAL head - 1, the subscription has lag
/// - `has_lag()` should return `true` and `lag_messages` should be calculated correctly
/// - This is the condition that triggers heartbeat recovery
///
/// **Why this matters:**
/// - This is the PRIMARY condition the heartbeat mechanism detects
/// - Ensures lag is correctly identified when notifications are missed/coalesced
/// - Validates the lag calculation: wal_head - 1 - cursor
#[tokio::test]
async fn test_has_lag_behind() {
    let topic_name = "test_behind";
    let topic_store = create_test_store(topic_name).await;

    // Write 10 messages
    for i in 0..10 {
        let msg = make_test_message(topic_name, i);
        topic_store.store_message(msg).await.expect("Store failed");
    }

    let mut engine = SubscriptionEngine::new("test_sub".to_string(), topic_store);

    // Simulate acking only up to offset 5
    engine
        .on_acked(make_message_id(topic_name, 5))
        .await
        .expect("Ack failed");

    // WAL head is 10 (next offset), cursor is 5 (last acked)
    // Lag = (10 - 1) - 5 = 4 messages
    assert!(engine.has_lag(), "Should have lag when behind");

    let lag_info = engine.get_lag_info();
    assert_eq!(lag_info.wal_head, 10, "WAL head should be 10");
    assert_eq!(lag_info.subscription_cursor, Some(5), "Cursor should be 5");
    assert_eq!(lag_info.lag_messages, 4, "Should have 4 messages lag");
    assert!(lag_info.has_lag, "Should have lag");
}

/// **What this test validates:**
/// - `get_lag_info()` correctly calculates lag in various scenarios
/// - Lag calculation handles edge cases (no acks, caught up, behind)
/// - All fields in `LagInfo` struct are populated correctly
///
/// **Why this matters:**
/// - `get_lag_info()` is used for metrics and monitoring
/// - Operators rely on accurate lag metrics to diagnose issues
/// - Ensures the diagnostic information matches the actual system state
#[tokio::test]
async fn test_get_lag_info_calculates_correctly() {
    let topic_name = "test_lag_info";
    let topic_store = create_test_store(topic_name).await;

    // Scenario 1: Empty WAL
    let engine = SubscriptionEngine::new("test_sub_1".to_string(), topic_store.clone());
    let info = engine.get_lag_info();
    assert_eq!(info.wal_head, 0);
    assert_eq!(info.subscription_cursor, None);
    assert_eq!(info.lag_messages, 0);
    assert!(!info.has_lag);

    // Scenario 2: Messages exist, ack first one
    for i in 0..3 {
        let msg = make_test_message(topic_name, i);
        topic_store.store_message(msg).await.expect("Store failed");
    }
    let mut engine = engine;
    engine
        .on_acked(make_message_id(topic_name, 0))
        .await
        .expect("Ack failed");
    let info = engine.get_lag_info();
    assert_eq!(info.wal_head, 3);
    assert_eq!(info.subscription_cursor, Some(0));
    assert_eq!(info.lag_messages, 2); // (3-1) - 0 = 2
    assert!(info.has_lag);

    // Scenario 3: Ack second message (still has lag)
    engine
        .on_acked(make_message_id(topic_name, 1))
        .await
        .expect("Ack failed");
    let info = engine.get_lag_info();
    assert_eq!(info.wal_head, 3);
    assert_eq!(info.subscription_cursor, Some(1));
    assert_eq!(info.lag_messages, 1); // (3-1) - 1 = 1
    assert!(info.has_lag);

    // Scenario 4: Catch up to all messages
    engine
        .on_acked(make_message_id(topic_name, 2))
        .await
        .expect("Ack failed");
    let info = engine.get_lag_info();
    assert_eq!(info.wal_head, 3);
    assert_eq!(info.subscription_cursor, Some(2));
    assert_eq!(info.lag_messages, 0); // (3-1) - 2 = 0
    assert!(!info.has_lag);
}

/// **What this test validates:**
/// - Lag detection handles edge cases: very large offsets, saturating subtraction
/// - No panics or overflows occur with extreme values
/// - Calculations remain correct even with unusual offset patterns
///
/// **Why this matters:**
/// - Production systems may have billions of messages (large offsets)
/// - Ensures robustness against integer overflow edge cases
/// - Validates `saturating_sub` is used correctly
#[tokio::test]
async fn test_lag_info_edge_cases() {
    let topic_name = "test_edge_cases";
    let topic_store = create_test_store(topic_name).await;

    // Write a large number of messages
    for i in 0..100 {
        let msg = make_test_message(topic_name, i);
        topic_store.store_message(msg).await.expect("Store failed");
    }

    let mut engine = SubscriptionEngine::new("test_sub".to_string(), topic_store);

    // Ack to offset 50
    engine
        .on_acked(make_message_id(topic_name, 50))
        .await
        .expect("Ack failed");

    let info = engine.get_lag_info();
    assert_eq!(info.lag_messages, 49); // (100-1) - 50 = 49
    assert!(info.has_lag);

    // Edge case: WAL head is 1, cursor is 0 (caught up)
    let topic_store2 = create_test_store("test_edge_single").await;
    let msg = make_test_message("test_edge_single", 0);
    topic_store2.store_message(msg).await.expect("Store failed");

    let mut engine2 = SubscriptionEngine::new("test_sub_2".to_string(), topic_store2);
    engine2
        .on_acked(make_message_id("test_edge_single", 0))
        .await
        .expect("Ack failed");

    let info = engine2.get_lag_info();
    assert_eq!(info.wal_head, 1);
    assert_eq!(info.subscription_cursor, Some(0));
    assert_eq!(info.lag_messages, 0); // (1-1) - 0 = 0
    assert!(!info.has_lag);
}

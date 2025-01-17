#[cfg(test)]
use crate::{
    dispatch::SubscriptionDispatch, errors::ReliableDispatchError,
    storage_backend::InMemoryStorage, topic_storage::TopicStore,
};

#[cfg(test)]
use danube_core::{
    dispatch_strategy::{ReliableOptions, RetentionPolicy},
    message::{MessageID, StreamMessage},
    storage::{Segment, StorageBackend},
};
#[cfg(test)]
use std::collections::HashMap;
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
#[cfg(test)]
use std::sync::Arc;
#[cfg(test)]
use std::time::{SystemTime, UNIX_EPOCH};
#[cfg(test)]
use tokio::sync::{broadcast, RwLock};

/// Test helper to create a TopicStore with default settings
#[cfg(test)]
fn create_test_topic_store() -> TopicStore {
    // Using 1MB segment size and 60s TTL for testing
    let storage = Arc::new(InMemoryStorage::new());
    let reliable_options = ReliableOptions::new(
        1, // 1MB segment size
        RetentionPolicy::RetainUntilAck,
        60, // 60s retention period
    );
    TopicStore::new(storage, reliable_options)
}

#[cfg(test)]
fn create_test_message_id(sequence_id: u64) -> MessageID {
    MessageID {
        sequence_id,
        producer_id: 1,
        topic_name: "test-topic".to_string(),
        broker_addr: "localhost:6650".to_string(),
    }
}

#[cfg(test)]
fn create_test_message(payload: Vec<u8>) -> StreamMessage {
    StreamMessage {
        request_id: 1,
        msg_id: MessageID {
            sequence_id: 1,
            producer_id: 1,
            topic_name: "/default/test-topic".to_string(),
            broker_addr: "localhost:6650".to_string(),
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

#[cfg(test)]
fn create_test_notification_channel() -> broadcast::Receiver<MessageID> {
    let (_tx, rx) = broadcast::channel(100);
    rx
}

/// Tests the creation of a new SubscriptionDispatch instance
/// Verifies that all initial values are properly set to their default states:
/// - No active segment
/// - No current segment ID
/// - No pending acknowledgments
/// - Empty acknowledged messages map
#[tokio::test]
async fn test_new_subscription_dispatch() {
    let topic_store = create_test_topic_store();
    let last_acked = Arc::new(AtomicUsize::new(0));
    let notify_rx = create_test_notification_channel();
    let dispatch = SubscriptionDispatch::new(topic_store, last_acked, notify_rx);

    assert!(dispatch.segment.is_none());
    assert!(dispatch.current_segment_id.is_none());
    assert!(dispatch.pending_ack_message.is_none());
    assert!(dispatch.acked_messages.is_empty());
}

/// Tests processing behavior when no segments are available
/// Expects an InvalidState error when attempting to process an empty segment
#[tokio::test]
async fn test_process_empty_segment() {
    let topic_store = create_test_topic_store();
    let last_acked = Arc::new(AtomicUsize::new(0));
    let notify_rx = create_test_notification_channel();
    let mut dispatch = SubscriptionDispatch::new(topic_store, last_acked, notify_rx);

    let result = dispatch.process_current_segment().await;
    assert!(matches!(
        result,
        Err(ReliableDispatchError::InvalidState(_))
    ));
}

/// Tests the message acknowledgment flow
/// Verifies:
/// - Successful acknowledgment of a pending message
/// - Clearing of pending acknowledgment
/// - Addition to acknowledged messages map
#[tokio::test]
async fn test_message_acknowledgment() {
    let storage = Arc::new(InMemoryStorage::new());
    let topic_store = create_test_topic_store();
    let last_acked = Arc::new(AtomicUsize::new(0));
    let notify_rx = create_test_notification_channel();
    let mut dispatch = SubscriptionDispatch::new(topic_store, last_acked, notify_rx);

    // Create test message with matching IDs
    let test_message = create_test_message(vec![1]);
    let request_id = test_message.request_id;
    let msg_id = test_message.msg_id.clone();

    // Create and add segment with the test message
    let segment = Arc::new(RwLock::new(Segment::new(1, 1024 * 1024)));
    {
        let mut segment_write = segment.write().await;
        segment_write.messages.push(test_message);
    }

    // Store segment in topic store
    storage.put_segment(1, segment.clone()).await.unwrap();
    dispatch
        .topic_store
        .segments_index
        .write()
        .await
        .push((1, 0));

    dispatch.segment = Some(segment);
    dispatch.current_segment_id = Some(1);
    dispatch.pending_ack_message = Some((request_id, msg_id.clone()));

    let result = dispatch
        .handle_message_acked(request_id, msg_id.clone())
        .await;

    assert!(result.is_ok());
    assert!(dispatch.pending_ack_message.is_none());
    assert!(dispatch.acked_messages.contains_key(&msg_id));
    assert_eq!(dispatch.retry_count, 0);
}

/// Tests handling of invalid message acknowledgments
/// Verifies that attempting to acknowledge a non-pending message
/// results in an AcknowledgmentError
#[tokio::test]
async fn test_invalid_acknowledgment() {
    let topic_store = create_test_topic_store();
    let last_acked = Arc::new(AtomicUsize::new(0));
    let notify_rx = create_test_notification_channel();
    let mut dispatch = SubscriptionDispatch::new(topic_store, last_acked, notify_rx);

    let msg_id = create_test_message_id(1);
    let result = dispatch.handle_message_acked(1, msg_id).await;

    assert!(matches!(
        result,
        Err(ReliableDispatchError::AcknowledgmentError(_))
    ));
}

/// Tests the segment transition mechanism
/// Verifies:
/// - Successful transition to next segment
/// - Clearing of acknowledged messages during transition
#[tokio::test]
async fn test_segment_transition() {
    let topic_store = create_test_topic_store();
    let last_acked = Arc::new(AtomicUsize::new(0));
    let notify_rx = create_test_notification_channel();
    let mut dispatch = SubscriptionDispatch::new(topic_store, last_acked, notify_rx);

    let segment = Arc::new(RwLock::new(Segment::new(1, 1024 * 1024)));
    dispatch.segment = Some(segment);
    dispatch.current_segment_id = Some(1);

    let result = dispatch.move_to_next_segment();
    assert!(result.await.is_ok());
    assert!(dispatch.acked_messages.is_empty());
}

/// Tests the clearing of current segment state
/// Verifies that all segment-related state is properly cleared:
/// - Segment reference
/// - Current segment ID
/// - Acknowledged messages
#[tokio::test]
async fn test_clear_current_segment() {
    let topic_store = create_test_topic_store();
    let last_acked = Arc::new(AtomicUsize::new(0));
    let notify_rx = create_test_notification_channel();
    let mut dispatch = SubscriptionDispatch::new(topic_store, last_acked, notify_rx);

    let segment = Arc::new(RwLock::new(Segment::new(1, 1024 * 1024)));
    dispatch.segment = Some(segment);
    dispatch.current_segment_id = Some(1);
    let msg_id = create_test_message_id(1);
    dispatch.acked_messages.insert(msg_id, 1);

    dispatch.clear_current_segment();
    assert!(dispatch.segment.is_none());
    assert!(dispatch.current_segment_id.is_none());
    assert!(dispatch.acked_messages.is_empty());
}

/// The test validates core segment lifecycle behaviors:
/// 1. Initial state validation (Ok(false))
/// 2. Closing segment behavior (Ok(true))
/// 3. Message acknowledgment tracking
/// 4. Segment transition signals
#[tokio::test]
async fn test_validate_segment() {
    let storage = Arc::new(InMemoryStorage::new());
    let reliable_options = ReliableOptions::new(
        1, // 1MB segment size
        RetentionPolicy::RetainUntilAck,
        60, // 60s retention period
    );
    let topic_store = TopicStore::new(storage.clone(), reliable_options);
    let last_acked = Arc::new(AtomicUsize::new(0));
    let notify_rx = create_test_notification_channel();
    let mut dispatch = SubscriptionDispatch::new(topic_store, last_acked, notify_rx);

    // Create a test segment and add it to topic_store
    let segment = Arc::new(RwLock::new(Segment::new(1, 1024 * 1024)));

    // Store segment using the storage backend
    storage.put_segment(1, segment.clone()).await.unwrap();
    dispatch
        .topic_store
        .segments_index
        .write()
        .await
        .push((1, 0));

    // Test 1: Valid segment that exists and is not closed
    let result = dispatch.validate_segment(1, &segment).await;
    assert!(matches!(result, Ok(false)));

    // Test 2: Closed segment with properly acknowledged message
    {
        let mut segment_write = segment.write().await;
        segment_write.close_time = 1;
        let message = create_test_message(vec![1]);
        let msg_id = message.msg_id.clone();
        let request_id = message.request_id;
        segment_write.messages.push(message);
        drop(segment_write);

        dispatch.pending_ack_message = Some((request_id, msg_id.clone()));
        dispatch.acked_messages.insert(msg_id, request_id);
    }

    let result = dispatch.validate_segment(1, &segment).await;
    assert!(matches!(result, Ok(true)));
}

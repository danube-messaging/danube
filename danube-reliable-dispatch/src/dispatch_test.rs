#[cfg(test)]
use crate::{
    dispatch::SubscriptionDispatch, errors::ReliableDispatchError,
    storage_backend::InMemoryStorage, topic_storage::TopicStore,
};

#[cfg(test)]
use crate::topic_cache::TopicCache;

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
use tokio::sync::RwLock;

/// Test helper to create a TopicStore with default settings
#[cfg(test)]
fn create_test_topic_store(topic_name: &str) -> TopicStore {
    // Using 1MB segment size and 60s TTL for testing
    let storage = Arc::new(InMemoryStorage::new());
    let reliable_options = ReliableOptions::new(
        1, // 1MB segment size
        RetentionPolicy::RetainUntilAck,
        60, // 60s retention period
    );
    let topic_cache = TopicCache::new(storage, 10, 10);
    TopicStore::new(topic_name, topic_cache, reliable_options)
}

#[cfg(test)]
fn create_test_message_id(topic_name: &str, segment_id: u64, segment_offset: u64) -> MessageID {
    MessageID {
        producer_id: 1,
        topic_name: topic_name.to_string(),
        broker_addr: "localhost:6650".to_string(),
        segment_id,
        segment_offset,
    }
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

/// Tests the creation of a new SubscriptionDispatch instance
/// Verifies that all initial values are properly set to their default states:
/// - No active segment
/// - No current segment ID
/// - No pending acknowledgments
/// - Empty acknowledged messages map
#[tokio::test]
async fn test_new_subscription_dispatch() {
    let topic_name = "/default/test-topic";
    let topic_store = create_test_topic_store(topic_name);
    let last_acked = Arc::new(AtomicUsize::new(0));
    let dispatch = SubscriptionDispatch::new(topic_store, last_acked);

    assert!(dispatch.segment.is_none());
    assert!(dispatch.current_segment_id.is_none());
    assert!(dispatch.pending_ack_message.is_none());
    assert!(dispatch.acked_messages.is_empty());
}

/// Tests processing behavior when no segments are available
/// Expects an InvalidState error when attempting to process an empty segment
#[tokio::test]
async fn test_process_empty_segment() {
    let topic_name = "/default/test-topic";
    let topic_store = create_test_topic_store(topic_name);
    let last_acked = Arc::new(AtomicUsize::new(0));
    let mut dispatch = SubscriptionDispatch::new(topic_store, last_acked);

    let result = dispatch.process_current_segment().await;
    assert!(matches!(
        result,
        Err(ReliableDispatchError::SegmentError(_)) // Err(ReliableDispatchError::NoMessagesAvailable)
    ));
}

/// Tests the message acknowledgment flow
/// Verifies:
/// - Successful acknowledgment of a pending message
/// - Clearing of pending acknowledgment
/// - Addition to acknowledged messages map
#[tokio::test]
async fn test_message_acknowledgment() {
    let topic_name = "/default/test-topic";
    let storage = Arc::new(InMemoryStorage::new());
    let reliable_options = ReliableOptions::new(1, RetentionPolicy::RetainUntilAck, 60);
    let topic_cache = TopicCache::new(storage.clone(), 10, 10);
    let topic_store = TopicStore::new(topic_name, topic_cache, reliable_options);
    let last_acked = Arc::new(AtomicUsize::new(0));
    let mut dispatch = SubscriptionDispatch::new(topic_store, last_acked);

    // Create and setup segment with message
    let segment = Arc::new(RwLock::new(Segment::new(1, 1024 * 1024)));
    let message = create_test_message(topic_name, 0, 0, vec![1]);
    {
        let mut segment_write = segment.write().await;
        segment_write.messages.push(message.clone());
    }

    // Store segment in storage backend
    storage
        .put_segment(topic_name, 1, segment.clone())
        .await
        .unwrap();
    dispatch
        .topic_store
        .segments_index
        .write()
        .await
        .push((1, 0));

    let request_id = message.request_id;
    let msg_id = message.msg_id.clone();

    dispatch.segment = Some(segment);
    dispatch.current_segment_id = Some(1);
    dispatch.pending_ack_message = Some((request_id, msg_id.clone()));

    let result = dispatch
        .handle_message_acked(request_id, msg_id.clone())
        .await;
    assert!(result.is_ok());
    assert!(dispatch.pending_ack_message.is_none());
    assert!(dispatch.acked_messages.contains_key(&msg_id));
}

/// Tests handling of invalid message acknowledgments
/// Verifies that attempting to acknowledge a non-pending message
/// results in an AcknowledgmentError
#[tokio::test]
async fn test_invalid_acknowledgment() {
    let topic_name = "/default/test-topic";
    let topic_store = create_test_topic_store(topic_name);
    let last_acked = Arc::new(AtomicUsize::new(0));
    let mut dispatch = SubscriptionDispatch::new(topic_store, last_acked);

    let msg_id = create_test_message_id(topic_name, 0, 1);
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
    let topic_name = "/default/test-topic";
    let topic_store = create_test_topic_store(topic_name);
    let last_acked = Arc::new(AtomicUsize::new(0));
    let mut dispatch = SubscriptionDispatch::new(topic_store, last_acked);

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
    let topic_name = "/default/test-topic";
    let topic_store = create_test_topic_store(topic_name);
    let last_acked = Arc::new(AtomicUsize::new(0));
    let mut dispatch = SubscriptionDispatch::new(topic_store, last_acked);

    let segment = Arc::new(RwLock::new(Segment::new(1, 1024 * 1024)));
    dispatch.segment = Some(segment);
    dispatch.current_segment_id = Some(1);
    let msg_id = create_test_message_id(topic_name, 0, 1);
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
    let topic_name = "/default/test-topic";
    let storage = Arc::new(InMemoryStorage::new());
    let reliable_options = ReliableOptions::new(1, RetentionPolicy::RetainUntilAck, 60);
    let topic_cache = TopicCache::new(storage.clone(), 10, 10);
    let topic_store = TopicStore::new(topic_name, topic_cache, reliable_options);
    let last_acked = Arc::new(AtomicUsize::new(0));
    let mut dispatch = SubscriptionDispatch::new(topic_store, last_acked);

    let segment = Arc::new(RwLock::new(Segment::new(1, 1024 * 1024)));
    storage
        .put_segment(topic_name, 1, segment.clone())
        .await
        .unwrap();
    dispatch
        .topic_store
        .segments_index
        .write()
        .await
        .push((1, 0));

    // Test 1: Initial state
    let result = dispatch.validate_segment_state(1, &segment).await;
    assert!(matches!(result, Ok(false)));

    // Test 2: Closed segment with acknowledged message
    let message = create_test_message(topic_name, 0, 1, vec![1]);
    {
        let mut segment_write = segment.write().await;
        segment_write.close_time = 1;
        segment_write.messages.push(message.clone());
    }

    dispatch
        .acked_messages
        .insert(message.msg_id.clone(), message.request_id);

    let result = dispatch.validate_segment_state(1, &segment).await;
    assert!(matches!(result, Ok(true)));
}

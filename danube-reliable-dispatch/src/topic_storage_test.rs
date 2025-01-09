#[cfg(test)]
use crate::{
    storage_backend::InMemoryStorage,
    topic_storage::{Segment, TopicStore},
};
#[cfg(test)]
use danube_client::{MessageID, ReliableOptions, RetentionPolicy, StreamMessage};
#[cfg(test)]
use dashmap::DashMap;
#[cfg(test)]
use std::collections::HashMap;
#[cfg(test)]
use std::sync::{atomic::AtomicUsize, Arc};
#[cfg(test)]
use std::time::{SystemTime, UNIX_EPOCH};

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

/// Tests basic segment initialization with correct default values
/// Validates:
/// - Segment ID assignment
/// - Initial close time is 0
/// - Empty message vector
/// - Zero initial size
#[test]
fn test_segment_creation() {
    let segment = Segment::new(1, 1024);
    assert_eq!(segment.id, 1);
    assert_eq!(segment.close_time, 0);
    assert_eq!(segment.current_size, 0);
    assert!(segment.messages.is_empty());
}

/// Tests adding messages to a segment
/// Validates:
/// - Message storage
/// - Size tracking
/// - Message count
#[test]
fn test_segment_message_handling() {
    let mut segment = Segment::new(1, 1024);
    let message = create_test_message(vec![1, 2, 3]);

    segment.add_message(message.clone());
    assert_eq!(segment.messages.len(), 1);
    assert_eq!(segment.current_size, message.size());
}

/// Tests segment size limit behavior
/// Validates:
/// - Size limit checks
/// - Segment full condition
/// - Multiple message additions
#[test]
fn test_segment_size_limit() {
    let mut segment = Segment::new(1, 1024);
    let message = create_test_message(vec![0; 512]);

    assert!(!segment.is_full(1024));
    segment.add_message(message.clone());
    assert!(!segment.is_full(1024));
    segment.add_message(message);
    assert!(segment.is_full(1024));
}

/// Tests basic message storage in TopicStore
/// Validates:
/// - Message storage functionality
/// - Initial segment creation
/// - Message retrieval
#[tokio::test]
async fn test_topic_store_message_storage() {
    let storage = Arc::new(InMemoryStorage::new());
    let reliable_options = ReliableOptions::new(
        1, // 1MB segment size
        RetentionPolicy::RetainUntilAck,
        3600, // 3600s retention period
    );
    let topic_store = TopicStore::new(storage.clone(), reliable_options);
    let message = create_test_message(vec![1, 2, 3]);

    topic_store.store_message(message.clone()).await.unwrap();
    let segment = topic_store.get_next_segment(None).await.unwrap().unwrap();
    let segment_read = segment.read().await;
    assert_eq!(segment_read.messages.len(), 1);
}

/// Tests segment transition when size limit is reached
/// Validates:
/// - New segment creation on size limit
/// - Segment ID progression
/// - Message distribution across segments
#[tokio::test]
async fn test_topic_store_segment_transition() {
    let storage = Arc::new(InMemoryStorage::new());
    let reliable_options = ReliableOptions::new(
        1, // 1MB segment size
        RetentionPolicy::RetainUntilAck,
        3600, // 3600s retention period
    );
    let topic_store = TopicStore::new(storage.clone(), reliable_options);
    let large_message = create_test_message(vec![0; 1024 * 1024]); // 1MB message

    topic_store
        .store_message(large_message.clone())
        .await
        .unwrap();
    let message = create_test_message(vec![1]);
    topic_store.store_message(message).await.unwrap(); // Should create new segment

    let first_segment = topic_store.get_next_segment(None).await.unwrap().unwrap();
    let second_segment = topic_store
        .get_next_segment(Some(first_segment.read().await.id))
        .await
        .unwrap()
        .unwrap();

    assert_ne!(
        first_segment.read().await.id,
        second_segment.read().await.id
    );
}

/// Tests segment cleanup based on TTL
/// Validates:
/// - Expired segment removal
/// - TTL enforcement
/// - Segment tracking after cleanup
#[tokio::test]
async fn test_topic_store_cleanup() {
    let storage = Arc::new(InMemoryStorage::new());
    let reliable_options = ReliableOptions::new(
        1, // 1MB segment size
        RetentionPolicy::RetainUntilAck,
        1, // 1s retention period
    );
    let topic_store = TopicStore::new(storage.clone(), reliable_options);
    let subscriptions = Arc::new(DashMap::new());
    let subscription_id = "test_sub".to_string();
    subscriptions.insert(subscription_id.clone(), Arc::new(AtomicUsize::new(0)));

    let message = create_test_message(vec![1, 2, 3]);
    topic_store.store_message(message).await.unwrap();

    let close_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        - 2;

    {
        // Get the segment and update its close time
        let segment = topic_store.get_next_segment(None).await.unwrap().unwrap();
        let mut segment_write = segment.write().await;
        segment_write.close_time = close_time;
    }

    // Update the segments_index with the new close_time
    {
        let mut index = topic_store.segments_index.write().await;
        if let Some(entry) = index.get_mut(0) {
            entry.1 = close_time;
        }
    }

    TopicStore::cleanup_expired_segments(&topic_store.storage, &topic_store.segments_index, 1)
        .await;

    assert!(!topic_store.contains_segment(0).await.unwrap());
}

/// Tests segment cleanup based on acknowledgments
/// Validates:
/// - Acknowledged segment removal
/// - Subscription tracking
/// - Segment cleanup based on subscription state
#[tokio::test]
async fn test_topic_store_acknowledged_cleanup() {
    let storage = Arc::new(InMemoryStorage::new());
    let reliable_options = ReliableOptions::new(
        1, // 1MB segment size
        RetentionPolicy::RetainUntilAck,
        3600, // 3600s retention period
    );
    let topic_store = TopicStore::new(storage.clone(), reliable_options);
    let subscriptions = Arc::new(DashMap::new());
    let subscription_id = "test_sub".to_string();
    subscriptions.insert(subscription_id.clone(), Arc::new(AtomicUsize::new(1)));

    let message = create_test_message(vec![1, 2, 3]);
    topic_store.store_message(message).await.unwrap();

    let segment = topic_store.get_next_segment(None).await.unwrap().unwrap();
    let close_time = 1;

    // Update segment in a separate scope
    {
        let mut segment_write = segment.write().await;
        segment_write.close_time = close_time;
    }

    // Update index in a separate scope
    {
        let mut index = topic_store.segments_index.write().await;
        if let Some(entry) = index.get_mut(0) {
            entry.1 = close_time;
        }
    }

    TopicStore::cleanup_acknowledged_segments(
        &topic_store.storage,
        &topic_store.segments_index,
        &subscriptions,
    )
    .await;

    assert!(!topic_store.contains_segment(0).await.unwrap());
}

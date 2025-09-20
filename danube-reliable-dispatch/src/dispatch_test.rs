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
#[cfg(test)]
use tokio_stream::StreamExt;

#[cfg(test)]
use crate::ReliableDispatch;
#[cfg(test)]
use danube_core::storage::StartPosition;

#[cfg(test)]
use danube_persistent_storage::{
    wal::{Wal, WalConfig},
    WalStorage,
};

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

/// E2E (WAL): Verify replay-from-offset and tailing
/// Why: Consumers may start from an older offset; the system should replay historical entries
/// (from WAL cache/file) and then seamlessly continue with live messages without duplicates.
/// Expected: Starting from offset 3 returns payloads [3], [4], then subsequent appends appear.
///
/// NOTE: Temporary explicit WAL wiring via `ReliableDispatch::new_with_persistent` is used here
/// while the WAL path is opt-in. Once implementation reaches Phase D (legacy removal and
/// WAL-first default), revert these tests to use `ReliableDispatch::new(...)` and remove the
/// explicit Wal/WalStorage setup.
#[tokio::test]
async fn test_wal_replay_from_offset() {
    let topic_name = "/default/wal-replay";
    let storage = Arc::new(InMemoryStorage::new());
    let reliable_options = ReliableOptions::new(1, RetentionPolicy::RetainUntilAck, 60);
    let topic_cache = TopicCache::new(storage.clone(), 100, 10);

    // Use an explicit WAL for deterministic behavior
    let tmp = tempfile::tempdir().unwrap();
    let wal = Wal::with_config(WalConfig {
        dir: Some(tmp.path().to_path_buf()),
        file_name: Some("wal.log".to_string()),
        cache_capacity: Some(1024),
        fsync_interval_ms: Some(1),
        max_batch_bytes: Some(1),
        ..Default::default()
    })
    .await
    .expect("create wal");
    let wal_storage = WalStorage::from_wal(wal);
    let dispatch = ReliableDispatch::new_with_persistent(
        topic_name,
        reliable_options,
        topic_cache,
        wal_storage,
    );

    // Append 5 messages before creating reader
    for i in 0..5u8 {
        let msg = create_test_message(topic_name, 0, i as u64, vec![i]);
        dispatch.store_message(msg).await.unwrap();
    }

    // Create reader from offset 3 => expect two cached messages [3], [4]
    let mut stream = dispatch
        .topic_store
        .create_reader(StartPosition::Offset(3))
        .await
        .expect("stream created");

    let r1 = stream.next().await.expect("first cached").expect("ok");
    let r2 = stream.next().await.expect("second cached").expect("ok");
    assert_eq!(r1.payload, vec![3]);
    assert_eq!(r2.payload, vec![4]);

    // Then append a live message and verify it appears next without duplication
    let live = create_test_message(topic_name, 0, 5, b"live".to_vec());
    dispatch.store_message(live.clone()).await.unwrap();
    let r3 = stream.next().await.expect("live item").expect("ok");
    assert_eq!(r3.payload, b"live");
}

/// E2E (WAL): Verify tailing from Latest
/// Why: A consumer starting at Latest should only see messages appended after the subscription
/// starts, with stable latency, backed by the WAL broadcast path.
/// Expected: Messages appended after `create_stream_latest()` are delivered in order.
///
/// NOTE: Temporary explicit WAL wiring via `ReliableDispatch::new_with_persistent` is used here
/// while the WAL path is opt-in. Once implementation reaches Phase D (legacy removal and
/// WAL-first default), revert these tests to use `ReliableDispatch::new(...)` and remove the
/// explicit Wal/WalStorage setup.
#[tokio::test]
async fn test_wal_stream_latest() {
    let topic_name = "/default/wal-latest";
    let storage = Arc::new(InMemoryStorage::new());
    let reliable_options = ReliableOptions::new(1, RetentionPolicy::RetainUntilAck, 60);
    let topic_cache = TopicCache::new(storage.clone(), 100, 10);

    // Use an explicit WAL for deterministic behavior
    let tmp = tempfile::tempdir().unwrap();
    let wal = Wal::with_config(WalConfig {
        dir: Some(tmp.path().to_path_buf()),
        file_name: Some("wal.log".to_string()),
        cache_capacity: Some(1024),
        fsync_interval_ms: Some(1),
        max_batch_bytes: Some(1),
        ..Default::default()
    })
    .await
    .expect("create wal");
    let wal_storage = WalStorage::from_wal(wal);
    let dispatch = ReliableDispatch::new_with_persistent(
        topic_name,
        reliable_options,
        topic_cache,
        wal_storage,
    );

    // Start stream at Latest
    let mut stream = dispatch
        .create_stream_latest()
        .await
        .expect("stream should be created");

    // Append messages after stream subscription
    let m1 = create_test_message(topic_name, 0, 0, b"m1".to_vec());
    let m2 = create_test_message(topic_name, 0, 1, b"m2".to_vec());
    dispatch.store_message(m1.clone()).await.unwrap();
    dispatch.store_message(m2.clone()).await.unwrap();

    // Collect 2 messages
    let r1 = stream.next().await.expect("first item").expect("ok");
    let r2 = stream.next().await.expect("second item").expect("ok");
    assert_eq!(r1.payload, b"m1");
    assert_eq!(r2.payload, b"m2");
}

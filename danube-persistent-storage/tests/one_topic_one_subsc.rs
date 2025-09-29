mod common;

use common::{
    count_cloud_objects, create_test_factory, get_latest_object_descriptor, make_test_message,
    wait_for_condition,
};
use danube_core::storage::{PersistentStorage, StartPosition};
use std::time::Duration;
use tokio_stream::StreamExt;

/// Test: Single topic basic message flow
///
/// Purpose
/// - Validate basic message append and read operations for a single topic
/// - Ensure message ordering and data integrity through WAL storage
///
/// Flow
/// - Create WAL storage for a single topic
/// - Append 10 sequential messages
/// - Create reader from offset 0 and read all messages
/// - Verify message count, ordering, and payload content
///
/// Expected
/// - All 10 messages are stored and retrievable
/// - Messages maintain correct offset ordering (0-9)
/// - Payload content matches original data
#[tokio::test]
async fn test_single_topic_basic_flow() {
    let (factory, _memory_store) = create_test_factory().await;
    let topic_name = "integration/single-basic";

    // Create WAL storage for topic
    let storage = factory.for_topic(topic_name).await.expect("create storage");

    // Append messages
    let messages = (0..10u64)
        .map(|i| make_test_message(i, 1, topic_name, i, &format!("message-{}", i)))
        .collect::<Vec<_>>();

    for msg in &messages {
        storage
            .append_message(topic_name, msg.clone())
            .await
            .expect("append message");
    }

    // Create reader to verify messages were written
    let mut reader = storage
        .create_reader(topic_name, StartPosition::Offset(0))
        .await
        .expect("create reader");

    // Reader already created above

    // Read all messages
    let mut read_messages = Vec::new();
    for _ in 0..10 {
        if let Some(msg) = reader.next().await {
            read_messages.push(msg.expect("read message"));
        }
    }

    assert_eq!(read_messages.len(), 10);
    for (i, msg) in read_messages.iter().enumerate() {
        assert_eq!(msg.msg_id.segment_offset, i as u64);
        assert_eq!(msg.payload, format!("message-{}", i).as_bytes());
    }
}

/// Test: Single topic WAL rotation and cloud upload
///
/// Purpose
/// - Validate WAL file rotation triggers and cloud upload process
/// - Ensure messages are uploaded to cloud storage after WAL rotation
///
/// Flow
/// - Create WAL storage with small rotation threshold
/// - Write enough messages to trigger WAL rotation
/// - Wait for uploader to process and upload to cloud
/// - Verify cloud objects are created with correct metadata
///
/// Expected
/// - WAL rotation occurs when size/time thresholds are met
/// - Uploader creates cloud objects in DNB1 format
/// - Cloud object metadata is stored in ETCD
#[tokio::test]
#[ignore]
async fn test_single_topic_wal_rotation_and_upload() {
    let (factory, memory_store) = create_test_factory().await;
    let topic_name = "integration/rotation";

    let storage = factory.for_topic(topic_name).await.expect("create storage");

    // Write enough messages to trigger WAL rotation and upload
    let batch_size = 50;
    for i in 0..batch_size {
        let msg = make_test_message(i, 1, topic_name, i, &format!("rotation-msg-{}", i));
        storage
            .append_message(topic_name, msg)
            .await
            .expect("append message");
    }

    // Wait for uploader to process messages
    let uploaded = wait_for_condition(
        || async { count_cloud_objects(&memory_store, topic_name).await > 0 },
        10000, // 10 second timeout
    )
    .await;

    assert!(uploaded, "Messages should be uploaded to cloud storage");

    // Verify object was created
    let object_count = count_cloud_objects(&memory_store, topic_name).await;
    assert!(object_count > 0, "Should have at least one cloud object");

    // Verify object descriptor
    let descriptor = get_latest_object_descriptor(&memory_store, topic_name).await;
    assert!(descriptor.is_some(), "Should have object descriptor");

    let desc = descriptor.unwrap();
    assert!(desc.start_offset <= desc.end_offset);
    assert!(desc.object_id.starts_with("data-"));
    assert!(desc.object_id.ends_with(".dnb1"));
}

/// Test: Single topic cloud handoff reading
///
/// Purpose
/// - Validate seamless reading from both cloud storage and WAL
/// - Ensure cloud handoff provides complete message history
///
/// Flow
/// - Write messages and wait for cloud upload
/// - Create reader from offset 0 (should use cloud + WAL)
/// - Read all messages and verify ordering and content
///
/// Expected
/// - Reader seamlessly combines cloud and WAL data
/// - All messages are delivered in correct offset order
/// - No gaps or duplicates in message stream
#[tokio::test]
#[ignore]
async fn test_single_topic_cloud_handoff_reading() {
    let (factory, memory_store) = create_test_factory().await;
    let topic_name = "integration/handoff";

    let storage = factory.for_topic(topic_name).await.expect("create storage");

    // Write messages and wait for upload
    let message_count = 30u64;
    for i in 0..message_count {
        let msg = make_test_message(i, 1, topic_name, i, &format!("handoff-msg-{}", i));
        storage
            .append_message(topic_name, msg)
            .await
            .expect("append message");
    }

    // Wait for upload
    let uploaded = wait_for_condition(
        || async { count_cloud_objects(&memory_store, topic_name).await > 0 },
        10000,
    )
    .await;

    assert!(uploaded, "Messages should be uploaded");

    // Create reader that should use cloud handoff for historical data
    let mut reader = storage
        .create_reader(topic_name, StartPosition::Offset(0))
        .await
        .expect("create reader");

    // Read all messages (should come from both cloud and WAL)
    let mut read_messages = Vec::new();
    for _ in 0..message_count {
        if let Some(msg_result) = reader.next().await {
            read_messages.push(msg_result.expect("read message"));
        }
    }

    assert_eq!(read_messages.len(), message_count as usize);

    // Verify message ordering and content
    for (i, msg) in read_messages.iter().enumerate() {
        assert_eq!(msg.msg_id.segment_offset, i as u64);
        assert_eq!(msg.payload, format!("handoff-msg-{}", i).as_bytes());
    }
}

/// Test: Single topic resume from specific position
///
/// Purpose
/// - Validate reading from arbitrary offset positions
/// - Ensure readers can skip historical messages correctly
///
/// Flow
/// - Write initial batch (0-19) and wait for upload
/// - Write additional batch (20-39)
/// - Create reader starting from offset 10
/// - Verify only messages 10-39 are read
///
/// Expected
/// - Reader starts from exact specified offset
/// - Earlier messages (0-9) are skipped
/// - Message sequence is continuous from start offset
#[tokio::test]
async fn test_single_topic_resume_from_position() {
    let (factory, memory_store) = create_test_factory().await;
    let topic_name = "integration/resume";

    let storage = factory.for_topic(topic_name).await.expect("create storage");

    // Write initial batch
    for i in 0..20u64 {
        let msg = make_test_message(i, 1, topic_name, i, &format!("initial-{}", i));
        storage
            .append_message(topic_name, msg)
            .await
            .expect("append message");
    }

    // Wait for upload
    wait_for_condition(
        || async { count_cloud_objects(&memory_store, topic_name).await > 0 },
        10000,
    )
    .await;

    // Write more messages
    for i in 20..40u64 {
        let msg = make_test_message(i, 1, topic_name, i, &format!("additional-{}", i));
        storage
            .append_message(topic_name, msg)
            .await
            .expect("append message");
    }

    // Create reader starting from middle position
    let start_offset = 10u64;
    let mut reader = storage
        .create_reader(topic_name, StartPosition::Offset(start_offset))
        .await
        .expect("create reader");

    // Read remaining messages
    let mut read_messages = Vec::new();
    for _ in start_offset..40 {
        if let Some(msg_result) = reader.next().await {
            read_messages.push(msg_result.expect("read message"));
        }
    }

    assert_eq!(read_messages.len(), 30); // 40 - 10

    // Verify first message is from correct offset
    assert_eq!(read_messages[0].msg_id.segment_offset, start_offset);

    // Verify last message
    assert_eq!(read_messages.last().unwrap().msg_id.segment_offset, 39);
}

/// Test: Single topic tail reading (live messages)
///
/// Purpose
/// - Validate tail reading functionality for live message consumption
/// - Ensure readers can receive messages written after reader creation
///
/// Flow
/// - Write initial messages (0-4)
/// - Create tail reader (StartPosition::Latest)
/// - Write new messages (5-9) after reader creation
/// - Verify reader receives only new messages
///
/// Expected
/// - Tail reader ignores historical messages
/// - New messages are delivered in real-time
/// - Message ordering is preserved for live stream
#[tokio::test]
#[ignore]
async fn test_single_topic_tail_reading() {
    let (factory, _memory_store) = create_test_factory().await;
    let topic_name = "integration/tail";

    let storage = factory.for_topic(topic_name).await.expect("create storage");

    // Write initial messages
    for i in 0..5u64 {
        let msg = make_test_message(i, 1, topic_name, i, &format!("initial-{}", i));
        storage
            .append_message(topic_name, msg)
            .await
            .expect("append message");
    }

    // Create tail reader (starts from latest)
    let mut reader = storage
        .create_reader(topic_name, StartPosition::Latest)
        .await
        .expect("create reader");

    // Write new messages after reader creation
    tokio::spawn({
        let storage = storage.clone();
        let topic_name = topic_name.to_string();
        async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            for i in 5..10u64 {
                let msg = make_test_message(i, 1, &topic_name, i, &format!("tail-{}", i));
                let _ = storage.append_message(&topic_name, msg).await;
            }
        }
    });

    // Read the new messages from tail
    let mut tail_messages = Vec::new();
    for _ in 0..5 {
        // Use timeout to avoid infinite wait
        let timeout = tokio::time::timeout(Duration::from_secs(2), reader.next()).await;
        if let Ok(Some(msg_result)) = timeout {
            tail_messages.push(msg_result.expect("read tail message"));
        }
    }

    assert_eq!(tail_messages.len(), 5);

    // Verify tail messages are the new ones
    for (i, msg) in tail_messages.iter().enumerate() {
        assert_eq!(msg.msg_id.segment_offset, (i + 5) as u64);
        assert_eq!(msg.payload, format!("tail-{}", i + 5).as_bytes());
    }
}

/// Test: Single topic concurrent write and read operations
///
/// Purpose
/// - Validate concurrent write/read operations work correctly
/// - Ensure no race conditions or data corruption under concurrent access
///
/// Flow
/// - Start reader from offset 0
/// - Spawn concurrent writer task writing 20 messages
/// - Read messages as they're being written
/// - Verify all messages are received correctly
///
/// Expected
/// - Concurrent operations complete without errors
/// - All written messages are readable
/// - Message ordering is preserved under concurrency
#[tokio::test]
#[ignore]
async fn test_single_topic_concurrent_write_read() {
    let (factory, _memory_store) = create_test_factory().await;
    let topic_name = "integration/concurrent";

    let storage = factory.for_topic(topic_name).await.expect("create storage");

    // Start reader from beginning
    let mut reader = storage
        .create_reader(topic_name, StartPosition::Offset(0))
        .await
        .expect("create reader");

    // Spawn writer task
    let writer_storage = storage.clone();
    let write_handle = tokio::spawn(async move {
        for i in 0..20u64 {
            let msg = make_test_message(i, 1, topic_name, i, &format!("concurrent-{}", i));
            writer_storage
                .append_message(topic_name, msg)
                .await
                .expect("write message");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });

    // Read messages as they're written
    let mut read_messages = Vec::new();
    for _ in 0..20 {
        let timeout = tokio::time::timeout(Duration::from_secs(5), reader.next()).await;
        if let Ok(Some(msg_result)) = timeout {
            read_messages.push(msg_result.expect("read concurrent message"));
        }
    }

    // Wait for writer to complete
    write_handle.await.expect("writer task");

    assert_eq!(read_messages.len(), 20);

    // Verify message ordering
    for (i, msg) in read_messages.iter().enumerate() {
        assert_eq!(msg.msg_id.segment_offset, i as u64);
        assert_eq!(msg.payload, format!("concurrent-{}", i).as_bytes());
    }
}

/// Test: Single topic large message handling
///
/// Purpose
/// - Validate handling of large messages (1MB+) through WAL and cloud storage
/// - Ensure large messages don't break upload/download processes
///
/// Flow
/// - Create and append 1MB message followed by normal messages
/// - Wait for cloud upload completion
/// - Read back all messages and verify integrity
///
/// Expected
/// - Large messages are stored and uploaded successfully
/// - Large message content is preserved exactly
/// - Normal messages after large ones work correctly
#[tokio::test]
async fn test_single_topic_large_message_handling() {
    let (factory, memory_store) = create_test_factory().await;
    let topic_name = "integration/large";

    let storage = factory.for_topic(topic_name).await.expect("create storage");

    // Create large message (1MB)
    let large_payload = vec![0u8; 1024 * 1024];
    let large_msg = make_test_message(0, 1, topic_name, 0, "");
    let mut large_msg = large_msg;
    large_msg.payload = large_payload.clone();

    // Append large message
    storage
        .append_message(topic_name, large_msg)
        .await
        .expect("append large message");

    // Add some normal messages
    for i in 1..5u64 {
        let msg = make_test_message(i, 1, topic_name, i, &format!("normal-{}", i));
        storage
            .append_message(topic_name, msg)
            .await
            .expect("append normal message");
    }

    // Wait for upload
    wait_for_condition(
        || async { count_cloud_objects(&memory_store, topic_name).await > 0 },
        15000, // Longer timeout for large message
    )
    .await;

    // Read back all messages
    let mut reader = storage
        .create_reader(topic_name, StartPosition::Offset(0))
        .await
        .expect("create reader");

    let mut read_messages = Vec::new();
    for _ in 0..5 {
        if let Some(msg_result) = reader.next().await {
            read_messages.push(msg_result.expect("read message"));
        }
    }

    assert_eq!(read_messages.len(), 5);

    // Verify large message
    assert_eq!(read_messages[0].payload.len(), 1024 * 1024);
    assert_eq!(read_messages[0].payload, large_payload);

    // Verify normal messages
    for i in 1..5 {
        assert_eq!(read_messages[i].msg_id.segment_offset, i as u64);
        assert_eq!(read_messages[i].payload, format!("normal-{}", i).as_bytes());
    }
}

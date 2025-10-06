mod common;

use common::{count_cloud_objects, create_test_factory, make_test_message, wait_for_condition};
use danube_core::storage::{PersistentStorage, StartPosition};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Barrier;
use tokio_stream::StreamExt;

/// Test: Multiple subscriptions with independent reading positions
///
/// Purpose
/// - Validate that multiple readers can start from different offsets independently
/// - Ensure each subscription maintains its own position without interference
///
/// Flow
/// - Write 20 messages to topic
/// - Create 3 readers starting from offsets 0, 5, and 10
/// - Read messages from each reader and verify correct ranges
///
/// Expected
/// - Reader 1 gets messages 0-19 (20 messages)
/// - Reader 2 gets messages 5-19 (15 messages)
/// - Reader 3 gets messages 10-19 (10 messages)
/// - Each reader maintains independent position
#[tokio::test]
async fn test_multiple_subscriptions_independent_positions() {
    let (factory, _memory_store) = create_test_factory().await;
    let topic_name = "integration/multi-subsc-positions";

    let storage = factory.for_topic(topic_name).await.expect("create storage");

    // Write initial batch of messages
    for i in 0..20u64 {
        let msg = make_test_message(i, 1, topic_name, i, &format!("msg-{}", i));
        storage
            .append_message(topic_name, msg)
            .await
            .expect("append message");
    }

    // Create multiple readers starting from different positions
    let mut reader1 = storage
        .create_reader(topic_name, StartPosition::Offset(0))
        .await
        .expect("create reader1");
    let mut reader2 = storage
        .create_reader(topic_name, StartPosition::Offset(5))
        .await
        .expect("create reader2");
    let mut reader3 = storage
        .create_reader(topic_name, StartPosition::Offset(10))
        .await
        .expect("create reader3");

    // Reader 1: Read from beginning (0-19)
    let mut messages1 = Vec::new();
    for _ in 0..20 {
        if let Some(msg_result) = reader1.next().await {
            messages1.push(msg_result.expect("read message"));
        }
    }

    // Reader 2: Read from offset 5 (5-19)
    let mut messages2 = Vec::new();
    for _ in 0..15 {
        if let Some(msg_result) = reader2.next().await {
            messages2.push(msg_result.expect("read message"));
        }
    }

    // Reader 3: Read from offset 10 (10-19)
    let mut messages3 = Vec::new();
    for _ in 0..10 {
        if let Some(msg_result) = reader3.next().await {
            messages3.push(msg_result.expect("read message"));
        }
    }

    // Verify each reader got the correct messages
    assert_eq!(messages1.len(), 20);
    assert_eq!(messages1[0].msg_id.segment_offset, 0);
    assert_eq!(messages1[19].msg_id.segment_offset, 19);

    assert_eq!(messages2.len(), 15);
    assert_eq!(messages2[0].msg_id.segment_offset, 5);
    assert_eq!(messages2[14].msg_id.segment_offset, 19);

    assert_eq!(messages3.len(), 10);
    assert_eq!(messages3[0].msg_id.segment_offset, 10);
    assert_eq!(messages3[9].msg_id.segment_offset, 19);
}

/// Test: Multiple subscriptions concurrent reading
///
/// Purpose
/// - Validate that multiple readers can operate concurrently without interference
/// - Ensure concurrent access doesn't cause race conditions or data corruption
///
/// Flow
/// - Write initial messages, then spawn 3 concurrent readers
/// - Start writer task to add more messages during reading
/// - Synchronize all tasks and verify each reader gets correct messages
///
/// Expected
/// - All readers complete successfully without errors
/// - Each reader receives messages in correct order
/// - No data corruption under concurrent access
#[tokio::test]
async fn test_multiple_subscriptions_concurrent_reading() {
    let (factory, _memory_store) = create_test_factory().await;
    let topic_name = "integration/multi-subsc-concurrent";

    let storage = factory.for_topic(topic_name).await.expect("create storage");

    // Write initial messages
    for i in 0..10u64 {
        let msg = make_test_message(i, 1, topic_name, i, &format!("initial-{}", i));
        storage
            .append_message(topic_name, msg)
            .await
            .expect("append message");
    }

    // Create multiple concurrent readers
    let storage1 = storage.clone();
    let storage2 = storage.clone();
    let storage3 = storage.clone();

    let barrier = Arc::new(Barrier::new(4)); // 3 readers + 1 writer

    // Spawn concurrent readers
    let reader1_handle = {
        let barrier = barrier.clone();
        let topic_name = topic_name.to_string();
        tokio::spawn(async move {
            let mut reader = storage1
                .create_reader(&topic_name, StartPosition::Offset(0))
                .await
                .expect("create reader1");
            barrier.wait().await;

            let mut messages = Vec::new();
            for _ in 0..15 {
                let timeout = tokio::time::timeout(Duration::from_secs(3), reader.next()).await;
                if let Ok(Some(msg_result)) = timeout {
                    messages.push(msg_result.expect("read message"));
                }
            }
            messages
        })
    };

    let reader2_handle = {
        let barrier = barrier.clone();
        let topic_name = topic_name.to_string();
        tokio::spawn(async move {
            let mut reader = storage2
                .create_reader(&topic_name, StartPosition::Offset(5))
                .await
                .expect("create reader2");
            barrier.wait().await;

            let mut messages = Vec::new();
            for _ in 0..10 {
                let timeout = tokio::time::timeout(Duration::from_secs(3), reader.next()).await;
                if let Ok(Some(msg_result)) = timeout {
                    messages.push(msg_result.expect("read message"));
                }
            }
            messages
        })
    };

    let reader3_handle = {
        let barrier = barrier.clone();
        let topic_name = topic_name.to_string();
        tokio::spawn(async move {
            let mut reader = storage3
                .create_reader(&topic_name, StartPosition::Latest)
                .await
                .expect("create reader3");
            barrier.wait().await;

            let mut messages = Vec::new();
            for _ in 0..5 {
                let timeout = tokio::time::timeout(Duration::from_secs(3), reader.next()).await;
                if let Ok(Some(msg_result)) = timeout {
                    messages.push(msg_result.expect("read message"));
                }
            }
            messages
        })
    };

    // Writer task - writes new messages after readers start
    let writer_handle = {
        let barrier = barrier.clone();
        tokio::spawn(async move {
            barrier.wait().await;
            tokio::time::sleep(Duration::from_millis(100)).await;

            for i in 10..15u64 {
                let msg = make_test_message(i, 1, topic_name, i, &format!("concurrent-{}", i));
                storage
                    .append_message(topic_name, msg)
                    .await
                    .expect("append message");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
    };

    // Wait for all tasks to complete
    let (messages1, messages2, messages3, _) = tokio::join!(
        reader1_handle,
        reader2_handle,
        reader3_handle,
        writer_handle
    );

    let messages1 = messages1.expect("reader1 task");
    let messages2 = messages2.expect("reader2 task");
    let messages3 = messages3.expect("reader3 task");

    // Verify reader1 got all messages (0-14)
    assert_eq!(messages1.len(), 15);
    assert_eq!(messages1[0].msg_id.segment_offset, 0);
    assert_eq!(messages1[14].msg_id.segment_offset, 14);

    // Verify reader2 got messages from offset 5 (5-14)
    assert_eq!(messages2.len(), 10);
    assert_eq!(messages2[0].msg_id.segment_offset, 5);
    assert_eq!(messages2[9].msg_id.segment_offset, 14);

    // Verify reader3 got only new messages (10-14)
    assert_eq!(messages3.len(), 5);
    assert_eq!(messages3[0].msg_id.segment_offset, 10);
    assert_eq!(messages3[4].msg_id.segment_offset, 14);
}

#[tokio::test]
async fn test_multiple_subscriptions_with_cloud_handoff() {
    let (factory, memory_store) = create_test_factory().await;
    let topic_name = "integration/multi-subsc-cloud";

    let storage = factory.for_topic(topic_name).await.expect("create storage");

    // Write enough messages to trigger upload
    for i in 0..50u64 {
        let msg = make_test_message(i, 1, topic_name, i, &format!("cloud-msg-{}", i));
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

    // Write more messages after upload
    for i in 50..70u64 {
        let msg = make_test_message(i, 1, topic_name, i, &format!("post-upload-{}", i));
        storage
            .append_message(topic_name, msg)
            .await
            .expect("append message");
    }

    // Create multiple readers that will use cloud handoff
    let mut reader1 = storage
        .create_reader(topic_name, StartPosition::Offset(0))
        .await
        .expect("create reader1");
    let mut reader2 = storage
        .create_reader(topic_name, StartPosition::Offset(20))
        .await
        .expect("create reader2");
    let mut reader3 = storage
        .create_reader(topic_name, StartPosition::Offset(45))
        .await
        .expect("create reader3");

    // Reader 1: Read all messages (0-69)
    let mut messages1 = Vec::new();
    for _ in 0..70 {
        if let Some(msg_result) = reader1.next().await {
            messages1.push(msg_result.expect("read message"));
        }
    }

    // Reader 2: Read from middle (20-69)
    let mut messages2 = Vec::new();
    for _ in 0..50 {
        if let Some(msg_result) = reader2.next().await {
            messages2.push(msg_result.expect("read message"));
        }
    }

    // Reader 3: Read mostly from WAL (45-69)
    let mut messages3 = Vec::new();
    for _ in 0..25 {
        if let Some(msg_result) = reader3.next().await {
            messages3.push(msg_result.expect("read message"));
        }
    }

    // Verify all readers got correct messages
    assert_eq!(messages1.len(), 70);
    assert_eq!(messages1[0].msg_id.segment_offset, 0);
    assert_eq!(messages1[69].msg_id.segment_offset, 69);

    assert_eq!(messages2.len(), 50);
    assert_eq!(messages2[0].msg_id.segment_offset, 20);
    assert_eq!(messages2[49].msg_id.segment_offset, 69);

    assert_eq!(messages3.len(), 25);
    assert_eq!(messages3[0].msg_id.segment_offset, 45);
    assert_eq!(messages3[24].msg_id.segment_offset, 69);

    // Verify message content integrity across cloud/WAL boundary
    for (i, msg) in messages1.iter().enumerate() {
        let expected_payload = if i < 50 {
            format!("cloud-msg-{}", i)
        } else {
            format!("post-upload-{}", i)
        };
        assert_eq!(msg.payload, expected_payload.as_bytes());
    }
}

#[tokio::test]
async fn test_multiple_subscriptions_different_speeds() {
    let (factory, _memory_store) = create_test_factory().await;
    let topic_name = "integration/multi-subsc-speeds";

    let storage = factory.for_topic(topic_name).await.expect("create storage");

    // Write initial messages
    for i in 0..20u64 {
        let msg = make_test_message(i, 1, topic_name, i, &format!("speed-msg-{}", i));
        storage
            .append_message(topic_name, msg)
            .await
            .expect("append message");
    }

    let storage1 = storage.clone();
    let storage2 = storage.clone();
    let storage3 = storage.clone();

    // Fast reader - reads immediately
    let fast_reader_handle = {
        let topic_name = topic_name.to_string();
        tokio::spawn(async move {
            let mut reader = storage1
                .create_reader(&topic_name, StartPosition::Offset(0))
                .await
                .expect("create fast reader");
            let mut messages = Vec::new();
            for _ in 0..20 {
                if let Some(msg_result) = reader.next().await {
                    messages.push(msg_result.expect("read message"));
                }
            }
            messages
        })
    };

    // Medium reader - reads with small delays
    let medium_reader_handle = {
        let topic_name = topic_name.to_string();
        tokio::spawn(async move {
            let mut reader = storage2
                .create_reader(&topic_name, StartPosition::Offset(0))
                .await
                .expect("create medium reader");
            let mut messages = Vec::new();
            for _ in 0..20 {
                if let Some(msg_result) = reader.next().await {
                    messages.push(msg_result.expect("read message"));
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
            messages
        })
    };

    // Slow reader - reads with longer delays
    let slow_reader_handle = tokio::spawn(async move {
        let topic_name = topic_name.to_string();
        let mut reader = storage3
            .create_reader(&topic_name, StartPosition::Offset(0))
            .await
            .expect("create slow reader");
        let mut messages = Vec::new();
        for _ in 0..20 {
            if let Some(msg_result) = reader.next().await {
                messages.push(msg_result.expect("read message"));
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
        messages
    });

    // Wait for all readers to complete
    let (fast_messages, medium_messages, slow_messages) =
        tokio::join!(fast_reader_handle, medium_reader_handle, slow_reader_handle);

    let fast_messages = fast_messages.expect("fast reader task");
    let medium_messages = medium_messages.expect("medium reader task");
    let slow_messages = slow_messages.expect("slow reader task");

    // All readers should get the same messages despite different speeds
    assert_eq!(fast_messages.len(), 20);
    assert_eq!(medium_messages.len(), 20);
    assert_eq!(slow_messages.len(), 20);

    // Verify message content is identical
    for i in 0..20 {
        assert_eq!(fast_messages[i].msg_id.segment_offset, i as u64);
        assert_eq!(medium_messages[i].msg_id.segment_offset, i as u64);
        assert_eq!(slow_messages[i].msg_id.segment_offset, i as u64);

        assert_eq!(fast_messages[i].payload, medium_messages[i].payload);
        assert_eq!(fast_messages[i].payload, slow_messages[i].payload);
    }
}

#[tokio::test]
async fn test_multiple_subscriptions_tail_reading() {
    let (factory, _memory_store) = create_test_factory().await;
    let topic_name = "integration/multi-subsc-tail";

    let storage = factory.for_topic(topic_name).await.expect("create storage");

    // Write initial messages
    for i in 0..10u64 {
        let msg = make_test_message(i, 1, topic_name, i, &format!("initial-{}", i));
        storage
            .append_message(topic_name, msg)
            .await
            .expect("append message");
    }

    // Create multiple tail readers
    let mut tail_reader1 = storage
        .create_reader(topic_name, StartPosition::Latest)
        .await
        .expect("create tail reader1");
    let mut tail_reader2 = storage
        .create_reader(topic_name, StartPosition::Latest)
        .await
        .expect("create tail reader2");
    let mut tail_reader3 = storage
        .create_reader(topic_name, StartPosition::Latest)
        .await
        .expect("create tail reader3");

    // Write new messages after readers are created
    tokio::spawn({
        let storage = storage.clone();
        async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            for i in 10..20u64 {
                let msg = make_test_message(i, 1, topic_name, i, &format!("tail-{}", i));
                let _ = storage.append_message(topic_name, msg).await;
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    });

    // Each reader should get the new messages
    let mut tail_messages1 = Vec::new();
    let mut tail_messages2 = Vec::new();
    let mut tail_messages3 = Vec::new();

    for _ in 0..10 {
        let timeout = Duration::from_secs(3);

        if let Ok(Some(msg_result)) = tokio::time::timeout(timeout, tail_reader1.next()).await {
            tail_messages1.push(msg_result.expect("read tail message"));
        }

        if let Ok(Some(msg_result)) = tokio::time::timeout(timeout, tail_reader2.next()).await {
            tail_messages2.push(msg_result.expect("read tail message"));
        }

        if let Ok(Some(msg_result)) = tokio::time::timeout(timeout, tail_reader3.next()).await {
            tail_messages3.push(msg_result.expect("read tail message"));
        }
    }

    // All tail readers should get the same new messages
    assert_eq!(tail_messages1.len(), 10);
    assert_eq!(tail_messages2.len(), 10);
    assert_eq!(tail_messages3.len(), 10);

    // Verify all readers got messages starting from offset 10
    for i in 0..10 {
        assert_eq!(tail_messages1[i].msg_id.segment_offset, (i + 10) as u64);
        assert_eq!(tail_messages2[i].msg_id.segment_offset, (i + 10) as u64);
        assert_eq!(tail_messages3[i].msg_id.segment_offset, (i + 10) as u64);

        let expected_payload = format!("tail-{}", i + 10);
        assert_eq!(tail_messages1[i].payload, expected_payload.as_bytes());
        assert_eq!(tail_messages2[i].payload, expected_payload.as_bytes());
        assert_eq!(tail_messages3[i].payload, expected_payload.as_bytes());
    }
}

#[tokio::test]
async fn test_multiple_subscriptions_mixed_start_positions() {
    let (factory, memory_store) = create_test_factory().await;
    let topic_name = "integration/multi-subsc-mixed";

    let storage = factory.for_topic(topic_name).await.expect("create storage");

    // Write messages and trigger upload
    for i in 0..30u64 {
        let msg = make_test_message(i, 1, topic_name, i, &format!("mixed-{}", i));
        storage
            .append_message(topic_name, msg)
            .await
            .expect("append message");
    }

    // Wait for some upload activity
    wait_for_condition(
        || async { count_cloud_objects(&memory_store, topic_name).await > 0 },
        8000,
    )
    .await;

    // Write more messages
    for i in 30..40u64 {
        let msg = make_test_message(i, 1, topic_name, i, &format!("post-upload-{}", i));
        storage
            .append_message(topic_name, msg)
            .await
            .expect("append message");
    }

    // Create readers with mixed start positions
    let mut earliest_reader = storage
        .create_reader(topic_name, StartPosition::Offset(0))
        .await
        .expect("create earliest reader");
    let mut middle_reader = storage
        .create_reader(topic_name, StartPosition::Offset(15))
        .await
        .expect("create middle reader");
    let mut recent_reader = storage
        .create_reader(topic_name, StartPosition::Offset(35))
        .await
        .expect("create recent reader");
    let mut latest_reader = storage
        .create_reader(topic_name, StartPosition::Latest)
        .await
        .expect("create latest reader");

    // Write final batch for latest reader
    tokio::spawn({
        let storage = storage.clone();
        async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            for i in 40..45u64 {
                let msg = make_test_message(i, 1, topic_name, i, &format!("final-{}", i));
                let _ = storage.append_message(topic_name, msg).await;
            }
        }
    });

    // Read from each subscription
    let mut earliest_messages = Vec::new();
    for _ in 0..45 {
        if let Some(msg_result) = earliest_reader.next().await {
            earliest_messages.push(msg_result.expect("read earliest message"));
        }
    }

    let mut middle_messages = Vec::new();
    for _ in 0..30 {
        if let Some(msg_result) = middle_reader.next().await {
            middle_messages.push(msg_result.expect("read middle message"));
        }
    }

    let mut recent_messages = Vec::new();
    for _ in 0..10 {
        if let Some(msg_result) = recent_reader.next().await {
            recent_messages.push(msg_result.expect("read recent message"));
        }
    }

    let mut latest_messages = Vec::new();
    for _ in 0..5 {
        let timeout = tokio::time::timeout(Duration::from_secs(2), latest_reader.next()).await;
        if let Ok(Some(msg_result)) = timeout {
            latest_messages.push(msg_result.expect("read latest message"));
        }
    }

    // Verify each reader got correct range of messages
    assert_eq!(earliest_messages.len(), 45);
    assert_eq!(earliest_messages[0].msg_id.segment_offset, 0);
    assert_eq!(earliest_messages[44].msg_id.segment_offset, 44);

    assert_eq!(middle_messages.len(), 30);
    assert_eq!(middle_messages[0].msg_id.segment_offset, 15);
    assert_eq!(middle_messages[29].msg_id.segment_offset, 44);

    assert_eq!(recent_messages.len(), 10);
    assert_eq!(recent_messages[0].msg_id.segment_offset, 35);
    assert_eq!(recent_messages[9].msg_id.segment_offset, 44);

    assert_eq!(latest_messages.len(), 5);
    assert_eq!(latest_messages[0].msg_id.segment_offset, 40);
    assert_eq!(latest_messages[4].msg_id.segment_offset, 44);
}

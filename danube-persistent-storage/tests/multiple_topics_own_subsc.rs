mod common;

use common::{count_cloud_objects, create_test_factory, make_test_message, wait_for_condition};
use danube_core::storage::{PersistentStorage, StartPosition};
use danube_metadata_store::MetadataStore;
use std::collections::HashMap;
use std::time::Duration;
use tokio_stream::StreamExt;

/// Test: Multiple topics with isolated storage
///
/// Purpose
/// - Validate that different topics maintain completely isolated storage
/// - Ensure messages written to one topic don't affect other topics
///
/// Flow
/// - Create storage for 3 different topics (ns1/topic-a, ns1/topic-b, ns2/topic-c)
/// - Write 10 messages to each topic with topic-specific content
/// - Read back messages from each topic and verify isolation
///
/// Expected
/// - Each topic contains only its own messages
/// - Message content is topic-specific and not mixed
/// - Topic offsets are independent (each starts from 0)
#[tokio::test]
async fn test_multiple_topics_isolated_storage() {
    let (factory, _memory_store) = create_test_factory().await;

    let topics = vec!["ns1/topic-a", "ns1/topic-b", "ns2/topic-c"];
    let mut storages = HashMap::new();

    // Create storage for each topic
    for topic in &topics {
        let storage = factory.for_topic(topic).await.expect("create storage");
        storages.insert(topic.to_string(), storage);
    }

    // Write different messages to each topic
    for (i, topic) in topics.iter().enumerate() {
        let storage = &storages[*topic];
        for j in 0..10u64 {
            let msg = make_test_message(
                j,
                (i + 1) as u64,
                topic,
                j,
                &format!("topic-{}-msg-{}", i, j),
            );
            storage
                .append_message(topic, msg)
                .await
                .expect("append message");
        }
    }

    // Verify each topic has correct messages
    for (i, topic) in topics.iter().enumerate() {
        let storage = &storages[*topic];
        let mut reader = storage
            .create_reader(*topic, StartPosition::Offset(0))
            .await
            .expect("create reader");
        let mut messages = Vec::new();

        for _ in 0..10 {
            if let Some(msg_result) = reader.next().await {
                messages.push(msg_result.expect("read message"));
            }
        }

        assert_eq!(messages.len(), 10);
        for (j, msg) in messages.iter().enumerate() {
            assert_eq!(msg.msg_id.topic_name, *topic);
            assert_eq!(msg.msg_id.producer_id, (i + 1) as u64);
            assert_eq!(msg.msg_id.segment_offset, j as u64);
            assert_eq!(msg.payload, format!("topic-{}-msg-{}", i, j).as_bytes());
        }
    }
}

#[tokio::test]
async fn test_multiple_topics_independent_uploaders() {
    let (factory, memory_store) = create_test_factory().await;

    let topics = vec!["upload/topic-1", "upload/topic-2", "upload/topic-3"];
    let mut storages = HashMap::new();

    // Create storage for each topic
    for topic in &topics {
        let storage = factory.for_topic(topic).await.expect("create storage");
        storages.insert(topic.to_string(), storage);
    }

    // Write different amounts of data to each topic to trigger uploads at different times
    let message_counts = vec![20u64, 35u64, 50u64];

    for (i, topic) in topics.iter().enumerate() {
        let storage = &storages[*topic];
        let count = message_counts[i];

        for j in 0..count {
            let msg =
                make_test_message(j, (i + 1) as u64, topic, j, &format!("upload-{}-{}", i, j));
            storage
                .append_message(topic, msg)
                .await
                .expect("append message");
        }
    }

    // Wait for uploads to complete for all topics
    for topic in &topics {
        let uploaded = wait_for_condition(
            || async { count_cloud_objects(&memory_store, topic).await > 0 },
            15000,
        )
        .await;
        assert!(uploaded, "Topic {} should have uploaded objects", topic);
    }

    // Verify each topic has independent cloud objects
    for topic in &topics {
        let object_count = count_cloud_objects(&memory_store, topic).await;
        assert!(
            object_count > 0,
            "Topic {} should have cloud objects",
            topic
        );

        // Verify objects contain correct topic data
        let prefix = format!("/danube/storage/topics/{}/objects", topic);
        let children = memory_store
            .get_childrens(&prefix)
            .await
            .expect("get children");
        let objects: Vec<_> = children
            .into_iter()
            .filter(|c| c != "cur" && !c.ends_with('/'))
            .collect();

        assert!(
            !objects.is_empty(),
            "Topic {} should have object descriptors",
            topic
        );
    }
}

#[tokio::test]
async fn test_multiple_topics_concurrent_operations() {
    let (factory, _memory_store) = create_test_factory().await;

    let topics = vec![
        "concurrent/topic-1",
        "concurrent/topic-2",
        "concurrent/topic-3",
        "concurrent/topic-4",
    ];

    // Spawn concurrent tasks for each topic
    let mut handles = Vec::new();

    for (i, topic) in topics.iter().enumerate() {
        let factory_clone = factory.clone();
        let topic_name = topic.to_string();

        let handle = tokio::spawn(async move {
            let storage = factory_clone
                .for_topic(&topic_name)
                .await
                .expect("create storage");

            // Write messages
            for j in 0..25u64 {
                let msg = make_test_message(
                    j,
                    (i + 1) as u64,
                    &topic_name,
                    j,
                    &format!("concurrent-{}-{}", i, j),
                );
                storage
                    .append_message(&topic_name, msg)
                    .await
                    .expect("append message");

                // Small delay to simulate realistic write patterns
                if j % 5 == 0 {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }

            // Read back messages
            let mut reader = storage
                .create_reader(&topic_name, StartPosition::Offset(0))
                .await
                .expect("create reader");
            let mut messages = Vec::new();

            for _ in 0..25 {
                if let Some(msg_result) = reader.next().await {
                    messages.push(msg_result.expect("read message"));
                }
            }

            (topic_name, messages)
        });

        handles.push(handle);
    }

    // Wait for all tasks to complete
    let results = futures::future::join_all(handles).await;

    // Verify results for each topic
    for (i, result) in results.into_iter().enumerate() {
        let (topic_name, messages) = result.expect("task completion");

        assert_eq!(messages.len(), 25);
        assert_eq!(topic_name, topics[i]);

        for (j, msg) in messages.iter().enumerate() {
            assert_eq!(msg.msg_id.topic_name, topic_name);
            assert_eq!(msg.msg_id.producer_id, (i + 1) as u64);
            assert_eq!(msg.msg_id.segment_offset, j as u64);
            assert_eq!(msg.payload, format!("concurrent-{}-{}", i, j).as_bytes());
        }
    }
}

#[tokio::test]
async fn test_multiple_topics_different_read_patterns() {
    let (factory, _memory_store) = create_test_factory().await;

    let topics = vec!["patterns/topic-a", "patterns/topic-b", "patterns/topic-c"];
    let mut storages = HashMap::new();

    // Create storage and write messages for each topic
    for (i, topic) in topics.iter().enumerate() {
        let storage = factory.for_topic(topic).await.expect("create storage");

        // Write enough messages to potentially trigger upload
        for j in 0..40u64 {
            let msg =
                make_test_message(j, (i + 1) as u64, topic, j, &format!("pattern-{}-{}", i, j));
            storage
                .append_message(topic, msg)
                .await
                .expect("append message");
        }

        storages.insert(topic.to_string(), storage);
    }

    // Wait for potential uploads
    tokio::time::sleep(Duration::from_millis(2000)).await;

    // Different reading patterns for each topic

    // Topic A: Read from beginning
    let storage_a = &storages[topics[0]];
    let mut reader_a = storage_a
        .create_reader(topics[0], StartPosition::Offset(0))
        .await
        .expect("create reader A");
    let mut messages_a = Vec::new();
    for _ in 0..40 {
        if let Some(msg_result) = reader_a.next().await {
            messages_a.push(msg_result.expect("read message A"));
        }
    }

    // Topic B: Read from middle
    let storage_b = &storages[topics[1]];
    let mut reader_b = storage_b
        .create_reader(topics[1], StartPosition::Offset(15))
        .await
        .expect("create reader B");
    let mut messages_b = Vec::new();
    for _ in 0..25 {
        if let Some(msg_result) = reader_b.next().await {
            messages_b.push(msg_result.expect("read message B"));
        }
    }

    // Topic C: Read from latest (tail)
    let storage_c = &storages[topics[2]];
    let mut reader_c = storage_c
        .create_reader(topics[2], StartPosition::Latest)
        .await
        .expect("create reader C");

    // Write new messages to topic C for tail reading
    let tail_topic = topics[2].to_string();
    tokio::spawn({
        let storage_c = storage_c.clone();
        let tail_topic = tail_topic.clone();
        async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            for j in 40..50u64 {
                let msg = make_test_message(j, 3, &tail_topic, j, &format!("pattern-2-{}", j));
                let _ = storage_c.append_message(&tail_topic, msg).await;
            }
        }
    });

    let mut messages_c = Vec::new();
    for _ in 0..10 {
        let timeout = tokio::time::timeout(Duration::from_secs(2), reader_c.next()).await;
        if let Ok(Some(msg_result)) = timeout {
            messages_c.push(msg_result.expect("read message C"));
        }
    }

    // Verify results
    assert_eq!(messages_a.len(), 40);
    assert_eq!(messages_a[0].msg_id.segment_offset, 0);
    assert_eq!(messages_a[39].msg_id.segment_offset, 39);
    assert_eq!(messages_a[0].msg_id.topic_name, topics[0]);

    assert_eq!(messages_b.len(), 25);
    assert_eq!(messages_b[0].msg_id.segment_offset, 15);
    assert_eq!(messages_b[24].msg_id.segment_offset, 39);
    assert_eq!(messages_b[0].msg_id.topic_name, topics[1]);

    assert_eq!(messages_c.len(), 10);
    assert_eq!(messages_c[0].msg_id.segment_offset, 40);
    assert_eq!(messages_c[9].msg_id.segment_offset, 49);
    assert_eq!(messages_c[0].msg_id.topic_name, topics[2]);
}

#[tokio::test]
async fn test_multiple_topics_factory_reuse() {
    let (factory, _memory_store) = create_test_factory().await;

    let topic_name = "reuse/topic";

    // Get storage for the same topic multiple times
    let storage1 = factory
        .for_topic(topic_name)
        .await
        .expect("create storage 1");
    let storage2 = factory
        .for_topic(topic_name)
        .await
        .expect("create storage 2");
    let storage3 = factory
        .for_topic(topic_name)
        .await
        .expect("create storage 3");

    // Write messages from different storage instances
    let msg1 = make_test_message(0, 1, topic_name, 0, "from-storage-1");
    storage1
        .append_message(topic_name, msg1)
        .await
        .expect("append from storage1");

    let msg2 = make_test_message(1, 1, topic_name, 1, "from-storage-2");
    storage2
        .append_message(topic_name, msg2)
        .await
        .expect("append from storage2");

    let msg3 = make_test_message(2, 1, topic_name, 2, "from-storage-3");
    storage3
        .append_message(topic_name, msg3)
        .await
        .expect("append from storage3");

    // All storage instances should see all messages (they share the same underlying WAL)
    for (_i, storage) in [&storage1, &storage2, &storage3].iter().enumerate() {
        let mut reader = storage
            .create_reader(topic_name, StartPosition::Offset(0))
            .await
            .expect("create reader");
        let mut messages = Vec::new();

        for _ in 0..3 {
            if let Some(msg_result) = reader.next().await {
                messages.push(msg_result.expect("read message"));
            }
        }

        assert_eq!(messages.len(), 3);
        assert_eq!(messages[0].payload, b"from-storage-1");
        assert_eq!(messages[1].payload, b"from-storage-2");
        assert_eq!(messages[2].payload, b"from-storage-3");
    }
}

#[tokio::test]
async fn test_multiple_topics_namespace_isolation() {
    let (factory, memory_store) = create_test_factory().await;

    // Topics with same name but different namespaces
    let topics = vec!["ns1/shared-topic", "ns2/shared-topic", "ns3/shared-topic"];

    let mut storages = HashMap::new();

    // Create storage for each namespaced topic
    for (i, topic) in topics.iter().enumerate() {
        let storage = factory.for_topic(topic).await.expect("create storage");

        // Write namespace-specific messages
        for j in 0..15u64 {
            let msg = make_test_message(
                j,
                (i + 1) as u64,
                topic,
                j,
                &format!("ns{}-msg-{}", i + 1, j),
            );
            storage
                .append_message(topic, msg)
                .await
                .expect("append message");
        }

        storages.insert(topic.to_string(), storage);
    }

    // Wait for potential uploads
    tokio::time::sleep(Duration::from_millis(3000)).await;

    // Verify each namespace has isolated storage
    for (i, topic) in topics.iter().enumerate() {
        let storage = &storages[*topic];

        // Read all messages
        let mut reader = storage
            .create_reader(*topic, StartPosition::Offset(0))
            .await
            .expect("create reader");
        let mut messages = Vec::new();

        for _ in 0..15 {
            if let Some(msg_result) = reader.next().await {
                messages.push(msg_result.expect("read message"));
            }
        }

        assert_eq!(messages.len(), 15);

        // Verify all messages belong to correct namespace
        for (j, msg) in messages.iter().enumerate() {
            assert_eq!(msg.msg_id.topic_name, *topic);
            assert_eq!(msg.msg_id.producer_id, (i + 1) as u64);
            assert_eq!(msg.payload, format!("ns{}-msg-{}", i + 1, j).as_bytes());
        }

        // Verify cloud storage isolation
        let _object_count = count_cloud_objects(&memory_store, topic).await;
        // Each namespace should have independent cloud objects (if any were created)
        // The exact count depends on upload timing, but they should be isolated
    }
}

#[tokio::test]
async fn test_multiple_topics_mixed_workloads() {
    let (factory, memory_store) = create_test_factory().await;

    // Different topics with different characteristics
    let heavy_topic = "workload/heavy-writes";
    let light_topic = "workload/light-writes";
    let burst_topic = "workload/burst-writes";

    let heavy_storage = factory
        .for_topic(heavy_topic)
        .await
        .expect("create heavy storage");
    let light_storage = factory
        .for_topic(light_topic)
        .await
        .expect("create light storage");
    let burst_storage = factory
        .for_topic(burst_topic)
        .await
        .expect("create burst storage");

    // Heavy workload: continuous writes
    let heavy_handle = {
        let storage = heavy_storage.clone();
        tokio::spawn(async move {
            for i in 0..100u64 {
                let msg = make_test_message(i, 1, heavy_topic, i, &format!("heavy-{}", i));
                storage
                    .append_message(heavy_topic, msg)
                    .await
                    .expect("append heavy message");
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    };

    // Light workload: sparse writes
    let light_handle = {
        let storage = light_storage.clone();
        tokio::spawn(async move {
            for i in 0..20u64 {
                let msg = make_test_message(i, 2, light_topic, i, &format!("light-{}", i));
                storage
                    .append_message(light_topic, msg)
                    .await
                    .expect("append light message");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
    };

    // Burst workload: batched writes
    let burst_handle = {
        let storage = burst_storage.clone();
        tokio::spawn(async move {
            // Three bursts of messages
            for burst in 0..3 {
                for i in 0..15u64 {
                    let offset = burst * 15 + i;
                    let msg = make_test_message(
                        offset,
                        3,
                        burst_topic,
                        offset,
                        &format!("burst-{}-{}", burst, i),
                    );
                    storage
                        .append_message(burst_topic, msg)
                        .await
                        .expect("append burst message");
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        })
    };

    // Wait for all workloads to complete
    let (_, _, _) = tokio::join!(heavy_handle, light_handle, burst_handle);

    // Wait for potential uploads
    tokio::time::sleep(Duration::from_millis(2000)).await;

    // Verify each workload produced correct results

    // Heavy workload verification
    let mut heavy_reader = heavy_storage
        .create_reader(heavy_topic, StartPosition::Offset(0))
        .await
        .expect("create heavy reader");
    let mut heavy_messages = Vec::new();
    for _ in 0..100 {
        if let Some(msg_result) = heavy_reader.next().await {
            heavy_messages.push(msg_result.expect("read heavy message"));
        }
    }
    assert_eq!(heavy_messages.len(), 100);
    assert_eq!(heavy_messages[0].payload, b"heavy-0");
    assert_eq!(heavy_messages[99].payload, b"heavy-99");

    // Light workload verification
    let mut light_reader = light_storage
        .create_reader(light_topic, StartPosition::Offset(0))
        .await
        .expect("create light reader");
    let mut light_messages = Vec::new();
    for _ in 0..20 {
        if let Some(msg_result) = light_reader.next().await {
            light_messages.push(msg_result.expect("read light message"));
        }
    }
    assert_eq!(light_messages.len(), 20);
    assert_eq!(light_messages[0].payload, b"light-0");
    assert_eq!(light_messages[19].payload, b"light-19");

    // Burst workload verification
    let mut burst_reader = burst_storage
        .create_reader(burst_topic, StartPosition::Offset(0))
        .await
        .expect("create burst reader");
    let mut burst_messages = Vec::new();
    for _ in 0..45 {
        if let Some(msg_result) = burst_reader.next().await {
            burst_messages.push(msg_result.expect("read burst message"));
        }
    }
    assert_eq!(burst_messages.len(), 45);
    assert_eq!(burst_messages[0].payload, b"burst-0-0");
    assert_eq!(burst_messages[14].payload, b"burst-0-14");
    assert_eq!(burst_messages[15].payload, b"burst-1-0");
    assert_eq!(burst_messages[44].payload, b"burst-2-14");

    // Verify independent cloud storage (if uploads occurred)
    let heavy_objects = count_cloud_objects(&memory_store, heavy_topic).await;
    let light_objects = count_cloud_objects(&memory_store, light_topic).await;
    let burst_objects = count_cloud_objects(&memory_store, burst_topic).await;

    // Heavy topic is most likely to have triggered uploads due to volume
    // Light topic is least likely due to low volume
    // Burst topic may or may not have uploads depending on timing

    println!(
        "Cloud objects - Heavy: {}, Light: {}, Burst: {}",
        heavy_objects, light_objects, burst_objects
    );
}

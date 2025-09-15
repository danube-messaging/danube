//! # Danube Iceberg Storage Integration Tests
//!
//! This module contains comprehensive integration tests for the Danube Iceberg Storage backend.
//! These tests validate the complete end-to-end flow from message ingestion through Write-Ahead Log (WAL)
//! to Apache Iceberg Parquet files and back to message streaming.
//!
//! ## Test Architecture Overview
//!
//! The tests use an in-memory configuration to ensure fast, isolated execution:
//! - **Memory Catalog**: Apache Iceberg MemoryCatalog for table metadata
//! - **Memory Object Store**: In-memory storage backend for Parquet files
//! - **Temporary WAL**: File-based WAL in temporary directory
//! - **Fast Timings**: Reduced intervals for quick test execution
//!
//! ## Test Flow Verification
//!
//! Each test validates the following pipeline stages:
//!
//! 1. **Message Ingestion**: Messages are written to Write-Ahead Log (WAL)
//! 2. **Background Processing**: TopicWriter asynchronously processes WAL entries
//! 3. **Parquet Generation**: Messages are converted to Arrow format and written as Parquet files
//! 4. **Iceberg Integration**: Parquet files are registered in Iceberg table snapshots
//! 5. **Message Streaming**: TopicReader streams messages from Iceberg snapshots
//! 6. **Data Integrity**: All message fields are preserved through the entire pipeline
//!
//! ## Key Validation Points
//!
//! - **Position Tracking**: WAL write position vs committed position accuracy
//! - **Message Integrity**: ID, payload, properties, sequence_id preservation
//! - **Async Processing**: Background WAL-to-Parquet conversion timing
//! - **Stream Isolation**: Multiple topics operate independently
//! - **Resume Capability**: Streaming from specific offsets works correctly

use crate::config::{
    CatalogConfig, IcebergConfig, ObjectStoreConfig, ReaderConfig, WalConfig, WriterConfig,
};
use crate::iceberg_storage::IcebergStorage;
use danube_core::message::StreamMessage;
use danube_core::storage::PersistentStorage;
use std::collections::HashMap;
use tempfile::TempDir;
use tokio::time::{sleep, Duration};
use tracing_test::traced_test;

/// Create test configuration optimized for fast, filesystem-based testing
///
/// ## Configuration Details
///
/// - **Catalog**: Memory catalog (simple, no HTTP dependencies)
/// - **Object Store**: Local filesystem storage (reliable, no network)
/// - **WAL**: Temporary directory with small file sizes for testing
/// - **Writer**: Small batches and fast flush intervals for quick processing
/// - **Reader**: Fast polling intervals for responsive streaming
///
/// ## Returns
///
/// - `IcebergConfig`: Complete configuration for IcebergStorage
/// - `TempDir`: Temporary directory that must be kept alive during test
fn create_test_config() -> (IcebergConfig, TempDir) {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let wal_path = temp_dir.path().join("wal");
    let warehouse_path = temp_dir.path().join("warehouse");

    std::fs::create_dir_all(&wal_path).expect("Failed to create WAL directory");
    std::fs::create_dir_all(&warehouse_path).expect("Failed to create warehouse directory");

    let config = IcebergConfig {
        catalog: CatalogConfig::Memory {},
        object_store: ObjectStoreConfig::Local {
            path: warehouse_path.to_string_lossy().to_string(),
        },
        wal: WalConfig {
            base_path: wal_path.to_string_lossy().to_string(),
            max_file_size: 1024 * 1024, // 1MB for testing
            sync_mode: crate::config::SyncMode::Always,
        },
        warehouse: format!("file://{}", warehouse_path.to_string_lossy()),
        writer: WriterConfig {
            batch_size: 10,                // Small batch for faster testing
            flush_interval_ms: 100,        // Fast flush for testing
            max_memory_bytes: 1024 * 1024, // 1MB
        },
        reader: ReaderConfig {
            poll_interval_ms: 50, // Fast polling for testing
            max_concurrent_reads: 2,
            prefetch_size: 1,
        },
    };

    (config, temp_dir)
}

/// Create test messages with predictable, verifiable content
///
/// ## Message Structure
///
/// Each message contains:
/// - **Request ID**: Sequential numbering starting from 0
/// - **Message ID**: Complete MessageID with producer_id, topic_name, broker_addr, segment_id, segment_offset
/// - **Payload**: UTF-8 encoded test content with sequence number
/// - **Publish Time**: Current timestamp for realistic data
/// - **Producer Name**: Fixed "test-producer" for consistency
/// - **Subscription Name**: Optional, set to None for testing
/// - **Attributes**: Test key-value pair with sequence number
///
/// ## Parameters
///
/// - `count`: Number of messages to generate
/// - `topic`: Topic name for all generated messages
///
/// ## Returns
///
/// Vector of `StreamMessage` instances ready for storage
fn create_test_messages(count: usize, topic: &str) -> Vec<StreamMessage> {
    (0..count)
        .map(|i| StreamMessage {
            request_id: i as u64,
            msg_id: danube_core::message::MessageID {
                producer_id: 1001, // Fixed producer ID for testing
                topic_name: topic.to_string(),
                broker_addr: "test-broker:6650".to_string(),
                segment_id: 0, // Single segment for testing
                segment_offset: i as u64,
            },
            payload: format!("test payload {}", i).into_bytes(),
            publish_time: chrono::Utc::now().timestamp_millis() as u64,
            producer_name: "test-producer".to_string(),
            subscription_name: None, // No subscription for storage testing
            attributes: {
                let mut attrs = HashMap::new();
                attrs.insert("test_key".to_string(), format!("test_value_{}", i));
                attrs
            },
        })
        .collect()
}

#[tokio::test]
#[traced_test]
#[ignore = "MemoryCatalog does not support table updates - waiting for iceberg crate update"]
async fn test_iceberg_storage_happy_path() {
    //! # Happy Path Test
    //!
    //! This test validates the complete pipeline from message ingestion to streaming.
    //!
    //! ## Test Flow
    //!
    //! 1. **Create Topic**: Create a test topic
    //! 2. **Store Messages**: Store test messages in the topic
    //! 3. **Wait for Commit**: Wait for messages to be committed to Parquet files
    //! 4. **Create Message Stream**: Create a message stream for the topic
    //! 5. **Verify Message Integrity**: Verify all message fields are preserved
    //!
    //! ## Key Validation Points
    //!
    //! - **Position Tracking**: WAL write position vs committed position accuracy
    //! - **Message Integrity**: ID, payload, properties, sequence_id preservation
    //! - **Async Processing**: Background WAL-to-Parquet conversion timing

    // Setup test configuration
    let (config, _temp_dir) = create_test_config();
    let topic_name = "test-topic";

    // Create IcebergStorage instance
    let storage = IcebergStorage::new(config)
        .await
        .expect("Failed to create IcebergStorage");

    // Create topic
    storage
        .create_topic(topic_name)
        .await
        .expect("Failed to create topic");

    // Create test messages
    let test_messages = create_test_messages(20, topic_name);
    let expected_message_count = test_messages.len();

    println!("Created {} test messages", expected_message_count);

    // Step 1: Store messages (should go to WAL)
    storage
        .store_messages(topic_name, test_messages.clone())
        .await
        .expect("Failed to store messages");

    println!("Messages stored to WAL");

    // Verify WAL write position
    let write_position = storage
        .get_write_position(topic_name)
        .await
        .expect("Failed to get write position");

    assert_eq!(write_position, expected_message_count as u64);
    println!("WAL write position: {}", write_position);

    // Step 2: Wait for TopicWriter to process WAL entries into Parquet files
    // The writer should automatically start processing WAL entries in the background
    let mut attempts = 0;
    let max_attempts = 50; // 5 seconds with 100ms intervals

    loop {
        let committed_position = storage
            .get_committed_position(topic_name)
            .await
            .expect("Failed to get committed position");

        println!(
            "Attempt {}: Committed position: {}",
            attempts + 1,
            committed_position
        );

        if committed_position >= expected_message_count as u64 {
            println!("All messages committed to Parquet files");
            break;
        }

        if attempts >= max_attempts {
            panic!(
                "TopicWriter did not commit all messages within timeout. Committed: {}, Expected: {}",
                committed_position, expected_message_count
            );
        }

        attempts += 1;
        sleep(Duration::from_millis(100)).await;
    }

    // Step 3: Create message stream and verify TopicReader streams messages
    let mut message_receiver = storage
        .create_message_stream(topic_name, Some(0))
        .await
        .expect("Failed to create message stream");

    println!("Created message stream, starting to receive messages");

    // Collect streamed messages
    let mut received_messages = Vec::new();
    let mut receive_attempts = 0;
    let max_receive_attempts = 100; // 10 seconds with 100ms timeouts

    while received_messages.len() < expected_message_count {
        match tokio::time::timeout(Duration::from_millis(100), message_receiver.recv()).await {
            Ok(Some(message)) => {
                println!("Received message: {}", message.msg_id);
                received_messages.push(message);
            }
            Ok(None) => {
                println!("Message stream closed");
                break;
            }
            Err(_) => {
                // Timeout - continue trying
                receive_attempts += 1;
                if receive_attempts >= max_receive_attempts {
                    break;
                }
            }
        }
    }

    println!("Received {} messages via stream", received_messages.len());

    // Step 4: Verify message integrity
    assert_eq!(
        received_messages.len(),
        expected_message_count,
        "Should receive all messages via stream"
    );

    // Sort both collections by sequence_id for comparison
    let mut expected_sorted = test_messages.clone();
    expected_sorted.sort_by_key(|m| m.msg_id.segment_offset);

    let mut received_sorted = received_messages.clone();
    received_sorted.sort_by_key(|m| m.msg_id.segment_offset);

    // Verify each message
    for (i, (expected, received)) in expected_sorted
        .iter()
        .zip(received_sorted.iter())
        .enumerate()
    {
        assert_eq!(
            expected.msg_id, received.msg_id,
            "Message {} ID mismatch",
            i
        );
        assert_eq!(
            expected.payload, received.payload,
            "Message {} payload mismatch",
            i
        );
        assert_eq!(
            expected.producer_name, received.producer_name,
            "Message {} producer_name mismatch",
            i
        );
        assert_eq!(
            expected.attributes, received.attributes,
            "Message {} attributes mismatch",
            i
        );
    }

    println!("All message integrity checks passed!");

    // Step 5: Verify storage statistics
    let final_write_position = storage
        .get_write_position(topic_name)
        .await
        .expect("Failed to get final write position");

    let final_committed_position = storage
        .get_committed_position(topic_name)
        .await
        .expect("Failed to get final committed position");

    assert_eq!(final_write_position, expected_message_count as u64);
    assert_eq!(final_committed_position, expected_message_count as u64);

    println!(
        "Final positions - Write: {}, Committed: {}",
        final_write_position, final_committed_position
    );

    // Cleanup
    storage
        .shutdown()
        .await
        .expect("Failed to shutdown storage");

    println!("IcebergStorage happy path test completed successfully!");
}

#[tokio::test]
#[traced_test]
#[ignore = "MemoryCatalog does not support table updates - waiting for iceberg crate update"]
async fn test_iceberg_storage_multiple_topics() {
    //! # Multiple Topics Test
    //!
    //! This test validates the ability to create and stream multiple topics independently.
    //!
    //! ## Test Flow
    //!
    //! 1. **Create Topics**: Create multiple test topics
    //! 2. **Store Messages**: Store test messages in each topic
    //! 3. **Wait for Commit**: Wait for messages to be committed to Parquet files
    //! 4. **Create Message Streams**: Create message streams for each topic
    //! 5. **Verify Message Integrity**: Verify all message fields are preserved for each topic
    //!
    //! ## Key Validation Points
    //!
    //! - **Stream Isolation**: Multiple topics operate independently
    //! - **Message Integrity**: ID, payload, properties, sequence_id preservation for each topic

    let (config, _temp_dir) = create_test_config();
    let storage = IcebergStorage::new(config)
        .await
        .expect("Failed to create IcebergStorage");

    let topics = vec!["topic-1", "topic-2", "topic-3"];
    let messages_per_topic = 5;

    // Create topics and store messages
    for topic in &topics {
        storage
            .create_topic(topic)
            .await
            .expect("Failed to create topic");

        let messages = create_test_messages(messages_per_topic, topic);
        storage
            .store_messages(topic, messages)
            .await
            .expect("Failed to store messages");
    }

    // Wait for all messages to be committed
    for topic in &topics {
        let mut attempts = 0;
        loop {
            let committed = storage
                .get_committed_position(topic)
                .await
                .expect("Failed to get committed position");
            if committed >= messages_per_topic as u64 {
                break;
            }
            if attempts >= 50 {
                panic!("Messages not committed for topic {}", topic);
            }
            attempts += 1;
            sleep(Duration::from_millis(100)).await;
        }
    }

    // Verify each topic can be read independently
    for topic in &topics {
        let mut receiver = storage
            .create_message_stream(topic, Some(0))
            .await
            .expect("Failed to create stream");
        let mut count = 0;

        while count < messages_per_topic {
            // Should receive messages 5-9
            match tokio::time::timeout(Duration::from_millis(100), receiver.recv()).await {
                Ok(Some(message)) => {
                    assert_eq!(message.msg_id.topic_name, *topic);
                    count += 1;
                }
                Ok(None) => break,
                Err(_) => continue,
            }
        }

        assert_eq!(
            count, messages_per_topic,
            "Topic {} should have {} messages",
            topic, messages_per_topic
        );
    }

    storage
        .shutdown()
        .await
        .expect("Failed to shutdown storage");
    println!("Multiple topics test completed successfully!");
}

#[tokio::test]
#[traced_test]
#[ignore = "MemoryCatalog does not support table updates - waiting for iceberg crate update"]
async fn test_iceberg_storage_resume_from_position() {
    //! # Resume from Position Test
    //!
    //! This test validates the ability to resume streaming from a specific position.
    //!
    //! ## Test Flow
    //!
    //! 1. **Create Topic**: Create a test topic
    //! 2. **Store Messages**: Store test messages in the topic
    //! 3. **Wait for Commit**: Wait for messages to be committed to Parquet files
    //! 4. **Create Message Stream**: Create a message stream for the topic with a specific offset
    //! 5. **Verify Message Integrity**: Verify all message fields are preserved
    //!
    //! ## Key Validation Points
    //!
    //! - **Resume Capability**: Streaming from specific offsets works correctly
    //! - **Message Integrity**: ID, payload, properties, sequence_id preservation

    let (config, _temp_dir) = create_test_config();
    let storage = IcebergStorage::new(config)
        .await
        .expect("Failed to create IcebergStorage");

    let topic_name = "resume-test-topic";
    storage
        .create_topic(topic_name)
        .await
        .expect("Failed to create topic");

    // Store messages
    let messages = create_test_messages(10, topic_name);
    storage
        .store_messages(topic_name, messages.clone())
        .await
        .expect("Failed to store messages");

    // Wait for commit
    let mut attempts = 0;
    loop {
        let committed = storage
            .get_committed_position(topic_name)
            .await
            .expect("Failed to get committed position");
        if committed >= 10 {
            break;
        }
        if attempts >= 50 {
            panic!("Messages not committed");
        }
        attempts += 1;
        sleep(Duration::from_millis(100)).await;
    }

    // Read from position 5
    let mut receiver = storage
        .create_message_stream(topic_name, Some(5))
        .await
        .expect("Failed to create stream");
    let mut received_count = 0;
    let mut min_sequence_id = u64::MAX;

    while received_count < 5 {
        // Should receive messages 5-9
        match tokio::time::timeout(Duration::from_millis(100), receiver.recv()).await {
            Ok(Some(message)) => {
                min_sequence_id = min_sequence_id.min(message.msg_id.segment_offset);
                received_count += 1;
            }
            Ok(None) => break,
            Err(_) => continue,
        }
    }

    assert_eq!(
        received_count, 5,
        "Should receive 5 messages from position 5"
    );
    assert!(
        min_sequence_id >= 5,
        "Should not receive messages before position 5"
    );

    storage
        .shutdown()
        .await
        .expect("Failed to shutdown storage");
    println!("Resume from position test completed successfully!");
}

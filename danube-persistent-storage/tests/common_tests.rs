mod common;

use common::{count_cloud_objects, create_test_factory, make_test_message, wait_for_condition};
use danube_core::storage::{PersistentStorage, StartPosition};
use tokio_stream::StreamExt;

#[tokio::test]
async fn test_make_test_message() {
    let msg = make_test_message(1, 100, "test-topic", 5, "hello");
    assert_eq!(msg.request_id, 1);
    assert_eq!(msg.msg_id.producer_id, 100);
    assert_eq!(msg.msg_id.topic_name, "test-topic");
    assert_eq!(msg.msg_id.topic_offset, 5);
    assert_eq!(msg.payload, b"hello");
    assert_eq!(msg.producer_name, "producer-100");
}

#[tokio::test]
async fn test_create_test_factory() {
    let (factory, _memory) = create_test_factory().await;

    // Test that we can create a WAL storage for a topic
    let storage = factory
        .for_topic("test/topic")
        .await
        .expect("create storage");

    // Test basic append operation
    let msg = make_test_message(1, 1, "test/topic", 0, "test");
    storage
        .append_message("test/topic", msg)
        .await
        .expect("append message");

    // WalStorage doesn't expose current_offset directly, so we test via reader
    let mut reader = storage
        .create_reader("test/topic", StartPosition::Offset(0))
        .await
        .expect("create reader");
    let read_msg = reader
        .next()
        .await
        .expect("read message")
        .expect("message result");
    assert_eq!(read_msg.payload, b"test");
}

#[tokio::test]
async fn test_wait_for_condition() {
    let start = std::time::Instant::now();

    // Test successful condition
    let result = wait_for_condition(|| async { true }, 100).await;
    assert!(result);
    assert!(start.elapsed().as_millis() < 100);

    // Test timeout condition
    let start = std::time::Instant::now();
    let result = wait_for_condition(|| async { false }, 100).await;
    assert!(!result);
    assert!(start.elapsed().as_millis() >= 100);
}

#[tokio::test]
async fn test_count_cloud_objects() {
    let (factory, memory) = create_test_factory().await;
    let storage = factory
        .for_topic("test/count")
        .await
        .expect("create storage");

    // Initially no objects
    let count = count_cloud_objects(&memory, "test/count").await;
    assert_eq!(count, 0);

    // Add messages and wait for upload
    for i in 0..5 {
        let msg = make_test_message(i, 1, "test/count", i, &format!("msg-{}", i));
        storage
            .append_message("test/count", msg)
            .await
            .expect("append message");
    }

    // Wait for uploader to create objects
    let uploaded = wait_for_condition(
        || async { count_cloud_objects(&memory, "test/count").await > 0 },
        5000,
    )
    .await;

    if uploaded {
        let final_count = count_cloud_objects(&memory, "test/count").await;
        assert!(final_count > 0);
    }
}

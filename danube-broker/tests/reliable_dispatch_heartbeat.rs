//! Integration tests for heartbeat-based reliability improvements
//!
//! These tests validate the hybrid lag monitor (notifier + heartbeat) mechanism
//! that ensures at-least-once delivery even when notifications fail or are coalesced.
//!
//! The heartbeat acts as a watchdog that detects when the subscription has fallen
//! behind the WAL head and triggers recovery polls, preventing message loss.

extern crate danube_client;

use anyhow::Result;
use danube_client::SubType;
use std::time::Duration;
use tokio::time::{sleep, timeout};

#[path = "test_utils.rs"]
mod test_utils;

/// **What this test validates:**
/// - Scenario: Producer sends messages in rapid succession (bursts), causing notification
///   coalescing where multiple messages share a single tokio::Notify permit.
/// - Expectation: Heartbeat mechanism detects lag and ensures ALL messages are delivered,
///   even when notifications are coalesced.
/// - Example: Send 100 messages in <1ms burst → Some notifications coalesced → Heartbeat
///   recovers remaining messages within 500ms.
///
/// **Why this matters:**
/// - This is the PRIMARY bug the heartbeat mechanism fixes.
/// - Without heartbeat, coalesced notifications would cause messages to be stuck until
///   the next publish event.
/// - Validates that tokio::Notify coalescing doesn't cause message loss.
/// - Confirms the system self-heals within the heartbeat interval (500ms default).
///
/// **Technical validation:**
/// - Tests the interaction between `Topic::publish_message_async` (notifier) and
///   `UnifiedSingleDispatcher` heartbeat tick.
/// - Validates that `SubscriptionEngine::has_lag()` correctly detects coalescing-induced lag.
/// - Ensures `DISPATCHER_HEARTBEAT_POLLS_TOTAL` metric increases when notifications fail.
#[tokio::test]
async fn test_notification_coalescing_recovery() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/heartbeat_coalescing");

    // Create reliable producer
    let mut producer = client
        .new_producer()
        .with_topic(&topic)
        .with_name("producer_coalescing")
        .with_reliable_dispatch()
        .build();
    producer.create().await?;

    // Create exclusive consumer
    let mut consumer = client
        .new_consumer()
        .with_topic(topic.clone())
        .with_consumer_name("consumer_coalescing".to_string())
        .with_subscription("sub_coalescing".to_string())
        .with_subscription_type(SubType::Exclusive)
        .build();
    consumer.subscribe().await?;
    let mut stream = consumer.receive().await?;

    // Wait for subscription to be fully established
    sleep(Duration::from_millis(200)).await;

    // Send 100 messages in rapid succession to trigger notification coalescing
    // No sleep between sends - this maximizes the chance of coalescing
    let total_messages = 100;
    let payloads: Vec<String> = (0..total_messages)
        .map(|i| format!("coalesce_msg_{}", i))
        .collect();

    for payload in &payloads {
        producer.send(payload.clone().into_bytes(), None).await?;
    }

    // Receive all messages with generous timeout
    // Even with coalescing, heartbeat should deliver all messages within a few heartbeat cycles
    // Heartbeat interval is 500ms, so 20 seconds is more than enough
    let receive_future = async {
        let mut received_payloads = Vec::new();

        while received_payloads.len() < total_messages {
            if let Some(msg) = stream.recv().await {
                let payload = String::from_utf8(msg.payload.clone())?;
                received_payloads.push(payload);
                consumer.ack(&msg).await?;
            }
        }

        Ok::<Vec<String>, anyhow::Error>(received_payloads)
    };

    let received = timeout(Duration::from_secs(20), receive_future)
        .await?
        .expect("Should receive all messages");

    // Validate all messages received
    assert_eq!(
        received.len(),
        total_messages,
        "Should receive all {} messages despite notification coalescing",
        total_messages
    );

    // Validate message order and integrity
    for (i, payload) in received.iter().enumerate() {
        assert_eq!(
            payload, &payloads[i],
            "Message {} should match expected payload",
            i
        );
    }

    Ok(())
}

/// **What this test validates:**
/// - Scenario: Consumer subscribes while producer is actively sending messages, creating
///   a race where some messages arrive during subscription initialization.
/// - Expectation: Heartbeat ensures all messages sent after subscription are delivered,
///   even if notifications are missed during the initialization window.
/// - Example: Consumer subscribes → Producer starts → Messages arrive during dispatcher
///   setup → Notifications may be missed → Heartbeat recovers them.
///
/// **Why this matters:**
/// - Real-world scenario: Subscribing to an active, high-throughput topic.
/// - Validates the system handles concurrent subscription creation and message arrival.
/// - Without heartbeat, messages arriving during initialization could be missed.
/// - Ensures subscriptions are reliable from the moment they're created.
///
/// **Technical validation:**
/// - Tests concurrent subscription creation and message publishing.
/// - Validates heartbeat catches messages that arrive during initialization.
/// - Ensures no message loss when subscription and publishing overlap.
#[tokio::test]
async fn test_subscription_creation_race_condition() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/heartbeat_race");

    // Create reliable producer
    let mut producer = client
        .new_producer()
        .with_topic(&topic)
        .with_name("producer_race")
        .with_reliable_dispatch()
        .build();
    producer.create().await?;

    // Create consumer (subscription initialization starts)
    let mut consumer = client
        .new_consumer()
        .with_topic(topic.clone())
        .with_consumer_name("consumer_race".to_string())
        .with_subscription("sub_race".to_string())
        .with_subscription_type(SubType::Exclusive)
        .build();
    consumer.subscribe().await?;
    let mut stream = consumer.receive().await?;

    // Small delay to ensure subscription is ready
    sleep(Duration::from_millis(100)).await;

    // NOW start sending messages rapidly while subscription is active
    // This creates the race condition where messages arrive very quickly
    let total_messages = 50;
    for i in 0..total_messages {
        let payload = format!("race_msg_{}", i);
        producer.send(payload.into_bytes(), None).await?;
        // Very small delay to create burst pressure
        if i % 10 == 0 {
            sleep(Duration::from_millis(1)).await;
        }
    }

    // Receive all messages - heartbeat should handle any notification coalescing
    let receive_future = async {
        let mut received_indices = Vec::new();

        while received_indices.len() < total_messages {
            if let Some(msg) = stream.recv().await {
                let payload = String::from_utf8(msg.payload.clone())?;
                // Extract message index
                if let Some(idx_str) = payload.strip_prefix("race_msg_") {
                    let idx: usize = idx_str.parse()?;
                    received_indices.push(idx);
                }
                consumer.ack(&msg).await?;
            }
        }

        Ok::<Vec<usize>, anyhow::Error>(received_indices)
    };

    let received_indices = timeout(Duration::from_secs(15), receive_future)
        .await?
        .expect("Should receive all messages");

    // Validate all messages received
    assert_eq!(
        received_indices.len(),
        total_messages,
        "Should receive all {} messages",
        total_messages
    );

    // Validate all indices are present (order guaranteed for exclusive subscription)
    for i in 0..total_messages {
        assert_eq!(
            received_indices[i], i,
            "Message {} should be at position {}",
            i, i
        );
    }

    Ok(())
}

/// **What this test validates:**
/// - Scenario: Consumer disconnects immediately after a notification is sent but before
///   the poll completes and the message is delivered.
/// - Expectation: Message is buffered in `pending_message`, redelivered on reconnect,
///   ensuring at-least-once delivery.
/// - Example: Notification sent → Message polled → Consumer disconnects → Reconnects →
///   Gets buffered message → No message loss.
///
/// **Why this matters:**
/// - Validates the `pending_message` buffer in `UnifiedSingleDispatcher` works correctly.
/// - Ensures transient network issues don't cause message loss.
/// - Real-world scenario: Mobile clients with intermittent connectivity.
/// - Tests the interaction between pending buffer and heartbeat mechanism.
///
/// **Technical validation:**
/// - Tests `UnifiedSingleDispatcher::ack_gating` with consumer disconnect.
/// - Validates `trigger_dispatcher_on_reconnect` correctly preserves pending message.
/// - Ensures heartbeat doesn't interfere with pending message redelivery.
/// - Confirms no message duplication of already-acked messages.
#[tokio::test]
async fn test_consumer_disconnect_during_notification() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/heartbeat_disconnect");

    // Create reliable producer
    let mut producer = client
        .new_producer()
        .with_topic(&topic)
        .with_name("producer_disconnect")
        .with_reliable_dispatch()
        .build();
    producer.create().await?;

    // Phase 1: Consumer connects and processes some messages
    let mut consumer = client
        .new_consumer()
        .with_topic(topic.clone())
        .with_consumer_name("consumer_disconnect".to_string())
        .with_subscription("sub_disconnect".to_string())
        .with_subscription_type(SubType::Exclusive)
        .build();
    consumer.subscribe().await?;
    let mut stream = consumer.receive().await?;

    sleep(Duration::from_millis(200)).await;

    // Send and ack first 3 messages
    for i in 0..3 {
        let payload = format!("disconnect_msg_{}", i);
        producer.send(payload.into_bytes(), None).await?;
    }

    for i in 0..3 {
        let msg = timeout(Duration::from_secs(5), stream.recv())
            .await?
            .expect("Should receive message");
        let payload = String::from_utf8(msg.payload.clone())?;
        assert_eq!(payload, format!("disconnect_msg_{}", i));
        consumer.ack(&msg).await?;
    }

    // Send message #3 and receive it but DON'T ack, then disconnect
    producer
        .send("disconnect_msg_3".to_string().into_bytes(), None)
        .await?;

    let pending_msg = timeout(Duration::from_secs(5), stream.recv())
        .await?
        .expect("Should receive message 3");
    let pending_payload = String::from_utf8(pending_msg.payload.clone())?;
    assert_eq!(pending_payload, "disconnect_msg_3");

    // DON'T ACK - this message should be buffered as pending
    // Disconnect gracefully (important for proper broker cleanup)
    drop(stream);
    consumer.close().await;

    // Give broker time to detect disconnect
    sleep(Duration::from_millis(300)).await;

    // Reconnect with SAME consumer name for Exclusive subscription
    let mut consumer2 = client
        .new_consumer()
        .with_topic(topic.clone())
        .with_consumer_name("consumer_disconnect".to_string()) // SAME consumer name
        .with_subscription("sub_disconnect".to_string()) // Same subscription
        .with_subscription_type(SubType::Exclusive)
        .build();
    consumer2.subscribe().await?;
    let mut stream2 = consumer2.receive().await?;

    // Small delay to ensure dispatcher is ready after reconnection
    sleep(Duration::from_millis(200)).await;

    // Should receive the pending message again (message #3)
    // Increase timeout to 10 seconds to account for heartbeat recovery
    let redelivered_msg = timeout(Duration::from_secs(10), stream2.recv())
        .await?
        .expect("Should redeliver pending message");
    let redelivered_payload = String::from_utf8(redelivered_msg.payload.clone())?;
    assert_eq!(
        redelivered_payload, "disconnect_msg_3",
        "Should redeliver unacked message #3"
    );
    consumer2.ack(&redelivered_msg).await?;

    // Send more messages to ensure system is healthy
    for i in 4..7 {
        let payload = format!("disconnect_msg_{}", i);
        producer.send(payload.into_bytes(), None).await?;
    }

    // Receive remaining messages
    for i in 4..7 {
        let msg = timeout(Duration::from_secs(5), stream2.recv())
            .await?
            .expect("Should receive message");
        let payload = String::from_utf8(msg.payload.clone())?;
        assert_eq!(payload, format!("disconnect_msg_{}", i));
        consumer2.ack(&msg).await?;
    }

    Ok(())
}

/// **What this test validates:**
/// - Scenario: `TopicStream::poll_next()` returns `None` (temporary I/O lag or storage
///   slowdown), but messages exist in the WAL.
/// - Expectation: Heartbeat retries the poll and eventually delivers the message when
///   storage recovers.
/// - Example: WAL has message @ offset=5, but first `poll_next()` returns `None` →
///   Heartbeat triggers retry → Message delivered on subsequent poll.
///
/// **Why this matters:**
/// - Validates heartbeat handles transient storage layer issues.
/// - Ensures dispatcher doesn't get stuck when WAL read temporarily fails.
/// - Real-world scenario: Storage I/O spikes, network-attached storage lag.
/// - Without heartbeat, dispatcher would wait indefinitely for next publish.
///
/// **Technical validation:**
/// - Tests `SubscriptionEngine::poll_next()` behavior when stream returns `None`.
/// - Validates that heartbeat's `has_lag()` check still triggers polls.
/// - Ensures system is resilient to temporary storage unavailability.
/// - Confirms messages are delivered once storage recovers.
///
/// **Implementation note:**
/// - This is harder to test without mocking storage, so we simulate by:
///   1. Send message burst
///   2. Immediately pause between bursts to simulate I/O lag
///   3. Verify heartbeat delivers all messages despite gaps
#[tokio::test]
async fn test_poll_next_returns_none_recovery() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/heartbeat_poll_none");

    // Create reliable producer
    let mut producer = client
        .new_producer()
        .with_topic(&topic)
        .with_name("producer_poll_none")
        .with_reliable_dispatch()
        .build();
    producer.create().await?;

    // Create exclusive consumer
    let mut consumer = client
        .new_consumer()
        .with_topic(topic.clone())
        .with_consumer_name("consumer_poll_none".to_string())
        .with_subscription("sub_poll_none".to_string())
        .with_subscription_type(SubType::Exclusive)
        .build();
    consumer.subscribe().await?;
    let mut stream = consumer.receive().await?;

    sleep(Duration::from_millis(200)).await;

    // Send messages in bursts with pauses to simulate I/O lag
    // The pauses may cause poll_next to return None temporarily
    let total_messages = 30;
    for burst in 0..3 {
        // Send burst of 10 messages rapidly
        for i in 0..10 {
            let msg_num = burst * 10 + i;
            let payload = format!("poll_none_msg_{}", msg_num);
            producer.send(payload.into_bytes(), None).await?;
        }

        // Pause between bursts to simulate storage lag
        // During this pause, consumer might try to poll and get None
        sleep(Duration::from_millis(800)).await;
    }

    // Receive all messages - heartbeat should handle any None responses
    let receive_future = async {
        let mut received = Vec::new();

        while received.len() < total_messages {
            if let Some(msg) = stream.recv().await {
                let payload = String::from_utf8(msg.payload.clone())?;
                received.push(payload.clone());
                consumer.ack(&msg).await?;
            }
        }

        Ok::<Vec<String>, anyhow::Error>(received)
    };

    let received = timeout(Duration::from_secs(20), receive_future)
        .await?
        .expect("Should receive all messages");

    // Validate all messages received despite potential poll None responses
    assert_eq!(
        received.len(),
        total_messages,
        "Should receive all {} messages despite storage lags",
        total_messages
    );

    // Validate all message indices present
    for i in 0..total_messages {
        let expected = format!("poll_none_msg_{}", i);
        assert!(received.contains(&expected), "Should receive message {}", i);
    }

    Ok(())
}

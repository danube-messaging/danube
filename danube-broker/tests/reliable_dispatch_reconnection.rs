//! Reliable dispatch reconnection tests
//!
//! These tests validate at-least-once delivery semantics when consumers disconnect and reconnect.
//! The broker must buffer unacknowledged messages and redeliver them upon reconnection to prevent
//! message loss.

extern crate danube_client;

use anyhow::Result;
use danube_client::{SchemaType, SubType};
use tokio::time::{sleep, timeout, Duration};

#[path = "test_utils.rs"]
mod test_utils;

/// What this test validates
///
/// - Scenario: An exclusive consumer receives a message from a reliable producer but disconnects
///   before acknowledging it. The same consumer then reconnects.
/// - Expectation: The broker must redeliver the unacknowledged message (not skip to the next one),
///   ensuring at-least-once delivery semantics.
/// - Example: If message #5 was sent but not acked before disconnect, upon reconnection the consumer
///   should receive message #5 again (not message #6).
///
/// Why this matters
/// - Reliable dispatch with exclusive subscription must guarantee no message loss during consumer
///   reconnection. The broker's pending message buffer prevents the stream from advancing past
///   unacknowledged messages.
///
/// Technical validation
/// - This test directly validates the `pending_message` buffer implementation in `unified_single.rs`.
/// - On disconnect, the buffer retains the unacked message.
/// - On reconnect + `ResetPending`, the buffer is preserved and the message is resent.
#[tokio::test]
async fn reliable_exclusive_reconnection_resends_pending_message() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/reliable_exclusive_reconnect");

    // Create reliable producer
    let mut producer = client
        .new_producer()
        .with_topic(&topic)
        .with_name("producer_exclusive_reconnect")
        .with_schema("schema_str".into(), SchemaType::String)
        .with_reliable_dispatch()
        .build();
    producer.create().await?;

    // Send 10 messages before consumer connects
    let total_messages = 10usize;
    let payloads: Vec<String> = (0..total_messages).map(|i| format!("msg_{}", i)).collect();
    for body in &payloads {
        let _ = producer.send(body.clone().into_bytes(), None).await?;
    }

    sleep(Duration::from_millis(200)).await;

    // Phase 1: Consumer connects and receives first 3 messages with acks
    let mut consumer = client
        .new_consumer()
        .with_topic(topic.clone())
        .with_consumer_name("cons_exclusive_reconnect".to_string())
        .with_subscription("sub_exclusive_reconnect".to_string())
        .with_subscription_type(SubType::Exclusive)
        .build();
    consumer.subscribe().await?;
    let mut stream = consumer.receive().await?;

    // Receive and ack first 3 messages
    for i in 0..3 {
        let msg = timeout(Duration::from_secs(5), stream.recv())
            .await?
            .expect("Expected message");
        let payload = String::from_utf8(msg.payload.clone())?;
        assert_eq!(payload, payloads[i], "Message {} should match", i);
        consumer.ack(&msg).await?;
    }

    // Phase 2: Receive message #3 (index 3) but DON'T ack it, then disconnect
    let pending_msg = timeout(Duration::from_secs(5), stream.recv())
        .await?
        .expect("Expected pending message");
    let pending_payload = String::from_utf8(pending_msg.payload.clone())?;
    assert_eq!(pending_payload, payloads[3], "Pending message should be msg_3");
    
    // Simulate disconnect by dropping consumer and stream WITHOUT acking
    drop(stream);
    drop(consumer);
    
    // Give broker time to detect disconnect
    sleep(Duration::from_millis(500)).await;

    // Phase 3: Reconnect the same consumer
    let mut consumer_reconnected = client
        .new_consumer()
        .with_topic(topic.clone())
        .with_consumer_name("cons_exclusive_reconnect".to_string())
        .with_subscription("sub_exclusive_reconnect".to_string())
        .with_subscription_type(SubType::Exclusive)
        .build();
    consumer_reconnected.subscribe().await?;
    let mut stream_reconnected = consumer_reconnected.receive().await?;

    // Phase 4: CRITICAL TEST - First message after reconnect should be the unacked message (msg_3)
    let redelivered_msg = timeout(Duration::from_secs(5), stream_reconnected.recv())
        .await?
        .expect("Expected redelivered message");
    let redelivered_payload = String::from_utf8(redelivered_msg.payload.clone())?;
    
    assert_eq!(
        redelivered_payload, payloads[3],
        "CRITICAL: Redelivered message should be msg_3 (the unacked message), not msg_4. \
         This validates the pending_message buffer is working correctly."
    );
    
    // Ack the redelivered message
    consumer_reconnected.ack(&redelivered_msg).await?;

    // Phase 5: Continue receiving remaining messages (4-9)
    for i in 4..total_messages {
        let msg = timeout(Duration::from_secs(5), stream_reconnected.recv())
            .await?
            .expect("Expected remaining message");
        let payload = String::from_utf8(msg.payload.clone())?;
        assert_eq!(payload, payloads[i], "Message {} should match", i);
        consumer_reconnected.ack(&msg).await?;
    }

    Ok(())
}

/// What this test validates
///
/// - Scenario: Two consumers (c1 and c2) in a shared subscription receive messages from a reliable
///   producer. Consumer c1 receives a message but disconnects before acknowledging. Consumer c2 is
///   still active.
/// - Expectation: The broker must redeliver the unacknowledged message to consumer c2 (automatic failover),
///   ensuring at-least-once delivery and continued progress even when one consumer fails.
/// - Example: If c1 receives message #5 but disconnects without acking, c2 should receive message #5
///   (not lose it).
///
/// Why this matters
/// - Shared subscriptions enable load balancing and fault tolerance. When one consumer fails, the
///   broker must automatically failover unacknowledged messages to healthy consumers without message loss.
///
/// Technical validation
/// - This test validates the `pending_message` buffer in `unified_multiple.rs` with failover logic.
/// - On disconnect, the buffer retains the unacked message.
/// - On `ResetPending`, the round-robin dispatcher attempts to send the buffered message to another
///   healthy consumer.
#[tokio::test]
async fn reliable_shared_reconnection_failover_to_another_consumer() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/reliable_shared_reconnect");

    // Create reliable producer
    let mut producer = client
        .new_producer()
        .with_topic(&topic)
        .with_name("producer_shared_reconnect")
        .with_schema("schema_str".into(), SchemaType::String)
        .with_reliable_dispatch()
        .build();
    producer.create().await?;

    let sub_name = "sub_shared_reconnect";

    // Start two consumers: c1 and c2
    let mut c1 = client
        .new_consumer()
        .with_topic(topic.clone())
        .with_consumer_name("c1_shared_reconnect".to_string())
        .with_subscription(sub_name.to_string())
        .with_subscription_type(SubType::Shared)
        .build();
    c1.subscribe().await?;
    let mut s1 = c1.receive().await?;

    let mut c2 = client
        .new_consumer()
        .with_topic(topic.clone())
        .with_consumer_name("c2_shared_reconnect".to_string())
        .with_subscription(sub_name.to_string())
        .with_subscription_type(SubType::Shared)
        .build();
    c2.subscribe().await?;
    let mut s2 = c2.receive().await?;

    sleep(Duration::from_millis(400)).await;

    // Send 10 messages
    let total_messages = 10usize;
    let payloads: Vec<String> = (0..total_messages).map(|i| format!("msg_{}", i)).collect();
    for body in &payloads {
        let _ = producer.send(body.clone().into_bytes(), None).await?;
    }

    // Phase 1: Both consumers receive and ack messages normally
    // We'll receive first 3 messages and ack them
    let mut received_by_c1 = Vec::new();
    let mut received_by_c2 = Vec::new();

    for _ in 0..3 {
        tokio::select! {
            Some(msg) = s1.recv() => {
                let payload = String::from_utf8(msg.payload.clone())?;
                received_by_c1.push(payload.clone());
                c1.ack(&msg).await?;
            }
            Some(msg) = s2.recv() => {
                let payload = String::from_utf8(msg.payload.clone())?;
                received_by_c2.push(payload.clone());
                c2.ack(&msg).await?;
            }
        }
    }

    // Phase 2: c1 receives next message but doesn't ack, then disconnects
    let pending_msg = timeout(Duration::from_secs(5), async {
        tokio::select! {
            Some(msg) = s1.recv() => Some(("c1", msg)),
            Some(msg) = s2.recv() => Some(("c2", msg)),
        }
    })
    .await?
    .expect("Expected a message");

    let (receiver, msg) = pending_msg;
    let pending_payload = String::from_utf8(msg.payload.clone())?;

    if receiver == "c1" {
        // c1 got the message, don't ack it and disconnect c1
        drop(s1);
        drop(c1);
        println!("c1 received '{}' but didn't ack, now disconnected", pending_payload);
        
        // Give broker time to detect disconnect and trigger failover
        sleep(Duration::from_millis(500)).await;

        // Phase 3: c2 should receive the same message (failover)
        let failover_msg = timeout(Duration::from_secs(5), s2.recv())
            .await?
            .expect("Expected failover message for c2");
        let failover_payload = String::from_utf8(failover_msg.payload.clone())?;
        
        assert_eq!(
            failover_payload, pending_payload,
            "CRITICAL: c2 should receive the same message '{}' that c1 didn't ack. \
             This validates automatic failover with pending_message buffer.",
            pending_payload
        );
        
        c2.ack(&failover_msg).await?;
        received_by_c2.push(failover_payload);
    } else {
        // c2 got the message, ack it normally (test still valid but different path)
        c2.ack(&msg).await?;
        received_by_c2.push(pending_payload.clone());
        println!("c2 received '{}' and acked", pending_payload);
    }

    // Phase 4: Continue receiving remaining messages with c2 only
    loop {
        match timeout(Duration::from_secs(2), s2.recv()).await {
            Ok(Some(msg)) => {
                let payload = String::from_utf8(msg.payload.clone())?;
                received_by_c2.push(payload);
                c2.ack(&msg).await?;
            }
            _ => break, // Timeout or None, assume we got all messages
        }
    }

    // Verify all messages were received across both consumers
    let mut all_received = received_by_c1;
    all_received.extend(received_by_c2);
    all_received.sort();
    
    assert_eq!(
        all_received.len(),
        total_messages,
        "Should have received all {} messages", total_messages
    );

    Ok(())
}

/// What this test validates
///
/// - Scenario: A consumer receives a message but disconnects and reconnects multiple times without
///   acknowledging. Each reconnection should redeliver the same unacknowledged message.
/// - Expectation: The broker must persistently buffer the unacked message across multiple reconnection
///   cycles until it receives an acknowledgment.
/// - Example: Message #5 is delivered, consumer disconnects, reconnects (gets #5 again), disconnects
///   again, reconnects (still gets #5), finally acks it, then receives message #6.
///
/// Why this matters
/// - Edge case validation: ensures the pending_message buffer is robust across multiple reconnection
///   attempts and doesn't accidentally advance the stream or lose the message.
///
/// Technical validation
/// - Validates that `ResetPending` preserves the `pending_message` buffer across multiple invocations.
/// - Ensures the buffer is only cleared on explicit `MessageAcked`, not on reconnection.
#[tokio::test]
async fn reliable_multiple_reconnections_same_message() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/reliable_multi_reconnect");

    // Create reliable producer
    let mut producer = client
        .new_producer()
        .with_topic(&topic)
        .with_name("producer_multi_reconnect")
        .with_schema("schema_str".into(), SchemaType::String)
        .with_reliable_dispatch()
        .build();
    producer.create().await?;

    // Send 5 messages
    let payloads: Vec<String> = (0..5).map(|i| format!("msg_{}", i)).collect();
    for body in &payloads {
        let _ = producer.send(body.clone().into_bytes(), None).await?;
    }

    sleep(Duration::from_millis(200)).await;

    let consumer_name = "cons_multi_reconnect";
    let sub_name = "sub_multi_reconnect";

    // Connect and ack first message
    let mut consumer = client
        .new_consumer()
        .with_topic(topic.clone())
        .with_consumer_name(consumer_name.to_string())
        .with_subscription(sub_name.to_string())
        .with_subscription_type(SubType::Exclusive)
        .build();
    consumer.subscribe().await?;
    let mut stream = consumer.receive().await?;

    let msg0 = timeout(Duration::from_secs(5), stream.recv()).await?.expect("msg0");
    assert_eq!(String::from_utf8(msg0.payload.clone())?, payloads[0]);
    consumer.ack(&msg0).await?;

    // Get second message but don't ack
    let msg1 = timeout(Duration::from_secs(5), stream.recv()).await?.expect("msg1");
    let pending_payload = String::from_utf8(msg1.payload.clone())?;
    assert_eq!(pending_payload, payloads[1]);
    // Don't ack!

    // Reconnection cycle 1
    drop(stream);
    drop(consumer);
    sleep(Duration::from_millis(500)).await;

    let mut consumer = client
        .new_consumer()
        .with_topic(topic.clone())
        .with_consumer_name(consumer_name.to_string())
        .with_subscription(sub_name.to_string())
        .with_subscription_type(SubType::Exclusive)
        .build();
    consumer.subscribe().await?;
    let mut stream = consumer.receive().await?;

    let redelivered1 = timeout(Duration::from_secs(5), stream.recv()).await?.expect("redelivered1");
    assert_eq!(
        String::from_utf8(redelivered1.payload.clone())?,
        payloads[1],
        "First reconnection should redeliver msg_1"
    );
    // Still don't ack!

    // Reconnection cycle 2
    drop(stream);
    drop(consumer);
    sleep(Duration::from_millis(500)).await;

    let mut consumer = client
        .new_consumer()
        .with_topic(topic.clone())
        .with_consumer_name(consumer_name.to_string())
        .with_subscription(sub_name.to_string())
        .with_subscription_type(SubType::Exclusive)
        .build();
    consumer.subscribe().await?;
    let mut stream = consumer.receive().await?;

    let redelivered2 = timeout(Duration::from_secs(5), stream.recv()).await?.expect("redelivered2");
    assert_eq!(
        String::from_utf8(redelivered2.payload.clone())?,
        payloads[1],
        "CRITICAL: Second reconnection should STILL redeliver msg_1, not advance to msg_2"
    );

    // Finally ack it
    consumer.ack(&redelivered2).await?;

    // Now should get msg_2 (index 2)
    let msg2 = timeout(Duration::from_secs(5), stream.recv()).await?.expect("msg2");
    assert_eq!(
        String::from_utf8(msg2.payload.clone())?,
        payloads[2],
        "After acking msg_1, should receive msg_2"
    );
    consumer.ack(&msg2).await?;

    Ok(())
}

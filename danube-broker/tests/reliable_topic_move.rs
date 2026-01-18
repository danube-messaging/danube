//! Integration test for reliable topic move with offset continuity
//!
//! This test validates that when a reliable topic is moved from one broker to another,
//! the offset sequence remains continuous and consumers can read all messages without gaps.
//!
//! **IMPORTANT**: This test requires `cloud.backend: "fs"` (or s3/gcs) in the broker config.
//! Using `backend: "memory"` will cause this test to fail because each broker has isolated
//! in-memory storage, and Broker B cannot access messages uploaded by Broker A.

extern crate danube_client;

use anyhow::Result;
use danube_client::SubType;
use std::fs;
use std::process::Command;
use tokio::time::{sleep, timeout, Duration};

#[path = "test_utils.rs"]
mod test_utils;

/// Helper to run admin CLI command
fn admin_cli() -> Command {
    let mut cmd = Command::new("cargo");
    cmd.args([
        "run",
        "-p",
        "danube-admin-cli",
        "--bin",
        "danube-admin-cli",
        "--",
    ]);
    cmd.env(
        "DANUBE_ADMIN_ENDPOINT",
        std::env::var("DANUBE_ADMIN_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:50051".into()),
    );
    cmd
}

#[tokio::test]
/// Integration test: Topic Move with Offset Continuity
///
/// Test Steps:
/// 1. Create topic on broker A via producer
/// 2. Produce messages 0-9
/// 3. Consumer reads 0-4, cursor at 5
/// 4. Unload topic from broker A (moves to broker B)
/// 5. Topic assigned to broker B
/// 6. Produce more messages on broker B
/// 7. Verify: new messages get offsets 10, 11, 12... (continuous)
/// 8. Consumer reconnects, reads from offset 5
/// 9. Verify: receives messages 5-12+ in order without gaps
///
///## Success Criteria
///
/// The test passes if and only if:
///
/// 1. **Offset Continuity**: Consumer receives exactly offsets 5-17 in order
/// 2. **No Gaps**: Each offset `n+1` immediately follows offset `n`
/// 3. **No Duplicates**: Each offset appears exactly once
/// 4. **No Loss**: All 13 messages (5-17) are received
/// 5. **Correct Payload**: All messages have the expected payload
///
/// ## Verification Points
///
/// - **Offset Array Match**: `offsets_seen == [5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17]`
/// - **Sequential Check**: For each pair of offsets, verify `curr == prev + 1`
/// - **Payload Integrity**: Each message payload matches the original test blob
///
/// ## What This Proves
///
/// - ✅ Sealed state mechanism works (written during unload, read during load)
/// - ✅ WAL initialization respects sealed state (starts from offset 10, not 0)
/// - ✅ Cloud storage preserves historical messages (5-9 readable after move)
/// - ✅ Subscription cursor persists across moves (consumer resumes from offset 5)
/// - ✅ Offset continuity maintained across broker boundaries
/// - ✅ No message loss during topic migration
///
async fn test_reliable_topic_move_with_offset_continuity() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/reliable_move");

    eprintln!("[TEST] Starting reliable topic move test for topic: {}", topic);

    // ========================================================================
    // Phase 1: Initial setup - Create producer and consumer
    // ========================================================================
    eprintln!("[PHASE 1] Creating producer and consumer");
    let mut producer = client
        .new_producer()
        .with_topic(&topic)
        .with_name("producer_topic_move")
        .with_reliable_dispatch()
        .build();
    producer.create().await?;
    eprintln!("[PHASE 1] Producer created");

    // Create and subscribe consumer BEFORE producing messages
    let mut consumer = client
        .new_consumer()
        .with_topic(topic.clone())
        .with_consumer_name("consumer_topic_move".to_string())
        .with_subscription("sub_topic_move".to_string())
        .with_subscription_type(SubType::Exclusive)
        .build();
    consumer.subscribe().await?;
    let mut stream = consumer.receive().await?;
    eprintln!("[PHASE 1] Consumer subscribed and stream ready");

    // Give consumer time to be ready
    sleep(Duration::from_millis(500)).await;

    // ========================================================================
    // Phase 2: Produce messages and consumer reads first 5
    // ========================================================================
    eprintln!("[PHASE 2] Producing 10 messages");
    // Produce 10 messages (offsets 0-9)
    let blob_data = fs::read("./tests/test.blob")?;
    for _ in 0..10 {
        producer.send(blob_data.clone(), None).await?;
    }

    eprintln!("[PHASE 2] Messages sent, waiting for persistence");
    // Give time for messages to be written
    sleep(Duration::from_millis(300)).await;

    eprintln!("[PHASE 2] Reading first 5 messages");
    // Read and ack first 5 messages (0-4)
    let receive_first_batch = async {
        let mut received = 0;
        while received < 5 {
            if let Some(msg) = stream.recv().await {
                assert_eq!(
                    msg.payload, blob_data,
                    "Payload mismatch for message {}",
                    received
                );
                consumer.ack(&msg).await?;
                received += 1;
            }
        }
        Ok::<(), anyhow::Error>(())
    };
    timeout(Duration::from_secs(20), receive_first_batch).await??;
    eprintln!("[PHASE 2] Successfully read and acked messages 0-4");

    // Disconnect consumer
    drop(stream);
    drop(consumer);

    // ========================================================================
    // Phase 3: Unload topic (moves to another broker)
    // ========================================================================
    eprintln!("[PHASE 3] Disconnecting producer and consumer");

    // Stop producer before unload
    drop(producer);

    // Give time for graceful shutdown
    sleep(Duration::from_millis(500)).await;

    eprintln!("[PHASE 3] Executing topic unload via admin CLI");
    // Unload topic using admin CLI
    let output = admin_cli()
        .args(["topics", "unload", &topic])
        .output()
        .expect("Failed to execute admin CLI unload");

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        println!("❌ Unload failed:");
        println!("  stdout: {}", stdout);
        println!("  stderr: {}", stderr);
        anyhow::bail!("Topic unload failed");
    }
    eprintln!("[PHASE 3] Topic unload successful, waiting for reassignment");

    // Wait for topic to be reassigned to another broker
    // In CI environments, this can take longer due to slower I/O and CPU
    sleep(Duration::from_secs(5)).await;

    // Verify topic moved using describe command
    // let describe_output = admin_cli()
    //     .args(["topics", "describe", &topic, "--output", "json"])
    //     .output()
    //     .expect("Failed to describe topic");

    // if describe_output.status.success() {
    //     let body = String::from_utf8_lossy(&describe_output.stdout);
    //     println!("📋 Topic after move: {}", body);
    // }

    // ========================================================================
    // Phase 4: Reconnect producer and send more messages
    // ========================================================================
    eprintln!("[PHASE 4] Reconnecting producer to new broker");

    // Create new producer connection (will connect to new broker)
    let mut producer2 = client
        .new_producer()
        .with_topic(&topic)
        .with_name("producer_topic_move_2")
        .with_reliable_dispatch()
        .build();
    producer2.create().await?;
    eprintln!("[PHASE 4] Producer reconnected");

    // Give producer time to fully connect in CI environment
    sleep(Duration::from_millis(1000)).await;

    eprintln!("[PHASE 4] Sending 8 more messages");
    // Send 8 more messages (should get offsets 10-17 if continuity preserved)
    for _ in 10..18 {
        producer2.send(blob_data.clone(), None).await?;
    }

    eprintln!("[PHASE 4] Messages sent, waiting for persistence");
    // Give time for messages to be persisted
    sleep(Duration::from_millis(500)).await;

    // ========================================================================
    // Phase 5: Consumer reconnects and reads remaining messages
    // ========================================================================
    eprintln!("[PHASE 5] Reconnecting consumer");

    let mut consumer2 = client
        .new_consumer()
        .with_topic(topic.clone())
        .with_consumer_name("consumer_topic_move_2".to_string())
        .with_subscription("sub_topic_move".to_string()) // Same subscription
        .with_subscription_type(SubType::Exclusive)
        .build();
    consumer2.subscribe().await?;
    let mut stream2 = consumer2.receive().await?;
    eprintln!("[PHASE 5] Consumer subscribed, stream ready");

    // Give consumer time to initialize tiered reader (cloud + WAL) in CI
    sleep(Duration::from_millis(1000)).await;

    eprintln!("[PHASE 5] Starting to read remaining 13 messages (offsets 5-17)");
    // Read remaining messages (5-17 = 13 messages)
    let receive_remaining = async {
        let mut received = 0;
        let mut offsets_seen = Vec::new();

        // We expect to receive messages 5-17 (13 messages total)
        while received < 13 {
            if let Some(msg) = stream2.recv().await {
                assert_eq!(msg.payload, blob_data, "Payload mismatch for message");

                let offset = msg.msg_id.topic_offset;
                offsets_seen.push(offset);

                consumer2.ack(&msg).await?;
                received += 1;
                
                // Log progress every 5 messages
                if received % 5 == 0 {
                    eprintln!("[PHASE 5] Received {} messages so far, last offset: {}", received, offset);
                }
            }
        }

        // ====================================================================
        // Critical Verification: Check offset continuity
        // ====================================================================

        // Verify we got exactly the expected offsets (5-17)
        let expected_offsets: Vec<u64> = (5..18).collect();
        assert_eq!(
            offsets_seen, expected_offsets,
            "❌ FAILED: Offset sequence broken!\n  Expected: {:?}\n  Got: {:?}",
            expected_offsets, offsets_seen
        );

        // Verify no gaps in sequence
        for i in 1..offsets_seen.len() {
            let prev = offsets_seen[i - 1];
            let curr = offsets_seen[i];
            assert_eq!(
                curr,
                prev + 1,
                "Gap detected: offset {} followed by {} (expected {})",
                prev,
                curr,
                prev + 1
            );
        }

        eprintln!("[PHASE 5] Successfully received all 13 messages");
        Ok::<(), anyhow::Error>(())
    };

    eprintln!("[PHASE 5] Waiting for messages with 30s timeout...");
    // Longer timeout for CI: reading from cloud storage + WAL can be slow
    timeout(Duration::from_secs(30), receive_remaining).await??;

    eprintln!("[TEST] ✅ Test completed successfully!");
    Ok(())
}

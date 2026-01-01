mod common;
use assert_cmd::assert::OutputAssertExt;
use common::*;
use predicates::prelude::*;
use std::process::Stdio;
use std::time::Duration;

/// Tests the basic end-to-end message flow: produce → consume
///
/// **What we're testing:**
/// - Producer can successfully create and send multiple messages
/// - Messages are delivered to the correct topic
/// - Consumer can subscribe and receive the sent messages
/// - Message content is preserved through the delivery pipeline
///
/// **Why it's important:**
/// This validates the core functionality of Danube CLI - the ability to
/// reliably send and receive messages, which is the foundation for all
/// other features. If this fails, the entire messaging system is broken.
#[test]
fn produce_and_consume_messages() {
    let topic = unique_topic();
    let addr = service_addr();

    // Step 1: Produce 1 message to create the topic
    let mut produce_init = cli();
    produce_init
        .args([
            "produce",
            "-s",
            &addr,
            "-t",
            &topic,
            "-m",
            "Init message",
            "-c",
            "1",
        ])
        .assert()
        .success();

    // Step 2: Start consumer in background
    let mut consume = cli();
    let mut child = consume
        .args([
            "consume",
            "-s",
            &addr,
            "-t",
            &topic,
            "-m",
            "test-subscription",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn consume command");

    // Give consumer time to connect
    std::thread::sleep(Duration::from_millis(500));

    // Step 3: Produce messages while consumer is running
    let mut produce = cli();
    produce
        .args([
            "produce",
            "-s",
            &addr,
            "-t",
            &topic,
            "-m",
            "Hello from integration test",
            "-c",
            "5",
            "-i",
            "100",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Success: 5"));

    // Wait for messages to be consumed (longer to ensure all messages processed)
    std::thread::sleep(Duration::from_secs(2));

    // Kill the consumer (it runs indefinitely)
    let _ = child.kill();
    let output = child.wait_with_output().expect("wait for output");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{}{}", stdout, stderr);

    // Verify we received messages (check both stdout and stderr)
    assert!(
        combined.contains("Received message: Hello from integration test"),
        "Expected message not found. stdout: {}, stderr: {}",
        stdout,
        stderr
    );
    assert!(combined.contains("Total received:"));
}

/// Tests message attributes and reliable delivery mode
///
/// **What we're testing:**
/// - Producer can attach custom attributes to messages (key:value pairs)
/// - Reliable delivery flag (`--reliable`) works correctly
/// - Messages with metadata are successfully sent
/// - Producer reports correct success count
///
/// **Why it's important:**
/// Message attributes enable routing, filtering, and metadata tracking in
/// production systems. Reliable delivery ensures messages are persisted to
/// disk before acknowledgment, preventing data loss. These features are
/// critical for enterprise use cases.
#[test]
fn produce_with_attributes_and_reliable() {
    let topic = unique_topic();
    let addr = service_addr();

    // Produce with attributes and reliable delivery
    let mut produce = cli();
    produce
        .args([
            "produce",
            "-s",
            &addr,
            "-t",
            &topic,
            "-m",
            "Message with metadata",
            "-a",
            "priority:high,source:test",
            "--reliable",
            "-c",
            "3",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Success: 3"));
}

/// Tests different subscription types: exclusive and shared
///
/// **What we're testing:**
/// - Exclusive subscription type works (single consumer, ordered processing)
/// - Shared subscription type works (load balanced across consumers)
/// - Both subscription types can receive messages from the same topic
/// - Subscription type flag (`--sub-type`) is correctly processed
///
/// **Why it's important:**
/// Different subscription types enable different messaging patterns:
/// - Exclusive: Ordered processing, single consumer workloads
/// - Shared: Parallel processing, load balancing, scalability
/// - Failover: High availability with ordered processing
/// These patterns are essential for building production messaging systems.
#[test]
fn subscription_types() {
    let topic = unique_topic();
    let addr = service_addr();

    // Step 1: Produce 1 message to create the topic
    let mut produce_init = cli();
    produce_init
        .args([
            "produce",
            "-s",
            &addr,
            "-t",
            &topic,
            "-m",
            "Init message",
            "-c",
            "1",
        ])
        .assert()
        .success();

    // Test exclusive subscription
    // Step 2: Start exclusive consumer
    let mut consume_exclusive = cli();
    let mut child = consume_exclusive
        .args([
            "consume",
            "-s",
            &addr,
            "-t",
            &topic,
            "-m",
            "exclusive-sub",
            "--sub-type",
            "exclusive",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn consume exclusive");

    // Give consumer time to connect
    std::thread::sleep(Duration::from_millis(500));

    // Step 3: Produce messages while exclusive consumer is running
    let mut produce = cli();
    produce
        .args([
            "produce",
            "-s",
            &addr,
            "-t",
            &topic,
            "-m",
            "Test message",
            "-c",
            "3",
        ])
        .assert()
        .success();

    std::thread::sleep(Duration::from_secs(2));
    let _ = child.kill();
    let output = child.wait_with_output().expect("wait for output");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{}{}", stdout, stderr);
    
    assert!(
        combined.contains("Received message: Test message"),
        "Expected 'Test message' not found in exclusive consumer. stdout: {}, stderr: {}",
        stdout,
        stderr
    );

    // Test shared subscription
    // Step 4: Start shared consumer
    let mut consume_shared = cli();
    let mut child = consume_shared
        .args([
            "consume",
            "-s",
            &addr,
            "-t",
            &topic,
            "-m",
            "shared-sub",
            "--sub-type",
            "shared",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn consume shared");

    // Give consumer time to connect
    std::thread::sleep(Duration::from_millis(500));

    // Step 5: Produce messages while shared consumer is running
    let mut produce2 = cli();
    produce2
        .args([
            "produce",
            "-s",
            &addr,
            "-t",
            &topic,
            "-m",
            "Shared test message",
            "-c",
            "3",
        ])
        .assert()
        .success();

    std::thread::sleep(Duration::from_secs(2));
    let _ = child.kill();
    let output = child.wait_with_output().expect("wait for output");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{}{}", stdout, stderr);
    
    assert!(
        combined.contains("Received message: Shared test message"),
        "Expected 'Shared test message' not found in shared consumer. stdout: {}, stderr: {}",
        stdout,
        stderr
    );
}

//! Key-Shared safety & backpressure integration tests
//!
//! Tests verify the safety mechanisms added to the Key-Shared reliable dispatcher:
//! - Poison handling (Drop/Block/DLQ) for retry-exhausted messages
//! - Inactive consumer eviction and key redistribution
//!
//! These tests use the same pattern as `reliable_dispatch_failure_handling.rs`,
//! adapted for KeyShared subscription type with routing keys.

extern crate danube_client;

use anyhow::{bail, Context, Result};
use danube_client::{DanubeClient, SubType};
use serde::Deserialize;
use std::collections::HashSet;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::Command;
use tokio::time::{sleep, timeout, Duration};

#[path = "test_utils.rs"]
mod test_utils;

// --- Admin CLI helpers (shared with reliable_dispatch_failure_handling.rs) ---

#[derive(Debug, Clone, PartialEq, Eq)]
struct FailurePolicySpec {
    max_redelivery_count: u32,
    ack_timeout_ms: u64,
    base_redelivery_delay_ms: u64,
    max_redelivery_delay_ms: u64,
    backoff_strategy: &'static str,
    dead_letter_topic: Option<String>,
    poison_policy: &'static str,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[allow(dead_code)]
struct FailurePolicyDescription {
    max_redelivery_count: u32,
    ack_timeout_ms: u64,
    base_redelivery_delay_ms: u64,
    max_redelivery_delay_ms: u64,
    backoff_strategy: String,
    dead_letter_topic: Option<String>,
    poison_policy: String,
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap_or_else(|| Path::new(env!("CARGO_MANIFEST_DIR")))
        .to_path_buf()
}

fn resolve_existing_path(path: impl AsRef<Path>) -> Option<PathBuf> {
    let path = path.as_ref();
    let candidates = if path.is_absolute() {
        vec![path.to_path_buf()]
    } else {
        let mut candidates = Vec::new();
        if let Ok(current_dir) = std::env::current_dir() {
            candidates.push(current_dir.join(path));
        }
        candidates.push(PathBuf::from(path));
        candidates.push(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(path));
        candidates.push(workspace_root().join(path));
        candidates
    };

    candidates.into_iter().find(|candidate| candidate.exists())
}

fn admin_cli() -> (Command, String) {
    let mut resolved_program = None;

    if let Ok(bin) = std::env::var("DANUBE_ADMIN_BIN") {
        resolved_program = resolve_existing_path(&bin).or_else(|| Some(PathBuf::from(bin)));
    }

    if resolved_program.is_none() {
        resolved_program = resolve_existing_path("./target/release/danube-admin");
    }
    if resolved_program.is_none() {
        resolved_program = resolve_existing_path("./target/debug/danube-admin");
    }

    let (mut cmd, program_description) = if let Some(program) = resolved_program {
        let description = program.display().to_string();
        (Command::new(&program), description)
    } else {
        let mut cargo = Command::new("cargo");
        cargo.current_dir(workspace_root());
        cargo.args(["run", "-p", "danube-admin", "--bin", "danube-admin", "--"]);
        (
            cargo,
            "cargo run -p danube-admin --bin danube-admin --".to_string(),
        )
    };

    cmd.env(
        "DANUBE_ADMIN_ENDPOINT",
        std::env::var("DANUBE_ADMIN_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:50051".into()),
    );
    (cmd, program_description)
}

fn run_admin<S: AsRef<OsStr>>(args: &[S]) -> Result<String> {
    let (mut cmd, program_description) = admin_cli();
    let output = cmd.args(args).output().with_context(|| {
        format!(
            "failed to execute danube-admin using {}",
            program_description
        )
    })?;

    if !output.status.success() {
        bail!(
            "danube-admin failed using {}\nstdout:\n{}\nstderr:\n{}",
            program_description,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    String::from_utf8(output.stdout).context("danube-admin output was not valid utf-8")
}

fn create_topic(topic: &str, dispatch_strategy: &str) -> Result<()> {
    let args = vec![
        "topics".to_string(),
        "create".to_string(),
        topic.to_string(),
        "--dispatch-strategy".to_string(),
        dispatch_strategy.to_string(),
    ];
    let _ = run_admin(&args)?;
    Ok(())
}

fn set_failure_policy(
    topic: &str,
    subscription: &str,
    failure_policy: &FailurePolicySpec,
) -> Result<()> {
    let mut args = vec![
        "topics".to_string(),
        "set-failure-policy".to_string(),
        topic.to_string(),
        "--subscription".to_string(),
        subscription.to_string(),
        "--max-redelivery-count".to_string(),
        failure_policy.max_redelivery_count.to_string(),
        "--ack-timeout-ms".to_string(),
        failure_policy.ack_timeout_ms.to_string(),
        "--base-redelivery-delay-ms".to_string(),
        failure_policy.base_redelivery_delay_ms.to_string(),
        "--max-redelivery-delay-ms".to_string(),
        failure_policy.max_redelivery_delay_ms.to_string(),
        "--backoff-strategy".to_string(),
        failure_policy.backoff_strategy.to_string(),
        "--poison-policy".to_string(),
        failure_policy.poison_policy.to_string(),
    ];

    if let Some(dead_letter_topic) = failure_policy.dead_letter_topic.as_ref() {
        args.push("--dead-letter-topic".to_string());
        args.push(dead_letter_topic.clone());
    }

    let _ = run_admin(&args)?;
    Ok(())
}

fn fixed_failure_policy(
    max_redelivery_count: u32,
    ack_timeout_ms: u64,
    poison_policy: &'static str,
    dead_letter_topic: Option<String>,
) -> FailurePolicySpec {
    FailurePolicySpec {
        max_redelivery_count,
        ack_timeout_ms,
        base_redelivery_delay_ms: 50,
        max_redelivery_delay_ms: 50,
        backoff_strategy: "fixed",
        dead_letter_topic,
        poison_policy,
    }
}

async fn create_reliable_producer(
    client: &DanubeClient,
    topic: &str,
    producer_name: &str,
) -> Result<danube_client::Producer> {
    let mut producer = client
        .new_producer()
        .with_topic(topic)
        .with_name(producer_name)
        .with_reliable_dispatch()
        .build()?;
    producer.create().await?;
    Ok(producer)
}

async fn create_key_shared_consumer(
    client: &DanubeClient,
    topic: &str,
    consumer_name: &str,
    subscription: &str,
) -> Result<danube_client::Consumer> {
    let mut consumer = client
        .new_consumer()
        .with_topic(topic.to_string())
        .with_consumer_name(consumer_name.to_string())
        .with_subscription(subscription.to_string())
        .with_subscription_type(SubType::KeyShared)
        .build()?;
    consumer.subscribe().await?;
    Ok(consumer)
}

// =============================================================================
// Test 1: Key-Shared Drop poison policy — nack exhausts retries, message is
// dropped, and the subscription progresses to the next message for that key.
// =============================================================================

/// Verifies that the `drop` poison policy on a Key-Shared subscription discards
/// a retry-exhausted message and allows the key to continue receiving messages.
#[tokio::test]
async fn key_shared_reliable_drop_poison_skips_exhausted_message() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/ks_safety_drop");
    let subscription = "sub_ks_safety_drop";

    create_topic(&topic, "reliable")?;

    // Set failure policy BEFORE consumer subscribes (dispatcher caches policy at creation)
    set_failure_policy(
        &topic,
        subscription,
        &fixed_failure_policy(0, 5_000, "drop", None),
    )?;

    let mut consumer =
        create_key_shared_consumer(&client, &topic, "consumer_ks_drop", subscription).await?;
    let mut stream = consumer.receive().await?;
    sleep(Duration::from_millis(200)).await;

    let producer = create_reliable_producer(&client, &topic, "producer_ks_drop").await?;

    // Send first message with key "order"
    producer
        .send_with_key(b"drop-me".to_vec(), None, "order")
        .await?;

    let first = timeout(Duration::from_secs(5), stream.recv())
        .await?
        .expect("expected first delivery");
    assert_eq!(first.payload.as_ref(), b"drop-me");

    // NACK it — with max_redelivery_count=0, this exhausts retries immediately
    consumer
        .nack(&first, Some(0), Some("drop it".to_string()))
        .await?;

    // Wait for poison handler to process
    sleep(Duration::from_millis(500)).await;

    // Send another message with the SAME key — should be delivered (key freed)
    producer
        .send_with_key(b"after-drop".to_vec(), None, "order")
        .await?;

    let next = timeout(Duration::from_secs(5), stream.recv())
        .await?
        .expect("expected next message after drop — key should be freed");
    assert_eq!(next.payload.as_ref(), b"after-drop");
    consumer.ack(&next).await?;

    Ok(())
}

// =============================================================================
// Test 2: Key-Shared Block poison policy — nack exhausts retries, key is
// blocked, but other keys on the same consumer continue working.
// =============================================================================

/// Verifies that the `block` poison policy on a Key-Shared subscription blocks
/// only the affected key while other keys continue to be delivered.
#[tokio::test]
async fn key_shared_reliable_block_poison_stalls_only_affected_key() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/ks_safety_block");
    let subscription = "sub_ks_safety_block";

    create_topic(&topic, "reliable")?;

    // Set failure policy BEFORE consumer subscribes
    set_failure_policy(
        &topic,
        subscription,
        &fixed_failure_policy(0, 5_000, "block", None),
    )?;

    let mut consumer =
        create_key_shared_consumer(&client, &topic, "consumer_ks_block", subscription).await?;
    let mut stream = consumer.receive().await?;
    sleep(Duration::from_millis(200)).await;

    let producer = create_reliable_producer(&client, &topic, "producer_ks_block").await?;

    // Send message with key "blocked-key"
    producer
        .send_with_key(b"block-me".to_vec(), None, "blocked-key")
        .await?;

    let first = timeout(Duration::from_secs(5), stream.recv())
        .await?
        .expect("expected first delivery");
    assert_eq!(first.payload.as_ref(), b"block-me");

    // NACK it to exhaust retries
    consumer
        .nack(&first, Some(0), Some("block it".to_string()))
        .await?;

    sleep(Duration::from_millis(500)).await;

    // Send message with a DIFFERENT key — should still be delivered
    producer
        .send_with_key(b"other-key-msg".to_vec(), None, "healthy-key")
        .await?;

    let next = timeout(Duration::from_secs(5), stream.recv())
        .await?
        .expect("expected message on healthy key — only blocked-key should stall");
    assert_eq!(next.payload.as_ref(), b"other-key-msg");
    consumer.ack(&next).await?;

    // Send another message with the BLOCKED key — should NOT be delivered
    producer
        .send_with_key(b"still-blocked".to_vec(), None, "blocked-key")
        .await?;

    let blocked = timeout(Duration::from_secs(2), stream.recv()).await;
    assert!(
        blocked.is_err(),
        "blocked key should not deliver new messages"
    );

    Ok(())
}

// =============================================================================
// Test 3: Key-Shared DLQ poison policy — nack exhausts retries, message is
// routed to DLQ with metadata, and the key resumes.
// =============================================================================

/// Verifies that the `dead_letter` poison policy on Key-Shared routes the
/// exhausted message to DLQ with origin metadata and frees the key.
#[tokio::test]
async fn key_shared_reliable_deadletter_routes_to_dlq_and_frees_key() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/ks_safety_dlq");
    let dlq_topic = format!("{}-dlq", topic);
    let subscription = "sub_ks_safety_dlq";

    create_topic(&topic, "reliable")?;
    create_topic(&dlq_topic, "reliable")?;

    // Set failure policy BEFORE consumer subscribes
    set_failure_policy(
        &topic,
        subscription,
        &fixed_failure_policy(0, 5_000, "dead_letter", Some(dlq_topic.clone())),
    )?;

    let mut consumer =
        create_key_shared_consumer(&client, &topic, "consumer_ks_dlq", subscription).await?;
    let mut stream = consumer.receive().await?;

    // DLQ consumer
    let mut dlq_consumer = client
        .new_consumer()
        .with_topic(dlq_topic.to_string())
        .with_consumer_name("consumer_ks_dlq_reader".to_string())
        .with_subscription("sub_ks_dlq_reader".to_string())
        .with_subscription_type(SubType::Exclusive)
        .build()?;
    dlq_consumer.subscribe().await?;
    let mut dlq_stream = dlq_consumer.receive().await?;

    sleep(Duration::from_millis(200)).await;

    let producer = create_reliable_producer(&client, &topic, "producer_ks_dlq").await?;

    producer
        .send_with_key(b"to-dlq".to_vec(), None, "dlq-key")
        .await?;

    let first = timeout(Duration::from_secs(5), stream.recv())
        .await?
        .expect("expected first delivery");
    assert_eq!(first.payload.as_ref(), b"to-dlq");

    consumer
        .nack(&first, Some(0), Some("dead-letter it".to_string()))
        .await?;

    // Verify DLQ receives the message with metadata
    let dlq_msg = timeout(Duration::from_secs(5), dlq_stream.recv())
        .await?
        .expect("expected dlq message");
    assert_eq!(dlq_msg.payload.as_ref(), b"to-dlq");
    assert_eq!(dlq_msg.msg_id.topic_name, dlq_topic);
    assert_eq!(
        dlq_msg
            .attributes
            .get("x-original-topic")
            .map(String::as_str),
        Some(topic.as_str())
    );
    assert_eq!(
        dlq_msg
            .attributes
            .get("x-poison-policy")
            .map(String::as_str),
        Some("dead_letter")
    );

    // Verify key is freed — send another message with the same key
    sleep(Duration::from_millis(200)).await;
    producer
        .send_with_key(b"after-dlq".to_vec(), None, "dlq-key")
        .await?;

    let next = timeout(Duration::from_secs(5), stream.recv())
        .await?
        .expect("expected progress after DLQ routing — key should be freed");
    assert_eq!(next.payload.as_ref(), b"after-dlq");
    consumer.ack(&next).await?;

    Ok(())
}

// =============================================================================
// Test 4: Key-Shared inactive consumer eviction — disconnect one consumer,
// verify the surviving consumer receives all keys after eviction grace period.
// =============================================================================

/// Verifies that when a Key-Shared consumer disconnects, the broker evicts it
/// from the hash ring after the grace period and redistributes its keys to
/// the surviving consumer.
///
/// Strategy: send one message at a time and wait for delivery, avoiding
/// PollAndDispatch notification coalescing.
///
/// IGNORED: Requires cross-broker eviction protocol. In a multi-broker cluster,
/// the consumer's gRPC handler runs on the broker the client connected to (which
/// calls `set_status_inactive()` on a local Arc<AtomicBool>), but the dispatcher
/// runs on the broker owning the topic. Since the Arc is not shared across brokers,
/// the heartbeat never sees the consumer as inactive but continues to work as expected
/// as long as the consumer is on the same broker as the dispatcher (single-broker cluster).
#[tokio::test]
#[ignore = "requires single-broker cluster or cross-broker eviction protocol"]
async fn key_shared_reliable_inactive_consumer_eviction() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/ks_safety_evict");
    let subscription = "sub_ks_safety_evict";

    create_topic(&topic, "reliable")?;

    // Create two consumers on the same subscription
    let mut consumer_a =
        create_key_shared_consumer(&client, &topic, "consumer_evict_a", subscription).await?;
    let mut consumer_b =
        create_key_shared_consumer(&client, &topic, "consumer_evict_b", subscription).await?;

    let mut stream_a = consumer_a.receive().await?;
    let mut stream_b = consumer_b.receive().await?;
    sleep(Duration::from_millis(500)).await;

    let producer = create_reliable_producer(&client, &topic, "producer_ks_evict").await?;

    // Phase 1: Both consumers active — figure out which keys go to which consumer.
    // Send a "probe" message for each key and record which consumer gets it.
    let keys = vec!["alpha", "beta", "gamma", "delta", "epsilon"];
    let mut b_keys = Vec::new();

    for key in &keys {
        producer
            .send_with_key(format!("pre-{}", key).into_bytes(), None, key)
            .await?;

        // Wait for delivery to either consumer
        tokio::select! {
            msg = stream_a.recv() => {
                if let Some(m) = msg {
                    consumer_a.ack(&m).await?;
                }
            }
            msg = stream_b.recv() => {
                if let Some(m) = msg {
                    b_keys.push(key.to_string());
                    consumer_b.ack(&m).await?;
                }
            }
        }
    }

    // Phase 2: Disconnect consumer B
    drop(stream_b);
    drop(consumer_b);

    // Wait for eviction grace period (3s = 6 ticks at 500ms) + buffer
    sleep(Duration::from_secs(6)).await;

    // Phase 3: Send messages for keys that USED to belong to consumer B.
    // After eviction, consumer A should receive them (proving redistribution).
    // If no keys went to B (unlikely with 5 keys), the test is vacuously true.
    assert!(
        !b_keys.is_empty(),
        "expected at least one key routed to consumer B for redistribution test"
    );

    for key in &b_keys {
        producer
            .send_with_key(format!("post-{}", key).into_bytes(), None, key.as_str())
            .await?;

        // Consumer A must receive this — previously it was consumer B's key
        let msg = timeout(Duration::from_secs(10), stream_a.recv())
            .await?
            .expect(&format!(
                "consumer A should receive key '{}' after eviction",
                key
            ));

        let payload = String::from_utf8_lossy(&msg.payload).to_string();
        assert_eq!(payload, format!("post-{}", key));
        consumer_a.ack(&msg).await?;
    }

    Ok(())
}

// =============================================================================
// Test 5: Key-Shared nack redelivery — nack a message, verify the same message
// is redelivered to the same consumer (same key binding).
// =============================================================================

/// Verifies that Key-Shared reliable dispatch redelivers messages after a NACK.
/// The redelivered message should go to the same consumer (key affinity is preserved).
///
/// NOTE: Ack-timeout-based redelivery also works but the engine caches the failure
/// policy at subscription creation time. Since the admin CLI can't update the
/// in-memory ack_timeout_ms on an already-running dispatcher, we test the nack
/// path instead (which exercises the same retry/redeliver logic).
#[tokio::test]
async fn key_shared_reliable_nack_redelivers_same_message() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/ks_safety_nack_redeliver");
    let subscription = "sub_ks_safety_nack_redeliver";

    create_topic(&topic, "reliable")?;

    // max_redelivery_count=2 so nack triggers retry, not exhaustion
    set_failure_policy(
        &topic,
        subscription,
        &fixed_failure_policy(2, 5_000, "block", None),
    )?;

    let mut consumer =
        create_key_shared_consumer(&client, &topic, "consumer_ks_nack_redeliver", subscription)
            .await?;
    let mut stream = consumer.receive().await?;
    sleep(Duration::from_millis(200)).await;

    let producer = create_reliable_producer(&client, &topic, "producer_ks_nack_redeliver").await?;

    producer
        .send_with_key(b"nack-me".to_vec(), None, "nack-key")
        .await?;

    // Receive first delivery
    let first = timeout(Duration::from_secs(5), stream.recv())
        .await?
        .expect("expected first delivery");
    assert_eq!(first.payload.as_ref(), b"nack-me");

    // NACK with delay_ms=0 for immediate retry
    consumer
        .nack(&first, Some(0), Some("retry please".to_string()))
        .await?;

    // Wait for heartbeat to re-dispatch the retry-ready entry
    let redelivered = timeout(Duration::from_secs(10), stream.recv())
        .await?
        .expect("expected redelivery after nack");
    assert_eq!(redelivered.payload, first.payload);
    assert_eq!(redelivered.msg_id.topic_offset, first.msg_id.topic_offset);
    consumer.ack(&redelivered).await?;

    Ok(())
}

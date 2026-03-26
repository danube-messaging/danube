extern crate danube_client;

use anyhow::{bail, Context, Result};
use danube_client::{DanubeClient, SubType};
use serde::Deserialize;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::Command;
use tokio::time::{sleep, timeout, Duration};

#[path = "test_utils.rs"]
mod test_utils;

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
        (cargo, "cargo run -p danube-admin --bin danube-admin --".to_string())
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

    let stored = get_failure_policy(topic, subscription)?;

    assert_eq!(stored.max_redelivery_count, failure_policy.max_redelivery_count);
    assert_eq!(stored.ack_timeout_ms, failure_policy.ack_timeout_ms);
    assert_eq!(
        stored.base_redelivery_delay_ms,
        failure_policy.base_redelivery_delay_ms
    );
    assert_eq!(
        stored.max_redelivery_delay_ms,
        failure_policy.max_redelivery_delay_ms
    );
    assert_eq!(stored.backoff_strategy, failure_policy.backoff_strategy);
    assert_eq!(stored.dead_letter_topic, failure_policy.dead_letter_topic);
    assert_eq!(stored.poison_policy, failure_policy.poison_policy);

    Ok(())
}

fn get_failure_policy(topic: &str, subscription: &str) -> Result<FailurePolicyDescription> {
    let args = vec![
        "topics".to_string(),
        "get-failure-policy".to_string(),
        topic.to_string(),
        "--subscription".to_string(),
        subscription.to_string(),
        "--output".to_string(),
        "json".to_string(),
    ];
    let body = run_admin(&args)?;
    serde_json::from_str(&body).context("failed to parse failure policy output")
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

async fn create_consumer(
    client: &DanubeClient,
    topic: &str,
    consumer_name: &str,
    subscription: &str,
    sub_type: SubType,
) -> Result<danube_client::Consumer> {
    let mut consumer = client
        .new_consumer()
        .with_topic(topic.to_string())
        .with_consumer_name(consumer_name.to_string())
        .with_subscription(subscription.to_string())
        .with_subscription_type(sub_type)
        .build()?;
    consumer.subscribe().await?;
    Ok(consumer)
}

/// Verifies that a reliable exclusive subscription redelivers the same message
/// after the consumer issues a negative acknowledgment.
/// This protects the at-least-once contract for the explicit failure path.
#[tokio::test]
async fn reliable_exclusive_nack_redelivers_same_message() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/reliable_failure_nack_exclusive");
    let subscription = "sub_failure_nack_exclusive";

    create_topic(&topic, "reliable")?;
    set_failure_policy(
        &topic,
        subscription,
        &fixed_failure_policy(2, 5_000, "block", None),
    )
    ?;

    let producer =
        create_reliable_producer(&client, &topic, "producer_failure_nack_exclusive").await?;
    let mut consumer = create_consumer(
        &client,
        &topic,
        "consumer_failure_nack_exclusive",
        subscription,
        SubType::Exclusive,
    )
    .await?;
    let mut stream = consumer.receive().await?;

    sleep(Duration::from_millis(200)).await;

    producer.send(b"nack-exclusive".to_vec(), None).await?;

    let first = timeout(Duration::from_secs(5), stream.recv())
        .await?
        .expect("expected first delivery");
    consumer
        .nack(&first, Some(0), Some("retry please".to_string()))
        .await?;

    let redelivered = timeout(Duration::from_secs(5), stream.recv())
        .await?
        .expect("expected redelivery");
    assert_eq!(redelivered.payload, first.payload);
    assert_eq!(redelivered.msg_id.topic_offset, first.msg_id.topic_offset);
    consumer.ack(&redelivered).await?;

    Ok(())
}

/// Verifies that the reliable shared dispatcher also redelivers the same
/// message after a negative acknowledgment.
/// This ensures the shared path matches the exclusive path for nack handling.
#[tokio::test]
async fn reliable_shared_nack_redelivers_same_message() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/reliable_failure_nack_shared");
    let subscription = "sub_failure_nack_shared";

    create_topic(&topic, "reliable")?;
    set_failure_policy(
        &topic,
        subscription,
        &fixed_failure_policy(2, 5_000, "block", None),
    )
    ?;

    let producer =
        create_reliable_producer(&client, &topic, "producer_failure_nack_shared").await?;
    let mut consumer = create_consumer(
        &client,
        &topic,
        "consumer_failure_nack_shared",
        subscription,
        SubType::Shared,
    )
    .await?;
    let mut stream = consumer.receive().await?;

    sleep(Duration::from_millis(200)).await;

    producer.send(b"nack-shared".to_vec(), None).await?;

    let first = timeout(Duration::from_secs(5), stream.recv())
        .await?
        .expect("expected first delivery");
    consumer
        .nack(&first, Some(0), Some("retry shared".to_string()))
        .await?;

    let redelivered = timeout(Duration::from_secs(5), stream.recv())
        .await?
        .expect("expected redelivery");
    assert_eq!(redelivered.payload, first.payload);
    assert_eq!(redelivered.msg_id.topic_offset, first.msg_id.topic_offset);
    consumer.ack(&redelivered).await?;

    Ok(())
}

/// Verifies that when a consumer does not acknowledge a message before the
/// configured timeout, the broker redelivers that same message automatically.
/// This covers the implicit failure path where no explicit nack is sent.
#[tokio::test]
async fn reliable_exclusive_ack_timeout_redelivers_same_message() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/reliable_failure_ack_timeout");
    let subscription = "sub_failure_ack_timeout";

    create_topic(&topic, "reliable")?;
    set_failure_policy(
        &topic,
        subscription,
        &fixed_failure_policy(2, 200, "block", None),
    )
    ?;

    let producer =
        create_reliable_producer(&client, &topic, "producer_failure_ack_timeout").await?;
    let mut consumer = create_consumer(
        &client,
        &topic,
        "consumer_failure_ack_timeout",
        subscription,
        SubType::Exclusive,
    )
    .await?;
    let mut stream = consumer.receive().await?;

    sleep(Duration::from_millis(200)).await;

    producer.send(b"ack-timeout".to_vec(), None).await?;

    let first = timeout(Duration::from_secs(5), stream.recv())
        .await?
        .expect("expected first delivery");

    let redelivered = timeout(Duration::from_secs(10), stream.recv())
        .await?
        .expect("expected timeout-based redelivery");
    assert_eq!(redelivered.payload, first.payload);
    assert_eq!(redelivered.msg_id.topic_offset, first.msg_id.topic_offset);
    consumer.ack(&redelivered).await?;

    Ok(())
}

/// Verifies that the `block` poison policy stops subscription progress once a
/// message exhausts its retry budget.
/// The test confirms later messages are not delivered past the blocked one.
#[tokio::test]
async fn reliable_retry_exhausted_block_prevents_progress() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/reliable_failure_block");
    let subscription = "sub_failure_block";

    create_topic(&topic, "reliable")?;
    set_failure_policy(
        &topic,
        subscription,
        &fixed_failure_policy(0, 5_000, "block", None),
    )
    ?;

    let producer = create_reliable_producer(&client, &topic, "producer_failure_block").await?;
    let mut consumer = create_consumer(
        &client,
        &topic,
        "consumer_failure_block",
        subscription,
        SubType::Exclusive,
    )
    .await?;
    let mut stream = consumer.receive().await?;

    sleep(Duration::from_millis(200)).await;

    producer.send(b"blocked-msg".to_vec(), None).await?;

    let first = timeout(Duration::from_secs(5), stream.recv())
        .await?
        .expect("expected first delivery");
    consumer
        .nack(&first, Some(0), Some("block it".to_string()))
        .await?;

    producer.send(b"after-block".to_vec(), None).await?;

    let blocked = timeout(Duration::from_secs(2), stream.recv()).await;
    assert!(blocked.is_err(), "blocked poison policy should stall progress");

    Ok(())
}

/// Verifies that the `drop` poison policy discards a message after retry
/// exhaustion and allows the subscription to continue with the next message.
/// This protects forward progress when operators choose to skip poison items.
#[tokio::test]
async fn reliable_retry_exhausted_drop_skips_poisoned_message() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/reliable_failure_drop");
    let subscription = "sub_failure_drop";

    create_topic(&topic, "reliable")?;
    set_failure_policy(
        &topic,
        subscription,
        &fixed_failure_policy(0, 5_000, "drop", None),
    )
    ?;

    let producer = create_reliable_producer(&client, &topic, "producer_failure_drop").await?;
    let mut consumer = create_consumer(
        &client,
        &topic,
        "consumer_failure_drop",
        subscription,
        SubType::Exclusive,
    )
    .await?;
    let mut stream = consumer.receive().await?;

    sleep(Duration::from_millis(200)).await;

    producer.send(b"drop-me".to_vec(), None).await?;

    let first = timeout(Duration::from_secs(5), stream.recv())
        .await?
        .expect("expected first delivery");
    consumer
        .nack(&first, Some(0), Some("drop it".to_string()))
        .await?;

    sleep(Duration::from_millis(200)).await;
    producer.send(b"after-drop".to_vec(), None).await?;

    let next = timeout(Duration::from_secs(5), stream.recv())
        .await?
        .expect("expected next message after drop");
    assert_eq!(next.payload.as_ref(), b"after-drop");
    consumer.ack(&next).await?;

    Ok(())
}

/// Verifies that the `dead_letter` poison policy republishes the exhausted
/// message to the configured DLQ with origin and failure metadata attached.
/// It also confirms the main subscription resumes progress after DLQ routing.
#[tokio::test]
async fn reliable_retry_exhausted_deadletter_routes_message_to_dlq() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/reliable_failure_deadletter");
    let dlq_topic = format!("{}-dlq", topic);
    let subscription = "sub_failure_deadletter";

    create_topic(&topic, "reliable")?;
    create_topic(&dlq_topic, "non_reliable")?;
    set_failure_policy(
        &topic,
        subscription,
        &fixed_failure_policy(
            0,
            5_000,
            "dead_letter",
            Some(dlq_topic.clone()),
        ),
    )
    ?;

    let producer =
        create_reliable_producer(&client, &topic, "producer_failure_deadletter").await?;
    let mut consumer = create_consumer(
        &client,
        &topic,
        "consumer_failure_deadletter",
        subscription,
        SubType::Exclusive,
    )
    .await?;
    let mut stream = consumer.receive().await?;

    let mut dlq_consumer = create_consumer(
        &client,
        &dlq_topic,
        "consumer_failure_deadletter_dlq",
        "sub_failure_deadletter_dlq",
        SubType::Exclusive,
    )
    .await?;
    let mut dlq_stream = dlq_consumer.receive().await?;

    sleep(Duration::from_millis(200)).await;

    producer.send(b"to-dlq".to_vec(), None).await?;

    let first = timeout(Duration::from_secs(5), stream.recv())
        .await?
        .expect("expected first delivery");
    consumer
        .nack(&first, Some(0), Some("dead-letter it".to_string()))
        .await?;

    let dlq_msg = timeout(Duration::from_secs(5), dlq_stream.recv())
        .await?
        .expect("expected dlq message");
    assert_eq!(dlq_msg.payload.as_ref(), b"to-dlq");
    assert_eq!(dlq_msg.msg_id.topic_name, dlq_topic);
    assert_eq!(
        dlq_msg.attributes.get("x-original-topic").map(String::as_str),
        Some(topic.as_str())
    );
    assert_eq!(
        dlq_msg
            .attributes
            .get("x-original-subscription")
            .map(String::as_str),
        Some(subscription)
    );
    assert_eq!(
        dlq_msg.attributes.get("x-poison-policy").map(String::as_str),
        Some("dead_letter")
    );
    assert_eq!(
        dlq_msg.attributes.get("x-failure-reason").map(String::as_str),
        Some("dead-letter it")
    );

    sleep(Duration::from_millis(200)).await;
    producer.send(b"after-dlq".to_vec(), None).await?;

    let next = timeout(Duration::from_secs(5), stream.recv())
        .await?
        .expect("expected progress after dlq routing");
    assert_eq!(next.payload.as_ref(), b"after-dlq");
    consumer.ack(&next).await?;

    Ok(())
}

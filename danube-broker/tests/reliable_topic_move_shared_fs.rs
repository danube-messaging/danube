//! Shared-fs end-to-end coverage for reliable topic unload and reassignment.
//!
//! These ignored integration tests exercise reliable topics running on a
//! multi-broker cluster configured with `shared_fs` durable storage. The goal is
//! to verify that an explicit unload moves topic ownership to a different broker
//! without losing durable state or breaking reliable-dispatch semantics.
//!
//! The scenarios covered here are:
//!
//! 1. Offset continuity across a broker move. Messages acknowledged before the
//!    move must stay acknowledged, messages not yet consumed must remain
//!    available, and newly produced messages on the new owner must continue the
//!    existing topic offset sequence.
//! 2. Redelivery of an unacked message across a broker move. A message delivered
//!    before unload but left unacknowledged must be replayed after reassignment
//!    before the rest of the remaining stream is consumed.

extern crate danube_client;

use anyhow::{bail, Context, Result};
use danube_client::SubType;
use serde::Deserialize;
use std::path::{Path, PathBuf};
use std::process::Command;
use tokio::time::{sleep, timeout, Duration, Instant};

#[path = "test_utils.rs"]
mod test_utils;

#[derive(Debug, Deserialize)]
struct TopicDescription {
    broker_id: String,
    delivery: String,
}

#[derive(Debug, Deserialize)]
struct BrokerSummary {
    broker_id: String,
}

#[derive(Debug, Deserialize)]
struct BrokerTopicEntry {
    name: String,
    broker_id: String,
    delivery: String,
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

fn run_admin(args: &[&str]) -> Result<String> {
    let (mut cmd, program_description) = admin_cli();
    let output = cmd
        .args(args)
        .output()
        .with_context(|| {
            format!(
                "failed to execute danube-admin {:?} using {}",
                args, program_description
            )
        })?;

    if !output.status.success() {
        bail!(
            "danube-admin {:?} failed using {}\nstdout:\n{}\nstderr:\n{}",
            args,
            program_description,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    String::from_utf8(output.stdout).context("danube-admin output was not valid utf-8")
}

fn describe_topic(topic: &str) -> Result<TopicDescription> {
    let body = run_admin(&["topics", "describe", topic, "--output", "json"])?;
    serde_json::from_str(&body).with_context(|| format!("failed to parse topic description for {topic}"))
}

fn list_brokers() -> Result<Vec<BrokerSummary>> {
    let body = run_admin(&["brokers", "list", "--output", "json"])?;
    serde_json::from_str(&body).context("failed to parse broker list")
}

fn list_topics_for_broker(broker_id: &str) -> Result<Vec<BrokerTopicEntry>> {
    let body = run_admin(&["topics", "list", "--broker", broker_id, "--output", "json"])?;
    serde_json::from_str(&body)
        .with_context(|| format!("failed to parse topics for broker {broker_id}"))
}

fn find_topic_assignment(topic: &str) -> Result<Option<TopicDescription>> {
    for broker in list_brokers()? {
        let topics = list_topics_for_broker(&broker.broker_id)?;
        if let Some(entry) = topics.into_iter().find(|entry| entry.name == topic) {
            return Ok(Some(TopicDescription {
                broker_id: entry.broker_id,
                delivery: entry.delivery,
            }));
        }
    }

    Ok(None)
}

fn unload_topic(topic: &str) -> Result<()> {
    let _ = run_admin(&["topics", "unload", topic])?;
    Ok(())
}

async fn wait_for_assignment(topic: &str, previous_broker_id: Option<&str>) -> Result<TopicDescription> {
    let deadline = Instant::now() + Duration::from_secs(45);
    let mut last_error = None;

    loop {
        match find_topic_assignment(topic) {
            Ok(Some(description)) => {
                let assigned = !description.broker_id.is_empty();
                let changed = previous_broker_id
                    .map(|previous| description.broker_id != previous)
                    .unwrap_or(true);

                if assigned && changed {
                    return Ok(description);
                }
            }
            Ok(None) => {}
            Err(err) => {
                last_error = Some(err.to_string());
            }
        }

        if Instant::now() >= deadline {
            if let Some(description) = describe_topic(topic).ok() {
                bail!(
                    "timed out waiting for topic assignment change for {topic} (previous broker: {:?}, last observed broker: {}, delivery: {}, last admin error: {:?})",
                    previous_broker_id,
                    description.broker_id,
                    description.delivery,
                    last_error
                );
            } else {
                bail!(
                    "timed out waiting for topic assignment change for {topic} (previous broker: {:?}, last admin error: {:?})",
                    previous_broker_id,
                    last_error
                );
            }
        }

        sleep(Duration::from_secs(1)).await;
    }
}

fn assert_offsets(offsets: &[u64], expected: std::ops::Range<u64>) {
    let expected_offsets: Vec<u64> = expected.collect();
    assert_eq!(offsets, expected_offsets, "unexpected offset sequence");
}

/// Verifies that a reliable topic can be unloaded from one broker and assigned
/// to another broker under `shared_fs` storage without breaking message
/// continuity.
///
/// The test produces ten identical messages, consumes and acknowledges the first
/// five, unloads the topic, waits for ownership to move to a different broker,
/// then produces eight more messages on the new owner. After resubscribing, it
/// expects to read the unconsumed tail of the original stream followed by the
/// newly produced messages with a continuous offset range of `5..18`.
///
/// This confirms that:
/// - acknowledged messages before the move are not replayed,
/// - unconsumed durable data survives reassignment,
/// - the delivery mode remains reliable after reassignment, and
/// - offset allocation continues from the pre-move durable state instead of
///   restarting from an empty topic.
#[tokio::test]
#[ignore = "requires shared_fs e2e workflow"]
async fn reliable_topic_move_shared_fs_continues_offsets() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/reliable_move_shared_fs");
    let subscription = format!("sub-{}", topic.replace('/', "-"));
    let blob_data = std::fs::read("./tests/test.blob")?;

    let mut producer = client
        .new_producer()
        .with_topic(&topic)
        .with_name("producer_topic_move_shared_fs")
        .with_reliable_dispatch()
        .build()?;
    producer.create().await?;

    let mut consumer = client
        .new_consumer()
        .with_topic(topic.clone())
        .with_consumer_name("consumer_topic_move_shared_fs".to_string())
        .with_subscription(subscription.clone())
        .with_subscription_type(SubType::Exclusive)
        .build()?;
    consumer.subscribe().await?;
    let mut stream = consumer.receive().await?;

    sleep(Duration::from_millis(500)).await;

    for _ in 0..10 {
        producer.send(blob_data.clone(), None).await?;
    }

    let original_description = wait_for_assignment(&topic, None).await?;
    assert!(!original_description.delivery.is_empty());

    let receive_first_batch = async {
        let mut received = 0usize;
        while received < 5 {
            let msg = stream.recv().await.context("expected message before unload")?;
            assert_eq!(msg.payload, blob_data, "payload mismatch before unload");
            consumer.ack(&msg).await?;
            received += 1;
        }
        Ok::<(), anyhow::Error>(())
    };
    timeout(Duration::from_secs(15), receive_first_batch).await??;

    drop(stream);
    consumer.close().await;
    drop(producer);
    sleep(Duration::from_millis(500)).await;

    unload_topic(&topic)?;
    let moved_description = wait_for_assignment(&topic, Some(&original_description.broker_id)).await?;
    assert_ne!(moved_description.broker_id, original_description.broker_id);
    assert_eq!(moved_description.delivery, original_description.delivery);

    let mut producer_after_move = client
        .new_producer()
        .with_topic(&topic)
        .with_name("producer_topic_move_shared_fs_after_move")
        .with_reliable_dispatch()
        .build()?;
    producer_after_move.create().await?;

    sleep(Duration::from_millis(500)).await;

    for _ in 10..18 {
        producer_after_move.send(blob_data.clone(), None).await?;
    }

    let mut consumer_after_move = client
        .new_consumer()
        .with_topic(topic.clone())
        .with_consumer_name("consumer_topic_move_shared_fs_after_move".to_string())
        .with_subscription(subscription)
        .with_subscription_type(SubType::Exclusive)
        .build()?;
    consumer_after_move.subscribe().await?;
    let mut stream_after_move = consumer_after_move.receive().await?;

    sleep(Duration::from_millis(500)).await;

    let receive_remaining = async {
        let mut offsets = Vec::new();
        while offsets.len() < 13 {
            let msg = stream_after_move
                .recv()
                .await
                .context("expected message after move")?;
            assert_eq!(msg.payload, blob_data, "payload mismatch after move");
            offsets.push(msg.msg_id.topic_offset);
            consumer_after_move.ack(&msg).await?;
        }
        Ok::<Vec<u64>, anyhow::Error>(offsets)
    };

    let offsets = timeout(Duration::from_secs(20), receive_remaining).await??;
    assert_offsets(&offsets, 5..18);

    Ok(())
}

/// Verifies that a reliable topic move preserves pending-delivery semantics for
/// an unacknowledged message under `shared_fs` storage.
///
/// The test produces ten messages, acknowledges offsets `0..3`, receives offset
/// `3` without acknowledging it, unloads the topic, waits for reassignment, and
/// then produces five more messages on the new owner. After resubscribing with
/// the same consumer identity and subscription, it expects the previously
/// unacked message to be redelivered first, followed by the remaining messages
/// in order with offsets `4..15`.
///
/// This confirms that:
/// - consumer cursor progress acknowledged before the move is preserved,
/// - pending reliable-delivery state is not lost during reassignment,
/// - the same unacked message is replayed after the move, and
/// - the remaining stream continues in order from durable state on the new
///   broker.
#[tokio::test]
#[ignore = "requires shared_fs e2e workflow"]
async fn reliable_topic_move_shared_fs_redelivers_unacked_message() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/reliable_move_pending_shared_fs");
    let subscription = format!("sub-{}", topic.replace('/', "-"));
    let payloads: Vec<Vec<u8>> = (0..15)
        .map(|i| format!("msg_{i}").into_bytes())
        .collect();

    let mut producer = client
        .new_producer()
        .with_topic(&topic)
        .with_name("producer_topic_move_pending_shared_fs")
        .with_reliable_dispatch()
        .build()?;
    producer.create().await?;

    let consumer_name = "consumer_topic_move_pending_shared_fs";
    let mut consumer = client
        .new_consumer()
        .with_topic(topic.clone())
        .with_consumer_name(consumer_name.to_string())
        .with_subscription(subscription.clone())
        .with_subscription_type(SubType::Exclusive)
        .build()?;
    consumer.subscribe().await?;
    let mut stream = consumer.receive().await?;

    sleep(Duration::from_millis(500)).await;

    for payload in payloads.iter().take(10) {
        producer.send(payload.clone(), None).await?;
    }

    let original_description = wait_for_assignment(&topic, None).await?;
    assert!(!original_description.delivery.is_empty());

    for (expected_offset, expected_payload) in payloads.iter().take(3).enumerate() {
        let msg = timeout(Duration::from_secs(10), stream.recv())
            .await?
            .context("expected acknowledged message before move")?;
        assert_eq!(msg.msg_id.topic_offset, expected_offset as u64);
        assert_eq!(msg.payload, *expected_payload);
        consumer.ack(&msg).await?;
    }

    let pending = timeout(Duration::from_secs(10), stream.recv())
        .await?
        .context("expected pending message before move")?;
    let pending_offset = pending.msg_id.topic_offset;
    let pending_payload = pending.payload.to_vec();
    assert_eq!(pending_offset, 3);
    assert_eq!(pending_payload, payloads[3]);

    drop(pending);
    drop(stream);
    consumer.close().await;
    drop(producer);
    sleep(Duration::from_millis(500)).await;

    unload_topic(&topic)?;
    let moved_description = wait_for_assignment(&topic, Some(&original_description.broker_id)).await?;
    assert_ne!(moved_description.broker_id, original_description.broker_id);
    assert_eq!(moved_description.delivery, original_description.delivery);

    let mut producer_after_move = client
        .new_producer()
        .with_topic(&topic)
        .with_name("producer_topic_move_pending_shared_fs_after_move")
        .with_reliable_dispatch()
        .build()?;
    producer_after_move.create().await?;

    sleep(Duration::from_millis(500)).await;

    for payload in payloads.iter().skip(10) {
        producer_after_move.send(payload.clone(), None).await?;
    }

    let mut consumer_after_move = client
        .new_consumer()
        .with_topic(topic.clone())
        .with_consumer_name(consumer_name.to_string())
        .with_subscription(subscription)
        .with_subscription_type(SubType::Exclusive)
        .build()?;
    consumer_after_move.subscribe().await?;
    let mut stream_after_move = consumer_after_move.receive().await?;

    let redelivered = timeout(Duration::from_secs(15), stream_after_move.recv())
        .await?
        .context("expected redelivered pending message after move")?;
    assert_eq!(redelivered.msg_id.topic_offset, pending_offset);
    assert_eq!(redelivered.payload, pending_payload);
    consumer_after_move.ack(&redelivered).await?;

    let receive_remaining = async {
        let mut offsets = Vec::new();
        let mut received_payloads = Vec::new();
        while offsets.len() < 11 {
            let msg = stream_after_move
                .recv()
                .await
                .context("expected remaining message after move")?;
            offsets.push(msg.msg_id.topic_offset);
            received_payloads.push(msg.payload.to_vec());
            consumer_after_move.ack(&msg).await?;
        }
        Ok::<(Vec<u64>, Vec<Vec<u8>>), anyhow::Error>((offsets, received_payloads))
    };

    let (offsets, received_payloads) = timeout(Duration::from_secs(20), receive_remaining).await??;
    assert_offsets(&offsets, 4..15);
    assert_eq!(received_payloads, payloads[4..15].to_vec());

    Ok(())
}

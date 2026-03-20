extern crate danube_client;

use anyhow::{bail, Context, Result};
use danube_client::SubType;
use serde::Deserialize;
use std::path::Path;
use std::process::Command;
use tokio::time::{sleep, timeout, Duration, Instant};

#[path = "test_utils.rs"]
mod test_utils;

#[derive(Debug, Deserialize)]
struct TopicDescription {
    broker_id: String,
    delivery: String,
}

fn admin_cli() -> Command {
    let mut cmd = if let Ok(bin) = std::env::var("DANUBE_ADMIN_BIN") {
        Command::new(bin)
    } else if Path::new("./target/release/danube-admin").exists() {
        Command::new("./target/release/danube-admin")
    } else if Path::new("./target/debug/danube-admin").exists() {
        Command::new("./target/debug/danube-admin")
    } else {
        let mut cargo = Command::new("cargo");
        cargo.args(["run", "-p", "danube-admin", "--bin", "danube-admin", "--"]);
        cargo
    };

    cmd.env(
        "DANUBE_ADMIN_ENDPOINT",
        std::env::var("DANUBE_ADMIN_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:50051".into()),
    );
    cmd
}

fn run_admin(args: &[&str]) -> Result<String> {
    let output = admin_cli()
        .args(args)
        .output()
        .with_context(|| format!("failed to execute danube-admin {:?}", args))?;

    if !output.status.success() {
        bail!(
            "danube-admin {:?} failed\nstdout:\n{}\nstderr:\n{}",
            args,
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

fn unload_topic(topic: &str) -> Result<()> {
    let _ = run_admin(&["topics", "unload", topic])?;
    Ok(())
}

async fn wait_for_assignment(topic: &str, previous_broker_id: Option<&str>) -> Result<TopicDescription> {
    let deadline = Instant::now() + Duration::from_secs(45);

    loop {
        if let Ok(description) = describe_topic(topic) {
            let assigned = !description.broker_id.is_empty();
            let changed = previous_broker_id
                .map(|previous| description.broker_id != previous)
                .unwrap_or(true);

            if assigned && changed {
                return Ok(description);
            }
        }

        if Instant::now() >= deadline {
            bail!(
                "timed out waiting for topic assignment change for {topic} (previous broker: {:?})",
                previous_broker_id
            );
        }

        sleep(Duration::from_secs(1)).await;
    }
}

fn assert_offsets(offsets: &[u64], expected: std::ops::Range<u64>) {
    let expected_offsets: Vec<u64> = expected.collect();
    assert_eq!(offsets, expected_offsets, "unexpected offset sequence");
}

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

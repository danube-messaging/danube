//! End-to-end integration tests for Danube v2 agentic messaging primitives:
//!
//! 1. Lease extension (`extend_lease`) and progress checkpoint persistence (`report_progress`)
//!    across redelivery (`x-danube-checkpoint-b64`).
//! 2. Cascading task-tree cancellation (`cancel_task_tree`) skipping messages belonging
//!    to a cancelled root task while delivering unrelated messages.
//! 3. Correlation-based request-reply (`DanubeClient::request_reply`).

extern crate danube_client;

use anyhow::{Context, Result};
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use danube_client::SubType;
use danube_core::message::{ATTR_PARENT_MSG_ID, ATTR_ROOT_MSG_ID};
use std::collections::HashMap;
use tokio::time::{sleep, timeout, Duration};

#[path = "test_utils.rs"]
mod test_utils;

#[tokio::test]
async fn agentic_lease_extension_and_progress_checkpoint_redelivery() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/agentic_checkpoint");
    let sub_name = format!("sub-cp-{}", topic.replace('/', "-"));

    let mut producer = client
        .new_producer()
        .with_topic(&topic)
        .with_name("agentic_cp_producer")
        .with_reliable_dispatch()
        .build()?;
    producer.create().await?;

    let mut consumer = client
        .new_consumer()
        .with_topic(topic.clone())
        .with_consumer_name("agentic_cp_consumer".to_string())
        .with_subscription(sub_name.clone())
        .with_subscription_type(SubType::Exclusive)
        .build()?;
    consumer.subscribe().await?;
    let mut stream = consumer.receive().await?;

    sleep(Duration::from_millis(400)).await;

    producer
        .send(b"long-running-agent-step".to_vec(), None)
        .await?;

    let first_delivery = timeout(Duration::from_secs(10), stream.recv())
        .await?
        .context("expected initial delivery")?;
    assert_eq!(first_delivery.payload.as_ref(), b"long-running-agent-step");

    // 1. Extend visibility lease for this in-flight delivery
    let lease_resp = consumer.extend_lease(&first_delivery, 5_000).await?;
    assert!(lease_resp.success, "extend_lease should succeed");
    assert!(
        lease_resp.new_deadline_ms > 0,
        "extend_lease should return updated deadline"
    );

    // 2. Persist incremental progress checkpoint and nack to trigger redelivery
    let checkpoint_bytes = br#"{"step":3,"cursor":"page-42"}"#.to_vec();
    let progress_resp = consumer
        .report_progress(&first_delivery, checkpoint_bytes.clone(), 5_000)
        .await?;
    assert!(progress_resp.accepted, "report_progress should be accepted");

    consumer
        .nack(
            &first_delivery,
            Some(50),
            Some("simulate worker handoff".to_string()),
        )
        .await?;

    // 3. Redelivered message must carry the persisted checkpoint attribute
    let redelivered = timeout(Duration::from_secs(10), stream.recv())
        .await?
        .context("expected redelivered message with checkpoint")?;
    assert_eq!(
        redelivered.msg_id.topic_offset,
        first_delivery.msg_id.topic_offset
    );

    let b64 = redelivered
        .checkpoint_b64()
        .context("redelivered message should include x-danube-checkpoint-b64")?;
    let decoded = B64.decode(b64).context("checkpoint must be valid base64")?;
    assert_eq!(decoded, checkpoint_bytes);

    consumer.ack(&redelivered).await?;
    consumer.close().await;

    Ok(())
}

#[tokio::test]
async fn agentic_cancel_task_tree_skips_cancelled_descendants() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/agentic_cancel_tree");
    let sub_name = format!("sub-cancel-{}", topic.replace('/', "-"));

    let mut producer = client
        .new_producer()
        .with_topic(&topic)
        .with_name("agentic_cancel_producer")
        .with_reliable_dispatch()
        .build()?;
    producer.create().await?;

    let mut consumer = client
        .new_consumer()
        .with_topic(topic.clone())
        .with_consumer_name("agentic_cancel_consumer".to_string())
        .with_subscription(sub_name)
        .with_subscription_type(SubType::Exclusive)
        .build()?;
    consumer.subscribe().await?;
    let mut stream = consumer.receive().await?;

    sleep(Duration::from_millis(400)).await;

    let cancelled_root = "root-task-abort-42";
    let cancel_resp = consumer
        .cancel_task_tree(
            &topic,
            cancelled_root,
            Some("user aborted workflow".to_string()),
        )
        .await?;
    assert!(cancel_resp.cancelled, "cancel_task_tree should succeed");

    let mut cancelled_attrs = HashMap::new();
    cancelled_attrs.insert(ATTR_ROOT_MSG_ID.to_string(), cancelled_root.to_string());
    cancelled_attrs.insert(ATTR_PARENT_MSG_ID.to_string(), "step-1".to_string());
    producer
        .send(b"cancelled-child-work".to_vec(), Some(cancelled_attrs))
        .await?;

    let mut active_attrs = HashMap::new();
    active_attrs.insert(
        ATTR_ROOT_MSG_ID.to_string(),
        "root-task-active-99".to_string(),
    );
    producer
        .send(b"active-child-work".to_vec(), Some(active_attrs))
        .await?;

    let received = timeout(Duration::from_secs(10), stream.recv())
        .await?
        .context("expected active task message")?;

    assert_eq!(
        received.payload.as_ref(),
        b"active-child-work",
        "cancelled root descendants must be skipped before active work"
    );
    assert_eq!(received.root_msg_id(), Some("root-task-active-99"));

    consumer.ack(&received).await?;
    consumer.close().await;

    Ok(())
}

#[tokio::test]
async fn agentic_request_reply_roundtrip() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let req_topic = test_utils::unique_topic("/default/agentic_rpc_req");
    let reply_topic = test_utils::unique_topic("/default/agentic_rpc_reply");

    // Pre-create topics so both sides resolve ownership cleanly
    let mut req_init = client
        .new_producer()
        .with_topic(&req_topic)
        .with_name("agentic_req_init")
        .build()?;
    req_init.create().await?;

    let mut req_consumer = client
        .new_consumer()
        .with_topic(req_topic.clone())
        .with_consumer_name("agentic_responder".to_string())
        .with_subscription("agentic_responder_sub".to_string())
        .with_subscription_type(SubType::Exclusive)
        .build()?;
    req_consumer.subscribe().await?;
    let mut req_stream = req_consumer.receive().await?;

    let responder_client = client.clone();
    let responder_handle = tokio::spawn(async move {
        if let Some(req_msg) = timeout(Duration::from_secs(10), req_stream.recv())
            .await
            .ok()
            .flatten()
        {
            let correlation_id = req_msg
                .correlation_id()
                .expect("request should include x-danube-correlation-id")
                .to_string();
            let reply_to = req_msg
                .reply_to()
                .expect("request should include x-danube-reply-to")
                .to_string();

            let mut reply_producer = responder_client
                .new_producer()
                .with_topic(&reply_to)
                .with_name("agentic_reply_producer")
                .build()?;
            reply_producer.create().await?;

            let mut reply_attrs = HashMap::new();
            reply_attrs.insert(
                danube_core::message::ATTR_CORRELATION_ID.to_string(),
                correlation_id,
            );
            let mut reply_bytes = b"ack:".to_vec();
            reply_bytes.extend_from_slice(&req_msg.payload);
            reply_producer.send(reply_bytes, Some(reply_attrs)).await?;
            let _ = req_consumer.ack(&req_msg).await;
        }
        req_consumer.close().await;
        Ok::<(), anyhow::Error>(())
    });

    sleep(Duration::from_millis(400)).await;

    let response = client
        .request_reply(
            &req_topic,
            &reply_topic,
            b"ping-agent".to_vec(),
            None,
            Duration::from_secs(10),
        )
        .await?;

    assert_eq!(response.payload.as_ref(), b"ack:ping-agent");
    responder_handle.await??;

    Ok(())
}

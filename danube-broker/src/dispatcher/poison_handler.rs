//! Poison message handling — DLQ routing, Drop, and Block.
//!
//! This module handles messages that have exhausted their retry budget.
//! It coordinates between the engine (cursor advancement) and the replicator
//! (DLQ publishing) — the one piece that straddles both concerns.

use anyhow::{anyhow, Result};
use danube_core::message::{MessageID, StreamMessage};
use metrics::counter;
use std::collections::HashMap;

use crate::broker_metrics::SUBSCRIPTION_DLQ_TOTAL;
use crate::replicator::Replicator;
use crate::subscription::SubscriptionPoisonPolicy;

use super::metrics::{
    poison_policy_label, subscription_metric_context, update_pending_delivery_metrics,
};
use super::pending_delivery::PendingDelivery;
use super::subscription_engine::SubscriptionEngine;

fn build_dead_letter_message(
    pending: &PendingDelivery,
    subscription_name: &str,
    dead_letter_topic: &str,
    poison_policy: &SubscriptionPoisonPolicy,
) -> StreamMessage {
    let mut attributes: HashMap<String, String> = pending.message.attributes.clone();
    attributes.insert(
        "x-original-topic".to_string(),
        pending.message.msg_id.topic_name.clone(),
    );
    attributes.insert(
        "x-original-subscription".to_string(),
        subscription_name.to_string(),
    );
    attributes.insert(
        "x-original-topic-offset".to_string(),
        pending.message.msg_id.topic_offset.to_string(),
    );
    attributes.insert(
        "x-original-producer-id".to_string(),
        pending.message.msg_id.producer_id.to_string(),
    );
    attributes.insert(
        "x-original-broker-addr".to_string(),
        pending.message.msg_id.broker_addr.clone(),
    );
    attributes.insert(
        "x-poison-policy".to_string(),
        poison_policy_label(poison_policy).to_string(),
    );
    attributes.insert(
        "x-delivery-attempt".to_string(),
        pending.delivery_attempt.to_string(),
    );

    if let Some(reason) = pending.last_failure_reason.as_ref() {
        attributes.insert("x-failure-reason".to_string(), reason.clone());
    }

    let mut dead_letter_message = pending.message.clone();
    dead_letter_message.msg_id = MessageID {
        producer_id: 0,
        topic_name: dead_letter_topic.to_string(),
        broker_addr: pending.message.msg_id.broker_addr.clone(),
        topic_offset: 0,
    };
    dead_letter_message.subscription_name = None;
    dead_letter_message.attributes = attributes;
    dead_letter_message
}

/// Handle a message that has exhausted its retry budget.
///
/// Returns `Ok(true)` if the message was resolved (DLQ-routed or dropped),
/// `Ok(false)` if no action was taken (not exhausted, or blocked).
pub(super) async fn handle_retry_exhausted_pending(
    engine: &mut SubscriptionEngine,
    replicator: Option<&Replicator>,
    pending_delivery: &mut Option<PendingDelivery>,
) -> Result<bool> {
    let failure_policy = engine.failure_policy();
    let metric_context = subscription_metric_context(engine, pending_delivery.as_ref());
    let Some(pending) = pending_delivery.as_ref() else {
        update_pending_delivery_metrics(&metric_context, failure_policy, pending_delivery);
        return Ok(false);
    };

    if !pending.is_retry_exhausted() {
        update_pending_delivery_metrics(&metric_context, failure_policy, pending_delivery);
        return Ok(false);
    }

    match failure_policy.poison_policy {
        SubscriptionPoisonPolicy::DeadLetter => {
            let dead_letter_topic = failure_policy
                .dead_letter_topic
                .as_deref()
                .ok_or_else(|| anyhow!("dead_letter_topic must be configured for DeadLetter"))?;
            let publisher = replicator
                .ok_or_else(|| anyhow!("replicator unavailable for dead-letter routing"))?;
            let dead_letter_message = build_dead_letter_message(
                pending,
                &engine._subscription_name,
                dead_letter_topic,
                &failure_policy.poison_policy,
            );
            let original_msg_id = pending.message.msg_id.clone();

            publisher
                .publish_message_async(dead_letter_topic, dead_letter_message)
                .await?;
            counter!(
                SUBSCRIPTION_DLQ_TOTAL.name,
                "topic" => metric_context.topic.clone(),
                "subscription" => metric_context.subscription.clone(),
                "poison_policy" => poison_policy_label(&failure_policy.poison_policy).to_string(),
            )
            .increment(1);
            engine.skip_poisoned(original_msg_id).await?;
            *pending_delivery = None;
            let failure_policy = engine.failure_policy();
            update_pending_delivery_metrics(&metric_context, failure_policy, pending_delivery);
            Ok(true)
        }
        SubscriptionPoisonPolicy::Block => {
            update_pending_delivery_metrics(&metric_context, failure_policy, pending_delivery);
            Ok(false)
        }
        SubscriptionPoisonPolicy::Drop => {
            let original_msg_id = pending.message.msg_id.clone();
            engine.skip_poisoned(original_msg_id).await?;
            *pending_delivery = None;
            let failure_policy = engine.failure_policy();
            update_pending_delivery_metrics(&metric_context, failure_policy, pending_delivery);
            Ok(true)
        }
    }
}

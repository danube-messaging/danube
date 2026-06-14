//! Poison message handling — DLQ routing, Drop, and Block.
//!
//! This module handles messages that have exhausted their retry budget.
//! It coordinates between the engine (cursor advancement) and the replicator
//! (DLQ publishing) — the one piece that straddles both concerns.
//!
//! # API
//!
//! - **`resolve_poisoned_delivery`** — Applies poison policy (DLQ publish,
//!   Drop, Block decision) without touching cursor or state management. Used by
//!   all dispatchers that manage cursors via `DispatchWindow` or `InFlightWindow`.

use anyhow::{anyhow, Result};
use danube_core::message::{MessageID, StreamMessage};
use metrics::counter;
use std::collections::HashMap;
use tracing::warn;

use crate::broker_metrics::SUBSCRIPTION_DLQ_TOTAL;
use crate::replicator::Replicator;
use crate::subscription::{SubscriptionFailurePolicy, SubscriptionPoisonPolicy};

use super::metrics::poison_policy_label;
use super::pending_delivery::PendingDelivery;

/// Result of applying the poison policy to a retry-exhausted message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PoisonResolution {
    /// Message was resolved: dropped or successfully routed to DLQ.
    /// Caller should remove the message from its state and advance cursor.
    Resolved,
    /// Block policy: message stays in place, dispatch paused for this slot/key.
    Blocked,
}

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

/// Apply the poison policy to a retry-exhausted PendingDelivery.
///
/// This is the low-level function that handles:
/// - **Drop**: returns `Resolved` immediately (caller handles state cleanup)
/// - **DeadLetter**: builds DLQ message, publishes, emits metrics, returns `Resolved`
/// - **Block**: returns `Blocked` (caller leaves message in place)
///
/// Does NOT touch cursor management or state cleanup — the caller is responsible for:
/// - Removing the entry from its state (pending slot, in-flight window, etc.)
/// - Advancing the cursor (via `engine.skip_poisoned()` or `window.on_skipped()`)
///
/// Takes extracted data instead of `&SubscriptionEngine` to avoid holding a non-Sync
/// reference across await points (SubscriptionEngine contains `dyn Stream`).
///
/// Returns `Err` only if DLQ publishing fails when DeadLetter policy is configured.
pub(super) async fn resolve_poisoned_delivery(
    failure_policy: &SubscriptionFailurePolicy,
    subscription_name: &str,
    replicator: Option<&Replicator>,
    pending: &PendingDelivery,
) -> Result<PoisonResolution> {
    match failure_policy.poison_policy {
        SubscriptionPoisonPolicy::Drop => {
            warn!(
                offset = %pending.message.msg_id.topic_offset,
                delivery_attempt = %pending.delivery_attempt,
                "dropping retry-exhausted message (poison policy: Drop)"
            );
            Ok(PoisonResolution::Resolved)
        }
        SubscriptionPoisonPolicy::DeadLetter => {
            let dead_letter_topic = failure_policy
                .dead_letter_topic
                .as_deref()
                .ok_or_else(|| anyhow!("dead_letter_topic must be configured for DeadLetter"))?;
            let publisher = replicator
                .ok_or_else(|| anyhow!("replicator unavailable for dead-letter routing"))?;

            let dead_letter_message = build_dead_letter_message(
                pending,
                subscription_name,
                dead_letter_topic,
                &failure_policy.poison_policy,
            );

            publisher
                .publish_message_async(dead_letter_topic, dead_letter_message)
                .await?;

            let topic = pending.message.msg_id.topic_name.clone();
            counter!(
                SUBSCRIPTION_DLQ_TOTAL.name,
                "topic" => topic,
                "subscription" => subscription_name.to_string(),
                "poison_policy" => poison_policy_label(&failure_policy.poison_policy).to_string(),
            )
            .increment(1);

            warn!(
                offset = %pending.message.msg_id.topic_offset,
                delivery_attempt = %pending.delivery_attempt,
                dlq_topic = %dead_letter_topic,
                "routed retry-exhausted message to DLQ"
            );
            Ok(PoisonResolution::Resolved)
        }
        SubscriptionPoisonPolicy::Block => Ok(PoisonResolution::Blocked),
    }
}

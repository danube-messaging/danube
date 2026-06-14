//! Subscription dispatch metrics.
//!
//! Pure metric helpers used by both reliable dispatchers and the poison handler.
//! These functions build metric contexts and emit gauges/counters — they do not
//! mutate any dispatch state.

use metrics::{counter, gauge};

use crate::broker_metrics::{
    SUBSCRIPTION_PENDING_REDELIVERY, SUBSCRIPTION_RETRY_EXHAUSTED_TOTAL,
    SUBSCRIPTION_TERMINAL_BLOCKED,
};
use crate::subscription::{SubscriptionFailurePolicy, SubscriptionPoisonPolicy};

use super::pending_delivery::PendingDelivery;
use super::subscription_engine::SubscriptionEngine;

pub(super) fn poison_policy_label(poison_policy: &SubscriptionPoisonPolicy) -> &'static str {
    match poison_policy {
        SubscriptionPoisonPolicy::DeadLetter => "dead_letter",
        SubscriptionPoisonPolicy::Block => "block",
        SubscriptionPoisonPolicy::Drop => "drop",
    }
}

#[derive(Debug, Clone)]
pub(super) struct SubscriptionMetricContext {
    pub(super) topic: String,
    pub(super) subscription: String,
}

pub(super) fn subscription_metric_topic(
    engine: &SubscriptionEngine,
    pending_delivery: Option<&PendingDelivery>,
) -> String {
    engine
        .topic_name
        .clone()
        .or_else(|| pending_delivery.map(|pending| pending.message.msg_id.topic_name.clone()))
        .unwrap_or_default()
}

pub(super) fn subscription_metric_context(
    engine: &SubscriptionEngine,
    pending_delivery: Option<&PendingDelivery>,
) -> SubscriptionMetricContext {
    SubscriptionMetricContext {
        topic: subscription_metric_topic(engine, pending_delivery),
        subscription: engine._subscription_name.clone(),
    }
}

pub(super) fn record_retry_exhausted_metric(
    metric_context: &SubscriptionMetricContext,
    failure_policy: &SubscriptionFailurePolicy,
) {
    counter!(
        SUBSCRIPTION_RETRY_EXHAUSTED_TOTAL.name,
        "topic" => metric_context.topic.clone(),
        "subscription" => metric_context.subscription.clone(),
        "poison_policy" => poison_policy_label(&failure_policy.poison_policy).to_string(),
    )
    .increment(1);
}

/// Update dispatch metrics for a windowed dispatcher.
///
/// Reports the count of entries in WaitingToRetry and RetryExhausted+Block states
/// across the entire dispatch window.
pub(super) fn update_window_metrics(
    metric_context: &SubscriptionMetricContext,
    failure_policy: &SubscriptionFailurePolicy,
    window: &super::dispatch_window::DispatchWindow,
) {
    let pending_redelivery = window.count_waiting_to_retry();
    let terminal_blocked = if failure_policy.poison_policy == SubscriptionPoisonPolicy::Block {
        window.count_retry_exhausted()
    } else {
        0
    };

    gauge!(
        SUBSCRIPTION_PENDING_REDELIVERY.name,
        "topic" => metric_context.topic.clone(),
        "subscription" => metric_context.subscription.clone(),
    )
    .set(pending_redelivery as f64);

    gauge!(
        SUBSCRIPTION_TERMINAL_BLOCKED.name,
        "topic" => metric_context.topic.clone(),
        "subscription" => metric_context.subscription.clone(),
    )
    .set(terminal_blocked as f64);
}

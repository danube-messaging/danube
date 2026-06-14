//! Per-message delivery state machine.
//!
//! `PendingDelivery` tracks the lifecycle of a single in-flight message:
//! ready → sent (awaiting ack) → ack'd / nack'd → retry or exhausted.
//!
//! This module has no dependency on the engine, replicator, or metrics —
//! it is a pure state machine operated by the reliable dispatchers.

use danube_core::message::StreamMessage;
use tokio::time::{Duration, Instant};

use crate::subscription::{SubscriptionBackoffStrategy, SubscriptionFailurePolicy};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PendingDeliveryStatus {
    ReadyToSend,
    AwaitingAck,
    WaitingToRetry,
    RetryExhausted,
}

#[derive(Debug, Clone)]
pub(super) struct PendingDelivery {
    pub(super) message: StreamMessage,
    pub(super) delivery_attempt: u32,
    pub(super) first_sent_at: Instant,
    pub(super) last_sent_at: Instant,
    pub(super) ack_deadline_at: Instant,
    pub(super) next_redelivery_at: Instant,
    pub(super) last_failure_reason: Option<String>,
    pub(super) target_consumer_id: Option<u64>,
    pub(super) status: PendingDeliveryStatus,
}

impl PendingDelivery {
    pub(super) fn new(message: StreamMessage) -> Self {
        let now = Instant::now();
        Self {
            message,
            delivery_attempt: 0,
            first_sent_at: now,
            last_sent_at: now,
            ack_deadline_at: now,
            next_redelivery_at: now,
            last_failure_reason: None,
            target_consumer_id: None,
            status: PendingDeliveryStatus::ReadyToSend,
        }
    }

    pub(super) fn is_awaiting_ack(&self) -> bool {
        self.status == PendingDeliveryStatus::AwaitingAck
    }

    pub(super) fn is_retry_exhausted(&self) -> bool {
        self.status == PendingDeliveryStatus::RetryExhausted
    }

    pub(super) fn is_waiting_to_retry(&self) -> bool {
        self.status == PendingDeliveryStatus::WaitingToRetry
    }

    pub(super) fn should_stop_retrying(&self, failure_policy: &SubscriptionFailurePolicy) -> bool {
        self.delivery_attempt > failure_policy.max_redelivery_count
    }

    pub(super) fn is_retry_ready(&self, now: Instant) -> bool {
        matches!(
            self.status,
            PendingDeliveryStatus::ReadyToSend | PendingDeliveryStatus::WaitingToRetry
        ) && now >= self.next_redelivery_at
    }

    pub(super) fn ack_timed_out(&self, now: Instant) -> bool {
        self.status == PendingDeliveryStatus::AwaitingAck && now >= self.ack_deadline_at
    }

    pub(super) fn on_send_attempt(&mut self, consumer_id: u64, ack_timeout: Duration) {
        let now = Instant::now();
        if self.delivery_attempt == 0 {
            self.first_sent_at = now;
        }
        self.delivery_attempt = self.delivery_attempt.saturating_add(1);
        self.last_sent_at = now;
        self.ack_deadline_at = now + ack_timeout;
        self.next_redelivery_at = now;
        self.target_consumer_id = Some(consumer_id);
        self.status = PendingDeliveryStatus::AwaitingAck;
    }

    fn policy_redelivery_delay_ms(&self, failure_policy: &SubscriptionFailurePolicy) -> u64 {
        let base_delay_ms = failure_policy.base_redelivery_delay_ms;

        match failure_policy.backoff_strategy {
            SubscriptionBackoffStrategy::Fixed => base_delay_ms,
            SubscriptionBackoffStrategy::Exponential => {
                let retry_attempt = self.delivery_attempt.max(1);
                let exponent = retry_attempt.saturating_sub(1).min(63);
                let multiplier = 1u64.checked_shl(exponent).unwrap_or(u64::MAX);
                base_delay_ms.saturating_mul(multiplier)
            }
        }
    }

    pub(super) fn schedule_retry_with_policy(
        &mut self,
        reason: Option<String>,
        requested_delay_ms: Option<u64>,
        failure_policy: &SubscriptionFailurePolicy,
    ) {
        if self.should_stop_retrying(failure_policy) {
            self.mark_retry_exhausted(reason);
            return;
        }

        let now = Instant::now();
        let policy_delay_ms = self.policy_redelivery_delay_ms(failure_policy);
        let requested_delay_ms = requested_delay_ms.unwrap_or(0);
        let effective_delay_ms = policy_delay_ms
            .max(requested_delay_ms)
            .min(failure_policy.max_redelivery_delay_ms);

        self.last_failure_reason = reason;
        self.next_redelivery_at = now + Duration::from_millis(effective_delay_ms);
        self.target_consumer_id = None;
        self.status = PendingDeliveryStatus::WaitingToRetry;
    }

    pub(super) fn mark_retry_exhausted(&mut self, reason: Option<String>) {
        self.last_failure_reason = reason;
        self.target_consumer_id = None;
        self.status = PendingDeliveryStatus::RetryExhausted;
    }

    pub(super) fn schedule_retry_now(&mut self, reason: Option<String>) {
        if self.is_retry_exhausted() {
            return;
        }

        let now = Instant::now();
        self.last_failure_reason = reason;
        self.next_redelivery_at = now;
        self.target_consumer_id = None;
        self.status = PendingDeliveryStatus::WaitingToRetry;
    }
}

#[cfg(test)]
mod tests {
    use super::{PendingDelivery, PendingDeliveryStatus};
    use crate::subscription::{SubscriptionBackoffStrategy, SubscriptionFailurePolicy};
    use danube_core::message::{MessageID, StreamMessage};
    use std::collections::HashMap;
    use tokio::time::{Duration, Instant};

    fn make_msg(req_id: u64, topic_off: u64, topic: &str) -> StreamMessage {
        StreamMessage {
            request_id: req_id,
            msg_id: MessageID {
                producer_id: 1,
                topic_name: topic.to_string(),
                broker_addr: "127.0.0.1:8080".to_string(),
                topic_offset: topic_off,
            },
            payload: format!("dispatcher-{req_id}").into_bytes().into(),
            publish_time: 0,
            producer_name: "producer-test".to_string(),
            subscription_name: None,
            attributes: HashMap::new(),
            schema_id: None,
            schema_version: None,
            routing_key: None,
        }
    }

    #[test]
    fn schedule_retry_with_policy_uses_fixed_policy_delay_as_minimum() {
        let mut pending = PendingDelivery::new(make_msg(1, 11, "/default/topic"));
        pending.on_send_attempt(7, Duration::from_secs(30));

        let failure_policy = SubscriptionFailurePolicy {
            base_redelivery_delay_ms: 1_000,
            max_redelivery_delay_ms: 5_000,
            backoff_strategy: SubscriptionBackoffStrategy::Fixed,
            ..SubscriptionFailurePolicy::new("/default/topic")
        };

        let before = Instant::now();
        pending.schedule_retry_with_policy(Some("nack".to_string()), Some(200), &failure_policy);
        let after = Instant::now();

        assert_eq!(pending.status, PendingDeliveryStatus::WaitingToRetry);
        assert_eq!(pending.last_failure_reason.as_deref(), Some("nack"));
        assert!(pending.next_redelivery_at >= before + Duration::from_millis(1_000));
        assert!(pending.next_redelivery_at <= after + Duration::from_millis(1_000));
    }

    #[test]
    fn schedule_retry_with_policy_honors_requested_delay_when_larger() {
        let mut pending = PendingDelivery::new(make_msg(2, 12, "/default/topic"));
        pending.on_send_attempt(7, Duration::from_secs(30));

        let failure_policy = SubscriptionFailurePolicy {
            base_redelivery_delay_ms: 1_000,
            max_redelivery_delay_ms: 5_000,
            backoff_strategy: SubscriptionBackoffStrategy::Fixed,
            ..SubscriptionFailurePolicy::new("/default/topic")
        };

        let before = Instant::now();
        pending.schedule_retry_with_policy(
            Some("client delay".to_string()),
            Some(2_500),
            &failure_policy,
        );
        let after = Instant::now();

        assert!(pending.next_redelivery_at >= before + Duration::from_millis(2_500));
        assert!(pending.next_redelivery_at <= after + Duration::from_millis(2_500));
    }

    #[test]
    fn schedule_retry_with_policy_clamps_exponential_backoff_to_max_delay() {
        let mut pending = PendingDelivery::new(make_msg(3, 13, "/default/topic"));
        pending.delivery_attempt = 3;

        let failure_policy = SubscriptionFailurePolicy {
            base_redelivery_delay_ms: 1_000,
            max_redelivery_delay_ms: 2_500,
            backoff_strategy: SubscriptionBackoffStrategy::Exponential,
            ..SubscriptionFailurePolicy::new("/default/topic")
        };

        let before = Instant::now();
        pending.schedule_retry_with_policy(Some("timeout".to_string()), None, &failure_policy);
        let after = Instant::now();

        assert!(pending.next_redelivery_at >= before + Duration::from_millis(2_500));
        assert!(pending.next_redelivery_at <= after + Duration::from_millis(2_500));
    }

    #[test]
    fn schedule_retry_with_policy_marks_retry_exhausted_after_limit() {
        let mut pending = PendingDelivery::new(make_msg(4, 14, "/default/topic"));
        pending.delivery_attempt = 2;

        let failure_policy = SubscriptionFailurePolicy {
            max_redelivery_count: 1,
            ..SubscriptionFailurePolicy::new("/default/topic")
        };

        pending.schedule_retry_with_policy(Some("nack".to_string()), None, &failure_policy);

        assert_eq!(pending.status, PendingDeliveryStatus::RetryExhausted);
        assert!(pending.is_retry_exhausted());
        assert_eq!(pending.last_failure_reason.as_deref(), Some("nack"));
    }

    #[test]
    fn schedule_retry_now_does_not_revive_retry_exhausted_message() {
        let mut pending = PendingDelivery::new(make_msg(5, 15, "/default/topic"));
        pending.mark_retry_exhausted(Some("exhausted".to_string()));
        let exhausted_deadline = pending.next_redelivery_at;

        pending.schedule_retry_now(Some("reset".to_string()));

        assert_eq!(pending.status, PendingDeliveryStatus::RetryExhausted);
        assert_eq!(pending.last_failure_reason.as_deref(), Some("exhausted"));
        assert_eq!(pending.next_redelivery_at, exhausted_deadline);
    }
}

//! Per-message delivery state machine.
//!
//! `PendingDelivery` tracks the lifecycle of a single in-flight message:
//! ready → sent (awaiting ack) → ack'd / nack'd → retry or exhausted.
//!
//! This module has no dependency on the engine, replicator, or metrics —
//! it is a pure state machine operated by the reliable dispatchers.

use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use danube_core::message::{StreamMessage, ATTR_CHECKPOINT_B64};
use dashmap::DashMap;
use tokio::time::{Duration, Instant};

use crate::subscription::{SubscriptionBackoffStrategy, SubscriptionFailurePolicy};

pub(crate) const MAX_CHECKPOINT_BYTES: usize = 65_536;
const MAX_CANCELLED_ROOTS: usize = 10_000;
const CANCELLED_ROOT_TTL: Duration = Duration::from_secs(3600);

/// Bounded TTL skip-list for cascading task-tree cancellation (`Option 8A`).
#[derive(Debug, Default)]
pub(crate) struct CancelledRootsCache {
    roots: DashMap<String, Instant>,
}

impl CancelledRootsCache {
    pub(crate) fn new() -> Self {
        Self {
            roots: DashMap::new(),
        }
    }

    pub(crate) fn insert(&self, root_id: impl Into<String>) {
        let now = Instant::now();
        if self.roots.len() >= MAX_CANCELLED_ROOTS {
            self.roots
                .retain(|_, inserted_at| now.duration_since(*inserted_at) < CANCELLED_ROOT_TTL);
            if self.roots.len() >= MAX_CANCELLED_ROOTS {
                if let Some(first_key) = self.roots.iter().next().map(|e| e.key().clone()) {
                    self.roots.remove(&first_key);
                }
            }
        }
        self.roots.insert(root_id.into(), now);
    }

    fn contains_active(&self, id: &str) -> bool {
        if let Some(entry) = self.roots.get(id) {
            if entry.elapsed() < CANCELLED_ROOT_TTL {
                return true;
            }
            drop(entry);
            self.roots.remove(id);
        }
        false
    }

    /// Returns true if the message belongs to a cancelled task tree OR its `deadline_ms` has passed.
    pub(crate) fn is_cancelled(&self, msg: &StreamMessage) -> bool {
        if let Some(root_id) = msg.root_msg_id() {
            if self.contains_active(root_id) {
                return true;
            }
        }
        if let Some(parent_id) = msg.parent_msg_id() {
            if self.contains_active(parent_id) {
                return true;
            }
        }
        let self_offset = msg.msg_id.topic_offset.to_string();
        if self.contains_active(&self_offset) {
            return true;
        }
        if let Some(deadline_ms) = msg.deadline_ms() {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            if now_ms > deadline_ms {
                return true;
            }
        }
        false
    }
}

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
    pub(super) checkpoint: Option<Vec<u8>>,
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
            checkpoint: None,
        }
    }

    pub(super) fn extend_lease(&mut self, extension_ms: u64) -> Instant {
        let base = self.ack_deadline_at.max(Instant::now());
        self.ack_deadline_at = base + Duration::from_millis(extension_ms);
        self.ack_deadline_at
    }

    pub(super) fn set_checkpoint(
        &mut self,
        data: Vec<u8>,
        extension_ms: u64,
    ) -> Result<(), &'static str> {
        if data.len() > MAX_CHECKPOINT_BYTES {
            return Err("checkpoint exceeds 64 KiB limit");
        }
        let encoded = B64.encode(&data);
        self.message
            .attributes
            .insert(ATTR_CHECKPOINT_B64.to_string(), encoded);
        self.checkpoint = Some(data);
        if extension_ms > 0 {
            self.extend_lease(extension_ms);
        }
        Ok(())
    }

    pub(super) fn checkpoint_b64(&self) -> Option<String> {
        self.checkpoint
            .as_ref()
            .map(|bytes| B64.encode(bytes))
            .or_else(|| self.message.checkpoint_b64().map(|s| s.to_string()))
    }

    pub(super) fn message_for_delivery(&self) -> StreamMessage {
        let mut msg = self.message.clone();
        if let Some(ref bytes) = self.checkpoint {
            msg.attributes
                .insert(ATTR_CHECKPOINT_B64.to_string(), B64.encode(bytes));
        }
        msg
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

    #[test]
    fn extend_lease_and_checkpoint_enforce_size_limit_and_persist_in_delivery() {
        let mut pending = PendingDelivery::new(make_msg(6, 16, "/default/topic"));
        pending.on_send_attempt(9, Duration::from_millis(500));
        let original_deadline = pending.ack_deadline_at;

        let extended = pending.extend_lease(1_000);
        assert!(extended >= original_deadline + Duration::from_millis(1_000));

        assert!(pending
            .set_checkpoint(vec![0u8; super::MAX_CHECKPOINT_BYTES + 1], 0)
            .is_err());

        pending
            .set_checkpoint(b"step-2".to_vec(), 500)
            .expect("checkpoint within 64KiB should succeed");
        let delivered = pending.message_for_delivery();
        assert_eq!(
            delivered.checkpoint_b64(),
            pending.checkpoint_b64().as_deref()
        );
    }

    #[test]
    fn cancelled_roots_cache_matches_root_parent_offset_and_deadline() {
        let cache = super::CancelledRootsCache::new();
        cache.insert("root-42");

        let mut child = make_msg(7, 17, "/default/topic");
        child
            .attributes
            .insert(danube_core::message::ATTR_ROOT_MSG_ID.to_string(), "root-42".to_string());
        assert!(cache.is_cancelled(&child));

        let mut expired = make_msg(8, 18, "/default/topic");
        expired
            .attributes
            .insert(danube_core::message::ATTR_DEADLINE_MS.to_string(), "1".to_string());
        assert!(cache.is_cancelled(&expired));

        let healthy = make_msg(9, 19, "/default/topic");
        assert!(!cache.is_cancelled(&healthy));
    }
}

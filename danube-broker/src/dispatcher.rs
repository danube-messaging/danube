use anyhow::{anyhow, Result};
use danube_core::message::{MessageID, StreamMessage};
use std::collections::HashMap;
use std::sync::{Arc, Weak};
use std::sync::atomic::AtomicUsize;
use tokio::sync::{mpsc, watch, Mutex};
use tokio::time::{Duration, Instant};

use crate::{
    consumer::Consumer,
    message::{AckMessage, NackMessage},
    subscription::{
        SubscriptionBackoffStrategy, SubscriptionFailurePolicy, SubscriptionPoisonPolicy,
    },
    topic_registry::TopicRegistry,
};

// Module declarations
pub(crate) mod commands;
pub(crate) mod exclusive;
pub(crate) mod shared;
pub(crate) mod subscription_engine;

use commands::DispatcherCommand;
use exclusive::ExclusiveDispatcher;
use shared::SharedDispatcher;
use subscription_engine::SubscriptionEngine;

#[derive(Debug, Clone)]
pub(crate) struct InternalPublisher {
    topic_registry: Weak<TopicRegistry>,
}

impl InternalPublisher {
    pub(crate) fn new(topic_registry: Weak<TopicRegistry>) -> Self {
        Self { topic_registry }
    }

    pub(crate) async fn publish_message_async(
        &self,
        topic_name: &str,
        message: StreamMessage,
    ) -> Result<()> {
        let topic_registry = self
            .topic_registry
            .upgrade()
            .ok_or_else(|| anyhow!("topic registry unavailable for internal publish"))?;
        let topic = topic_registry
            .get_topic(topic_name)
            .ok_or_else(|| anyhow!("Topic {} not found in local registry", topic_name))?;
        topic.publish_message_internal_async(message).await
    }
}

fn poison_policy_label(poison_policy: &SubscriptionPoisonPolicy) -> &'static str {
    match poison_policy {
        SubscriptionPoisonPolicy::DeadLetter => "dead_letter",
        SubscriptionPoisonPolicy::Block => "block",
        SubscriptionPoisonPolicy::Drop => "drop",
    }
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

pub(super) async fn handle_retry_exhausted_pending(
    engine: &mut SubscriptionEngine,
    failure_policy: &SubscriptionFailurePolicy,
    internal_publisher: Option<&InternalPublisher>,
    pending_delivery: &mut Option<PendingDelivery>,
) -> Result<bool> {
    let Some(pending) = pending_delivery.as_ref() else {
        return Ok(false);
    };

    if !pending.is_retry_exhausted() {
        return Ok(false);
    }

    match failure_policy.poison_policy {
        SubscriptionPoisonPolicy::DeadLetter => {
            let dead_letter_topic = failure_policy
                .dead_letter_topic
                .as_deref()
                .ok_or_else(|| anyhow!("dead_letter_topic must be configured for DeadLetter"))?;
            let publisher = internal_publisher
                .ok_or_else(|| anyhow!("internal publisher unavailable for dead-letter routing"))?;
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
            engine.on_acked(original_msg_id).await?;
            *pending_delivery = None;
            Ok(true)
        }
        SubscriptionPoisonPolicy::Block => Ok(false),
        SubscriptionPoisonPolicy::Drop => {
            let original_msg_id = pending.message.msg_id.clone();
            engine.on_acked(original_msg_id).await?;
            *pending_delivery = None;
            Ok(true)
        }
    }
}

#[derive(Debug, Clone)]
enum DispatcherHandle {
    NonReliableExclusive(Arc<Mutex<exclusive::ExclusiveConsumerState>>),
    NonReliableShared(Arc<Mutex<shared::SharedConsumerState>>),
    Reliable {
        control_tx: mpsc::Sender<DispatcherCommand>,
        ready_rx: watch::Receiver<bool>,
    },
}

#[derive(Debug)]
pub(crate) enum DispatchStrategy {
    // Does not store messages, sends them directly to the dispatcher
    NonReliable,
    // Stores messages for reliable delivery
    Reliable,
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

    pub(super) fn matches_offset(&self, topic_offset: u64) -> bool {
        self.message.msg_id.topic_offset == topic_offset
    }

    pub(super) fn is_awaiting_ack(&self) -> bool {
        self.status == PendingDeliveryStatus::AwaitingAck
    }

    pub(super) fn is_retry_exhausted(&self) -> bool {
        self.status == PendingDeliveryStatus::RetryExhausted
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

/// Single dispatcher handle — a thin facade over an mpsc command channel.
#[derive(Debug, Clone)]
pub(crate) struct Dispatcher {
    handle: DispatcherHandle,
}

impl Dispatcher {
    // ── Factory constructors ────────────────────────────────────────────

    /// Non-reliable exclusive (fire-and-forget, single active consumer).
    pub(crate) fn non_reliable_exclusive() -> Self {
        Self {
            handle: DispatcherHandle::NonReliableExclusive(Arc::new(Mutex::new(
                exclusive::ExclusiveConsumerState::new(),
            ))),
        }
    }

    /// Non-reliable shared (fire-and-forget, round-robin).
    pub(crate) fn non_reliable_shared() -> Self {
        Self {
            handle: DispatcherHandle::NonReliableShared(Arc::new(Mutex::new(
                shared::SharedConsumerState::new(Arc::new(AtomicUsize::new(0))),
            ))),
        }
    }

    /// Reliable exclusive (ack-gating, single active consumer, heartbeat).
    pub(crate) fn reliable_exclusive(
        engine: SubscriptionEngine,
        failure_policy: SubscriptionFailurePolicy,
        internal_publisher: Option<InternalPublisher>,
    ) -> Self {
        let (control_tx, control_rx) = mpsc::channel(32);
        let (ready_tx, ready_rx) = watch::channel(false);
        ExclusiveDispatcher::start_reliable(
            engine,
            failure_policy,
            internal_publisher,
            control_rx,
            ready_tx,
        );
        Self {
            handle: DispatcherHandle::Reliable {
                control_tx,
                ready_rx,
            },
        }
    }

    /// Reliable shared (ack-gating, round-robin, heartbeat).
    pub(crate) fn reliable_shared(
        engine: SubscriptionEngine,
        failure_policy: SubscriptionFailurePolicy,
        internal_publisher: Option<InternalPublisher>,
    ) -> Self {
        let (control_tx, control_rx) = mpsc::channel(32);
        let (ready_tx, ready_rx) = watch::channel(false);
        SharedDispatcher::start_reliable(
            engine,
            failure_policy,
            internal_publisher,
            control_rx,
            ready_tx,
        );
        Self {
            handle: DispatcherHandle::Reliable {
                control_tx,
                ready_rx,
            },
        }
    }

    // ── Public API (written once) ───────────────────────────────────────

    /// Block until the dispatcher is ready.
    /// Used by: reliable only (non-reliable dispatchers are ready immediately).
    pub(crate) async fn ready(&self) {
        if let DispatcherHandle::Reliable { ready_rx, .. } = &self.handle {
            if *ready_rx.borrow() {
                return;
            }
            let mut rx = ready_rx.clone();
            while rx.changed().await.is_ok() {
                if *rx.borrow() {
                    break;
                }
            }
        }
    }

    /// Push a single message for immediate dispatch.
    /// Used by: non-reliable only (reliable dispatchers are stream-driven via notifier).
    pub(crate) async fn dispatch_message(&self, message: StreamMessage) -> Result<()> {
        match &self.handle {
            DispatcherHandle::NonReliableExclusive(state) => {
                let mut state = state.lock().await;
                ExclusiveDispatcher::dispatch_non_reliable(&mut state, message).await
            }
            DispatcherHandle::NonReliableShared(state) => {
                let mut state = state.lock().await;
                SharedDispatcher::dispatch_non_reliable(&mut state, message).await
            }
            DispatcherHandle::Reliable { .. } => {
                Err(anyhow!("Reliable dispatcher is stream-driven, not push-per-message"))
            }
        }
    }

    /// Acknowledge a previously dispatched message.
    /// Used by: reliable only.
    pub(crate) async fn ack_message(&self, ack_msg: AckMessage) -> Result<()> {
        match &self.handle {
            DispatcherHandle::Reliable { control_tx, .. } => control_tx
                .send(DispatcherCommand::MessageAcked(ack_msg))
                .await
                .map_err(|_| anyhow!("Failed to send ack command")),
            _ => Ok(()),
        }
    }

    pub(crate) async fn nack_message(&self, nack_msg: NackMessage) -> Result<()> {
        match &self.handle {
            DispatcherHandle::Reliable { control_tx, .. } => control_tx
                .send(DispatcherCommand::MessageNacked(nack_msg))
                .await
                .map_err(|_| anyhow!("Failed to send nack command")),
            _ => Ok(()),
        }
    }

    pub(crate) async fn wake_dispatch(&self) -> Result<()> {
        match &self.handle {
            DispatcherHandle::Reliable { control_tx, .. } => control_tx
                .send(DispatcherCommand::PollAndDispatch)
                .await
                .map_err(|_| anyhow!("Failed to wake dispatcher")),
            _ => Ok(()),
        }
    }

    /// Register a new consumer with the dispatcher.
    /// Used by: both reliable and non-reliable.
    pub(crate) async fn add_consumer(&self, consumer: Consumer) -> Result<()> {
        match &self.handle {
            DispatcherHandle::NonReliableExclusive(state) => {
                state.lock().await.add_consumer(consumer);
                Ok(())
            }
            DispatcherHandle::NonReliableShared(state) => {
                state.lock().await.add_consumer(consumer);
                Ok(())
            }
            DispatcherHandle::Reliable { control_tx, .. } => control_tx
                .send(DispatcherCommand::AddConsumer(consumer))
                .await
                .map_err(|_| anyhow!("Failed to send add consumer command")),
        }
    }

    /// Remove a consumer by ID.
    /// Used by: both reliable and non-reliable.
    #[allow(dead_code)]
    pub(crate) async fn remove_consumer(&self, consumer_id: u64) -> Result<()> {
        match &self.handle {
            DispatcherHandle::NonReliableExclusive(state) => {
                state.lock().await.remove_consumer(consumer_id);
                Ok(())
            }
            DispatcherHandle::NonReliableShared(state) => {
                state.lock().await.remove_consumer(consumer_id);
                Ok(())
            }
            DispatcherHandle::Reliable { control_tx, .. } => control_tx
                .send(DispatcherCommand::RemoveConsumer(consumer_id))
                .await
                .map_err(|_| anyhow!("Failed to send remove consumer command")),
        }
    }

    /// Disconnect all consumers (flushes progress for reliable dispatchers).
    /// Used by: both reliable and non-reliable.
    pub(crate) async fn disconnect_all_consumers(&self) -> Result<()> {
        match &self.handle {
            DispatcherHandle::NonReliableExclusive(state) => {
                state.lock().await.disconnect_all();
                Ok(())
            }
            DispatcherHandle::NonReliableShared(state) => {
                state.lock().await.disconnect_all();
                Ok(())
            }
            DispatcherHandle::Reliable { control_tx, .. } => control_tx
                .send(DispatcherCommand::DisconnectAllConsumers)
                .await
                .map_err(|_| anyhow!("Failed to send disconnect all command")),
        }
    }

    /// Clear pending ack state so the next message can be dispatched.
    /// Used by: reliable only.
    pub(crate) async fn reset_pending(&self) -> Result<()> {
        match &self.handle {
            DispatcherHandle::Reliable { control_tx, .. } => control_tx
                .send(DispatcherCommand::RetryNow(None))
                .await
                .map_err(|_| anyhow!("Failed to send reset pending command")),
            _ => Ok(()),
        }
    }

    /// Force flush durable subscription progress.
    /// Used by: reliable only.
    pub(crate) async fn flush_progress_now(&self) -> Result<()> {
        match &self.handle {
            DispatcherHandle::Reliable { control_tx, .. } => control_tx
                .send(DispatcherCommand::FlushProgressNow)
                .await
                .map_err(|_| anyhow!("Failed to send flush progress command")),
            _ => Ok(()),
        }
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

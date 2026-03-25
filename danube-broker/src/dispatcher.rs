use anyhow::{anyhow, Result};
use danube_core::message::StreamMessage;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use tokio::sync::{mpsc, watch, Mutex};
use tokio::time::{Duration, Instant};

use crate::{
    consumer::Consumer,
    message::{AckMessage, NackMessage},
    subscription::SubscriptionFailurePolicy,
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

    pub(super) fn schedule_retry_now(&mut self, reason: Option<String>) {
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
    ) -> Self {
        let (control_tx, control_rx) = mpsc::channel(32);
        let (ready_tx, ready_rx) = watch::channel(false);
        ExclusiveDispatcher::start_reliable(engine, failure_policy, control_rx, ready_tx);
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
    ) -> Self {
        let (control_tx, control_rx) = mpsc::channel(32);
        let (ready_tx, ready_rx) = watch::channel(false);
        SharedDispatcher::start_reliable(engine, failure_policy, control_rx, ready_tx);
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

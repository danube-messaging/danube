use anyhow::{anyhow, Result};
use danube_core::message::StreamMessage;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, watch, Notify};

use crate::{consumer::Consumer, message::AckMessage};

// Module declarations
pub(crate) mod commands;
pub(crate) mod exclusive;
pub(crate) mod shared;
pub(crate) mod subscription_engine;

use commands::DispatcherCommand;
use exclusive::ExclusiveDispatcher;
use shared::SharedDispatcher;
use subscription_engine::SubscriptionEngine;

#[derive(Debug)]
pub(crate) enum DispatchStrategy {
    // Does not store messages, sends them directly to the dispatcher
    NonReliable,
    // Stores messages for reliable delivery
    Reliable,
}

/// Single dispatcher handle — a thin facade over an mpsc command channel.
///
/// All four variants (exclusive/shared × reliable/non-reliable) share the same
/// struct layout. The variation lives entirely in the background task spawned
/// by the factory constructor.
#[derive(Debug, Clone)]
pub(crate) struct Dispatcher {
    control_tx: mpsc::Sender<DispatcherCommand>,
    ready_rx: watch::Receiver<bool>,
}

impl Dispatcher {
    // ── Factory constructors ────────────────────────────────────────────

    /// Non-reliable exclusive (fire-and-forget, single active consumer).
    pub(crate) fn non_reliable_exclusive() -> Self {
        let (control_tx, control_rx) = mpsc::channel(16);
        let (_ready_tx, ready_rx) = watch::channel(true);
        ExclusiveDispatcher::start_non_reliable(control_rx);
        Self {
            control_tx,
            ready_rx,
        }
    }

    /// Non-reliable shared (fire-and-forget, round-robin).
    pub(crate) fn non_reliable_shared() -> Self {
        let (control_tx, control_rx) = mpsc::channel(16);
        let (_ready_tx, ready_rx) = watch::channel(true);
        SharedDispatcher::start_non_reliable(control_rx);
        Self {
            control_tx,
            ready_rx,
        }
    }

    /// Reliable exclusive (ack-gating, single active consumer, heartbeat).
    pub(crate) fn reliable_exclusive(engine: SubscriptionEngine) -> Self {
        let (control_tx, control_rx) = mpsc::channel(32);
        let (ready_tx, ready_rx) = watch::channel(false);
        ExclusiveDispatcher::start_reliable(engine, control_rx, control_tx.clone(), ready_tx);
        Self {
            control_tx,
            ready_rx,
        }
    }

    /// Reliable shared (ack-gating, round-robin, heartbeat).
    pub(crate) fn reliable_shared(engine: SubscriptionEngine) -> Self {
        let (control_tx, control_rx) = mpsc::channel(32);
        let (ready_tx, ready_rx) = watch::channel(false);
        SharedDispatcher::start_reliable(engine, control_rx, control_tx.clone(), ready_tx);
        Self {
            control_tx,
            ready_rx,
        }
    }

    // ── Public API (written once) ───────────────────────────────────────

    /// Block until the dispatcher is ready.
    /// Used by: reliable only (non-reliable dispatchers are ready immediately).
    pub(crate) async fn ready(&self) {
        if *self.ready_rx.borrow() {
            return;
        }
        let mut rx = self.ready_rx.clone();
        while rx.changed().await.is_ok() {
            if *rx.borrow() {
                break;
            }
        }
    }

    /// Get notifier for signaling new messages from Topic (reliable only).
    /// Spawns a listener that converts notifications into PollAndDispatch commands.
    pub(crate) fn get_notifier(&self) -> Arc<Notify> {
        let notify = Arc::new(Notify::new());
        let tx = self.control_tx.clone();
        let n = notify.clone();
        tokio::spawn(async move {
            loop {
                n.notified().await;
                use crate::broker_metrics::DISPATCHER_NOTIFIER_POLLS_TOTAL;
                use metrics::counter;
                counter!(DISPATCHER_NOTIFIER_POLLS_TOTAL.name).increment(1);
                let _ = tx.send(DispatcherCommand::PollAndDispatch).await;
            }
        });
        notify
    }

    /// Push a single message for immediate dispatch.
    /// Used by: non-reliable only (reliable dispatchers are stream-driven via notifier).
    pub(crate) async fn dispatch_message(&self, message: StreamMessage) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.control_tx
            .send(DispatcherCommand::DispatchMessage(message, tx))
            .await
            .map_err(|e| anyhow!("Failed to send dispatch command: {}", e))?;
        rx.await
            .map_err(|e| anyhow!("Failed to receive dispatch response: {}", e))?
    }

    /// Acknowledge a previously dispatched message.
    /// Used by: reliable only.
    pub(crate) async fn ack_message(&self, ack_msg: AckMessage) -> Result<()> {
        self.control_tx
            .send(DispatcherCommand::MessageAcked(ack_msg))
            .await
            .map_err(|_| anyhow!("Failed to send ack command"))
    }

    /// Register a new consumer with the dispatcher.
    /// Used by: both reliable and non-reliable.
    pub(crate) async fn add_consumer(&self, consumer: Consumer) -> Result<()> {
        self.control_tx
            .send(DispatcherCommand::AddConsumer(consumer))
            .await
            .map_err(|_| anyhow!("Failed to send add consumer command"))
    }

    /// Remove a consumer by ID.
    /// Used by: both reliable and non-reliable.
    #[allow(dead_code)]
    pub(crate) async fn remove_consumer(&self, consumer_id: u64) -> Result<()> {
        self.control_tx
            .send(DispatcherCommand::RemoveConsumer(consumer_id))
            .await
            .map_err(|_| anyhow!("Failed to send remove consumer command"))
    }

    /// Disconnect all consumers (flushes progress for reliable dispatchers).
    /// Used by: both reliable and non-reliable.
    pub(crate) async fn disconnect_all_consumers(&self) -> Result<()> {
        self.control_tx
            .send(DispatcherCommand::DisconnectAllConsumers)
            .await
            .map_err(|_| anyhow!("Failed to send disconnect all command"))
    }

    /// Clear pending ack state so the next message can be dispatched.
    /// Used by: reliable only.
    pub(crate) async fn reset_pending(&self) -> Result<()> {
        self.control_tx
            .send(DispatcherCommand::ResetPending)
            .await
            .map_err(|_| anyhow!("Failed to send reset pending command"))
    }

    /// Force flush durable subscription progress.
    /// Used by: reliable only.
    pub(crate) async fn flush_progress_now(&self) -> Result<()> {
        self.control_tx
            .send(DispatcherCommand::FlushProgressNow)
            .await
            .map_err(|_| anyhow!("Failed to send flush progress command"))
    }
}

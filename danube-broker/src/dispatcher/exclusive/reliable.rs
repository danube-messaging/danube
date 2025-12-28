//! Reliable exclusive dispatcher
//!
//! # Overview
//!
//! Provides **at-least-once delivery** to a single active consumer with strict ack-gating.
//! Used for durable subscriptions where message delivery guarantees are critical.
//!
//! # Dispatch Behavior
//!
//! - **Single Active Consumer**: Only one consumer is active at a time
//! - **Ack-Gating**: Only one message is in-flight at a time; next message waits for acknowledgment
//! - **Pending State**: Tracks the current in-flight message and blocks new dispatches until ack received
//! - **Automatic Retry**: If consumer disconnects, the pending message is resent to the next consumer
//! - **Progress Tracking**: Subscription progress is persisted to allow resumption from last acked offset
//!
//! # State Management
//!
//! The reliable dispatcher maintains several critical state variables:
//!
//! - `pending`: Boolean flag indicating if a message is currently in-flight
//! - `pending_message`: Buffer holding the current in-flight message (for retries)
//! - `consumers`: List of available consumers
//! - `active_consumer`: Currently selected consumer (from ExclusiveConsumerState)
//! - `engine`: SubscriptionEngine managing stream position and ack tracking
//!
//! # Message Flow
//!
//! 1. **Poll**: `PollAndDispatch` command triggers polling
//! 2. **Check Pending**: If `pending == true`, skip (message already in-flight)
//! 3. **Get Message**: Either resend buffered message OR poll next from SubscriptionEngine
//! 4. **Buffer**: Store message in `pending_message`
//! 5. **Send**: Attempt to send to active consumer via `send_message()`
//! 6. **Mark Pending**: Set `pending = true` (blocks further dispatch)
//! 7. **Wait for Ack**: Consumer must acknowledge before next message
//! 8. **On Ack**: Clear `pending` flag and `pending_message` buffer, trigger next poll
//!
//! # Heartbeat Watchdog
//!
//! A background heartbeat (500ms interval) monitors lag and triggers dispatch:
//!
//! - Checks subscription lag via `SubscriptionEngine::get_lag_info()`
//! - If lag detected (unread messages in WAL), triggers `PollAndDispatch`
//! - Ensures messages are dispatched even without explicit Topic notifications
//! - Reports lag metrics for monitoring
//!
//! # Notification System
//!
//! The `get_notifier()` method returns an `Arc<Notify>` for the Topic to signal new messages:
//!
//! - Topic calls `notifier.notify_one()` when new messages arrive
//! - Spawned task listens for notifications and sends `PollAndDispatch` command
//! - Ensures low-latency dispatch when messages are produced
//!
//! # Consumer Failover
//!
//! When the active consumer disconnects:
//!
//! 1. `RemoveConsumer` command is received
//! 2. If removed consumer was active, `active_consumer` is cleared
//! 3. Next `PollAndDispatch` will attempt to resend buffered message to a new consumer
//! 4. The `pending_message` ensures no message loss during failover
//!
//! # Persistence
//!
//! - Subscription progress (last acked offset) is persisted via SubscriptionEngine
//! - Allows broker restart without message loss
//! - Progress is flushed periodically and on disconnect
//!
//! # Use Cases
//!
//! - Durable subscriptions requiring guaranteed delivery
//! - Exclusive/Failover subscription types
//! - Financial transactions, order processing
//! - Any scenario where message loss is unacceptable

use anyhow::{anyhow, Result};
use danube_core::message::StreamMessage;
use metrics::{counter, gauge};
use std::sync::Arc;
use tokio::sync::{mpsc, watch, Mutex, Notify};
use tokio::time::Duration;
use tracing::{trace, warn};

use crate::broker_metrics::{
    DISPATCHER_HEARTBEAT_POLLS_TOTAL, DISPATCHER_NOTIFIER_POLLS_TOTAL, SUBSCRIPTION_LAG_MESSAGES,
};
use crate::consumer::Consumer;
use crate::message::AckMessage;

use super::super::commands::DispatcherCommand;
use super::super::exclusive::ExclusiveConsumerState;
use super::super::subscription_engine::SubscriptionEngine;

#[derive(Debug)]
pub(crate) struct ReliableExclusiveDispatcher {
    control_tx: mpsc::Sender<DispatcherCommand>,
    ready_rx: watch::Receiver<bool>,
}

impl ReliableExclusiveDispatcher {
    pub fn new(engine: SubscriptionEngine) -> Self {
        let (control_tx, control_rx) = mpsc::channel(32);
        let (ready_tx, ready_rx) = watch::channel(false);
        let control_tx_clone = control_tx.clone();

        tokio::spawn(async move {
            Self::run_reliable_loop(engine, control_rx, control_tx_clone, ready_tx).await;
        });

        Self {
            control_tx,
            ready_rx,
        }
    }

    async fn run_reliable_loop(
        engine: SubscriptionEngine,
        mut control_rx: mpsc::Receiver<DispatcherCommand>,
        control_tx: mpsc::Sender<DispatcherCommand>,
        ready_tx: watch::Sender<bool>,
    ) {
        let engine = Arc::new(Mutex::new(engine));
        let mut state = ExclusiveConsumerState::new();
        let mut pending = false;
        let mut pending_message: Option<StreamMessage> = None;

        // Initialize stream from persisted progress
        {
            if let Err(e) = engine
                .lock()
                .await
                .init_stream_from_progress_or_latest()
                .await
            {
                warn!("Reliable exclusive dispatcher failed to init stream: {}", e);
            }
            let _ = ready_tx.send(true);
        }

        // Heartbeat watchdog (500ms default)
        let heartbeat_interval = Duration::from_millis(500);
        let mut heartbeat = tokio::time::interval(heartbeat_interval);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        heartbeat.tick().await; // Skip first immediate tick

        loop {
            tokio::select! {
                cmd_result = control_rx.recv() => {
                    match cmd_result {
                        Some(cmd) => {
                            Self::handle_command(
                                cmd,
                                &mut state,
                                &engine,
                                &mut pending,
                                &mut pending_message,
                                &control_tx,
                            ).await;
                        }
                        None => break, // Channel closed
                    }
                }

                _ = heartbeat.tick() => {
                    Self::handle_heartbeat(
                        &state,
                        &engine,
                        pending,
                        &control_tx,
                    ).await;
                }
            }
        }
    }

    async fn handle_command(
        cmd: DispatcherCommand,
        state: &mut ExclusiveConsumerState,
        engine: &Arc<Mutex<SubscriptionEngine>>,
        pending: &mut bool,
        pending_message: &mut Option<StreamMessage>,
        control_tx: &mpsc::Sender<DispatcherCommand>,
    ) {
        match cmd {
            DispatcherCommand::AddConsumer(c) => {
                trace!(
                    "AddConsumer: consumer {} added to reliable exclusive dispatcher",
                    c.consumer_id
                );
                state.add_consumer(c);
                if !*pending {
                    let _ = control_tx.send(DispatcherCommand::PollAndDispatch).await;
                }
            }
            DispatcherCommand::RemoveConsumer(id) => {
                state.remove_consumer(id);
            }
            DispatcherCommand::DisconnectAllConsumers => {
                if let Err(e) = engine.lock().await.flush_progress_now().await {
                    warn!("DisconnectAllConsumers: flush progress failed: {}", e);
                }
                state.disconnect_all();
            }
            DispatcherCommand::MessageAcked(ack_msg) => {
                Self::handle_ack(engine, ack_msg, pending, pending_message).await;
                let _ = control_tx.send(DispatcherCommand::PollAndDispatch).await;
            }
            DispatcherCommand::DispatchMessage(_, response_tx) => {
                let _ = response_tx.send(Err(anyhow!(
                    "Reliable dispatcher is stream-driven, not push-per-message"
                )));
            }
            DispatcherCommand::PollAndDispatch => {
                // Increment notifier poll counter (fast path)
                counter!(DISPATCHER_NOTIFIER_POLLS_TOTAL.name).increment(1);

                Self::handle_poll_and_dispatch(state, engine, pending, pending_message).await;
            }
            DispatcherCommand::ResetPending => {
                trace!("ResetPending: clearing pending flag to allow retry");
                *pending = false;
                let _ = control_tx.send(DispatcherCommand::PollAndDispatch).await;
            }
            DispatcherCommand::FlushProgressNow => {
                if let Err(e) = engine.lock().await.flush_progress_now().await {
                    warn!("FlushProgressNow: failed to flush: {}", e);
                }
            }
        }
    }

    async fn handle_ack(
        engine: &Arc<Mutex<SubscriptionEngine>>,
        ack_msg: AckMessage,
        pending: &mut bool,
        pending_message: &mut Option<StreamMessage>,
    ) {
        let acked_offset = ack_msg.msg_id.topic_offset;

        if let Err(e) = engine.lock().await.on_acked(ack_msg.msg_id.clone()).await {
            warn!("Ack handling failed: {}", e);
        }

        let should_clear = pending_message
            .as_ref()
            .map(|msg| msg.msg_id.topic_offset == acked_offset)
            .unwrap_or(false);

        if should_clear {
            trace!(
                "Ack received for pending message req_id={}, offset={}, clearing buffer",
                ack_msg.request_id,
                acked_offset
            );
            *pending = false;
            *pending_message = None;
        } else {
            trace!(
                "Ignoring late ack for req_id={}, offset={} (not the pending message)",
                ack_msg.request_id,
                acked_offset
            );
        }
    }

    async fn handle_poll_and_dispatch(
        state: &mut ExclusiveConsumerState,
        engine: &Arc<Mutex<SubscriptionEngine>>,
        pending: &mut bool,
        pending_message: &mut Option<StreamMessage>,
    ) {
        if *pending {
            return;
        }

        if let Some(cons) = state.active_consumer_mut() {
            if !cons.get_status().await {
                return;
            }

            // Get message: buffered (resend) or new from stream
            let msg_to_send = if let Some(buffered_msg) = pending_message.take() {
                trace!(
                    "Resending buffered message offset={}",
                    buffered_msg.msg_id.topic_offset
                );
                Some(buffered_msg)
            } else {
                match engine.lock().await.poll_next().await {
                    Ok(msg_opt) => msg_opt,
                    Err(e) => {
                        warn!("poll_next error: {}", e);
                        None
                    }
                }
            };

            if let Some(msg) = msg_to_send {
                let offset = msg.msg_id.topic_offset;
                *pending_message = Some(msg.clone());

                if let Err(e) = cons.send_message(msg).await {
                    warn!(
                        "Failed to send message offset={}: {}. Will retry on reconnect.",
                        offset, e
                    );
                } else {
                    *pending = true;
                }
            }
        }
    }

    async fn handle_heartbeat(
        state: &ExclusiveConsumerState,
        engine: &Arc<Mutex<SubscriptionEngine>>,
        pending: bool,
        control_tx: &mpsc::Sender<DispatcherCommand>,
    ) {
        if !state.has_active_consumer() || pending {
            return;
        }

        let lag_info = {
            let engine_guard = engine.lock().await;
            engine_guard.get_lag_info()
        };

        // Report lag gauge
        gauge!(
            SUBSCRIPTION_LAG_MESSAGES.name,
            "subscription" => engine.lock().await._subscription_name.clone()
        )
        .set(lag_info.lag_messages as f64);

        if lag_info.has_lag {
            trace!(
                "Heartbeat detected lag: {} messages behind",
                lag_info.lag_messages
            );
            counter!(DISPATCHER_HEARTBEAT_POLLS_TOTAL.name).increment(1);
            let _ = control_tx.send(DispatcherCommand::PollAndDispatch).await;
        }
    }

    // Public API

    /// Get notifier for signaling new messages from Topic
    pub fn get_notifier(&self) -> Arc<Notify> {
        let notify = Arc::new(Notify::new());
        let tx = self.control_tx.clone();
        let n = notify.clone();
        tokio::spawn(async move {
            loop {
                n.notified().await;
                // Count polls triggered by notifier (fast path)
                counter!(DISPATCHER_NOTIFIER_POLLS_TOTAL.name).increment(1);
                let _ = tx.send(DispatcherCommand::PollAndDispatch).await;
            }
        });
        notify
    }

    pub async fn ready(&self) {
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

    pub async fn dispatch_message(&self, _msg: StreamMessage) -> Result<()> {
        Err(anyhow!(
            "Reliable single dispatcher is stream-driven, not push-per-message"
        ))
    }

    pub async fn add_consumer(&self, consumer: Consumer) -> Result<()> {
        self.control_tx
            .send(DispatcherCommand::AddConsumer(consumer))
            .await
            .map_err(|_| anyhow!("Failed to send add consumer command"))
    }

    pub async fn remove_consumer(&self, consumer_id: u64) -> Result<()> {
        self.control_tx
            .send(DispatcherCommand::RemoveConsumer(consumer_id))
            .await
            .map_err(|_| anyhow!("Failed to send remove consumer command"))
    }

    pub async fn disconnect_all_consumers(&self) -> Result<()> {
        self.control_tx
            .send(DispatcherCommand::DisconnectAllConsumers)
            .await
            .map_err(|_| anyhow!("Failed to send disconnect all command"))
    }

    pub async fn ack_message(&self, ack: AckMessage) -> Result<()> {
        self.control_tx
            .send(DispatcherCommand::MessageAcked(ack))
            .await
            .map_err(|_| anyhow!("Failed to send ack command"))
    }

    pub async fn reset_pending(&self) -> Result<()> {
        self.control_tx
            .send(DispatcherCommand::ResetPending)
            .await
            .map_err(|_| anyhow!("Failed to send reset pending command"))
    }

    pub async fn flush_progress_now(&self) -> Result<()> {
        self.control_tx
            .send(DispatcherCommand::FlushProgressNow)
            .await
            .map_err(|_| anyhow!("Failed to send flush progress command"))
    }
}

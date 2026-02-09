//! Reliable shared dispatcher
//!
//! # Overview
//!
//! Provides **at-least-once delivery** with **round-robin load balancing** across multiple consumers.
//! Combines strict ack-gating with fair message distribution for durable shared subscriptions.
//!
//! # Dispatch Behavior
//!
//! - **Multiple Consumers**: All connected consumers share the message load
//! - **Round-Robin**: Messages are distributed evenly using atomic counter
//! - **Ack-Gating**: Only one message is in-flight at a time across all consumers
//! - **Pending State**: Tracks the current in-flight message and blocks new dispatches until ack received
//! - **Automatic Retry**: If consumer disconnects, the pending message is resent to the next consumer
//! - **Progress Tracking**: Subscription progress is persisted to allow resumption from last acked offset
//!
//! # State Management
//!
//! The reliable shared dispatcher maintains several critical state variables:
//!
//! - `pending`: Boolean flag indicating if a message is currently in-flight
//! - `pending_message`: Buffer holding the current in-flight message (for retries)
//! - `consumers`: List of all available consumers
//! - `rr_index`: Atomic counter for round-robin position (shared across all dispatches)
//! - `engine`: SubscriptionEngine managing stream position and ack tracking
//!
//! # Round-Robin with Ack-Gating
//!
//! Unlike non-reliable mode, this dispatcher combines round-robin with strict ordering:
//!
//! 1. Messages are distributed round-robin across consumers
//! 2. BUT only one message is in-flight at a time (ack-gating)
//! 3. Next message waits for previous ack before being sent
//! 4. This ensures at-least-once delivery while maintaining load balance
//!
//! # Message Flow
//!
//! 1. **Poll**: `PollAndDispatch` command triggers polling
//! 2. **Check Pending**: If `pending == true`, skip (message already in-flight)
//! 3. **Get Message**: Either resend buffered message OR poll next from SubscriptionEngine
//! 4. **Select Consumer**: Use round-robin to pick target consumer
//! 5. **Attempt Send**: Try sending to selected consumer, check health first
//! 6. **Retry on Failure**: If unhealthy, try next consumer in rotation (up to N attempts)
//! 7. **Buffer & Mark**: Store in `pending_message`, set `pending = true`
//! 8. **Wait for Ack**: Consumer must acknowledge before next message
//! 9. **On Ack**: Clear `pending` flag and `pending_message` buffer, trigger next poll
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
//! When a consumer disconnects while a message is in-flight:
//!
//! 1. `RemoveConsumer` command removes consumer from list
//! 2. The `pending_message` remains buffered
//! 3. `ResetPending` command (from subscription) clears `pending` flag
//! 4. Next `PollAndDispatch` resends buffered message to a different consumer
//! 5. Round-robin continues with remaining consumers
//!
//! # Persistence
//!
//! - Subscription progress (last acked offset) is persisted via SubscriptionEngine
//! - Allows broker restart without message loss
//! - Progress is flushed periodically and on disconnect
//! - All consumers share the same subscription progress
//!
//! # Use Cases
//!
//! - Durable shared subscriptions requiring guaranteed delivery
//! - Work queues with multiple worker instances
//! - Message processing with load balancing
//! - Task distribution systems
//! - Any scenario requiring both load balancing AND delivery guarantees

use anyhow::anyhow;
use danube_core::message::StreamMessage;
use metrics::{counter, gauge};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, watch, Mutex};
use tokio::time::Duration;
use tracing::{trace, warn};

use crate::broker_metrics::{
    DISPATCHER_HEARTBEAT_POLLS_TOTAL, DISPATCHER_NOTIFIER_POLLS_TOTAL, SUBSCRIPTION_LAG_MESSAGES,
};
use crate::message::AckMessage;

use super::super::commands::DispatcherCommand;
use super::super::shared::SharedConsumerState;
use super::super::subscription_engine::SubscriptionEngine;

/// Spawn the reliable shared dispatcher background task.
pub(super) fn start(
    engine: SubscriptionEngine,
    control_rx: mpsc::Receiver<DispatcherCommand>,
    control_tx: mpsc::Sender<DispatcherCommand>,
    ready_tx: watch::Sender<bool>,
) {
    tokio::spawn(async move {
        run_reliable_loop(engine, control_rx, control_tx, ready_tx).await;
    });
}

async fn run_reliable_loop(
    engine: SubscriptionEngine,
    mut control_rx: mpsc::Receiver<DispatcherCommand>,
    control_tx: mpsc::Sender<DispatcherCommand>,
    ready_tx: watch::Sender<bool>,
) {
    let engine = Arc::new(Mutex::new(engine));
    let rr_index = Arc::new(AtomicUsize::new(0));
    let rr_task = rr_index.clone();
    let mut state = SharedConsumerState::new(rr_task);
    let mut pending = false;
    let mut pending_message: Option<StreamMessage> = None;

    // Initialize stream
    {
        if let Err(e) = engine
            .lock()
            .await
            .init_stream_from_progress_or_latest()
            .await
        {
            warn!(error = %e, "Reliable shared dispatcher failed to init stream");
        }
        let _ = ready_tx.send(true);
    }

    // Heartbeat watchdog
    let heartbeat_interval = Duration::from_millis(500);
    let mut heartbeat = tokio::time::interval(heartbeat_interval);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    heartbeat.tick().await;

    loop {
        tokio::select! {
            cmd_result = control_rx.recv() => {
                match cmd_result {
                    Some(cmd) => {
                        handle_command(
                            cmd,
                            &mut state,
                            &engine,
                            &mut pending,
                            &mut pending_message,
                            &control_tx,
                        ).await;
                    }
                    None => break,
                }
            }

            _ = heartbeat.tick() => {
                handle_heartbeat(&state, &engine, pending, &control_tx).await;
            }
        }
    }
}

async fn handle_command(
    cmd: DispatcherCommand,
    state: &mut SharedConsumerState,
    engine: &Arc<Mutex<SubscriptionEngine>>,
    pending: &mut bool,
    pending_message: &mut Option<StreamMessage>,
    control_tx: &mpsc::Sender<DispatcherCommand>,
) {
    match cmd {
        DispatcherCommand::AddConsumer(c) => {
            trace!(consumer_id = %c.consumer_id, "consumer added");
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
                warn!(error = %e, "DisconnectAllConsumers: flush failed");
            }
            state.disconnect_all();
        }
        DispatcherCommand::MessageAcked(ack_msg) => {
            handle_ack(engine, ack_msg, pending, pending_message).await;
            let _ = control_tx.send(DispatcherCommand::PollAndDispatch).await;
        }
        DispatcherCommand::DispatchMessage(_, response_tx) => {
            let _ = response_tx.send(Err(anyhow!(
                "Reliable dispatcher is stream-driven, not push-per-message"
            )));
        }
        DispatcherCommand::PollAndDispatch => {
            counter!(DISPATCHER_NOTIFIER_POLLS_TOTAL.name).increment(1);
            handle_poll_and_dispatch(state, engine, pending, pending_message).await;
        }
        DispatcherCommand::ResetPending => {
            if pending_message.is_some() {
                trace!("Consumer disconnected, will failover buffered message");
            }
            *pending = false;
            let _ = control_tx.send(DispatcherCommand::PollAndDispatch).await;
        }
        DispatcherCommand::FlushProgressNow => {
            if let Err(e) = engine.lock().await.flush_progress_now().await {
                warn!(error = %e, "FlushProgressNow: failed");
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
        warn!(offset = %acked_offset, error = %e, "Ack handling failed");
    }

    let should_clear = pending_message
        .as_ref()
        .map(|msg| msg.msg_id.topic_offset == acked_offset)
        .unwrap_or(false);

    if should_clear {
        trace!(offset = %acked_offset, "ack received, clearing buffer");
        *pending = false;
        *pending_message = None;
    } else {
        trace!(offset = %acked_offset, "ignoring late ack");
    }
}

async fn handle_poll_and_dispatch(
    state: &mut SharedConsumerState,
    engine: &Arc<Mutex<SubscriptionEngine>>,
    pending: &mut bool,
    pending_message: &mut Option<StreamMessage>,
) {
    if *pending || state.is_empty() {
        return;
    }

    // Get message
    let msg_to_send = if let Some(buffered_msg) = pending_message.take() {
        trace!(
            offset = %buffered_msg.msg_id.topic_offset,
            "resending buffered message"
        );
        Some(buffered_msg)
    } else {
        match engine.lock().await.poll_next().await {
            Ok(msg_opt) => msg_opt,
            Err(e) => {
                warn!(error = %e, "poll_next error");
                None
            }
        }
    };

    if let Some(msg) = msg_to_send {
        let mut attempts = 0;
        let mut sent = false;
        let offset = msg.msg_id.topic_offset;
        let num_consumers = state.len();

        while attempts < num_consumers && !sent {
            let idx = state.rr_index.fetch_add(1, Ordering::Relaxed) % num_consumers;
            if let Some(target) = state.get_consumer_mut(idx) {
                if !target.get_status().await {
                    attempts += 1;
                    continue;
                }

                *pending_message = Some(msg.clone());

                if let Err(e) = target.send_message(msg.clone()).await {
                    warn!(
                        offset = %offset,
                        consumer_id = %target.consumer_id,
                        error = %e,
                        "Failed to send message to consumer"
                    );
                    attempts += 1;
                    continue;
                } else {
                    *pending = true;
                    sent = true;
                }
            }
            attempts += 1;
        }
    }
}

async fn handle_heartbeat(
    state: &SharedConsumerState,
    engine: &Arc<Mutex<SubscriptionEngine>>,
    pending: bool,
    control_tx: &mpsc::Sender<DispatcherCommand>,
) {
    if state.is_empty() || pending {
        return;
    }

    let lag_info = {
        let engine_guard = engine.lock().await;
        engine_guard.get_lag_info()
    };

    gauge!(
        SUBSCRIPTION_LAG_MESSAGES.name,
        "subscription" => engine.lock().await._subscription_name.clone()
    )
    .set(lag_info.lag_messages as f64);

    if lag_info.has_lag {
        trace!(lag_messages = %lag_info.lag_messages, "heartbeat detected lag");
        counter!(DISPATCHER_HEARTBEAT_POLLS_TOTAL.name).increment(1);
        let _ = control_tx.send(DispatcherCommand::PollAndDispatch).await;
    }
}

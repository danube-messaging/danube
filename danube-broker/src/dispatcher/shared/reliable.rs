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
//! 1. **Wake**: External wakeups or the heartbeat trigger a dispatch attempt
//! 2. **Check Pending**: If `pending == true`, skip (message already in-flight)
//! 3. **Get Message**: Either resend buffered message OR poll next from SubscriptionEngine
//! 4. **Select Consumer**: Use round-robin to pick target consumer
//! 5. **Attempt Send**: Try sending to selected consumer, check health first
//! 6. **Retry on Failure**: If unhealthy, try next consumer in rotation (up to N attempts)
//! 7. **Buffer & Mark**: Store in `pending_message`, set `pending = true`
//! 8. **Wait for Ack**: Consumer must acknowledge before next message
//! 9. **On Ack**: Clear `pending` flag and `pending_message` buffer, then immediately attempt the next dispatch
//!
//! # Heartbeat Watchdog
//!
//! A background heartbeat (500ms interval) monitors lag and attempts dispatch:
//!
//! - Checks subscription lag via `SubscriptionEngine::get_lag_info()`
//! - If lag is detected (unread messages in WAL), runs another dispatch attempt directly in the loop
//! - Ensures messages are dispatched even without explicit Topic notifications
//! - Reports lag metrics for monitoring
//!
//! # Wakeup Path
//!
//! Reliable topics wake the dispatcher directly through the dispatcher facade:
//!
//! - Topic publish and reconnect paths call `Dispatcher::wake_dispatch()`
//! - The dispatcher loop receives `PollAndDispatch` directly, without an extra `Notify` bridge task
//! - Ensures low-latency dispatch when messages are produced
//!
//! # Consumer Failover
//!
//! When a consumer disconnects while a message is in-flight:
//!
//! 1. `RemoveConsumer` command removes consumer from list
//! 2. The `pending_message` remains buffered
//! 3. `ResetPending` command (from subscription) clears `pending` flag
//! 4. The dispatcher immediately attempts to resend the buffered message to a different consumer
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

use metrics::{counter, gauge};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tokio::time::{Duration, Instant};
use tracing::{trace, warn};

use crate::broker_metrics::{
    DISPATCHER_HEARTBEAT_POLLS_TOTAL, DISPATCHER_NOTIFIER_POLLS_TOTAL,
    SUBSCRIPTION_ACK_TIMEOUT_TOTAL, SUBSCRIPTION_LAG_MESSAGES, SUBSCRIPTION_NACK_TOTAL,
    SUBSCRIPTION_REDELIVERY_TOTAL,
};
use crate::message::{AckMessage, NackMessage};
use crate::replicator::Replicator;

use super::super::commands::DispatcherCommand;
use super::super::shared::SharedConsumerState;
use super::super::subscription_engine::SubscriptionEngine;
use super::super::{
    handle_retry_exhausted_pending, record_retry_exhausted_metric, subscription_metric_context,
    subscription_metric_topic, update_pending_delivery_metrics, PendingDelivery,
    SubscriptionMetricContext,
};

/// Spawn the reliable shared dispatcher background task.
pub(super) fn start(
    engine: SubscriptionEngine,
    replicator: Option<Arc<Replicator>>,
    control_rx: mpsc::Receiver<DispatcherCommand>,
    ready_tx: watch::Sender<bool>,
) {
    tokio::spawn(async move {
        run_reliable_loop(engine, replicator, control_rx, ready_tx).await;
    });
}

async fn run_reliable_loop(
    mut engine: SubscriptionEngine,
    replicator: Option<Arc<Replicator>>,
    mut control_rx: mpsc::Receiver<DispatcherCommand>,
    ready_tx: watch::Sender<bool>,
) {
    let rr_index = Arc::new(AtomicUsize::new(0));
    let rr_task = rr_index.clone();
    let mut state = SharedConsumerState::new(rr_task);
    let mut pending_delivery: Option<PendingDelivery> = None;

    // Initialize stream
    {
        if let Err(e) = engine.init_stream_from_progress_or_latest().await {
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
                            &mut engine,
                            replicator.as_deref(),
                            &mut pending_delivery,
                        ).await;
                    }
                    None => break,
                }
            }

            _ = heartbeat.tick() => {
                handle_heartbeat(
                    &mut state,
                    &mut engine,
                    replicator.as_deref(),
                    &mut pending_delivery,
                ).await;
            }
        }
    }
}

async fn handle_command(
    cmd: DispatcherCommand,
    state: &mut SharedConsumerState,
    engine: &mut SubscriptionEngine,
    replicator: Option<&Replicator>,
    pending_delivery: &mut Option<PendingDelivery>,
) {
    match cmd {
        DispatcherCommand::AddConsumer(c) => {
            trace!(consumer_id = %c.consumer_id, "consumer added");
            state.add_consumer(c);
            handle_poll_and_dispatch(
                state,
                engine,
                replicator,
                pending_delivery,
            ).await;
        }
        DispatcherCommand::RemoveConsumer(id) => {
            state.remove_consumer(id);
        }
        DispatcherCommand::DisconnectAllConsumers => {
            if let Err(e) = engine.flush_progress_now().await {
                warn!(error = %e, "DisconnectAllConsumers: flush failed");
            }
            state.disconnect_all();
        }
        DispatcherCommand::MessageAcked(ack_msg) => {
            handle_ack(engine, ack_msg, pending_delivery).await;
            handle_poll_and_dispatch(
                state,
                engine,
                replicator,
                pending_delivery,
            ).await;
        }
        DispatcherCommand::MessageNacked(nack_msg) => {
            let metric_context = subscription_metric_context(engine, pending_delivery.as_ref());
            handle_nack(engine, &metric_context, nack_msg, pending_delivery);
            handle_poll_and_dispatch(
                state,
                engine,
                replicator,
                pending_delivery,
            ).await;
        }
        DispatcherCommand::RetryNow(reason) => {
            let metric_context = subscription_metric_context(engine, pending_delivery.as_ref());
            handle_retry_now(engine, &metric_context, reason, pending_delivery);
            handle_poll_and_dispatch(
                state,
                engine,
                replicator,
                pending_delivery,
            ).await;
        }
        DispatcherCommand::AckTimedOut => {
            let metric_context = subscription_metric_context(engine, pending_delivery.as_ref());
            handle_ack_timed_out(engine, &metric_context, pending_delivery);
            handle_poll_and_dispatch(
                state,
                engine,
                replicator,
                pending_delivery,
            ).await;
        }
        DispatcherCommand::PollAndDispatch => {
            counter!(DISPATCHER_NOTIFIER_POLLS_TOTAL.name).increment(1);
            handle_poll_and_dispatch(
                state,
                engine,
                replicator,
                pending_delivery,
            ).await;
        }
        DispatcherCommand::FlushProgressNow => {
            if let Err(e) = engine.flush_progress_now().await {
                warn!(error = %e, "FlushProgressNow: failed");
            }
        }
    }
}

fn handle_nack(
    engine: &SubscriptionEngine,
    metric_context: &SubscriptionMetricContext,
    nack_msg: NackMessage,
    pending_delivery: &mut Option<PendingDelivery>,
) {
    let nacked_offset = nack_msg.msg_id.topic_offset;

    let matches_pending = pending_delivery
        .as_ref()
        .map(|pending| pending.matches_offset(nacked_offset))
        .unwrap_or(false);

    if matches_pending {
        trace!(
            request_id = %nack_msg.request_id,
            offset = %nacked_offset,
            delay_ms = ?nack_msg.delay_ms,
            reason = ?nack_msg.reason,
            "nack received for pending message, preserving buffer for retry"
        );
        counter!(
            SUBSCRIPTION_NACK_TOTAL.name,
            "topic" => metric_context.topic.clone(),
            "subscription" => metric_context.subscription.clone(),
        )
        .increment(1);
        if let Some(pending) = pending_delivery.as_mut() {
            engine.on_nacked(pending, nack_msg.reason, nack_msg.delay_ms);
        }
        if pending_delivery
            .as_ref()
            .map(|pending| pending.is_retry_exhausted())
            .unwrap_or(false)
        {
            record_retry_exhausted_metric(metric_context, engine.failure_policy());
        }
        update_pending_delivery_metrics(metric_context, engine.failure_policy(), pending_delivery);
    } else {
        trace!(
            request_id = %nack_msg.request_id,
            offset = %nacked_offset,
            "ignoring late nack (not the pending message)"
        );
    }
}

fn handle_retry_now(
    engine: &SubscriptionEngine,
    metric_context: &SubscriptionMetricContext,
    reason: Option<String>,
    pending_delivery: &mut Option<PendingDelivery>,
) {
    if let Some(pending) = pending_delivery.as_mut() {
        pending.schedule_retry_now(reason);
    }
    update_pending_delivery_metrics(metric_context, engine.failure_policy(), pending_delivery);
}

fn handle_ack_timed_out(
    engine: &SubscriptionEngine,
    metric_context: &SubscriptionMetricContext,
    pending_delivery: &mut Option<PendingDelivery>,
) {
    let mut retry_exhausted = false;
    let mut exhausted_offset = None;
    let mut exhausted_attempt = None;

    if let Some(pending) = pending_delivery.as_mut() {
        if pending.is_awaiting_ack() {
            trace!(
                offset = %pending.message.msg_id.topic_offset,
                delivery_attempt = %pending.delivery_attempt,
                "ack timeout detected for pending message"
            );
            counter!(
                SUBSCRIPTION_ACK_TIMEOUT_TOTAL.name,
                "topic" => metric_context.topic.clone(),
                "subscription" => metric_context.subscription.clone(),
            )
            .increment(1);
            engine.on_ack_timed_out(pending);
            if pending.is_retry_exhausted() {
                retry_exhausted = true;
                exhausted_offset = Some(pending.message.msg_id.topic_offset);
                exhausted_attempt = Some(pending.delivery_attempt);
            }
        }
    }

    if retry_exhausted {
        record_retry_exhausted_metric(metric_context, engine.failure_policy());
        warn!(
            offset = %exhausted_offset.unwrap_or_default(),
            delivery_attempt = %exhausted_attempt.unwrap_or_default(),
            max_redelivery_count = %engine.failure_policy().max_redelivery_count,
            "retry limit exhausted after ack timeout; leaving message in terminal state"
        );
    }

    update_pending_delivery_metrics(metric_context, engine.failure_policy(), pending_delivery);
}

async fn handle_ack(
    engine: &mut SubscriptionEngine,
    ack_msg: AckMessage,
    pending_delivery: &mut Option<PendingDelivery>,
) {
    let acked_offset = ack_msg.msg_id.topic_offset;

    let should_clear = pending_delivery
        .as_ref()
        .map(|pending| pending.matches_offset(acked_offset) && pending.is_awaiting_ack())
        .unwrap_or(false);

    if should_clear {
        if let Err(e) = engine.on_acked(ack_msg.msg_id.clone()).await {
            warn!(offset = %acked_offset, error = %e, "Ack handling failed");
            return;
        }
        trace!(offset = %acked_offset, "ack received, clearing buffer");
        *pending_delivery = None;
    } else {
        trace!(offset = %acked_offset, "ignoring late ack");
    }
}

async fn handle_poll_and_dispatch(
    state: &mut SharedConsumerState,
    engine: &mut SubscriptionEngine,
    replicator: Option<&Replicator>,
    pending_delivery: &mut Option<PendingDelivery>,
) {
    let failure_policy = engine.failure_policy();
    let metric_context = subscription_metric_context(engine, pending_delivery.as_ref());
    if state.is_empty() {
        update_pending_delivery_metrics(&metric_context, failure_policy, pending_delivery);
        return;
    }

    // Get message
    if pending_delivery.is_none() {
        let next_message = match engine.poll_next().await {
            Ok(msg_opt) => msg_opt,
            Err(e) => {
                warn!(error = %e, "poll_next error");
                None
            }
        };

        if let Some(msg) = next_message {
            *pending_delivery = Some(PendingDelivery::new(msg));
        }
    }

    match handle_retry_exhausted_pending(
        engine,
        replicator,
        pending_delivery,
    )
    .await
    {
        Ok(true) => return,
        Ok(false) => {}
        Err(e) => {
            warn!(error = %e, "retry-exhausted terminal handling failed");
            return;
        }
    }

    let failure_policy = engine.failure_policy();
    let is_retry_exhausted = pending_delivery
        .as_ref()
        .map(|pending| pending.is_retry_exhausted())
        .unwrap_or(false);

    if is_retry_exhausted {
        if let Some(pending) = pending_delivery.as_ref() {
            warn!(
                offset = %pending.message.msg_id.topic_offset,
                delivery_attempt = %pending.delivery_attempt,
                max_redelivery_count = %failure_policy.max_redelivery_count,
                "pending message is retry-exhausted; dispatch is paused pending terminal handling"
            );
        }
        update_pending_delivery_metrics(&metric_context, failure_policy, pending_delivery);
        return;
    }

    let now = Instant::now();
    let should_send = pending_delivery
        .as_ref()
        .map(|pending| pending.is_retry_ready(now))
        .unwrap_or(false);

    if !should_send {
        update_pending_delivery_metrics(&metric_context, failure_policy, pending_delivery);
        return;
    }

    if let Some(pending) = pending_delivery.as_mut() {
        let mut attempts = 0;
        let offset = pending.message.msg_id.topic_offset;
        let is_redelivery = pending.delivery_attempt > 0;

        let num_consumers = state.len();

        while attempts < num_consumers {
            let idx = state.rr_index.fetch_add(1, Ordering::Relaxed) % num_consumers;
            if let Some(target) = state.get_consumer_mut(idx) {
                if !target.get_status().await {
                    attempts += 1;
                    continue;
                }

                let msg = pending.message.clone();

                if let Err(e) = target.send_message(msg).await {
                    warn!(
                        offset = %offset,
                        consumer_id = %target.consumer_id,
                        error = %e,
                        "Failed to send message to consumer"
                    );
                    attempts += 1;
                    continue;
                } else {
                    if is_redelivery {
                        counter!(
                            SUBSCRIPTION_REDELIVERY_TOTAL.name,
                            "topic" => subscription_metric_topic(engine, Some(pending)),
                            "subscription" => engine._subscription_name.clone(),
                        )
                        .increment(1);
                    }
                    pending.on_send_attempt(
                        target.consumer_id,
                        Duration::from_millis(failure_policy.ack_timeout_ms),
                    );
                    break;
                }
            }
            attempts += 1;
        }

        if !pending.is_awaiting_ack() {
            pending.schedule_retry_now(Some("no active shared consumer available".to_string()));
        }
    }
    update_pending_delivery_metrics(&metric_context, failure_policy, pending_delivery);
}

async fn handle_heartbeat(
    state: &mut SharedConsumerState,
    engine: &mut SubscriptionEngine,
    replicator: Option<&Replicator>,
    pending_delivery: &mut Option<PendingDelivery>,
) {
    if state.is_empty() {
        return;
    }

    let lag_info = engine.get_lag_info();

    gauge!(
        SUBSCRIPTION_LAG_MESSAGES.name,
        "topic" => subscription_metric_topic(engine, pending_delivery.as_ref()),
        "subscription" => engine._subscription_name.clone()
    )
    .set(lag_info.lag_messages as f64);

    let now = Instant::now();

    if pending_delivery
        .as_ref()
        .map(|pending| pending.ack_timed_out(now))
        .unwrap_or(false)
    {
        counter!(DISPATCHER_HEARTBEAT_POLLS_TOTAL.name).increment(1);
        handle_command(
            DispatcherCommand::AckTimedOut,
            state,
            engine,
            replicator,
            pending_delivery,
        )
        .await;
        return;
    }

    let retry_ready = pending_delivery
        .as_ref()
        .map(|pending| pending.is_retry_ready(now))
        .unwrap_or(false);

    if retry_ready || (pending_delivery.is_none() && lag_info.has_lag) {
        trace!(lag_messages = %lag_info.lag_messages, "heartbeat detected lag");
        counter!(DISPATCHER_HEARTBEAT_POLLS_TOTAL.name).increment(1);
        handle_poll_and_dispatch(
            state,
            engine,
            replicator,
            pending_delivery,
        ).await;
    }
}

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
//! - **Automatic Retry**: If dispatch is interrupted, the pending message stays buffered and is retried once an active consumer is available
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
//! 1. **Wake**: External wakeups or the heartbeat trigger a dispatch attempt
//! 2. **Check Pending**: If `pending == true`, skip (message already in-flight)
//! 3. **Get Message**: Either resend buffered message OR poll next from SubscriptionEngine
//! 4. **Buffer**: Store message in `pending_message`
//! 5. **Send**: Attempt to send to active consumer via `send_message()`
//! 6. **Mark Pending**: Set `pending = true` (blocks further dispatch)
//! 7. **Wait for Ack**: Consumer must acknowledge before next message
//! 8. **On Ack**: Clear `pending` flag and `pending_message` buffer, then immediately attempt the next dispatch
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
//! When the active consumer disconnects:
//!
//! 1. `RemoveConsumer` command is received
//! 2. If removed consumer was active, `active_consumer` is cleared
//! 3. `ResetPending` clears the in-flight gate while preserving any buffered message
//! 4. The dispatcher retries the buffered message when an active consumer is available again
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

use metrics::{counter, gauge};
use tokio::sync::{mpsc, watch};
use tokio::time::{Duration, Instant};
use tracing::{trace, warn};

use crate::broker_metrics::{
    DISPATCHER_HEARTBEAT_POLLS_TOTAL, DISPATCHER_NOTIFIER_POLLS_TOTAL,
    SUBSCRIPTION_ACK_TIMEOUT_TOTAL, SUBSCRIPTION_LAG_MESSAGES, SUBSCRIPTION_NACK_TOTAL,
    SUBSCRIPTION_REDELIVERY_TOTAL,
};
use crate::message::{AckMessage, NackMessage};
use crate::subscription::SubscriptionFailurePolicy;

use super::super::commands::DispatcherCommand;
use super::super::exclusive::ExclusiveConsumerState;
use super::super::subscription_engine::SubscriptionEngine;
use super::super::{
    handle_retry_exhausted_pending, record_retry_exhausted_metric, subscription_metric_context,
    subscription_metric_topic, update_pending_delivery_metrics, InternalPublisher, PendingDelivery,
    SubscriptionMetricContext,
};

/// Spawn the reliable exclusive dispatcher background task.
pub(super) fn start(
    engine: SubscriptionEngine,
    failure_policy: SubscriptionFailurePolicy,
    internal_publisher: Option<InternalPublisher>,
    control_rx: mpsc::Receiver<DispatcherCommand>,
    ready_tx: watch::Sender<bool>,
) {
    tokio::spawn(async move {
        run_reliable_loop(engine, failure_policy, internal_publisher, control_rx, ready_tx).await;
    });
}

async fn run_reliable_loop(
    mut engine: SubscriptionEngine,
    failure_policy: SubscriptionFailurePolicy,
    internal_publisher: Option<InternalPublisher>,
    mut control_rx: mpsc::Receiver<DispatcherCommand>,
    ready_tx: watch::Sender<bool>,
) {
    let mut state = ExclusiveConsumerState::new();
    let mut pending_delivery: Option<PendingDelivery> = None;

    // Initialize stream from persisted progress
    {
        if let Err(e) = engine.init_stream_from_progress_or_latest().await {
            warn!(error = %e, "Reliable exclusive dispatcher failed to init stream");
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
                        handle_command(
                            cmd,
                            &mut state,
                            &mut engine,
                            &failure_policy,
                            internal_publisher.as_ref(),
                            &mut pending_delivery,
                        ).await;
                    }
                    None => break, // Channel closed
                }
            }

            _ = heartbeat.tick() => {
                handle_heartbeat(
                    &mut state,
                    &mut engine,
                    &failure_policy,
                    internal_publisher.as_ref(),
                    &mut pending_delivery,
                ).await;
            }
        }
    }
}

async fn handle_command(
    cmd: DispatcherCommand,
    state: &mut ExclusiveConsumerState,
    engine: &mut SubscriptionEngine,
    failure_policy: &SubscriptionFailurePolicy,
    internal_publisher: Option<&InternalPublisher>,
    pending_delivery: &mut Option<PendingDelivery>,
) {
    match cmd {
        DispatcherCommand::AddConsumer(c) => {
            trace!(
                consumer_id = %c.consumer_id,
                "consumer added to reliable exclusive dispatcher"
            );
            state.add_consumer(c);
            handle_poll_and_dispatch(
                state,
                engine,
                failure_policy,
                internal_publisher,
                pending_delivery,
            ).await;
        }
        DispatcherCommand::RemoveConsumer(id) => {
            state.remove_consumer(id);
        }
        DispatcherCommand::DisconnectAllConsumers => {
            if let Err(e) = engine.flush_progress_now().await {
                warn!(error = %e, "DisconnectAllConsumers: flush progress failed");
            }
            state.disconnect_all();
        }
        DispatcherCommand::MessageAcked(ack_msg) => {
            handle_ack(engine, ack_msg, pending_delivery).await;
            handle_poll_and_dispatch(
                state,
                engine,
                failure_policy,
                internal_publisher,
                pending_delivery,
            ).await;
        }
        DispatcherCommand::MessageNacked(nack_msg) => {
            let metric_context = subscription_metric_context(engine, pending_delivery.as_ref());
            handle_nack(&metric_context, nack_msg, failure_policy, pending_delivery);
            handle_poll_and_dispatch(
                state,
                engine,
                failure_policy,
                internal_publisher,
                pending_delivery,
            ).await;
        }
        DispatcherCommand::RetryNow(reason) => {
            let metric_context = subscription_metric_context(engine, pending_delivery.as_ref());
            handle_retry_now(&metric_context, failure_policy, reason, pending_delivery);
            handle_poll_and_dispatch(
                state,
                engine,
                failure_policy,
                internal_publisher,
                pending_delivery,
            ).await;
        }
        DispatcherCommand::AckTimedOut => {
            let metric_context = subscription_metric_context(engine, pending_delivery.as_ref());
            handle_ack_timed_out(&metric_context, failure_policy, pending_delivery);
            handle_poll_and_dispatch(
                state,
                engine,
                failure_policy,
                internal_publisher,
                pending_delivery,
            ).await;
        }
        DispatcherCommand::PollAndDispatch => {
            // Increment notifier poll counter (fast path)
            counter!(DISPATCHER_NOTIFIER_POLLS_TOTAL.name).increment(1);

            handle_poll_and_dispatch(
                state,
                engine,
                failure_policy,
                internal_publisher,
                pending_delivery,
            ).await;
        }
        DispatcherCommand::FlushProgressNow => {
            if let Err(e) = engine.flush_progress_now().await {
                warn!(error = %e, "FlushProgressNow: failed to flush");
            }
        }
    }
}

fn handle_nack(
    metric_context: &SubscriptionMetricContext,
    nack_msg: NackMessage,
    failure_policy: &SubscriptionFailurePolicy,
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
            pending.schedule_retry_with_policy(
                nack_msg.reason,
                nack_msg.delay_ms,
                failure_policy,
            );
        }
        if pending_delivery
            .as_ref()
            .map(|pending| pending.is_retry_exhausted())
            .unwrap_or(false)
        {
            record_retry_exhausted_metric(metric_context, failure_policy);
        }
        update_pending_delivery_metrics(metric_context, failure_policy, pending_delivery);
    } else {
        trace!(
            request_id = %nack_msg.request_id,
            offset = %nacked_offset,
            "ignoring late nack (not the pending message)"
        );
    }
}

fn handle_retry_now(
    metric_context: &SubscriptionMetricContext,
    failure_policy: &SubscriptionFailurePolicy,
    reason: Option<String>,
    pending_delivery: &mut Option<PendingDelivery>,
) {
    if let Some(pending) = pending_delivery.as_mut() {
        pending.schedule_retry_now(reason);
    }
    update_pending_delivery_metrics(metric_context, failure_policy, pending_delivery);
}

fn handle_ack_timed_out(
    metric_context: &SubscriptionMetricContext,
    failure_policy: &SubscriptionFailurePolicy,
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
            pending.schedule_retry_with_policy(
                Some("ack timeout".to_string()),
                None,
                failure_policy,
            );
            if pending.is_retry_exhausted() {
                retry_exhausted = true;
                exhausted_offset = Some(pending.message.msg_id.topic_offset);
                exhausted_attempt = Some(pending.delivery_attempt);
            }
        }
    }

    if retry_exhausted {
        record_retry_exhausted_metric(metric_context, failure_policy);
        warn!(
            offset = %exhausted_offset.unwrap_or_default(),
            delivery_attempt = %exhausted_attempt.unwrap_or_default(),
            max_redelivery_count = %failure_policy.max_redelivery_count,
            "retry limit exhausted after ack timeout; leaving message in terminal state"
        );
    }

    update_pending_delivery_metrics(metric_context, failure_policy, pending_delivery);
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
        trace!(
            request_id = %ack_msg.request_id,
            offset = %acked_offset,
            "ack received for pending message, clearing buffer"
        );
        *pending_delivery = None;
    } else {
        trace!(
            request_id = %ack_msg.request_id,
            offset = %acked_offset,
            "ignoring late ack (not the pending message)"
        );
    }
}

async fn handle_poll_and_dispatch(
    state: &mut ExclusiveConsumerState,
    engine: &mut SubscriptionEngine,
    failure_policy: &SubscriptionFailurePolicy,
    internal_publisher: Option<&InternalPublisher>,
    pending_delivery: &mut Option<PendingDelivery>,
) {
    let metric_context = subscription_metric_context(engine, pending_delivery.as_ref());
    if let Some(cons) = state.active_consumer_mut() {
        if !cons.get_status().await {
            update_pending_delivery_metrics(&metric_context, failure_policy, pending_delivery);
            return;
        }

        // Get message: buffered (resend) or new from stream
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
            failure_policy,
            internal_publisher,
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
            let offset = pending.message.msg_id.topic_offset;
            let is_redelivery = pending.delivery_attempt > 0;

            let msg = pending.message.clone();

            if let Err(e) = cons.send_message(msg).await {
                warn!(
                    offset = %offset,
                    error = %e,
                    "Failed to send message. Will retry on reconnect."
                );
                pending.schedule_retry_now(Some(format!("send failed: {e}")));
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
                    cons.consumer_id,
                    Duration::from_millis(failure_policy.ack_timeout_ms),
                );
            }
        }
        update_pending_delivery_metrics(&metric_context, failure_policy, pending_delivery);
    } else {
        update_pending_delivery_metrics(&metric_context, failure_policy, pending_delivery);
    }
}

async fn handle_heartbeat(
    state: &mut ExclusiveConsumerState,
    engine: &mut SubscriptionEngine,
    failure_policy: &SubscriptionFailurePolicy,
    internal_publisher: Option<&InternalPublisher>,
    pending_delivery: &mut Option<PendingDelivery>,
) {
    if !state.has_active_consumer() {
        return;
    }

    let lag_info = engine.get_lag_info();

    // Report lag gauge
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
            failure_policy,
            internal_publisher,
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
        trace!(
            lag_messages = %lag_info.lag_messages,
            "heartbeat detected lag"
        );
        counter!(DISPATCHER_HEARTBEAT_POLLS_TOTAL.name).increment(1);
        handle_poll_and_dispatch(
            state,
            engine,
            failure_policy,
            internal_publisher,
            pending_delivery,
        ).await;
    }
}

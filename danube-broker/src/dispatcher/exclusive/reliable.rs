//! Reliable exclusive dispatcher
//!
//! # Overview
//!
//! Provides **at-least-once delivery** to a single active consumer with strict ack-gating.
//! Used for Exclusive and Failover subscription types where delivery guarantees are critical.
//!
//! # Key Components
//!
//! - **`SubscriptionEngine`** — Owns the stream cursor, failure policy, and message lifecycle
//!   decisions (NACK handling, ack timeout, poison skip). See `subscription_engine.rs`.
//! - **`PendingDelivery`** — Per-message state machine tracking delivery attempt count,
//!   ack deadline, retry timing, and terminal status. See `pending_delivery.rs`.
//! - **`ExclusiveConsumerState`** — Manages consumer list and tracks which one is active.
//! - **`poison_handler`** — Handles retry-exhausted messages (DLQ routing, Drop, Block).
//!
//! # State
//!
//! The dispatch loop maintains two mutable variables:
//!
//! - `state: ExclusiveConsumerState` — Consumer list + active consumer tracking
//! - `pending_delivery: Option<PendingDelivery>` — The single in-flight message slot.
//!   `Some(...)` means a message is being tracked (may be awaiting ack, waiting to retry,
//!   or retry-exhausted). `None` means the slot is free and a new message can be polled.
//!
//! # Message Flow (`handle_poll_and_dispatch`)
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │ 1. Active consumer available and healthy?                      │
//! │    └─ No → return (wait for consumer)                          │
//! │                                                                │
//! │ 2. Pending slot empty? → Poll next message from engine         │
//! │    └─ Got message → wrap in PendingDelivery, put in slot       │
//! │                                                                │
//! │ 3. Poison gate: is pending message retry-exhausted?            │
//! │    └─ Yes → handle_retry_exhausted_pending (see poison_handler)│
//! │       ├─ DeadLetter: publish to DLQ via replicator, skip       │
//! │       ├─ Drop: skip message, advance cursor                    │
//! │       └─ Block: halt dispatch until operator intervenes        │
//! │                                                                │
//! │ 4. Retry timing: is pending ready to send?                     │
//! │    └─ No → return (backoff delay not elapsed)                  │
//! │                                                                │
//! │ 5. Send message to active consumer                             │
//! │    ├─ Success → mark AwaitingAck, start ack deadline timer     │
//! │    └─ Failure → schedule_retry_now (will retry on next tick)   │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Event Handlers
//!
//! All events flow through `handle_command`, which dispatches to:
//!
//! - **`MessageAcked`** → `handle_ack`: If ack matches the pending message and it is
//!   awaiting ack, advance the engine cursor (`on_acked`) and clear the pending slot.
//!   Then immediately attempt the next dispatch.
//!
//! - **`MessageNacked`** → `handle_nack`: Consumer explicitly rejected the message.
//!   Engine applies failure policy via `on_nacked` — either schedules a retry with
//!   backoff delay, or marks the message as retry-exhausted. Then triggers dispatch
//!   (which will hit the poison gate if exhausted).
//!
//! - **`AckTimedOut`** → `handle_ack_timed_out`: Ack deadline expired without consumer
//!   response. Engine applies failure policy via `on_ack_timed_out` — same retry/exhaust
//!   logic as NACK. Detected by the heartbeat watchdog.
//!
//! - **`RetryNow`** → `handle_retry_now`: Force an immediate retry (e.g., after consumer
//!   reconnect). Clears backoff delay but does not revive retry-exhausted messages.
//!
//! - **`PollAndDispatch`** → Direct dispatch attempt (triggered by topic publish
//!   or reconnect via `Dispatcher::wake_dispatch()`).
//!
//! - **`AddConsumer` / `RemoveConsumer` / `DisconnectAllConsumers`** → Consumer
//!   lifecycle management. AddConsumer triggers an immediate dispatch attempt.
//!   DisconnectAllConsumers flushes progress before clearing state.
//!
//! - **`FlushProgressNow`** → Force-flush the subscription cursor to metadata.
//!
//! # Heartbeat Watchdog
//!
//! A 500ms interval background timer that:
//!
//! 1. Reports subscription lag gauge metrics
//! 2. Detects ack timeouts (sends `AckTimedOut` command)
//! 3. Detects pending messages ready to retry (backoff elapsed)
//! 4. Detects new messages in WAL when no message is pending (lag catch-up)
//!
//! # Consumer Failover
//!
//! When the active consumer disconnects:
//!
//! 1. `RemoveConsumer` removes it from the consumer list and clears `active_consumer`
//! 2. The pending message stays in its slot (preserving delivery state)
//! 3. When a new consumer is added, `AddConsumer` triggers dispatch which
//!    retries the buffered message to the new active consumer
//!
//! # Persistence
//!
//! Subscription progress (last acked offset) is persisted via `SubscriptionEngine`:
//! - Debounced flush every 5s (configurable) to avoid metadata write on every ack
//! - Force-flushed on `DisconnectAllConsumers` and `FlushProgressNow`
//! - Allows broker restart without message loss

use metrics::{counter, gauge};
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
use super::super::exclusive::ExclusiveConsumerState;
use super::super::metrics::{
    record_retry_exhausted_metric, subscription_metric_context, subscription_metric_topic,
    update_pending_delivery_metrics, SubscriptionMetricContext,
};
use super::super::pending_delivery::PendingDelivery;
use super::super::poison_handler::handle_retry_exhausted_pending;
use super::super::subscription_engine::SubscriptionEngine;

/// Spawn the reliable exclusive dispatcher background task.
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
                            replicator.as_deref(),
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
                    replicator.as_deref(),
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
    replicator: Option<&Replicator>,
    pending_delivery: &mut Option<PendingDelivery>,
) {
    match cmd {
        DispatcherCommand::AddConsumer(c) => {
            trace!(
                consumer_id = %c.consumer_id,
                "consumer added to reliable exclusive dispatcher"
            );
            state.add_consumer(c);
            handle_poll_and_dispatch(state, engine, replicator, pending_delivery).await;
        }
        DispatcherCommand::AddConsumerKeyShared(c, _filters) => {
            // KeyShared consumers should not reach exclusive dispatcher;
            // treat as regular consumer as fallback.
            trace!(consumer_id = %c.consumer_id, "ignoring key filters in exclusive dispatcher");
            state.add_consumer(c);
            handle_poll_and_dispatch(state, engine, replicator, pending_delivery).await;
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
            handle_poll_and_dispatch(state, engine, replicator, pending_delivery).await;
        }
        DispatcherCommand::MessageNacked(nack_msg) => {
            let metric_context = subscription_metric_context(engine, pending_delivery.as_ref());
            handle_nack(engine, &metric_context, nack_msg, pending_delivery);
            handle_poll_and_dispatch(state, engine, replicator, pending_delivery).await;
        }
        DispatcherCommand::RetryNow(reason) => {
            let metric_context = subscription_metric_context(engine, pending_delivery.as_ref());
            handle_retry_now(engine, &metric_context, reason, pending_delivery);
            handle_poll_and_dispatch(state, engine, replicator, pending_delivery).await;
        }
        DispatcherCommand::AckTimedOut => {
            let metric_context = subscription_metric_context(engine, pending_delivery.as_ref());
            handle_ack_timed_out(engine, &metric_context, pending_delivery);
            handle_poll_and_dispatch(state, engine, replicator, pending_delivery).await;
        }
        DispatcherCommand::PollAndDispatch => {
            // Increment notifier poll counter (fast path)
            counter!(DISPATCHER_NOTIFIER_POLLS_TOTAL.name).increment(1);

            handle_poll_and_dispatch(state, engine, replicator, pending_delivery).await;
        }
        DispatcherCommand::FlushProgressNow => {
            if let Err(e) = engine.flush_progress_now().await {
                warn!(error = %e, "FlushProgressNow: failed to flush");
            }
        }
    }
}

/// Handle a consumer NACK: apply failure policy to decide retry vs exhaust.
///
/// If the nacked offset matches the pending message, the engine's failure policy
/// determines the outcome: schedule a retry (with backoff) or mark retry-exhausted.
/// Late NACKs (not matching pending) are ignored.
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

/// Force an immediate retry of the pending message (e.g., after consumer reconnect).
///
/// Clears any backoff delay so the message is eligible for dispatch on the next tick.
/// Does NOT revive messages that have already reached RetryExhausted status.
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

/// Handle ack deadline expiry: apply failure policy to decide retry vs exhaust.
///
/// Called by the heartbeat watchdog when `pending.ack_timed_out(now)` is true.
/// The engine applies the same retry/exhaust logic as NACK via `on_ack_timed_out`.
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

/// Handle a consumer ACK: advance cursor and free the pending slot.
///
/// Only processes the ack if it matches the pending message's offset AND the
/// message is currently awaiting ack. Late/duplicate acks are ignored.
/// On success, clears the pending slot so the next dispatch can poll a new message.
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

/// Core dispatch logic: poll a message from the stream and send it to the active consumer.
///
/// This is the main work function, called after every event that might unblock dispatch.
/// See the module-level "Message Flow" diagram for the full sequence of gates.
async fn handle_poll_and_dispatch(
    state: &mut ExclusiveConsumerState,
    engine: &mut SubscriptionEngine,
    replicator: Option<&Replicator>,
    pending_delivery: &mut Option<PendingDelivery>,
) {
    let failure_policy = engine.failure_policy();
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

        match handle_retry_exhausted_pending(engine, replicator, pending_delivery).await {
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

/// Heartbeat watchdog: periodic timer that detects ack timeouts, retry readiness, and lag.
///
/// Runs every 500ms. Checks (in priority order):
/// 1. Ack timeout → sends `AckTimedOut` command
/// 2. Pending message ready to retry → triggers dispatch
/// 3. No pending message + WAL has unread data → triggers dispatch (lag catch-up)
async fn handle_heartbeat(
    state: &mut ExclusiveConsumerState,
    engine: &mut SubscriptionEngine,
    replicator: Option<&Replicator>,
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
        trace!(
            lag_messages = %lag_info.lag_messages,
            "heartbeat detected lag"
        );
        counter!(DISPATCHER_HEARTBEAT_POLLS_TOTAL.name).increment(1);
        handle_poll_and_dispatch(state, engine, replicator, pending_delivery).await;
    }
}

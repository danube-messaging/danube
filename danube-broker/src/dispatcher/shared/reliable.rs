//! Reliable shared dispatcher
//!
//! # Overview
//!
//! Provides **at-least-once delivery** with **round-robin load balancing** across multiple consumers.
//! Used for Shared subscription type where both delivery guarantees and load distribution are needed.
//!
//! # Key Components
//!
//! - **`SubscriptionEngine`** — Owns the stream cursor, failure policy, and message lifecycle
//!   decisions (NACK handling, ack timeout, poison skip). See `subscription_engine.rs`.
//! - **`DispatchWindow`** — Pipelined in-flight message window with contiguous cursor tracking.
//!   Replaces the old single-slot `Option<PendingDelivery>`. See `dispatch_window.rs`.
//! - **`SharedConsumerState`** — Manages consumer list with atomic round-robin index.
//! - **`poison_handler`** — Handles retry-exhausted messages (DLQ routing, Drop, Block).
//!
//! # State
//!
//! The dispatch loop maintains two mutable variables:
//!
//! - `state: SharedConsumerState` — Consumer list + atomic round-robin counter
//! - `window: DispatchWindow` — Pipelined in-flight message window. Tracks multiple messages
//!   simultaneously, handles out-of-order ACKs, and advances a safe cursor for persistence.
//!
//! # Message Flow (`handle_poll_and_dispatch`)
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │ 1. Any consumers connected?                                    │
//! │    └─ No → return (wait for consumers)                         │
//! │                                                                │
//! │ 2. Handle any retry-exhausted entries (poison gate per entry)  │
//! │    ├─ DeadLetter: publish to DLQ via replicator, skip          │
//! │    ├─ Drop: skip message, advance cursor                       │
//! │    └─ Block: halt dispatch for that entry                      │
//! │                                                                │
//! │ 3. Handle any entries ready for retry (resend via round-robin) │
//! │                                                                │
//! │ 4. While window.has_capacity():                                │
//! │    a. Poll next message from engine                            │
//! │    b. If None → break (no more messages)                       │
//! │    c. Send message via round-robin consumer selection           │
//! │    d. Mark dispatched in window                                │
//! │                                                                │
//! │ 5. Persist safe_cursor to SubscriptionEngine                   │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Event Handlers
//!
//! All events flow through `handle_command`, which dispatches to:
//!
//! - **`MessageAcked`** → `handle_ack`: window.on_ack(offset) → if safe_cursor advanced
//!   → engine.advance_cursor_to(). Then try to fill the window.
//!
//! - **`MessageNacked`** → `handle_nack`: window.get_mut(offset) → engine.on_nacked(entry).
//!
//! - **`RetryNow`** → Force immediate retry of retry-eligible entries.
//!
//! - **`PollAndDispatch`** → Direct dispatch attempt (triggered by topic publish).
//!
//! - **`AddConsumer` / `RemoveConsumer` / `DisconnectAllConsumers`** → Consumer
//!   lifecycle. AddConsumer triggers immediate dispatch. DisconnectAll flushes progress.
//!
//! - **`FlushProgressNow`** → Force-flush subscription cursor to metadata.
//!
//! # Heartbeat Watchdog
//!
//! A 500ms interval background timer that:
//!
//! 1. Reports subscription lag gauge metrics
//! 2. Detects ack timeouts on all in-flight entries
//! 3. Detects entries ready to retry (backoff elapsed)
//! 4. Fills window when capacity available and WAL has lag
//!
//! # Consumer Failover
//!
//! When a consumer disconnects while messages are in-flight:
//!
//! 1. `RemoveConsumer` removes it from the consumer list
//! 2. In-flight messages stay in the window (preserving delivery state)
//! 3. On next dispatch attempt, round-robin selects a different healthy consumer
//!
//! # Persistence
//!
//! Subscription progress (last acked offset) is persisted via `SubscriptionEngine`:
//! - Uses `advance_cursor_to(safe_cursor)` — only advances past contiguously acked offsets
//! - Debounced flush every 5s (configurable) — all consumers share the same cursor
//! - Force-flushed on `DisconnectAllConsumers` and `FlushProgressNow`
//! - Allows broker restart without message loss

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
use crate::subscription::SubscriptionPoisonPolicy;

use super::super::commands::DispatcherCommand;
use super::super::dispatch_window::DispatchWindow;
use super::super::metrics::{
    record_retry_exhausted_metric, subscription_metric_context, subscription_metric_topic,
    update_window_metrics, SubscriptionMetricContext,
};
use super::super::pending_delivery::PendingDelivery;
use super::super::poison_handler::{resolve_poisoned_delivery, PoisonResolution};
use super::super::shared::SharedConsumerState;
use super::super::subscription_engine::SubscriptionEngine;

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
    let max_unacked = engine.failure_policy().max_unacked_messages;
    let mut window = DispatchWindow::new(max_unacked);

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
                            &mut window,
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
                    &mut window,
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
    window: &mut DispatchWindow,
) {
    match cmd {
        DispatcherCommand::AddConsumer(c) => {
            trace!(consumer_id = %c.consumer_id, "consumer added");
            state.add_consumer(c);
            // NOTE: Do NOT call handle_poll_and_dispatch here.
            // poll_next() blocks on the WAL stream when no messages exist,
            // which prevents subsequent AddConsumer commands from being processed.
            // Dispatch will be triggered by PollAndDispatch when messages arrive.
        }
        DispatcherCommand::AddConsumerKeyShared(c, _filters) => {
            // KeyShared consumers should not reach shared dispatcher;
            // treat as regular consumer as fallback.
            trace!(consumer_id = %c.consumer_id, "ignoring key filters in shared dispatcher");
            state.add_consumer(c);
        }
        DispatcherCommand::RemoveConsumer(id) => {
            state.remove_consumer(id);
        }
        DispatcherCommand::DisconnectAllConsumers => {
            // Log in-flight window gap for observability.
            // On topic seal/move, in-flight messages that haven't been acked yet
            // will NOT be reflected in safe_cursor. The new broker will re-dispatch
            // them from safe_cursor, which is correct for at-least-once semantics.
            //
            // TODO: In the future, consider adding a short drain timeout (e.g. 2s)
            // to wait for pending acks before flushing. This would reduce unnecessary
            // redeliveries when the consumer is healthy but slow to ack.
            if !window.is_empty() {
                warn!(
                    in_flight = %window.in_flight_count(),
                    safe_cursor = ?window.safe_cursor(),
                    "disconnecting with {} in-flight messages — new broker will re-dispatch from safe_cursor",
                    window.in_flight_count()
                );
            }
            if let Err(e) = engine.flush_progress_now().await {
                warn!(error = %e, "DisconnectAllConsumers: flush failed");
            }
            state.disconnect_all();
        }
        DispatcherCommand::MessageAcked(ack_msg) => {
            handle_ack(engine, ack_msg, window).await;
            handle_poll_and_dispatch(state, engine, replicator, window).await;
        }
        DispatcherCommand::MessageNacked(nack_msg) => {
            let metric_context = subscription_metric_context(engine, None);
            handle_nack(engine, &metric_context, nack_msg, window);
            handle_poll_and_dispatch(state, engine, replicator, window).await;
        }
        DispatcherCommand::RetryNow { reason, consumer_id } => {
            handle_retry_now(engine, reason, consumer_id, window);
            handle_poll_and_dispatch(state, engine, replicator, window).await;
        }
        DispatcherCommand::PollAndDispatch => {
            counter!(DISPATCHER_NOTIFIER_POLLS_TOTAL.name).increment(1);
            // Only poll if there's actually unread data in the WAL.
            // poll_next() blocks on stream.next() when caught up, which would
            // stall the entire dispatcher loop (heartbeat + command processing).
            let lag = engine.get_lag_info();
            if lag.has_lag {
                handle_poll_and_dispatch(state, engine, replicator, window).await;
            }
        }
        DispatcherCommand::FlushProgressNow => {
            if let Err(e) = engine.flush_progress_now().await {
                warn!(error = %e, "FlushProgressNow: failed");
            }
        }
    }
}

/// Handle a consumer NACK: apply failure policy to decide retry vs exhaust.
fn handle_nack(
    engine: &SubscriptionEngine,
    metric_context: &SubscriptionMetricContext,
    nack_msg: NackMessage,
    window: &mut DispatchWindow,
) {
    let nacked_offset = nack_msg.msg_id.topic_offset;

    if let Some(pending) = window.get_mut(nacked_offset) {
        trace!(
            request_id = %nack_msg.request_id,
            offset = %nacked_offset,
            delay_ms = ?nack_msg.delay_ms,
            reason = ?nack_msg.reason,
            "nack received for in-flight message"
        );
        counter!(
            SUBSCRIPTION_NACK_TOTAL.name,
            "topic" => metric_context.topic.clone(),
            "subscription" => metric_context.subscription.clone(),
        )
        .increment(1);
        engine.on_nacked(pending, nack_msg.reason, nack_msg.delay_ms);
        if pending.is_retry_exhausted() {
            record_retry_exhausted_metric(metric_context, engine.failure_policy());
        }
        update_window_metrics(metric_context, engine.failure_policy(), window);
    } else {
        trace!(
            request_id = %nack_msg.request_id,
            offset = %nacked_offset,
            "ignoring late nack (not in-flight)"
        );
    }
}

/// Force an immediate retry of retryable entries.
///
/// When `consumer_id` is provided, only entries assigned to that consumer are
/// retried (shared-subscription failover). When `None`, all entries are retried.
fn handle_retry_now(
    _engine: &SubscriptionEngine,
    reason: Option<String>,
    consumer_id: Option<u64>,
    window: &mut DispatchWindow,
) {
    match consumer_id {
        Some(cid) => window.force_retry_for_consumer(cid, reason),
        None => window.force_retry_all(reason),
    }
}

/// Check all in-flight entries for ack timeouts and apply failure policy.
fn handle_ack_timeouts(
    engine: &SubscriptionEngine,
    metric_context: &SubscriptionMetricContext,
    window: &mut DispatchWindow,
) {
    let now = Instant::now();
    let timed_out_offsets = window.collect_ack_timed_out(now);

    for offset in timed_out_offsets {
        if let Some(pending) = window.get_mut(offset) {
            if pending.is_awaiting_ack() {
                trace!(
                    offset = %offset,
                    delivery_attempt = %pending.delivery_attempt,
                    "ack timeout detected for in-flight message"
                );
                counter!(
                    SUBSCRIPTION_ACK_TIMEOUT_TOTAL.name,
                    "topic" => metric_context.topic.clone(),
                    "subscription" => metric_context.subscription.clone(),
                )
                .increment(1);
                engine.on_ack_timed_out(pending);
                if pending.is_retry_exhausted() {
                    record_retry_exhausted_metric(metric_context, engine.failure_policy());
                    warn!(
                        offset = %offset,
                        delivery_attempt = %pending.delivery_attempt,
                        max_redelivery_count = %engine.failure_policy().max_redelivery_count,
                        "retry limit exhausted after ack timeout"
                    );
                }
            }
        }
    }

    update_window_metrics(metric_context, engine.failure_policy(), window);
}

/// Handle a consumer ACK: advance cursor and free the window slot.
async fn handle_ack(
    engine: &mut SubscriptionEngine,
    ack_msg: AckMessage,
    window: &mut DispatchWindow,
) {
    let acked_offset = ack_msg.msg_id.topic_offset;

    let is_awaiting = window
        .get_mut(acked_offset)
        .map(|p| p.is_awaiting_ack())
        .unwrap_or(false);

    if is_awaiting {
        if let Some(new_cursor) = window.on_ack(acked_offset) {
            if let Err(e) = engine.advance_cursor_to(new_cursor).await {
                warn!(offset = %new_cursor, error = %e, "advance_cursor_to failed");
            }
        }
        trace!(offset = %acked_offset, "ack received, removed from window");
    } else {
        trace!(offset = %acked_offset, "ignoring late ack");
    }
}

/// Handle in-flight entries that have exhausted their retry budget.
async fn handle_retry_exhausted_entries(
    engine: &mut SubscriptionEngine,
    replicator: Option<&Replicator>,
    window: &mut DispatchWindow,
) {
    let exhausted_offsets = window.collect_retry_exhausted();
    if exhausted_offsets.is_empty() {
        return;
    }

    let failure_policy = engine.failure_policy().clone();
    let subscription_name = engine._subscription_name.clone();

    for offset in exhausted_offsets {
        let resolution = {
            let Some(pending) = window.get_mut(offset) else {
                continue;
            };
            match resolve_poisoned_delivery(
                &failure_policy,
                &subscription_name,
                replicator,
                pending,
            )
            .await
            {
                Ok(res) => res,
                Err(e) => {
                    warn!(offset = %offset, error = %e, "poison resolution failed");
                    continue;
                }
            }
        };

        match resolution {
            PoisonResolution::Resolved => {
                if let Some(safe) = window.on_skipped(offset) {
                    if let Err(e) = engine.advance_cursor_to(safe).await {
                        warn!(error = %e, "advance_cursor_to failed on poison resolution");
                    }
                }
            }
            PoisonResolution::Blocked => {
                trace!(
                    offset = %offset,
                    "retry-exhausted message blocked (poison policy: Block)"
                );
            }
        }
    }
}

/// Core dispatch logic: poll messages and send to consumers via round-robin while window has capacity.
async fn handle_poll_and_dispatch(
    state: &mut SharedConsumerState,
    engine: &mut SubscriptionEngine,
    replicator: Option<&Replicator>,
    window: &mut DispatchWindow,
) {
    let failure_policy = engine.failure_policy().clone();
    let metric_context = subscription_metric_context(engine, None);
    if state.is_empty() {
        update_window_metrics(&metric_context, &failure_policy, window);
        return;
    }

    // Phase 1: Handle retry-exhausted entries
    handle_retry_exhausted_entries(engine, replicator, window).await;

    // Phase 2: Handle entries ready for retry (resend via round-robin)
    let now = Instant::now();
    let retry_offsets = window.collect_retry_ready(now);
    for offset in retry_offsets {
        if let Some(pending) = window.get_mut(offset) {
            let is_redelivery = pending.delivery_attempt > 0;
            let msg = pending.message.clone();
            let num_consumers = state.len();

            let mut sent = false;
            let mut attempts = 0;
            while attempts < num_consumers {
                let idx = state.rr_index.fetch_add(1, Ordering::Relaxed) % num_consumers;
                if let Some(target) = state.get_consumer_mut(idx) {
                    if !target.get_status().await {
                        attempts += 1;
                        continue;
                    }
                    match target.send_message(msg.clone()).await {
                        Ok(()) => {
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
                            sent = true;
                            break;
                        }
                        Err(e) => {
                            warn!(
                                offset = %offset,
                                consumer_id = %target.consumer_id,
                                error = %e,
                                "Failed to resend message"
                            );
                            attempts += 1;
                        }
                    }
                } else {
                    attempts += 1;
                }
            }

            if !sent {
                if let Some(pending) = window.get_mut(offset) {
                    pending.schedule_retry_now(Some("no active shared consumer available".to_string()));
                }
            }
        }
    }

    // Phase 2b: Block-policy guard — if any entry is stuck in Block state,
    // do NOT dispatch new messages past the blocked offset.
    let has_blocked_entry = window.find_retry_exhausted().is_some()
        && failure_policy.poison_policy == SubscriptionPoisonPolicy::Block;

    // Phase 3: Fill window with new messages while capacity available.
    // Only attempt to poll if there's unread data in the WAL;
    // poll_next() blocks when caught up, which would stall the event loop.
    //
    // Skip entirely when a Block-policy entry is stuck.
    if has_blocked_entry {
        update_window_metrics(&metric_context, &failure_policy, window);
        return;
    }
    while window.has_capacity()
        && engine.has_unpolled_messages(window.highest_dispatched_offset())
    {
        let next_message = match engine.poll_next().await {
            Ok(msg_opt) => msg_opt,
            Err(e) => {
                warn!(error = %e, "poll_next error");
                break;
            }
        };

        let msg = match next_message {
            Some(msg) => msg,
            None => break, // caught up with WAL
        };

        let offset = msg.msg_id.topic_offset;
        let num_consumers = state.len();

        let mut sent = false;
        let mut attempts = 0;
        let mut target_consumer_id = 0u64;
        while attempts < num_consumers {
            let idx = state.rr_index.fetch_add(1, Ordering::Relaxed) % num_consumers;
            if let Some(target) = state.get_consumer_mut(idx) {
                if !target.get_status().await {
                    attempts += 1;
                    continue;
                }
                match target.send_message(msg.clone()).await {
                    Ok(()) => {
                        target_consumer_id = target.consumer_id;
                        sent = true;
                        break;
                    }
                    Err(e) => {
                        warn!(
                            offset = %offset,
                            consumer_id = %target.consumer_id,
                            error = %e,
                            "Failed to send message to consumer"
                        );
                        attempts += 1;
                    }
                }
            } else {
                attempts += 1;
            }
        }

        let mut delivery = PendingDelivery::new(msg);
        if sent {
            delivery.on_send_attempt(
                target_consumer_id,
                Duration::from_millis(failure_policy.ack_timeout_ms),
            );
            window.mark_dispatched(offset, delivery);
        } else {
            delivery.schedule_retry_now(Some("no active shared consumer available".to_string()));
            window.mark_dispatched(offset, delivery);
            break; // All consumers unhealthy, stop filling
        }
    }

    update_window_metrics(&metric_context, &failure_policy, window);
}

/// Heartbeat watchdog: periodic timer that detects ack timeouts, retry readiness, and lag.
async fn handle_heartbeat(
    state: &mut SharedConsumerState,
    engine: &mut SubscriptionEngine,
    replicator: Option<&Replicator>,
    window: &mut DispatchWindow,
) {
    if state.is_empty() {
        return;
    }

    let lag_info = engine.get_lag_info();

    gauge!(
        SUBSCRIPTION_LAG_MESSAGES.name,
        "topic" => subscription_metric_topic(engine, None),
        "subscription" => engine._subscription_name.clone()
    )
    .set(lag_info.lag_messages as f64);

    let now = Instant::now();

    // Phase 1: Check for ack timeouts
    if window.find_ack_timed_out(now).is_some() {
        counter!(DISPATCHER_HEARTBEAT_POLLS_TOTAL.name).increment(1);
        let metric_context = subscription_metric_context(engine, None);
        handle_ack_timeouts(engine, &metric_context, window);
        handle_poll_and_dispatch(state, engine, replicator, window).await;
        return;
    }

    // Phase 2: Check for retry-ready entries or lag
    let retry_ready = window.has_any_retry_ready(now);

    if retry_ready || (window.has_capacity() && lag_info.has_lag) {
        trace!(lag_messages = %lag_info.lag_messages, "heartbeat detected lag or retry-ready");
        counter!(DISPATCHER_HEARTBEAT_POLLS_TOTAL.name).increment(1);
        handle_poll_and_dispatch(state, engine, replicator, window).await;
    }
}

//! Reliable Key-Shared dispatcher
//!
//! # Overview
//!
//! Provides **at-least-once delivery** with **per-key ordering** across multiple consumers.
//! Used for KeyShared subscription type where messages with the same routing key must always
//! be delivered to the same consumer, in order, while different keys can be dispatched in parallel.
//!
//! # Key Components
//!
//! - **`SubscriptionEngine`** — Owns the stream cursor, failure policy, and message lifecycle.
//! - **`InFlightWindow`** — Tracks multiple in-flight messages with per-key blocking and
//!   contiguous cursor advancement (replaces the single-slot `Option<PendingDelivery>`).
//! - **`KeySharedConsumerState`** — Manages consumers with key filters and FNV-1a hash routing.
//! - **`poison_handler`** — Handles retry-exhausted messages (DLQ routing, Drop, Block).
//!
//! # Critical Difference from Shared Dispatcher
//!
//! The shared dispatcher has a single in-flight slot (`Option<PendingDelivery>`).
//! The Key-Shared dispatcher has a **multi-message window** (`InFlightWindow`) that:
//! - Allows one message per routing key to be in-flight simultaneously
//! - Blocks additional messages for the same key until the in-flight one is acked
//! - Advances the cursor only past contiguously-acked offsets (safe for broker restart)

use std::sync::Arc;

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
use crate::replicator::Replicator;

use super::super::commands::DispatcherCommand;
use super::super::metrics::{
    subscription_metric_topic,
};
use super::super::pending_delivery::PendingDelivery;
use super::super::subscription_engine::SubscriptionEngine;
use super::consumer_state::KeySharedConsumerState;
use super::in_flight_window::InFlightWindow;

/// Default maximum number of in-flight + blocked messages.
const DEFAULT_MAX_WINDOW_SIZE: usize = 50_000;

/// Spawn the reliable Key-Shared dispatcher background task.
pub(crate) fn start(
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
    let mut state = KeySharedConsumerState::new();
    let mut window = InFlightWindow::new(DEFAULT_MAX_WINDOW_SIZE);

    // Initialize stream
    {
        if let Err(e) = engine.init_stream_from_progress_or_latest().await {
            warn!(error = %e, "Reliable Key-Shared dispatcher failed to init stream");
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
    state: &mut KeySharedConsumerState,
    engine: &mut SubscriptionEngine,
    _replicator: Option<&Replicator>,
    window: &mut InFlightWindow,
) {
    match cmd {
        DispatcherCommand::AddConsumerKeyShared(c, filters) => {
            trace!(consumer_id = %c.consumer_id, filters = ?filters, total = state.consumers.len() + 1, "key-shared consumer added");
            state.add_consumer(c, filters);
            // NOTE: Do NOT call handle_poll_and_dispatch here.
            // poll_next() blocks on the WAL stream when no messages exist,
            // which prevents subsequent AddConsumer commands from being processed.
            // Dispatch will be triggered by PollAndDispatch when messages arrive.
        }
        DispatcherCommand::AddConsumer(c) => {
            // Fallback: add without filters (accepts all keys)
            trace!(consumer_id = %c.consumer_id, total = state.consumers.len() + 1, "key-shared consumer added (no filters)");
            state.add_consumer(c, Vec::new());
        }
        DispatcherCommand::RemoveConsumer(id) => {
            // Free any in-flight keys held by this consumer
            let freed_keys = window.remove_consumer_entries(id);
            state.remove_consumer(id);
            if !freed_keys.is_empty() {
                trace!(consumer_id = %id, freed_keys = %freed_keys.len(), "freed keys from removed consumer");
                // Try to dispatch unblocked messages
                dispatch_unblocked(state, engine, window).await;
            }
        }
        DispatcherCommand::DisconnectAllConsumers => {
            if let Err(e) = engine.flush_progress_now().await {
                warn!(error = %e, "DisconnectAllConsumers: flush failed");
            }
            state.disconnect_all();
        }
        DispatcherCommand::MessageAcked(ack_msg) => {
            handle_ack(state, engine, ack_msg, window).await;
            // NOTE: do NOT call handle_poll_and_dispatch here.
            // handle_ack already calls dispatch_unblocked for freed keys.
            // Polling new messages is driven by PollAndDispatch events.
        }
        DispatcherCommand::MessageNacked(nack_msg) => {
            handle_nack(engine, nack_msg, window);
            // Retry of nack'd messages handled by heartbeat watchdog
        }
        DispatcherCommand::RetryNow(_reason) => {
            dispatch_unblocked(state, engine, window).await;
        }
        DispatcherCommand::AckTimedOut => {
            handle_ack_timeouts(engine, window);
            dispatch_unblocked(state, engine, window).await;
        }
        DispatcherCommand::PollAndDispatch => {
            counter!(DISPATCHER_NOTIFIER_POLLS_TOTAL.name).increment(1);
            handle_poll_and_dispatch(state, engine, window).await;
        }
        DispatcherCommand::FlushProgressNow => {
            if let Err(e) = engine.flush_progress_now().await {
                warn!(error = %e, "FlushProgressNow: failed");
            }
        }
    }
}

/// Handle a consumer ACK: advance contiguous cursor and free the key lock.
async fn handle_ack(
    state: &mut KeySharedConsumerState,
    engine: &mut SubscriptionEngine,
    ack_msg: AckMessage,
    window: &mut InFlightWindow,
) {
    let acked_offset = ack_msg.msg_id.topic_offset;

    // Check if this offset is in our in-flight window
    let is_awaiting = window
        .in_flight
        .get(&acked_offset)
        .map(|e| e.delivery.is_awaiting_ack())
        .unwrap_or(false);

    if !is_awaiting {
        trace!(offset = %acked_offset, "ignoring late ack (not in-flight or not awaiting)");
        return;
    }

    let (unblocked_key, new_cursor) = window.on_ack(acked_offset);

    // Advance engine cursor to safe contiguous position
    if let Some(safe_cursor) = new_cursor {
        if let Err(e) = engine.advance_cursor_to(safe_cursor).await {
            warn!(offset = %safe_cursor, error = %e, "advance_cursor_to failed");
        }
    }

    trace!(offset = %acked_offset, unblocked_key = ?unblocked_key, "ack processed");

    // If a key was unblocked, try to dispatch blocked messages for that key
    if unblocked_key.is_some() {
        dispatch_unblocked(state, engine, window).await;
    }
}

/// Handle a consumer NACK: apply failure policy to decide retry vs exhaust.
fn handle_nack(
    engine: &SubscriptionEngine,
    nack_msg: NackMessage,
    window: &mut InFlightWindow,
) {
    let nacked_offset = nack_msg.msg_id.topic_offset;

    if let Some(entry) = window.in_flight.get_mut(&nacked_offset) {
        trace!(
            offset = %nacked_offset,
            delay_ms = ?nack_msg.delay_ms,
            reason = ?nack_msg.reason,
            "nack received for in-flight message"
        );
        counter!(SUBSCRIPTION_NACK_TOTAL.name).increment(1);
        engine.on_nacked(&mut entry.delivery, nack_msg.reason, nack_msg.delay_ms);
    } else {
        trace!(offset = %nacked_offset, "ignoring late nack (not in-flight)");
    }
}

/// Check all in-flight entries for ack timeouts.
fn handle_ack_timeouts(
    engine: &SubscriptionEngine,
    window: &mut InFlightWindow,
) {
    let now = Instant::now();
    let offsets: Vec<u64> = window
        .in_flight
        .iter()
        .filter(|(_, e)| e.delivery.ack_timed_out(now))
        .map(|(off, _)| *off)
        .collect();

    for offset in offsets {
        if let Some(entry) = window.in_flight.get_mut(&offset) {
            if entry.delivery.is_awaiting_ack() {
                trace!(
                    offset = %offset,
                    delivery_attempt = %entry.delivery.delivery_attempt,
                    "ack timeout detected for in-flight Key-Shared message"
                );
                counter!(SUBSCRIPTION_ACK_TIMEOUT_TOTAL.name).increment(1);
                engine.on_ack_timed_out(&mut entry.delivery);
            }
        }
    }
}

/// Try to dispatch blocked messages whose keys are now free.
async fn dispatch_unblocked(
    state: &mut KeySharedConsumerState,
    engine: &mut SubscriptionEngine,
    window: &mut InFlightWindow,
) {
    let failure_policy = engine.failure_policy();
    let ack_timeout = Duration::from_millis(failure_policy.ack_timeout_ms);

    while let Some(msg) = window.take_unblocked() {
        let offset = msg.msg_id.topic_offset;
        let key = msg.effective_routing_key().to_string();

        // Find which consumer should handle this key
        let consumer_idx = match state.select_consumer(&key) {
            Some(idx) => idx,
            None => {
                // No consumer wants this key anymore — skip
                if let Some(safe) = window.on_skipped(offset) {
                    if let Err(e) = engine.advance_cursor_to(safe).await {
                        warn!(error = %e, "advance_cursor_to failed on skip");
                    }
                }
                continue;
            }
        };

        let consumer = state.get_consumer_mut(consumer_idx);
        if !consumer.get_status().await {
            // Consumer unhealthy — put back in blocked queue
            window.push_blocked(msg);
            break;
        }

        let mut delivery = PendingDelivery::new(msg.clone());
        let consumer_id = consumer.consumer_id;

        match consumer.send_message(msg).await {
            Ok(()) => {
                delivery.on_send_attempt(consumer_id, ack_timeout);
                window.mark_dispatched(offset, key, consumer_id, delivery);
            }
            Err(e) => {
                warn!(
                    offset = %offset,
                    consumer_id = %consumer_id,
                    error = %e,
                    "Failed to send unblocked message"
                );
                // Put back for retry on next heartbeat
                window.push_blocked(delivery.message);
                break;
            }
        }
    }
}

/// Core dispatch logic: poll messages from WAL and dispatch to consumers via key hash.
///
/// Unlike the original while-loop approach, this polls ONE message per call.
/// The WAL stream is a live BroadcastStream that blocks when no data is available;
/// a while loop over poll_next would stall the entire event loop and prevent
/// processing of ack/nack commands. Each PollAndDispatch wake (one per published
/// message) triggers exactly one poll, matching the shared/exclusive reliable
/// dispatcher pattern.
async fn handle_poll_and_dispatch(
    state: &mut KeySharedConsumerState,
    engine: &mut SubscriptionEngine,
    window: &mut InFlightWindow,
) {
    if state.is_empty() || !window.has_capacity() {
        return;
    }

    let failure_policy = engine.failure_policy();
    let ack_timeout = Duration::from_millis(failure_policy.ack_timeout_ms);

    // Check rate limiter
    if let Some(limiter) = &engine.dispatch_rate_limiter {
        if !limiter.try_acquire(1).await {
            return;
        }
    }

    // Poll ONE message from the WAL stream
    let next_message = match engine.poll_next().await {
        Ok(msg_opt) => msg_opt,
        Err(e) => {
            warn!(error = %e, "poll_next error in Key-Shared dispatcher");
            return;
        }
    };

    let msg = match next_message {
        Some(msg) => msg,
        None => return, // caught up with WAL
    };

    let offset = msg.msg_id.topic_offset;
    let key = msg.effective_routing_key().to_string();
    window.record_polled(offset);

    // 1. Filter check: any consumer wants this key?
    let consumer_idx = match state.select_consumer(&key) {
        Some(idx) => idx,
        None => {
            // No consumer accepts this key → skip, advance cursor
            if let Some(safe) = window.on_skipped(offset) {
                if let Err(e) = engine.advance_cursor_to(safe).await {
                    warn!(error = %e, "advance_cursor_to failed on skip");
                }
            }
            return;
        }
    };

    // 2. Key blocking check: is this key already in-flight?
    if window.is_key_active(&key) {
        window.push_blocked(msg);
        return;
    }

    // 3. Dispatch!
    let consumer = state.get_consumer_mut(consumer_idx);
    if !consumer.get_status().await {
        // Consumer unhealthy — block the message for retry on heartbeat
        window.push_blocked(msg);
        return;
    }

    let mut delivery = PendingDelivery::new(msg.clone());
    let consumer_id = consumer.consumer_id;

    match consumer.send_message(msg).await {
        Ok(()) => {
            delivery.on_send_attempt(consumer_id, ack_timeout);
            window.mark_dispatched(offset, key, consumer_id, delivery);
        }
        Err(e) => {
            warn!(
                offset = %offset,
                consumer_id = %consumer_id,
                error = %e,
                "Failed to send message to Key-Shared consumer"
            );
            // Put back for retry
            window.push_blocked(delivery.message);
        }
    }
}

/// Heartbeat watchdog: periodic timer that detects ack timeouts, retry readiness, and lag.
async fn handle_heartbeat(
    state: &mut KeySharedConsumerState,
    engine: &mut SubscriptionEngine,
    replicator: Option<&Replicator>,
    window: &mut InFlightWindow,
) {
    let _ = replicator; // reserved for future DLQ support

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

    // Check ack timeouts across all in-flight entries
    let now = Instant::now();
    let has_timeouts = window
        .in_flight
        .values()
        .any(|e| e.delivery.ack_timed_out(now));

    if has_timeouts {
        counter!(DISPATCHER_HEARTBEAT_POLLS_TOTAL.name).increment(1);
        handle_ack_timeouts(engine, window);
        dispatch_unblocked(state, engine, window).await;
    }

    // Check for retry-ready in-flight entries
    let has_retry_ready = window
        .in_flight
        .values()
        .any(|e| e.delivery.is_retry_ready(now));

    if has_retry_ready {
        // Re-dispatch retry-ready entries
        let retry_offsets: Vec<u64> = window
            .in_flight
            .iter()
            .filter(|(_, e)| e.delivery.is_retry_ready(now))
            .map(|(off, _)| *off)
            .collect();

        let failure_policy = engine.failure_policy();
        let ack_timeout = Duration::from_millis(failure_policy.ack_timeout_ms);

        for offset in retry_offsets {
            if let Some(entry) = window.in_flight.get(&offset) {
                let key = entry.routing_key.clone();
                let consumer_idx = match state.select_consumer(&key) {
                    Some(idx) => idx,
                    None => continue,
                };
                let consumer = state.get_consumer_mut(consumer_idx);
                if !consumer.get_status().await {
                    continue;
                }
                let msg = entry.delivery.message.clone();
                let consumer_id = consumer.consumer_id;
                let is_redelivery = entry.delivery.delivery_attempt > 0;

                match consumer.send_message(msg).await {
                    Ok(()) => {
                        if is_redelivery {
                            counter!(SUBSCRIPTION_REDELIVERY_TOTAL.name).increment(1);
                        }
                        if let Some(entry) = window.in_flight.get_mut(&offset) {
                            entry.delivery.on_send_attempt(consumer_id, ack_timeout);
                            entry.consumer_id = consumer_id;
                        }
                    }
                    Err(e) => {
                        warn!(
                            offset = %offset,
                            error = %e,
                            "Failed to redeliver Key-Shared message"
                        );
                    }
                }
            }
        }
    }

    // If window has capacity and there's lag, poll more
    if window.has_capacity() && lag_info.has_lag {
        trace!(lag_messages = %lag_info.lag_messages, "heartbeat detected lag");
        counter!(DISPATCHER_HEARTBEAT_POLLS_TOTAL.name).increment(1);
        handle_poll_and_dispatch(state, engine, window).await;
    }
}

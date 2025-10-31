use anyhow::{anyhow, Result};
use danube_core::message::StreamMessage;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, watch, Mutex, Notify};
use tracing::{trace, warn};

use crate::consumer::Consumer;
use crate::message::AckMessage;

use super::subscription_engine::SubscriptionEngine;

#[derive(Debug, Clone, Copy)]
pub(crate) enum DispatchMode {
    Reliable,
    NonReliable,
}

#[derive(Debug)]
pub(crate) struct UnifiedSingleDispatcher {
    pub(crate) mode: DispatchMode,
    reliable: Option<mpsc::Sender<DispatcherCommand>>, // control channel for reliable loop
    control_tx: mpsc::Sender<DispatcherCommand>,
    ready_rx: watch::Receiver<bool>,
}

#[derive(Debug)]
enum DispatcherCommand {
    AddConsumer(Consumer),
    RemoveConsumer(u64),
    DisconnectAllConsumers,
    DispatchMessage(StreamMessage, oneshot::Sender<Result<()>>),
    MessageAcked(AckMessage),
    // Reliable-only
    PollAndDispatch,
    ResetPending, // Clear pending flag to allow retry after reconnection
    FlushProgressNow, // Force flush subscription progress (reliable only)
}

impl UnifiedSingleDispatcher {
    pub(crate) fn new_non_reliable() -> Self {
        let (control_tx, mut control_rx) = mpsc::channel(16);
        // Non-reliable dispatcher is ready immediately.
        let (_ready_tx, ready_rx) = watch::channel(true);

        tokio::spawn(async move {
            let mut consumers: Vec<Consumer> = Vec::new();
            let mut active_consumer: Option<Consumer> = None;
            // Use while let to properly exit when channel is closed
            while let Some(cmd) = control_rx.recv().await {
                match cmd {
                    DispatcherCommand::AddConsumer(c) => {
                        if consumers.is_empty() {
                            active_consumer = Some(c.clone());
                        }
                        consumers.push(c);
                    }
                    DispatcherCommand::RemoveConsumer(consumer_id) => {
                        consumers.retain(|c| c.consumer_id != consumer_id);
                        if let Some(ref ac) = active_consumer {
                            if ac.consumer_id == consumer_id {
                                active_consumer = None;
                            }
                        }
                    }
                    DispatcherCommand::DisconnectAllConsumers => {
                        consumers.clear();
                        active_consumer = None;
                    }
                    DispatcherCommand::DispatchMessage(msg, response_tx) => {
                        let result = if let Some(cons) = &mut active_consumer {
                            if !cons.get_status().await {
                                Err(anyhow!("No active consumer available to dispatch message"))
                            } else {
                                match cons.send_message(msg).await {
                                    Ok(()) => {
                                        trace!(
                                            "Message dispatched to active consumer {}",
                                            cons.consumer_id
                                        );
                                        Ok(())
                                    }
                                    Err(e) => {
                                        warn!("Failed to dispatch to active consumer: {}", e);
                                        Err(e)
                                    }
                                }
                            }
                        } else {
                            Err(anyhow!("No active consumer available to dispatch message"))
                        };
                        let _ = response_tx.send(result);
                    }
                    DispatcherCommand::MessageAcked(_) => {
                        // non-reliable ignores acks
                    }
                    DispatcherCommand::PollAndDispatch => {}
                    DispatcherCommand::ResetPending => {
                        // non-reliable has no pending state
                    }
                    DispatcherCommand::FlushProgressNow => {
                        // non-reliable: no-op
                    }
                }
            }
            trace!("Non-reliable single dispatcher task exiting gracefully");
        });

        Self {
            mode: DispatchMode::NonReliable,
            reliable: None,
            control_tx,
            ready_rx,
        }
    }

    pub(crate) fn new_reliable(engine: SubscriptionEngine) -> Self {
        let (control_tx, mut control_rx) = mpsc::channel(32);
        let reliable_tx_for_task = control_tx.clone();
        let reliable_tx_for_struct = control_tx.clone();
        let engine = Mutex::new(engine);
        // Readiness is false until the stream is initialized.
        let (ready_tx, ready_rx) = watch::channel(false);

        tokio::spawn(async move {
            // State
            let mut consumers: Vec<Consumer> = Vec::new();
            let mut active_consumer: Option<Consumer> = None;
            let mut pending = false; // strict ack-gating
            let mut pending_message: Option<StreamMessage> = None; // Buffer for unacked message
                                                                   // Initialize stream from persisted progress if available, otherwise Latest
            {
                if let Err(e) = engine
                    .lock()
                    .await
                    .init_stream_from_progress_or_latest()
                    .await
                {
                    warn!("Reliable single dispatcher failed to init stream: {}", e);
                }
                // Signal readiness regardless of success; dispatcher can still operate and retry.
                let _ = ready_tx.send(true);
            }

            loop {
                tokio::select! {
                    cmd_result = control_rx.recv() => {
                        match cmd_result {
                            Some(cmd) => {
                                match cmd {
                                    DispatcherCommand::AddConsumer(c) => {
                                        trace!("AddConsumer: consumer {} added to reliable dispatcher", c.consumer_id);
                                        if consumers.is_empty() {
                                            active_consumer = Some(c.clone());
                                        }
                                        consumers.push(c);
                                        // Try to kick the loop if idle and no pending
                                        if !pending {
                                            let _ = reliable_tx_for_task.send(DispatcherCommand::PollAndDispatch).await;
                                        }
                                    }
                                    DispatcherCommand::RemoveConsumer(id) => {
                                        consumers.retain(|c| c.consumer_id != id);
                                        if let Some(ref ac) = active_consumer {
                                            if ac.consumer_id == id {
                                                active_consumer = None;
                                            }
                                        }
                                    }
                                    DispatcherCommand::DisconnectAllConsumers => {
                                        // Reliable: best-effort flush progress and pause
                                        if let Err(e) = engine.lock().await.flush_progress_now().await {
                                            warn!("DisconnectAllConsumers: flush progress failed: {}", e);
                                        }
                                        consumers.clear();
                                        active_consumer = None;
                                    }
                                    DispatcherCommand::MessageAcked(ack_msg) => {
                                        let acked_offset = ack_msg.msg_id.topic_offset;

                                        // Clear pending and record ack
                                        if let Err(e) = engine.lock().await.on_acked(ack_msg.msg_id.clone()).await {
                                            warn!("Ack handling failed: {}", e);
                                        }

                                        // Only clear buffer if ack is for the currently pending message
                                        let should_clear_buffer = pending_message
                                            .as_ref()
                                            .map(|msg| msg.msg_id.topic_offset == acked_offset)
                                            .unwrap_or(false);

                                        if should_clear_buffer {
                                            trace!("Ack received for pending message req_id={}, offset={}, clearing buffer",
                                                ack_msg.request_id, acked_offset);
                                            pending = false;
                                            pending_message = None;
                                        } else {
                                            // Late ack for already-dispatched message, ignore
                                            trace!("Ignoring late ack for req_id={}, offset={} (not the pending message)",
                                                ack_msg.request_id, acked_offset);
                                        }

                                        // Immediately attempt next
                                        let _ = reliable_tx_for_task.send(DispatcherCommand::PollAndDispatch).await;
                                    }
                                    DispatcherCommand::DispatchMessage(_, response_tx) => {
                                        let _ = response_tx.send(Err(anyhow!("Reliable dispatcher does not support direct message dispatch")));
                                    }
                                    DispatcherCommand::PollAndDispatch => {
                                        if pending {
                                            continue;
                                        }
                                        // Need an active consumer to send to
                                        if let Some(cons) = &mut active_consumer {
                                            let status = cons.get_status().await;
                                            if !status {
                                                continue;
                                            }

                                            // First, try to resend pending message if it exists (for at-least-once delivery)
                                            let msg_to_send = if let Some(buffered_msg) = pending_message.take() {
                                                trace!("Resending buffered message offset={}", buffered_msg.msg_id.topic_offset);
                                                Some(buffered_msg)
                                            } else {
                                                // No pending message, poll new one from stream
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
                                                // Buffer message before sending (in case of failure)
                                                pending_message = Some(msg.clone());

                                                if let Err(e) = cons.send_message(msg).await {
                                                    warn!("Failed to send message offset={}: {}. Will retry on reconnect.", offset, e);
                                                    // Keep pending_message buffered for retry
                                                } else {
                                                    pending = true; // wait for ack
                                                    // pending_message stays buffered until ack
                                                }
                                            }
                                        }
                                    }
                                    DispatcherCommand::ResetPending => {
                                        trace!("ResetPending: clearing pending flag to allow retry of buffered message");
                                        pending = false;
                                        // Keep pending_message buffered - it will be resent on next PollAndDispatch
                                        // Trigger immediate poll attempt to resend the buffered message
                                        let _ = reliable_tx_for_task.send(DispatcherCommand::PollAndDispatch).await;
                                    }
                                    DispatcherCommand::FlushProgressNow => {
                                        if let Err(e) = engine.lock().await.flush_progress_now().await {
                                            warn!("FlushProgressNow: failed to flush subscription progress: {}", e);
                                        }
                                    }
                                }
                            }
                            None => {
                                // Channel closed, exit gracefully
                                break;
                            }
                        }
                    }
                }
            }
        });

        Self {
            mode: DispatchMode::Reliable,
            reliable: Some(reliable_tx_for_struct),
            control_tx,
            ready_rx,
        }
    }

    pub(crate) fn get_notifier(&self) -> Arc<Notify> {
        let notify = Arc::new(Notify::new());
        let tx = self.control_tx.clone();
        let n = notify.clone();
        tokio::spawn(async move {
            loop {
                n.notified().await;
                let _ = tx.send(DispatcherCommand::PollAndDispatch).await;
            }
        });
        notify
    }

    /// Waits until the dispatcher has completed its initial stream setup.
    ///
    /// For non-reliable mode this returns immediately. For reliable mode,
    /// it resolves after `init_stream_from_progress_or_latest()` completes.
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

    pub(crate) async fn dispatch_message(&self, message: StreamMessage) -> Result<()> {
        if let DispatchMode::NonReliable = self.mode {
            let (response_tx, response_rx) = oneshot::channel();
            self.control_tx
                .send(DispatcherCommand::DispatchMessage(message, response_tx))
                .await
                .map_err(|e| anyhow!("Failed to send dispatch command: {}", e))?;

            response_rx
                .await
                .map_err(|e| anyhow!("Failed to receive dispatch response: {}", e))?
        } else {
            Err(anyhow!(
                "Reliable single dispatcher is stream-driven, not push-per-message"
            ))
        }
    }

    pub(crate) async fn ack_message(&self, ack_msg: AckMessage) -> Result<()> {
        if let Some(tx) = &self.reliable {
            tx.send(DispatcherCommand::MessageAcked(ack_msg))
                .await
                .map_err(|_| anyhow!("Failed to send ack to reliable dispatcher"))
        } else {
            // Non-reliable: ignore
            Ok(())
        }
    }

    pub(crate) async fn add_consumer(&self, consumer: Consumer) -> Result<()> {
        self.control_tx
            .send(DispatcherCommand::AddConsumer(consumer))
            .await
            .map_err(|_| anyhow!("Failed to send add consumer command"))
    }

    pub(crate) async fn remove_consumer(&self, consumer_id: u64) -> Result<()> {
        self.control_tx
            .send(DispatcherCommand::RemoveConsumer(consumer_id))
            .await
            .map_err(|_| anyhow!("Failed to send remove consumer command"))
    }

    pub(crate) async fn disconnect_all_consumers(&self) -> Result<()> {
        self.control_tx
            .send(DispatcherCommand::DisconnectAllConsumers)
            .await
            .map_err(|_| anyhow!("Failed to send disconnect all consumers command"))
    }

    /// Reset the pending state to allow retry after consumer reconnection.
    /// Only relevant for reliable mode.
    pub(crate) async fn reset_pending(&self) -> Result<()> {
        if let Some(tx) = &self.reliable {
            tx.send(DispatcherCommand::ResetPending)
                .await
                .map_err(|_| anyhow!("Failed to send reset pending command"))
        } else {
            // Non-reliable: no-op
            Ok(())
        }
    }

    /// Force flush durable progress for reliable subscriptions. No-op for non-reliable mode.
    pub(crate) async fn flush_progress_now(&self) -> Result<()> {
        if let Some(tx) = &self.reliable {
            tx.send(DispatcherCommand::FlushProgressNow)
                .await
                .map_err(|_| anyhow!("Failed to send flush progress command"))
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
#[path = "unified_single_test.rs"]
mod unified_single_test;

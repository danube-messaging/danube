use anyhow::{anyhow, Result};
use danube_core::message::{MessageID, StreamMessage};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, watch, Mutex, Notify};
use tracing::{trace, warn};

use crate::consumer::Consumer;

use super::subscription_engine::SubscriptionEngine;

#[derive(Debug, Clone, Copy)]
pub(crate) enum DispatchMode {
    Reliable,
    NonReliable,
}

#[derive(Debug)]
pub(crate) struct UnifiedMultipleDispatcher {
    pub(crate) mode: DispatchMode,
    reliable: Option<mpsc::Sender<DispatcherCommand>>, // control channel for reliable loop
    control_tx: mpsc::Sender<DispatcherCommand>,
    _marker: PhantomData<()>,
    ready_rx: watch::Receiver<bool>,
}

#[derive(Debug)]
enum DispatcherCommand {
    AddConsumer(Consumer),
    RemoveConsumer(u64),
    DisconnectAllConsumers,
    DispatchMessage(StreamMessage, oneshot::Sender<Result<()>>),
    MessageAcked(u64, MessageID),
    // Reliable-only
    PollAndDispatch,
    ResetPending, // Clear pending flag to allow retry after consumer disconnect
}

impl UnifiedMultipleDispatcher {
    pub(crate) fn new_non_reliable() -> Self {
        let (control_tx, mut control_rx) = mpsc::channel(32);
        let rr = Arc::new(AtomicUsize::new(0));
        let rr_task = rr.clone();
        // Non-reliable dispatcher is ready immediately.
        let (_ready_tx, ready_rx) = watch::channel(true);

        tokio::spawn(async move {
            let mut consumers: Vec<Consumer> = Vec::new();
            // Use while let to properly exit when channel is closed
            while let Some(cmd) = control_rx.recv().await {
                match cmd {
                    DispatcherCommand::AddConsumer(c) => {
                        consumers.push(c);
                    }
                    DispatcherCommand::RemoveConsumer(id) => {
                        consumers.retain(|c| c.consumer_id != id);
                    }
                    DispatcherCommand::DisconnectAllConsumers => {
                        consumers.clear();
                    }
                    DispatcherCommand::DispatchMessage(msg, response_tx) => {
                        let result = if consumers.is_empty() {
                            Err(anyhow!("No consumers available to dispatch the message"))
                        } else {
                            let num_consumers = consumers.len();
                            let mut dispatched = false;

                            for _ in 0..num_consumers {
                                let idx = rr_task.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                                    % num_consumers;
                                if let Some(target) = consumers.get_mut(idx) {
                                    // Only send to healthy consumers
                                    if !target.get_status().await {
                                        continue;
                                    }
                                    match target.send_message(msg.clone()).await {
                                        Ok(()) => {
                                            trace!(
                                                "Dispatcher sent the message to consumer: {}",
                                                target.consumer_id
                                            );
                                            dispatched = true;
                                            break;
                                        }
                                        Err(e) => {
                                            warn!(
                                                "Failed to dispatch to consumer {}: {}",
                                                target.consumer_id, e
                                            );
                                        }
                                    }
                                }
                            }

                            if dispatched {
                                Ok(())
                            } else {
                                Err(anyhow!(
                                    "No active consumers available to handle the message"
                                ))
                            }
                        };
                        let _ = response_tx.send(result);
                    }
                    DispatcherCommand::MessageAcked(_, _) => {
                        // non-reliable ignores acks
                    }
                    DispatcherCommand::PollAndDispatch => {}
                    DispatcherCommand::ResetPending => {
                        // non-reliable has no pending state
                    }
                }
            }
            trace!("Non-reliable multiple dispatcher task exiting gracefully");
        });

        Self {
            mode: DispatchMode::NonReliable,
            reliable: None,
            control_tx,
            _marker: PhantomData,
            ready_rx,
        }
    }

    pub(crate) fn new_reliable(engine: SubscriptionEngine) -> Self {
        let (control_tx, mut control_rx) = mpsc::channel(32);
        let reliable_tx_for_task = control_tx.clone();
        let reliable_tx_for_struct = control_tx.clone();
        let rr = Arc::new(AtomicUsize::new(0));
        let rr_task = rr.clone();
        let engine = Mutex::new(engine);
        // Readiness is false until stream initialization completes.
        let (ready_tx, ready_rx) = watch::channel(false);

        tokio::spawn(async move {
            let mut consumers: Vec<Consumer> = Vec::new();
            let mut pending = false; // strict ack-gating across the subscription
            let mut pending_message: Option<StreamMessage> = None; // Buffer for unacked message

            // Initialize stream from persisted progress if available, otherwise Latest
            if let Err(e) = engine
                .lock()
                .await
                .init_stream_from_progress_or_latest()
                .await
            {
                warn!("Reliable multi dispatcher failed to init stream: {}", e);
            }
            // Signal readiness regardless of success.
            let _ = ready_tx.send(true);

            loop {
                tokio::select! {
                    cmd_result = control_rx.recv() => {
                        match cmd_result {
                            Some(cmd) => {
                                match cmd {
                                    DispatcherCommand::AddConsumer(c) => {
                                        consumers.push(c);
                                        if !pending {
                                            let _ = reliable_tx_for_task.send(DispatcherCommand::PollAndDispatch).await;
                                        }
                                    }
                                    DispatcherCommand::RemoveConsumer(id) => {
                                        consumers.retain(|c| c.consumer_id != id);
                                    }
                                    DispatcherCommand::DisconnectAllConsumers => {
                                        consumers.clear();
                                    }
                                    DispatcherCommand::MessageAcked(request_id, msg_id) => {
                                        let acked_offset = msg_id.topic_offset;

                                        if let Err(e) = engine.lock().await.on_acked(request_id, msg_id).await {
                                            warn!("Ack handling failed: {}", e);
                                        }

                                        // Only clear buffer if ack is for the currently pending message
                                        let should_clear_buffer = pending_message
                                            .as_ref()
                                            .map(|msg| msg.request_id == request_id)
                                            .unwrap_or(false);

                                        if should_clear_buffer {
                                            trace!("Ack received for pending message req_id={}, offset={}, clearing buffer",
                                                request_id, acked_offset);
                                            pending = false;
                                            pending_message = None;
                                        } else {
                                            // Late ack for already-dispatched message, ignore
                                            trace!("Ignoring late ack for req_id={}, offset={} (not the pending message)",
                                                request_id, acked_offset);
                                        }

                                        let _ = reliable_tx_for_task.send(DispatcherCommand::PollAndDispatch).await;
                                    }
                                    DispatcherCommand::DispatchMessage(_, response_tx) => {
                                        let _ = response_tx.send(Err(anyhow!("Reliable dispatcher does not support direct message dispatch")));
                                    }
                                    DispatcherCommand::PollAndDispatch => {
                                        if pending { continue; }
                                        if consumers.is_empty() { continue; }

                                        // First, try to resend pending message if it exists (for at-least-once delivery)
                                        let msg_to_send = if let Some(buffered_msg) = pending_message.take() {
                                            trace!("Resending buffered message offset={} to another consumer", buffered_msg.msg_id.topic_offset);
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
                                            // Try to send to a healthy consumer via round-robin
                                            let mut attempts = 0usize;
                                            let mut sent = false;
                                            let offset = msg.msg_id.topic_offset;

                                            while attempts < consumers.len() && !sent {
                                                let idx = rr_task.fetch_add(1, Ordering::Relaxed) % consumers.len();
                                                if let Some(target) = consumers.get_mut(idx) {
                                                    // Only send to healthy consumers
                                                    if !target.get_status().await {
                                                        attempts += 1;
                                                        continue;
                                                    }

                                                    // Buffer message before sending (in case of failure)
                                                    pending_message = Some(msg.clone());

                                                    if let Err(e) = target.send_message(msg.clone()).await {
                                                        warn!("Failed to send message offset={} to consumer {}: {}", offset, target.consumer_id, e);
                                                        // Keep pending_message buffered, try next consumer
                                                        attempts += 1;
                                                        continue;
                                                    } else {
                                                        pending = true;
                                                        sent = true;
                                                        // pending_message stays buffered until ack
                                                    }
                                                }
                                                attempts += 1;
                                            }
                                        }
                                    }
                                    DispatcherCommand::ResetPending => {
                                        if pending_message.is_some() {
                                            trace!("Consumer disconnected, will failover buffered message to available consumer");
                                        }
                                        pending = false;
                                        // Keep pending_message buffered - it will be resent to another healthy consumer
                                        // Trigger immediate poll attempt to failover the buffered message
                                        let _ = reliable_tx_for_task.send(DispatcherCommand::PollAndDispatch).await;
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
            _marker: PhantomData,
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
                "Reliable multiple dispatcher is stream-driven, not push-per-message"
            ))
        }
    }

    pub(crate) async fn ack_message(&self, request: u64, msg_id: MessageID) -> Result<()> {
        if let Some(tx) = &self.reliable {
            tx.send(DispatcherCommand::MessageAcked(request, msg_id))
                .await
                .map_err(|_| anyhow!("Failed to send ack to reliable dispatcher"))
        } else {
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

    /// Reset pending state to allow failover to another consumer after disconnect
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
}

#[cfg(test)]
#[path = "unified_multiple_test.rs"]
mod unified_multiple_test;

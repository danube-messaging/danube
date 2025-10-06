use anyhow::{anyhow, Result};
use danube_core::message::{MessageID, StreamMessage};
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
    MessageAcked(u64, MessageID),
    // Reliable-only
    PollAndDispatch,
}

impl UnifiedSingleDispatcher {
    pub(crate) fn new_non_reliable() -> Self {
        let (control_tx, mut control_rx) = mpsc::channel(16);
        // Non-reliable dispatcher is ready immediately.
        let (_ready_tx, ready_rx) = watch::channel(true);

        tokio::spawn(async move {
            let mut consumers: Vec<Consumer> = Vec::new();
            let mut active_consumer: Option<Consumer> = None;
            loop {
                if let Some(cmd) = control_rx.recv().await {
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
                        DispatcherCommand::MessageAcked(_, _) => {
                            // non-reliable ignores acks
                        }
                        DispatcherCommand::PollAndDispatch => {}
                    }
                }
            }
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
                    Some(cmd) = control_rx.recv() => {
                        match cmd {
                            DispatcherCommand::AddConsumer(c) => {
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
                                consumers.clear();
                                active_consumer = None;
                            }
                            DispatcherCommand::MessageAcked(request_id, msg_id) => {
                                // Clear pending and record ack
                                if let Err(e) = engine.lock().await.on_acked(request_id, msg_id).await {
                                    warn!("Ack handling failed: {}", e);
                                }
                                pending = false;
                                // Immediately attempt next
                                let _ = reliable_tx_for_task.send(DispatcherCommand::PollAndDispatch).await;
                            }
                            DispatcherCommand::DispatchMessage(_, response_tx) => {
                                let _ = response_tx.send(Err(anyhow!("Reliable dispatcher does not support direct message dispatch")));
                            }
                            DispatcherCommand::PollAndDispatch => {
                                if pending { continue; }
                                // Need an active consumer to send to
                                if let Some(cons) = &mut active_consumer {
                                    if !cons.get_status().await { continue; }
                                    match engine.lock().await.poll_next().await {
                                        Ok(Some(msg)) => {
                                            let rid = msg.request_id;
                                            let _mid = msg.msg_id.clone();
                                            if let Err(e) = cons.send_message(msg).await {
                                                warn!("Failed to send reliable message: {}", e);
                                            } else {
                                                trace!("Reliable dispatched req_id={} to consumer {}", rid, cons.consumer_id);
                                                pending = true; // wait for ack
                                            }
                                        }
                                        Ok(None) => { /* no data yet */ }
                                        Err(e) => warn!("poll_next error: {}", e),
                                    }
                                }
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

    pub(crate) async fn ack_message(&self, request: u64, msg_id: MessageID) -> Result<()> {
        if let Some(tx) = &self.reliable {
            tx.send(DispatcherCommand::MessageAcked(request, msg_id))
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
}

#[cfg(test)]
#[path = "unified_single_test.rs"]
mod unified_single_test;

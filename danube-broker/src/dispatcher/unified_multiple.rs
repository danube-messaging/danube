use anyhow::{anyhow, Result};
use danube_core::message::{MessageID, StreamMessage};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex, Notify};
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

impl UnifiedMultipleDispatcher {
    pub(crate) fn new_non_reliable() -> Self {
        let (control_tx, mut control_rx) = mpsc::channel(32);
        let rr = Arc::new(AtomicUsize::new(0));
        let rr_task = rr.clone();

        tokio::spawn(async move {
            let mut consumers: Vec<Consumer> = Vec::new();
            loop {
                if let Some(cmd) = control_rx.recv().await {
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
                                    let idx = rr_task.fetch_add(1, std::sync::atomic::Ordering::SeqCst) % num_consumers;
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
                                    Err(anyhow!("No active consumers available to handle the message"))
                                }
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
            _marker: PhantomData,
        }
    }

    pub(crate) fn new_reliable(engine: SubscriptionEngine) -> Self {
        let (control_tx, mut control_rx) = mpsc::channel(32);
        let reliable_tx_for_task = control_tx.clone();
        let reliable_tx_for_struct = control_tx.clone();
        let rr = Arc::new(AtomicUsize::new(0));
        let rr_task = rr.clone();
        let engine = Mutex::new(engine);

        tokio::spawn(async move {
            let mut consumers: Vec<Consumer> = Vec::new();
            let mut pending = false; // strict ack-gating across the subscription

            // Initialize stream from persisted progress if available, otherwise Latest
            if let Err(e) = engine
                .lock()
                .await
                .init_stream_from_progress_or_latest()
                .await
            {
                warn!("Reliable multi dispatcher failed to init stream: {}", e);
            }

            loop {
                tokio::select! {
                    Some(cmd) = control_rx.recv() => {
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
                                if let Err(e) = engine.lock().await.on_acked(request_id, msg_id).await {
                                    warn!("Ack handling failed: {}", e);
                                }
                                pending = false;
                                let _ = reliable_tx_for_task.send(DispatcherCommand::PollAndDispatch).await;
                            }
                            DispatcherCommand::DispatchMessage(_, response_tx) => { 
                                let _ = response_tx.send(Err(anyhow!("Reliable dispatcher does not support direct message dispatch")));
                            }
                            DispatcherCommand::PollAndDispatch => {
                                if pending { continue; }
                                if consumers.is_empty() { continue; }
                                // choose next consumer in round-robin order among active ones
                                let mut attempts = 0usize;
                                let mut sent = false;
                                while attempts < consumers.len() && !sent {
                                    let idx = rr_task.fetch_add(1, Ordering::Relaxed) % consumers.len();
                                    if let Some(target) = consumers.get_mut(idx) {
                                        // Only send to healthy consumers
                                        if !target.get_status().await {
                                            attempts += 1;
                                            continue;
                                        }
                                        match engine.lock().await.poll_next().await {
                                            Ok(Some(msg)) => {
                                                let rid = msg.request_id;
                                                if let Err(e) = target.send_message(msg).await {
                                                    warn!("Failed to send reliable message to {}: {}", target.consumer_id, e);
                                                } else {
                                                    trace!("Reliable dispatched req_id={} to consumer {}", rid, target.consumer_id);
                                                    pending = true;
                                                    sent = true;
                                                }
                                            }
                                            Ok(None) => { break; }
                                            Err(e) => { warn!("poll_next error: {}", e); break; }
                                        }
                                    }
                                    attempts += 1;
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
            _marker: PhantomData,
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
}

#[cfg(test)]
#[path = "unified_multiple_test.rs"]
mod unified_multiple_test;

use std::sync::{Arc, RwLock};

use anyhow::{anyhow, Result};
use tokio::{
    sync::mpsc,
    time::{self, Duration},
};
use tracing::{trace, warn};

use crate::{
    consumer::{Consumer, MessageToSend},
    dispatcher::DispatcherCommand,
    topic_storage::{Segment, TopicStore},
};

/// Reliable dispatcher for single consumer, it sends ordered messages to a single consumer
#[derive(Debug)]
pub(crate) struct DispatcherReliableSingleConsumer {
    control_tx: mpsc::Sender<DispatcherCommand>,
}

impl DispatcherReliableSingleConsumer {
    pub(crate) fn new(topic_store: TopicStore, last_acked_segment: Arc<RwLock<usize>>) -> Self {
        let (control_tx, mut control_rx) = mpsc::channel(16);

        // Spawn dispatcher task
        tokio::spawn(async move {
            let mut consumer_dispatch = ConsumerDispatch::new(topic_store, last_acked_segment);

            // TODO! The dispatcher should mark the segment as acknowledged on the TopicStore
            // The Subscription should send the next segment to the dispatcher if the dispatcher has no segment to read from
            // Basically, when a new message comes, the subscriptions check also the status of the dispatcher !!!!!
            let mut interval = time::interval(Duration::from_millis(100));

            loop {
                tokio::select! {
                    Some(command) = control_rx.recv() => {
                        match command {
                            DispatcherCommand::AddConsumer(consumer) => {
                                if let Err(e) = handle_add_consumer(&mut consumer_dispatch, consumer).await {
                                    warn!("Failed to add consumer: {}", e);
                                }
                            }
                            DispatcherCommand::RemoveConsumer(consumer_id) => {
                                handle_remove_consumer(&mut consumer_dispatch, consumer_id).await;
                            }
                            DispatcherCommand::DisconnectAllConsumers => {
                                handle_disconnect_all(&mut consumer_dispatch).await;
                            }
                            DispatcherCommand::DispatchMessage(_) => {
                                unreachable!("Reliable Dispatcher should not receive messages, just segments");
                            }
                            DispatcherCommand::MessageAcked(message_id) => {
                                if let Err(e) = handle_message_acked(&mut consumer_dispatch, message_id).await {
                                    warn!("Failed to handle message acked: {}", e);
                                }
                            }
                        }
                    }
                    _ = interval.tick() => {
                        // TODO! - don't use the segment if it passed the TTL since closed, go to next segment
                        // TODO! - send ordered messages from the segment to the consumers
                        // TODO! - go to next segment if all messages are acknowledged by consumers
                        if let Err(e) = consumer_dispatch.process_current_segment().await {
                            warn!("Failed to process current segment: {}", e);
                        }
                    }
                }
            }
        });

        DispatcherReliableSingleConsumer { control_tx }
    }

    /// Add a consumer
    pub(crate) async fn add_consumer(&self, consumer: Consumer) -> Result<()> {
        self.control_tx
            .send(DispatcherCommand::AddConsumer(consumer))
            .await
            .map_err(|_| anyhow!("Failed to send add consumer command"))
    }

    /// Remove a consumer
    #[allow(dead_code)]
    pub(crate) async fn remove_consumer(&self, consumer_id: u64) -> Result<()> {
        self.control_tx
            .send(DispatcherCommand::RemoveConsumer(consumer_id))
            .await
            .map_err(|_| anyhow!("Failed to send remove consumer command"))
    }

    /// Disconnect all consumers
    pub(crate) async fn disconnect_all_consumers(&self) -> Result<()> {
        self.control_tx
            .send(DispatcherCommand::DisconnectAllConsumers)
            .await
            .map_err(|_| anyhow!("Failed to send disconnect all consumers command"))
    }
}

/// Handle adding a consumer
async fn handle_add_consumer(
    consumer_dispatch: &mut ConsumerDispatch,
    consumer: Consumer,
) -> Result<()> {
    if consumer.subscription_type == 1 {
        return Err(anyhow!(
            "Shared subscription should use a multi-consumer dispatcher"
        ));
    }

    if consumer.subscription_type == 0 && !consumer_dispatch.consumers.is_empty() {
        warn!(
            "Exclusive subscription cannot be shared: consumer_id {}",
            consumer.consumer_id
        );
        return Err(anyhow!(
            "Exclusive subscription cannot be shared with other consumers"
        ));
    }

    consumer_dispatch.add_consumer(consumer);
    trace!("Consumer added to dispatcher");
    Ok(())
}

/// Handle removing a consumer
async fn handle_remove_consumer(consumer_dispatch: &mut ConsumerDispatch, consumer_id: u64) {
    consumer_dispatch
        .consumers
        .retain(|c| c.consumer_id != consumer_id);

    if let Some(active) = &consumer_dispatch.active_consumer {
        if active.consumer_id == consumer_id {
            consumer_dispatch.active_consumer = None;
        }
    }

    if consumer_dispatch.active_consumer.is_none() && !consumer_dispatch.consumers.is_empty() {
        // Use a separate async block to handle the async operation
        for consumer in &consumer_dispatch.consumers {
            if consumer.get_status().await {
                consumer_dispatch.active_consumer = Some(consumer.clone());
                break;
            }
        }
    }

    trace!("Consumer {} removed from dispatcher", consumer_id);
}

/// Handle disconnecting all consumers
async fn handle_disconnect_all(consumer_dispatch: &mut ConsumerDispatch) {
    consumer_dispatch.consumers.clear();
    consumer_dispatch.active_consumer = None;
    trace!("All consumers disconnected from dispatcher");
}

/// Handle the consumer message acknowledgement
async fn handle_message_acked(
    consumer_dispatch: &mut ConsumerDispatch,
    message_id: u64,
) -> Result<()> {
    if let Some(segment) = &consumer_dispatch.segment {
        let segment_lock = segment.write().unwrap();
        if message_id < segment_lock.messages.len() as u64 {
            consumer_dispatch.acked_messages[message_id as usize] = true;
            consumer_dispatch.last_acked_message_index = message_id;
            trace!("Message {} acknowledged by consumer", message_id);
            return Ok(());
        }
    }
    Err(anyhow!("Invalid message ID for acknowledgment"))
}

/// Dispatch a message to the active consumer
async fn dispatch_reliable_message(
    active_consumer: &mut Option<Consumer>,
    message: MessageToSend,
) -> Result<()> {
    if let Some(consumer) = active_consumer {
        if consumer.get_status().await {
            consumer.send_message(message).await?;
            trace!(
                "Message dispatched to active consumer {}",
                consumer.consumer_id
            );
            return Ok(());
        }
    }

    Err(anyhow!("No active consumer available to dispatch message"))
}

/// ConsumerDispatch is holding information about consumers and the messages within a segment
/// It is used to dispatch messages to consumers and to track the progress of the consumer
#[derive(Debug)]
pub(crate) struct ConsumerDispatch {
    // list of consumsers
    pub(crate) consumers: Vec<Consumer>,
    // active consumer is the consumer that is currently receiving messages
    pub(crate) active_consumer: Option<Consumer>,
    // topic store is the store of segments
    // topic store is used to get the next segment to be sent to the consumer
    pub(crate) topic_store: TopicStore,
    // last acked segment is the last segment that has all messages acknowledged by the consumer
    // it is used to track the progress of the subscription
    pub(crate) last_acked_segment: Arc<RwLock<usize>>,
    // segment holds the messages to be sent to the consumer
    // segment is replaced when the consumer is done with the segment and if there is another available segment
    pub(crate) segment: Option<Arc<RwLock<Segment>>>,
    // acked messages are the messages from the segment that have been acknowledged by the consumer
    pub(crate) acked_messages: Vec<bool>,
    // last acked message index is the index of the last message from the segment that has been acknowledged by the consumer
    pub(crate) last_acked_message_index: u64,
}

impl ConsumerDispatch {
    pub(crate) fn new(topic_store: TopicStore, last_acked_segment: Arc<RwLock<usize>>) -> Self {
        Self {
            consumers: Vec::new(),
            active_consumer: None,
            topic_store,
            last_acked_segment,
            segment: None,
            acked_messages: Vec::new(),
            last_acked_message_index: 0,
        }
    }
    pub(crate) fn add_consumer(&mut self, consumer: Consumer) {
        self.consumers.push(consumer.clone());
        if self.active_consumer.is_none() {
            self.active_consumer = Some(consumer.clone());
        }

        trace!(
            "Consumer {} added to single-consumer dispatcher",
            consumer.consumer_name
        );
    }
    pub(crate) async fn process_current_segment(&mut self) -> Result<(), String> {
        if let Some(segment) = &self.segment {
            let mut move_to_next_segment = false;

            let message = {
                let segment_lock = segment
                    .read()
                    .map_err(|_| "Failed to acquire read lock on segment")?;

                // Check if the current segment is closed and fully acknowledged
                if segment_lock.close_time > 0 && self.acked_messages.iter().all(|&acked| acked) {
                    move_to_next_segment = true;
                    None
                } else {
                    // Find the next unacknowledged message index
                    let next_message_index = if self.acked_messages.is_empty() {
                        self.acked_messages = vec![false; segment_lock.messages.len()];
                        0
                    } else {
                        self.last_acked_message_index + 1
                    };

                    // Check if there are more messages to send and get the message if available
                    if next_message_index < segment_lock.messages.len() as u64
                        && !self.acked_messages[next_message_index as usize]
                    {
                        Some(segment_lock.messages[next_message_index as usize].clone())
                    } else {
                        None
                    }
                }
            }; // RwLockReadGuard is dropped here

            // If the current segment is closed and fully acknowledged, or there are no more messages, move to the next segment
            if move_to_next_segment || message.is_none() {
                let next_segment = self
                    .topic_store
                    .get_segment(self.segment.as_ref().unwrap().read().unwrap().id.clone());

                if let Some(next_segment) = next_segment {
                    // Update the last acknowledged segment
                    {
                        let mut last_acked = self
                            .last_acked_segment
                            .write()
                            .map_err(|_| "Failed to acquire write lock on last_acked_segment")?;
                        *last_acked = segment
                            .read()
                            .map_err(|_| "Failed to acquire read lock on segment")?
                            .id;
                    }

                    // Assign the next segment
                    self.segment = Some(next_segment);
                    self.acked_messages.clear();
                    self.last_acked_message_index = 0;
                } else if move_to_next_segment {
                    // No next segment available; clear the current segment
                    self.segment = None;
                }

                return Ok(());
            }

            // Dispatch message outside the scope of the lock if we got one
            if let Some(msg) = message {
                dispatch_reliable_message(&mut self.active_consumer, msg)
                    .await
                    .map_err(|e| e.to_string())?;
            }
        } else {
            // If there is no current segment, attempt to fetch the next one
            let next_segment = self
                .topic_store
                .get_segment(self.segment.as_ref().unwrap().read().unwrap().id.clone());

            if let Some(next_segment) = next_segment {
                self.segment = Some(next_segment);
                self.acked_messages.clear();
                self.last_acked_message_index = 0;
            }
        }

        Ok(())
    }
}

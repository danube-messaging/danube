use danube_core::message::{MessageID, StreamMessage};
use danube_core::storage::Segment;
use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use tokio::time;
use tracing::trace;

use crate::{
    errors::{ReliableDispatchError, Result},
    topic_storage::TopicStore,
};

/// SubscriptionDispatch is holding information about consumers and the messages within a segment
/// It is used to dispatch messages to consumers and to track the progress of the consumer
#[derive(Debug)]
pub struct SubscriptionDispatch {
    // topic store is the store of segments
    // topic store is used to get the next segment to be sent to the consumer
    pub(crate) topic_store: TopicStore,
    // last acked segment is the last segment that has all messages acknowledged by the consumer
    // it is used to track the progress of the subscription
    pub(crate) last_acked_segment: Arc<AtomicUsize>,
    // segment holds the messages to be sent to the consumer
    // segment is replaced when the consumer is done with the segment and if there is another available segment
    pub(crate) segment: Option<Arc<RwLock<Segment>>>,
    // Cached segment ID to avoid frequent locks
    pub(crate) current_segment_id: Option<usize>,
    // single message awaiting acknowledgment from the consumer
    pub(crate) pending_ack_message: Option<(u64, MessageID)>,
    // maps MessageID to request_id of segment acknowledged messages
    pub(crate) acked_messages: HashMap<MessageID, u64>,
    // retry count for the pending ack message
    pub(crate) retry_count: u8,
    // retry interval for the pending ack message
    retry_interval: time::Duration,
    // notify_rx is the channel to receive notification from the topic
    notify_rx: broadcast::Receiver<MessageID>,
}

impl SubscriptionDispatch {
    pub(crate) fn new(
        topic_store: TopicStore,
        last_acked_segment: Arc<AtomicUsize>,
        notify_rx: broadcast::Receiver<MessageID>,
    ) -> Self {
        Self {
            topic_store,
            last_acked_segment,
            segment: None,
            current_segment_id: None,
            pending_ack_message: None,
            acked_messages: HashMap::new(),
            retry_count: 0,
            retry_interval: time::Duration::from_secs(5),
            notify_rx,
        }
    }

    pub async fn next_message(&mut self) -> Option<StreamMessage> {
        // First try to process any available messages without waiting
        if self.pending_ack_message.is_none() {
            if let Ok(message) = self.process_current_segment().await {
                return Some(message);
            }
        }

        tokio::select! {
            Ok(_) = self.notify_rx.recv() => {
                dbg!("Received notification from producer");
                if self.pending_ack_message.is_none() {
                    return self.process_current_segment().await.ok();
                }
            }
            _ = time::sleep(self.retry_interval), if self.pending_ack_message.is_some() && self.retry_count < 3 => {
                dbg!("Retrying to send message");

                    return self.process_current_segment().await.ok();

            }
        }
        None
    }

    /// Process the current segment and send the messages to the consumer
    pub async fn process_current_segment(&mut self) -> Result<StreamMessage> {
        if let Some(segment) = self.segment.as_ref() {
            let current_segment_id = self.current_segment_id;

            // Validate the current segment
            let move_to_next_segment = {
                let segment_id = current_segment_id.ok_or_else(|| {
                    ReliableDispatchError::InvalidState(
                        "Segment ID not cached while processing segment".to_string(),
                    )
                })?;

                self.validate_segment(segment_id, segment).await?
            };

            // If validation indicates we should move to the next segment
            if move_to_next_segment {
                self.move_to_next_segment().await?;
                return Err(ReliableDispatchError::SegmentError(
                    "Move to next segment".to_string(),
                ));
            }
        } else {
            // No current segment; attempt to move to the next segment
            self.move_to_next_segment().await?;
        }

        // Process the next message using the stored segment
        self.process_next_message().await
    }

    /// Validates the current segment. Returns `true` if the segment was invalidated or closed.
    pub(crate) async fn validate_segment(
        &self,
        segment_id: usize,
        segment: &Arc<RwLock<Segment>>,
    ) -> Result<bool> {
        // Check if the segment still exists
        if !self.topic_store.contains_segment(segment_id).await? {
            tracing::trace!(
                "Segment {} no longer exists, moving to next segment",
                segment_id
            );
            return Ok(true);
        }

        // Check if the segment is closed and all messages are acknowledged
        // Attempt to acquire a read lock on the segment
        let segment_data = segment.read().await;

        // When processing the last message of a segment, there's a brief window where:
        // 1. The message is sent to the consumer
        // 2. The segment is marked as closed
        // 3. We're still waiting for the final acknowledgment
        // During this window, acked_messages.len() will naturally be one less than segment_data.messages.len(),
        // but this is a valid state rather than an error condition.
        if segment_data.close_time > 0 && self.acked_messages.len() == segment_data.messages.len() {
            trace!("The subscription dispatcher id moving to the next segment, the current segment is closed and all messages consumed");
            return Ok(true);
        }

        Ok(false)
    }

    /// Moves to the next segment in the `TopicStore`.
    pub(crate) async fn move_to_next_segment(&mut self) -> Result<()> {
        let next_segment = self
            .topic_store
            .get_next_segment(self.current_segment_id)
            .await?;

        if let Some(next_segment) = next_segment {
            let next_segment_id = {
                let segment_data = next_segment.read().await;
                segment_data.id
            };

            // Update the last acknowledged segment
            if let Some(current_segment_id) = self.current_segment_id {
                self.last_acked_segment
                    .store(current_segment_id, std::sync::atomic::Ordering::Release);
            }

            // Clear acknowledgments before switching to a new segment
            self.acked_messages.clear();

            self.segment = Some(next_segment);
            self.current_segment_id = Some(next_segment_id);
        } else {
            self.clear_current_segment();
        }

        Ok(())
    }

    /// Clear the current segment and cached segment_id
    pub(crate) fn clear_current_segment(&mut self) {
        self.segment = None;
        self.current_segment_id = None;
        self.acked_messages.clear();
    }

    /// Processes the next unacknowledged message in the current segment.
    async fn process_next_message(&mut self) -> Result<StreamMessage> {
        // Only process next message if there's no pending acknowledgment
        match self.pending_ack_message {
            None => return self.send_message().await,
            Some(_) => {
                if self.retry_count < 3 {
                    self.retry_count += 1;
                    return self.send_message().await;
                }
            }
        }

        Err(ReliableDispatchError::InvalidState(
            "Pending ack message".to_string(),
        ))
    }

    async fn send_message(&mut self) -> Result<StreamMessage> {
        if let Some(segment) = &self.segment {
            let next_message = {
                let segment_data = segment.read().await;
                segment_data
                    .messages
                    .iter()
                    .find(|msg| !self.acked_messages.contains_key(&msg.msg_id))
                    .cloned()
            };

            if let Some(msg) = next_message {
                self.pending_ack_message = Some((msg.request_id, msg.msg_id.clone()));
                return Ok(msg);
            }
        }
        Err(ReliableDispatchError::InvalidState(
            "Pending ack message".to_string(),
        ))
    }

    /// Handle the consumer message acknowledgement
    pub async fn handle_message_acked(
        &mut self,
        request_id: u64,
        msg_id: MessageID,
    ) -> Result<Option<StreamMessage>> {
        // Validate that the message belongs to current segment
        // Not sure if needed
        // if !self.segment.as_ref().map_or(false, |seg| {
        //     seg.read()
        //         .map_or(false, |s| s.messages.iter().any(|m| m.msg_id == msg_id))
        // }) {
        //     return Err(ReliableDispatchError::AcknowledgmentError(
        //         "Acked message does not belong to current segment".to_string(),
        //     ));
        // }

        if let Some((pending_request_id, pending_msg_id)) = &self.pending_ack_message {
            if *pending_request_id == request_id && *pending_msg_id == msg_id {
                self.pending_ack_message = None;
                self.acked_messages.insert(msg_id.clone(), request_id);
                trace!(
                    "Message with request_id {} and msg_id {:?} acknowledged",
                    request_id,
                    msg_id
                );
                self.retry_count = 0;

                // Return next message immediately after successful ack
                return Ok(self.process_current_segment().await.ok());
            }
        }
        Err(ReliableDispatchError::AcknowledgmentError(
            "Invalid or unexpected acknowledgment".to_string(),
        ))
    }
}

use danube_core::message::{MessageID, StreamMessage};
use danube_core::storage::Segment;
use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use tokio::sync::RwLock;
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
    retry_count: u8,
    last_retry_timestamp: Option<tokio::time::Instant>,
}

impl SubscriptionDispatch {
    pub(crate) fn new(topic_store: TopicStore, last_acked_segment: Arc<AtomicUsize>) -> Self {
        Self {
            topic_store,
            last_acked_segment,
            segment: None,
            current_segment_id: None,
            pending_ack_message: None,
            acked_messages: HashMap::new(),
            retry_count: 0,
            last_retry_timestamp: None,
        }
    }

    /// Process the current segment and send the messages to the consumer
    pub async fn process_current_segment(&mut self) -> Result<StreamMessage> {
        // If we have a current segment, validate it
        if let Some(segment) = self.segment.as_ref() {
            let segment_id = self.current_segment_id.ok_or_else(|| {
                ReliableDispatchError::InvalidState(
                    "Segment ID not cached while processing segment".to_string(),
                )
            })?;

            // Validate current segment state
            if self.validate_segment_state(segment_id, segment).await? {
                self.move_to_next_segment().await?;
            }
        } else {
            // No current segment, move to next
            self.move_to_next_segment().await?;
        }

        // Process the next message - continue even if no messages available
        match self.process_next_message().await {
            Ok(msg) => Ok(msg),
            Err(ReliableDispatchError::NoMessagesAvailable) => {
                Err(ReliableDispatchError::NoMessagesAvailable)
            }
            Err(e) => Err(e),
        }
    }

    /// Validates the current segment. Returns `true` if the segment was invalidated or closed.
    pub(crate) async fn validate_segment_state(
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
        let segment_data = segment.read().await;

        // The conditions to move to the next segment are:
        // 1. The segment is closed
        // 2. All messages in the segment are acknowledged
        // 3. There is no pending ack message
        if segment_data.close_time > 0
            && self.acked_messages.len() == segment_data.messages.len()
            && self.pending_ack_message.is_none()
        {
            trace!("The subscription dispatcher id moving to the next segment, the current segment is closed and all messages consumed");
            return Ok(true);
        }

        Ok(false)
    }

    /// Moves to the next segment in the `TopicStore`.
    pub(crate) async fn move_to_next_segment(&mut self) -> Result<()> {
        dbg!(
            "Attempting to move from segment {} to next segment",
            self.current_segment_id.unwrap_or(1000)
        );

        // Update the last acknowledged segment
        if let Some(current_segment_id) = self.current_segment_id {
            self.last_acked_segment
                .store(current_segment_id, std::sync::atomic::Ordering::Release);
        }

        let next_segment = self
            .topic_store
            .get_next_segment(self.current_segment_id)
            .await?;

        if let Some(next_segment) = next_segment {
            let next_segment_id = {
                let segment_data = next_segment.read().await;
                dbg!(
                    "Found next segment {} with {} messages",
                    segment_data.id,
                    segment_data.messages.len()
                );
                segment_data.id
            };

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
                    let delay = match self.retry_count {
                        0 => tokio::time::Duration::from_secs(10),
                        1 => tokio::time::Duration::from_secs(20),
                        2 => tokio::time::Duration::from_secs(30),
                        _ => unreachable!(),
                    };

                    let now = tokio::time::Instant::now();
                    if let Some(last_retry) = self.last_retry_timestamp {
                        if now.duration_since(last_retry) < delay {
                            return Err(ReliableDispatchError::NoMessagesAvailable);
                        }
                    }

                    self.retry_count += 1;
                    self.last_retry_timestamp = Some(now);
                    self.send_message().await
                } else {
                    Err(ReliableDispatchError::MaxRetriesExceeded)
                }
            }
        }
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

            match next_message {
                Some(msg) => {
                    trace!("Sending message with id {:?}", msg.msg_id);
                    self.pending_ack_message = Some((msg.request_id, msg.msg_id.clone()));
                    Ok(msg)
                }
                None => Err(ReliableDispatchError::NoMessagesAvailable),
            }
        } else {
            Err(ReliableDispatchError::SegmentError(
                "No segment available".to_string(),
            ))
        }
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

                // Try to fetch the next message after acknowledgment
                match self.process_current_segment().await {
                    Ok(message) => {
                        dbg!("with message {}", &self.pending_ack_message);
                        return Ok(Some(message));
                    }
                    Err(ReliableDispatchError::NoMessagesAvailable) => {
                        dbg!("no more message {}", &self.pending_ack_message);
                        return Ok(None);
                    }
                    Err(e) => {
                        trace!(
                            "Error processing current segment after acknowledgment: {}",
                            e
                        );
                        dbg!(
                            "other errors while processing segment {}",
                            &self.pending_ack_message
                        );
                        return Ok(None);
                    }
                }
            } else {
                dbg!("Invalid acknowledgment: expected (request_id: {}, msg_id: {:?}), got (request_id: {}, msg_id: {:?})",
                    pending_request_id, pending_msg_id, request_id, &msg_id);
                // Received acknowledgment doesn't match the pending message
                return Err(ReliableDispatchError::AcknowledgmentError(
                format!(
                    "Invalid acknowledgment: expected (request_id: {}, msg_id: {:?}), got (request_id: {}, msg_id: {:?})",
                    pending_request_id, pending_msg_id, request_id, msg_id
                ),
            ));
            }
        } else {
            trace!(
                "Stray acknowledgment received for request_id {} and msg_id {:?}",
                request_id,
                msg_id
            );
            return Err(ReliableDispatchError::AcknowledgmentError(
                "No pending message to acknowledge".to_string(),
            ));
            // Handle stray acknowledgments (when there is no pending message) ?
            // self.acked_messages.insert(msg_id.clone(), request_id);
            // No pending message to process; return None
            //return Ok(None);
        }
    }
}

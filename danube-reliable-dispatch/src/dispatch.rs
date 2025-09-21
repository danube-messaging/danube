use danube_core::message::{MessageID, StreamMessage};
use danube_core::storage::{StartPosition, TopicStream};
use std::collections::HashMap;
use tokio_stream::StreamExt;
use tracing::trace;

use crate::{
    errors::{ReliableDispatchError, Result},
    topic_storage::TopicStore,
};

/// SubscriptionDispatch is holding information about consumers and the messages within a stream
/// It is used to dispatch messages to consumers and to track the progress of the consumer
pub struct SubscriptionDispatch {
    // Topic store used to create a streaming reader from persistent storage (WAL + Cloud)
    pub(crate) topic_store: TopicStore,
    // Streaming reader from persistent storage
    pub(crate) stream: Option<TopicStream>,
    // single message awaiting acknowledgment from the consumer
    pub(crate) pending_ack_message: Option<(u64, MessageID)>,
    // maps MessageID to request_id of segment acknowledged messages
    pub(crate) acked_messages: HashMap<MessageID, u64>,
    // retry count for the pending ack message
    retry_count: u8,
    last_retry_timestamp: Option<tokio::time::Instant>,
}

impl SubscriptionDispatch {
    pub(crate) fn new(topic_store: TopicStore) -> Self {
        Self {
            topic_store,
            stream: None,
            pending_ack_message: None,
            acked_messages: HashMap::new(),
            retry_count: 0,
            last_retry_timestamp: None,
        }
    }

    /// Poll next message from the persistent stream (WAL + Cloud handoff as needed)
    pub async fn poll_next(&mut self) -> Result<StreamMessage> {
        // Ensure a stream exists; start from beginning (offset 0) to read all messages
        if self.stream.is_none() {
            let s = self
                .topic_store
                .create_reader(StartPosition::Offset(0))
                .await
                .map_err(|e| ReliableDispatchError::StorageError(e.to_string()))?;
            self.stream = Some(s);
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
        // Ensure the stream exists
        if self.stream.is_none() {
            let s = self
                .topic_store
                .create_reader(StartPosition::Offset(0))
                .await
                .map_err(|e| ReliableDispatchError::StorageError(e.to_string()))?;
            self.stream = Some(s);
        }

        let stream = self.stream.as_mut().unwrap();
        match stream.next().await {
            Some(Ok(msg)) => {
                trace!("Sending message with id {:?}", msg.msg_id);
                self.pending_ack_message = Some((msg.request_id, msg.msg_id.clone()));
                Ok(msg)
            }
            Some(Err(_e)) => Err(ReliableDispatchError::StorageError(
                "stream error".to_string(),
            )),
            None => Err(ReliableDispatchError::NoMessagesAvailable),
        }
    }

    /// Handle the consumer message acknowledgement
    pub async fn handle_message_acked(
        &mut self,
        request_id: u64,
        msg_id: MessageID,
    ) -> Result<Option<StreamMessage>> {
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
                match self.process_next_message().await {
                    Ok(message) => {
                        return Ok(Some(message));
                    }
                    Err(ReliableDispatchError::NoMessagesAvailable) => {
                        return Ok(None);
                    }
                    Err(e) => {
                        trace!("Error processing next message after acknowledgment: {}", e);

                        return Ok(None);
                    }
                }
            } else {
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
        }
    }
}

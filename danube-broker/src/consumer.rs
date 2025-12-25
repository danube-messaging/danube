use anyhow::Result;
use danube_core::message::StreamMessage;
use metrics::counter;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;
use tracing::{trace, warn};

use crate::broker_metrics::{CONSUMER_BYTES_OUT_TOTAL, CONSUMER_MESSAGES_OUT_TOTAL};
use crate::utils::get_random_id;

/// Represents the session state for a consumer connection.
/// Tracks active status, cancellation token, and message receiver.
#[derive(Debug)]
pub(crate) struct ConsumerSession {
    /// Unique ID for this session (changes on reconnect/takeover)
    pub(crate) session_id: u64,
    /// Whether this consumer is currently active
    pub(crate) active: bool,
    /// Cancellation token for the streaming task
    pub(crate) cancellation: CancellationToken,
    /// Receiver for messages from dispatcher
    pub(crate) rx_cons: mpsc::Receiver<StreamMessage>,
}

impl ConsumerSession {
    /// Create a new session with the given receiver
    pub(crate) fn new(rx_cons: mpsc::Receiver<StreamMessage>) -> Self {
        Self {
            session_id: get_random_id(),
            active: true,
            cancellation: CancellationToken::new(),
            rx_cons,
        }
    }

    /// Takeover: cancel the current session and start a new one.
    /// Returns the new cancellation token for the streaming task.
    pub(crate) fn takeover(&mut self) -> CancellationToken {
        // Cancel the existing streaming task
        self.cancellation.cancel();

        // Create new session
        self.session_id = get_random_id();
        self.active = true;
        self.cancellation = CancellationToken::new();

        trace!(
            "Consumer session takeover: new session_id={}",
            self.session_id
        );
        self.cancellation.clone()
    }

    /// Mark this session as inactive (called on disconnect)
    #[allow(dead_code)]
    pub(crate) fn disconnect(&mut self) {
        self.active = false;
        trace!(
            "Consumer session disconnected: session_id={}",
            self.session_id
        );
    }

    /// Cancel the current streaming task without changing active status
    pub(crate) fn cancel_stream(&self) {
        self.cancellation.cancel();
    }
}

/// Represents a consumer connected and associated with a Subscription.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct Consumer {
    pub(crate) consumer_id: u64,
    pub(crate) consumer_name: String,
    pub(crate) subscription_type: i32,
    pub(crate) topic_name: String,
    pub(crate) subscription_name: String,
    pub(crate) tx_cons: mpsc::Sender<StreamMessage>,
    /// Unified session state (replaces separate status + cancellation token)
    pub(crate) session: Arc<Mutex<ConsumerSession>>,
}

impl Consumer {
    pub(crate) fn new(
        consumer_id: u64,
        consumer_name: &str,
        subscription_type: i32,
        topic_name: &str,
        subscription_name: &str,
        tx_cons: mpsc::Sender<StreamMessage>,
        session: Arc<Mutex<ConsumerSession>>,
    ) -> Self {
        Consumer {
            consumer_id: consumer_id.into(),
            consumer_name: consumer_name.into(),
            subscription_type,
            topic_name: topic_name.into(),
            subscription_name: subscription_name.into(),
            tx_cons,
            session,
        }
    }

    // The consumer task runs asynchronously, handling message delivery to the gRPC `ReceiverStream`.
    pub(crate) async fn send_message(&mut self, message: StreamMessage) -> Result<()> {
        // Since u8 is exactly 1 byte, the size in bytes will be equal to the number of elements in the vector.
        let payload_size = message.payload.len();
        // Send the message to the other channel
        if let Err(err) = self.tx_cons.send(message).await {
            // Log the error and handle the channel closure scenario
            warn!(
                "Failed to send message to consumer with id: {}. Error: {:?}",
                self.consumer_id, err
            );
        } else {
            trace!("Sending the message over channel to {}", self.consumer_id);
            counter!(CONSUMER_MESSAGES_OUT_TOTAL.name, "topic"=> self.topic_name.clone() , "subscription" => self.subscription_name.clone()).increment(1);
            counter!(CONSUMER_BYTES_OUT_TOTAL.name, "topic"=> self.topic_name.clone() , "subscription" => self.subscription_name.clone()).increment(payload_size as u64);
        }

        // info!("Consumer task ended for consumer_id: {}", self.consumer_id);
        Ok(())
    }

    /// Get the current active status of this consumer
    pub(crate) async fn get_status(&self) -> bool {
        self.session.lock().await.active
    }

    /// Set the consumer status to active
    pub(crate) async fn set_status_active(&self) {
        self.session.lock().await.active = true;
    }

    /// Set the consumer status to inactive
    pub(crate) async fn set_status_inactive(&self) {
        self.session.lock().await.active = false;
    }
}

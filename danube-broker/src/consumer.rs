use anyhow::Result;
use danube_core::message::StreamMessage;
use metrics::counter;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tracing::{trace, warn};

use crate::broker_metrics::{CONSUMER_BYTES_OUT_COUNTER, CONSUMER_MSG_OUT_COUNTER};

/// Represents a consumer connected and associated with a Subscription.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct Consumer {
    pub(crate) consumer_id: u64,
    pub(crate) consumer_name: String,
    pub(crate) subscription_type: i32,
    pub(crate) topic_name: String,
    pub(crate) tx_cons: mpsc::Sender<StreamMessage>,
    // status = true -> consumer OK, status = false -> Close the consumer
    pub(crate) status: Arc<Mutex<bool>>,
}

impl Consumer {
    pub(crate) fn new(
        consumer_id: u64,
        consumer_name: &str,
        subscription_type: i32,
        topic_name: &str,
        tx_cons: mpsc::Sender<StreamMessage>,
        status: Arc<Mutex<bool>>,
    ) -> Self {
        Consumer {
            consumer_id: consumer_id.into(),
            consumer_name: consumer_name.into(),
            subscription_type,
            topic_name: topic_name.into(),
            tx_cons,
            status,
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
            counter!(CONSUMER_MSG_OUT_COUNTER.name, "topic"=> self.topic_name.clone() , "consumer" => self.consumer_id.to_string()).increment(1);
            counter!(CONSUMER_BYTES_OUT_COUNTER.name, "topic"=> self.topic_name.clone() , "consumer" => self.consumer_id.to_string()).increment(payload_size as u64);
        }

        // info!("Consumer task ended for consumer_id: {}", self.consumer_id);
        Ok(())
    }

    pub(crate) async fn get_status(&self) -> bool {
        *self.status.lock().await
    }
}

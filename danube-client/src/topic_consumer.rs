use crate::{
    errors::{DanubeError, Result},
    retry_manager::{status_to_danube_error, RetryManager},
    ConsumerOptions, DanubeClient, SubType,
};

use tracing::warn;

use danube_core::message::MessageID;
use danube_core::proto::{
    consumer_service_client::ConsumerServiceClient, health_check_request::ClientType, AckRequest,
    AckResponse, ConsumerRequest, ReceiveRequest, StreamMessage,
};

use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use tokio_stream::Stream;
use tonic::{transport::Uri, Status};

/// Connection state for a topic consumer.
#[derive(Debug)]
enum ConsumerState {
    Disconnected,
    Ready {
        stream_client: ConsumerServiceClient<tonic::transport::Channel>,
        consumer_id: u64,
    },
}

/// Represents a Consumer
#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct TopicConsumer {
    // the Danube client
    client: DanubeClient,
    // the topic name, from where the messages are consumed
    topic_name: String,
    // the name of the Consumer
    consumer_name: String,
    // the name of the subscription the consumer is attached to
    subscription: String,
    // the type of the subscription, that can be Shared and Exclusive
    subscription_type: SubType,
    // other configurable options for the consumer
    consumer_options: ConsumerOptions,
    // unique identifier for every request sent by consumer
    request_id: AtomicU64,
    // connection state: Disconnected or Ready with stream_client + consumer_id
    state: ConsumerState,
    // the broker URI this consumer is connected to (avoids mutating shared client)
    broker_addr: Uri,
    // stop_signal received from broker, should close the consumer
    stop_signal: Arc<AtomicBool>,
    // unified reconnection manager
    retry_manager: RetryManager,
}

impl TopicConsumer {
    pub(crate) fn new(
        client: DanubeClient,
        topic_name: String,
        consumer_name: String,
        subscription: String,
        sub_type: Option<SubType>,
        consumer_options: ConsumerOptions,
    ) -> Self {
        let subscription_type = sub_type.unwrap_or(SubType::Shared);

        let retry_manager = RetryManager::new(
            consumer_options.max_retries,
            consumer_options.base_backoff_ms,
            consumer_options.max_backoff_ms,
        );

        let broker_addr = client.uri.clone();

        TopicConsumer {
            client,
            topic_name,
            consumer_name,
            subscription,
            subscription_type,
            consumer_options,
            request_id: AtomicU64::new(0),
            state: ConsumerState::Disconnected,
            broker_addr,
            stop_signal: Arc::new(AtomicBool::new(false)),
            retry_manager,
        }
    }

    /// Signal this consumer to stop background activities (e.g., health checks)
    pub(crate) fn stop(&self) {
        self.stop_signal.store(true, Ordering::SeqCst);
    }
    pub(crate) async fn subscribe(&mut self) -> Result<u64> {
        let mut attempts = 0;

        loop {
            match self.try_subscribe().await {
                Ok(consumer_id) => return Ok(consumer_id),
                Err(error) => {
                    if !self.retry_manager.is_retryable_error(&error) {
                        return Err(error);
                    }
                    attempts += 1;
                    if attempts > self.retry_manager.max_retries() {
                        return Err(error);
                    }
                    self.lookup_new_broker().await;
                    let backoff = self.retry_manager.calculate_backoff(attempts - 1);
                    tokio::time::sleep(backoff).await;
                }
            }
        }
    }

    /// Attempt a single subscribe RPC call: connect, register, and start health check.
    async fn try_subscribe(&mut self) -> Result<u64> {
        let mut stream_client = self.connect().await?;

        let consumer_request = ConsumerRequest {
            request_id: self.request_id.fetch_add(1, Ordering::SeqCst),
            topic_name: self.topic_name.clone(),
            consumer_name: self.consumer_name.clone(),
            subscription: self.subscription.clone(),
            subscription_type: self.subscription_type.clone() as i32,
        };

        let mut request = tonic::Request::new(consumer_request);
        RetryManager::insert_auth_token(&self.client, &mut request, &self.broker_addr).await?;

        let response = stream_client
            .subscribe(request)
            .await
            .map_err(|status| {
                if status.code() == tonic::Code::AlreadyExists {
                    warn!("consumer already exists, not allowed to create the same consumer twice");
                }
                status_to_danube_error(status)
            })?
            .into_inner();

        // Start health check
        let stop_signal = Arc::clone(&self.stop_signal);
        let _ = self
            .client
            .health_check_service
            .start_health_check(
                &self.broker_addr,
                ClientType::Consumer,
                response.consumer_id,
                stop_signal,
            )
            .await;

        // Transition to Ready state atomically
        self.state = ConsumerState::Ready {
            stream_client,
            consumer_id: response.consumer_id,
        };

        Ok(response.consumer_id)
    }

    /// Look up the current broker for this topic and update the connection target.
    async fn lookup_new_broker(&mut self) {
        if let Ok(new_addr) = self
            .client
            .lookup_service
            .handle_lookup(&self.broker_addr, &self.topic_name)
            .await
        {
            self.broker_addr = new_addr;
        }
    }

    // receive messages
    pub(crate) async fn receive(
        &mut self,
    ) -> Result<impl Stream<Item = std::result::Result<StreamMessage, Status>>> {
        let (stream_client, consumer_id) = match &mut self.state {
            ConsumerState::Ready {
                stream_client,
                consumer_id,
            } => (stream_client, *consumer_id),
            ConsumerState::Disconnected => {
                return Err(DanubeError::Unrecoverable(
                    "Receive: consumer is not connected".into(),
                ));
            }
        };

        let receive_request = ReceiveRequest {
            request_id: self.request_id.fetch_add(1, Ordering::SeqCst),
            consumer_id,
        };

        let mut request = tonic::Request::new(receive_request);
        RetryManager::insert_auth_token(&self.client, &mut request, &self.broker_addr).await?;

        let response = match stream_client.receive_messages(request).await {
            Ok(response) => response,
            Err(status) => {
                return Err(status_to_danube_error(status));
            }
        };
        Ok(response.into_inner())
    }

    pub(crate) async fn send_ack(
        &mut self,
        req_id: u64,
        msg_id: MessageID,
        subscription_name: &str,
    ) -> Result<AckResponse> {
        let stream_client = match &mut self.state {
            ConsumerState::Ready { stream_client, .. } => stream_client,
            ConsumerState::Disconnected => {
                return Err(DanubeError::Unrecoverable(
                    "SendAck: consumer is not connected".into(),
                ));
            }
        };

        let ack_request = AckRequest {
            request_id: req_id,
            msg_id: Some(msg_id.into()),
            subscription_name: subscription_name.to_string(),
        };

        let mut request = tonic::Request::new(ack_request);
        RetryManager::insert_auth_token(&self.client, &mut request, &self.broker_addr).await?;

        let response = match stream_client.ack(request).await {
            Ok(response) => response,
            Err(status) => {
                return Err(status_to_danube_error(status));
            }
        };
        Ok(response.into_inner())
    }

    pub(crate) fn get_topic_name(&self) -> &str {
        &self.topic_name
    }

    async fn connect(&self) -> Result<ConsumerServiceClient<tonic::transport::Channel>> {
        let grpc_cnx = self
            .client
            .cnx_manager
            .get_connection(&self.broker_addr, &self.broker_addr)
            .await?;
        Ok(ConsumerServiceClient::new(grpc_cnx.grpc_cnx.clone()))
    }
}

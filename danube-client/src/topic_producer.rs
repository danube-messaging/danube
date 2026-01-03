use crate::{
    errors::{DanubeError, Result},
    retry_manager::{status_to_danube_error, RetryManager},
    DanubeClient, ProducerOptions,
};
use danube_core::proto::{
    producer_service_client::ProducerServiceClient, DispatchStrategy as ProtoDispatchStrategy,
    ProducerAccessMode, ProducerRequest, SchemaReference, StreamMessage as ProtoStreamMessage,
};
use danube_core::{
    dispatch_strategy::ConfigDispatchStrategy,
    message::{MessageID, StreamMessage},
};

use tracing::warn;

use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use std::time::{SystemTime, UNIX_EPOCH};
use tonic::transport::Uri;

/// Represents a Producer
#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct TopicProducer {
    // the Danube client
    pub(crate) client: DanubeClient,
    // the topic name, used by the producer to publish messages
    pub(crate) topic: String,
    // the name of the producer
    producer_name: String,
    // unique identifier of the producer, provided by the Broker
    producer_id: Option<u64>,
    // unique identifier for every request sent by the producer
    request_id: AtomicU64,
    // optional schema reference for new schema registry
    schema_ref: Option<SchemaReference>,
    // resolved schema_id for message validation (cached)
    schema_id: Option<u64>,
    // resolved schema_version (cached)
    schema_version: Option<u32>,
    // the retention strategy for the topic
    dispatch_strategy: ConfigDispatchStrategy,
    // other configurable options for the producer
    producer_options: ProducerOptions,
    // the grpc client cnx
    stream_client: Option<ProducerServiceClient<tonic::transport::Channel>>,
    // stop_signal received from broker, should close the producer
    stop_signal: Arc<AtomicBool>,
    // unified reconnection manager
    retry_manager: RetryManager,
}

impl TopicProducer {
    pub(crate) fn new(
        client: DanubeClient,
        topic: String,
        producer_name: String,
        schema_ref: Option<SchemaReference>,
        dispatch_strategy: ConfigDispatchStrategy,
        producer_options: ProducerOptions,
    ) -> Self {
        let retry_manager = RetryManager::new(
            producer_options.max_retries,
            producer_options.base_backoff_ms,
            producer_options.max_backoff_ms,
        );

        TopicProducer {
            client,
            topic,
            producer_name,
            producer_id: None,
            request_id: AtomicU64::new(0),
            schema_ref,
            schema_id: None,
            schema_version: None,
            dispatch_strategy,
            producer_options,
            stream_client: None,
            stop_signal: Arc::new(AtomicBool::new(false)),
            retry_manager,
        }
    }
    pub(crate) async fn create(&mut self) -> Result<u64> {
        let mut attempts = 0;
        let max_retries = if self.producer_options.max_retries == 0 {
            5
        } else {
            self.producer_options.max_retries
        };

        loop {
            // Connect to current broker
            self.connect(&self.client.uri.clone()).await?;

            let producer_request = ProducerRequest {
                request_id: self.request_id.fetch_add(1, Ordering::SeqCst),
                producer_name: self.producer_name.clone(),
                topic_name: self.topic.clone(),
                schema_ref: self.schema_ref.clone(),
                producer_access_mode: ProducerAccessMode::Shared.into(),
                dispatch_strategy: match &self.dispatch_strategy {
                    ConfigDispatchStrategy::NonReliable => {
                        ProtoDispatchStrategy::NonReliable as i32
                    }
                    ConfigDispatchStrategy::Reliable => ProtoDispatchStrategy::Reliable as i32,
                },
            };

            let mut request = tonic::Request::new(producer_request);
            RetryManager::insert_auth_token(&self.client, &mut request, &self.client.uri).await?;

            let stream_client = self.stream_client.as_mut().unwrap();
            match stream_client.create_producer(request).await {
                Ok(resp) => {
                    let response = resp.into_inner();
                    self.producer_id = Some(response.producer_id);

                    // Start health check
                    let stop_signal = Arc::clone(&self.stop_signal);
                    let _ = self
                        .client
                        .health_check_service
                        .start_health_check(&self.client.uri, 0, response.producer_id, stop_signal)
                        .await;

                    // If producer has schema_ref, resolve schema metadata and cache it
                    if let Some(schema_ref) = self.schema_ref.clone() {
                        match self.resolve_schema_metadata(&schema_ref).await {
                            Ok((schema_id, schema_version)) => {
                                self.schema_id = Some(schema_id);
                                self.schema_version = Some(schema_version);
                                tracing::debug!(
                                    "Producer '{}' cached schema: id={}, version={}",
                                    self.producer_name,
                                    schema_id,
                                    schema_version
                                );
                            }
                            Err(e) => {
                                warn!(
                                    "Producer '{}' failed to resolve schema metadata: {}. Messages will not have schema info.",
                                    self.producer_name,
                                    e
                                );
                                // Don't fail producer creation, just log warning
                            }
                        }
                    }

                    return Ok(response.producer_id);
                }
                Err(status) => {
                    // Handle AlreadyExists specifically - producer already present on connection
                    if status.code() == tonic::Code::AlreadyExists {
                        warn!(
                            "The producer already exist, not allowed to create the same producer twice"
                        );
                        return Err(status_to_danube_error(status));
                    }

                    let error = status_to_danube_error(status);

                    if !self.retry_manager.is_retryable_error(&error) {
                        return Err(error);
                    }

                    attempts += 1;
                    if attempts > max_retries {
                        return Err(error);
                    }

                    // Perform lookup and backoff
                    if let Ok(new_addr) = self
                        .client
                        .lookup_service
                        .handle_lookup(&self.client.uri, &self.topic)
                        .await
                    {
                        self.client.uri = new_addr;
                    }

                    let backoff = self.retry_manager.calculate_backoff(attempts - 1);
                    tokio::time::sleep(backoff).await;
                }
            }
        }
    }

    // the Producer sends messages to the topic
    pub(crate) async fn send(
        &mut self,
        data: Vec<u8>,
        attributes: Option<HashMap<String, String>>,
    ) -> Result<u64> {
        let publish_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as u64;

        let attr = attributes.unwrap_or_default();

        let msg_id = MessageID {
            producer_id: self
                .producer_id
                .expect("Producer ID should be set before sending messages"),
            topic_name: self.topic.clone(),
            broker_addr: self.client.uri.to_string(),
            topic_offset: 0,
        };

        let send_message = StreamMessage {
            request_id: self.request_id.fetch_add(1, Ordering::SeqCst),
            msg_id,
            payload: data,
            publish_time,
            producer_name: self.producer_name.clone(),
            subscription_name: None,
            attributes: attr,
            schema_id: self.schema_id,
            schema_version: self.schema_version,
        };

        let req: ProtoStreamMessage = send_message.into();
        let mut request = tonic::Request::new(req);
        RetryManager::insert_auth_token(&self.client, &mut request, &self.client.uri).await?;

        let stream_client = self.stream_client.as_ref().ok_or_else(|| {
            DanubeError::Unrecoverable("Send: Stream client is not initialized".to_string())
        })?;

        let response = stream_client
            .clone()
            .send_message(request)
            .await
            .map_err(status_to_danube_error)?;
        Ok(response.into_inner().request_id)
    }

    pub(crate) async fn connect(&mut self, addr: &Uri) -> Result<()> {
        let grpc_cnx = self.client.cnx_manager.get_connection(addr, addr).await?;
        let client = ProducerServiceClient::new(grpc_cnx.grpc_cnx.clone());
        self.stream_client = Some(client);
        Ok(())
    }

    /// Resolve schema_id and version from schema registry
    ///
    /// This method queries the schema registry to get the latest schema metadata
    /// for the given schema subject. The metadata is cached and attached to all
    /// messages sent by this producer.
    ///
    /// Note: Creates a temporary SchemaRegistryClient. The connection is efficiently
    /// reused via the client's connection manager, so this is only done once during
    /// producer creation.
    async fn resolve_schema_metadata(
        &mut self,
        schema_ref: &SchemaReference,
    ) -> Result<(u64, u32)> {
        use crate::SchemaRegistryClient;

        let mut schema_client = SchemaRegistryClient::new(&self.client).await?;

        // Get latest schema for the subject
        let schema_response = schema_client.get_latest_schema(&schema_ref.subject).await?;

        Ok((schema_response.schema_id, schema_response.version))
    }
}

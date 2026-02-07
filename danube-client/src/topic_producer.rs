use crate::{
    errors::{DanubeError, Result},
    retry_manager::{status_to_danube_error, RetryManager},
    DanubeClient, ProducerOptions, SchemaRegistryClient,
};
use danube_core::proto::{
    health_check_request::ClientType, producer_service_client::ProducerServiceClient,
    schema_reference::VersionRef, DispatchStrategy as ProtoDispatchStrategy, ProducerAccessMode,
    ProducerRequest, SchemaReference, StreamMessage as ProtoStreamMessage,
};
use danube_core::{
    dispatch_strategy::ConfigDispatchStrategy,
    message::{MessageID, StreamMessage},
};
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use std::time::{SystemTime, UNIX_EPOCH};
use tonic::transport::Uri;
use tracing::warn;

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

        loop {
            match self.try_create().await {
                Ok(producer_id) => return Ok(producer_id),
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

    /// Attempt a single create_producer RPC call: connect, register, start health check,
    /// and resolve schema metadata if configured.
    async fn try_create(&mut self) -> Result<u64> {
        self.connect(&self.client.uri.clone()).await?;

        let producer_request = ProducerRequest {
            request_id: self.request_id.fetch_add(1, Ordering::SeqCst),
            producer_name: self.producer_name.clone(),
            topic_name: self.topic.clone(),
            schema_ref: self.schema_ref.clone(),
            producer_access_mode: ProducerAccessMode::Shared.into(),
            dispatch_strategy: match &self.dispatch_strategy {
                ConfigDispatchStrategy::NonReliable => ProtoDispatchStrategy::NonReliable as i32,
                ConfigDispatchStrategy::Reliable => ProtoDispatchStrategy::Reliable as i32,
            },
        };

        let mut request = tonic::Request::new(producer_request);
        RetryManager::insert_auth_token(&self.client, &mut request, &self.client.uri).await?;

        let stream_client = self.stream_client.as_mut().unwrap();
        let response = stream_client
            .create_producer(request)
            .await
            .map_err(|status| {
                if status.code() == tonic::Code::AlreadyExists {
                    warn!("producer already exists, not allowed to create the same producer twice");
                }
                status_to_danube_error(status)
            })?
            .into_inner();

        self.producer_id = Some(response.producer_id);

        // Start health check
        let stop_signal = Arc::clone(&self.stop_signal);
        let _ = self
            .client
            .health_check_service
            .start_health_check(
                &self.client.uri,
                ClientType::Producer,
                response.producer_id,
                stop_signal,
            )
            .await;

        // If producer has schema_ref, resolve schema metadata and cache it
        // IMPORTANT: Schema resolution errors must fail producer creation to ensure
        // that invalid versions are rejected early
        if let Some(schema_ref) = self.schema_ref.clone() {
            let (schema_id, schema_version) = self
                .resolve_schema_metadata(&schema_ref)
                .await
                .map_err(|e| {
                    DanubeError::Unrecoverable(format!(
                        "Failed to resolve schema for producer '{}': {}",
                        self.producer_name, e
                    ))
                })?;

            self.schema_id = Some(schema_id);
            self.schema_version = Some(schema_version);
            tracing::debug!(
                "Producer '{}' cached schema: id={}, version={}",
                self.producer_name,
                schema_id,
                schema_version
            );
        }

        Ok(response.producer_id)
    }

    /// Look up the current broker for this topic and update the connection target.
    async fn lookup_new_broker(&mut self) {
        if let Ok(new_addr) = self
            .client
            .lookup_service
            .handle_lookup(&self.client.uri, &self.topic)
            .await
        {
            self.client.uri = new_addr;
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
    /// This method queries the schema registry based on the version_ref:
    /// - PinnedVersion: Fetches the specific version
    /// - MinVersion: Fetches latest and validates it's >= min_version
    /// - UseLatest/None: Fetches the latest version
    ///
    /// The resolved metadata is cached and attached to all messages sent by this producer.
    ///
    /// Note: Creates a temporary SchemaRegistryClient. The connection is efficiently
    /// reused via the client's connection manager, so this is only done once during
    /// producer creation.
    async fn resolve_schema_metadata(
        &mut self,
        schema_ref: &SchemaReference,
    ) -> Result<(u64, u32)> {
        let mut schema_client = SchemaRegistryClient::new(&self.client).await?;

        // Resolve based on version_ref
        match &schema_ref.version_ref {
            Some(VersionRef::PinnedVersion(pinned_version)) => {
                tracing::debug!(
                    "Resolving pinned version {} for subject '{}'",
                    pinned_version,
                    schema_ref.subject
                );

                // First get latest to obtain schema_id for the subject
                let latest = schema_client.get_latest_schema(&schema_ref.subject).await?;

                // Validate pinned version exists (must be <= latest)
                if *pinned_version > latest.version {
                    return Err(DanubeError::Unrecoverable(format!(
                        "Pinned version {} does not exist for subject '{}'. Latest version is {}",
                        pinned_version, schema_ref.subject, latest.version
                    )));
                }

                // If pinned version is the latest, use it directly
                if *pinned_version == latest.version {
                    return Ok((latest.schema_id, latest.version));
                }

                // Otherwise fetch the specific pinned version
                let pinned_schema = schema_client
                    .get_schema_version(latest.schema_id, Some(*pinned_version))
                    .await?;

                Ok((pinned_schema.schema_id, pinned_schema.version))
            }

            Some(VersionRef::MinVersion(min_version)) => {
                tracing::debug!(
                    "Resolving minimum version {} for subject '{}'",
                    min_version,
                    schema_ref.subject
                );

                // Get latest schema
                let latest = schema_client.get_latest_schema(&schema_ref.subject).await?;

                // Validate latest version meets minimum requirement
                if latest.version < *min_version {
                    return Err(DanubeError::Unrecoverable(format!(
                        "Latest version {} does not meet minimum version requirement {} for subject '{}'",
                        latest.version, min_version, schema_ref.subject
                    )));
                }

                Ok((latest.schema_id, latest.version))
            }

            Some(VersionRef::UseLatest(_)) | None => {
                tracing::debug!(
                    "Resolving latest version for subject '{}'",
                    schema_ref.subject
                );

                // Get latest schema (default behavior)
                let latest = schema_client.get_latest_schema(&schema_ref.subject).await?;
                Ok((latest.schema_id, latest.version))
            }
        }
    }
}

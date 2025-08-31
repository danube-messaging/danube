use crate::{
    errors::{decode_error_details, DanubeError, Result},
    schema::Schema,
    DanubeClient, ProducerOptions,
};
use danube_core::proto::{
    producer_service_client::ProducerServiceClient, MessageResponse, ProducerAccessMode,
    ProducerRequest, ProducerResponse, StreamMessage as ProtoStreamMessage,
};
use danube_core::{
    dispatch_strategy::ConfigDispatchStrategy,
    message::{MessageID, StreamMessage},
};

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::{sleep, Duration};
use tonic::metadata::MetadataValue;
use tonic::{transport::Uri, Code, Response, Status};
use tracing::warn;
use rand::{thread_rng, Rng};

/// Represents a Producer
#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct TopicProducer {
    // the Danube client
    client: DanubeClient,
    // the topic name, used by the producer to publish messages
    topic: String,
    // the name of the producer
    producer_name: String,
    // unique identifier of the producer, provided by the Broker
    producer_id: Option<u64>,
    // unique identifier for every request sent by the producer
    request_id: AtomicU64,
    // the schema represent the message payload schema
    schema: Schema,
    // the retention strategy for the topic
    dispatch_strategy: ConfigDispatchStrategy,
    // other configurable options for the producer
    producer_options: ProducerOptions,
    // the grpc client cnx
    stream_client: Option<ProducerServiceClient<tonic::transport::Channel>>,
    // stop_signal received from broker, should close the producer
    stop_signal: Arc<AtomicBool>,
}

impl TopicProducer {
    pub(crate) fn new(
        client: DanubeClient,
        topic: String,
        producer_name: String,
        schema: Schema,
        dispatch_strategy: ConfigDispatchStrategy,
        producer_options: ProducerOptions,
    ) -> Self {
        TopicProducer {
            client,
            topic,
            producer_name,
            producer_id: None,
            request_id: AtomicU64::new(0),
            schema,
            dispatch_strategy,
            producer_options,
            stream_client: None,
            stop_signal: Arc::new(AtomicBool::new(false)),
        }
    }
    pub(crate) async fn create(&mut self) -> Result<u64> {
        // Initialize the gRPC client connection
        self.connect(&self.client.uri.clone()).await?;

        let producer_request = ProducerRequest {
            request_id: self.request_id.fetch_add(1, Ordering::SeqCst),
            producer_name: self.producer_name.clone(),
            topic_name: self.topic.clone(),
            schema: Some(self.schema.clone().into()),
            producer_access_mode: ProducerAccessMode::Shared.into(),
            dispatch_strategy: Some(self.dispatch_strategy.clone().into()),
        };

        let mut request = tonic::Request::new(producer_request);

        // Auth will be inserted per-attempt below

        let (max_retries, base_backoff_ms, max_backoff_ms) = self.retry_params();
        let mut attempts = 0usize;

        let mut broker_addr = self.client.uri.clone();

        // The loop construct continues to try the create_producer call
        // until it either succeeds in less max retries or fails with a different error.
        loop {
            let request = tonic::Request::new(request.get_ref().clone());

            let mut client = self.stream_client.as_mut().unwrap().clone();
            let response: std::result::Result<Response<ProducerResponse>, Status> =
                client.create_producer(request).await;

            match response {
                Ok(resp) => {
                    let response = resp.into_inner();
                    self.producer_id = Some(response.producer_id);

                    // start health_check service, which regularly check the status of the producer on the connected broker
                    let stop_signal = Arc::clone(&self.stop_signal);

                    let _ = self
                        .client
                        .health_check_service
                        .start_health_check(&broker_addr, 0, response.producer_id, stop_signal)
                        .await;

                    return Ok(response.producer_id);
                }
                Err(status) => {
                    let error_message = decode_error_details(&status);

                    if status.code() == Code::AlreadyExists {
                        // meaning that the producer is already present on the connection
                        // creating a producer with the same name is not allowed
                        return Err(DanubeError::FromStatus(status, error_message));
                    }

                    // Check retryable conditions: ServiceNotReady or transport retryables
                    let mut retryable = matches!(status.code(), Code::Unavailable | Code::DeadlineExceeded | Code::ResourceExhausted);
                    if let Some(error_m) = &error_message {
                        if error_m.error_type == 3 { // SERVICE_NOT_READY
                            retryable = true;
                        }
                    }

                    attempts += 1;
                    if !retryable || attempts > max_retries {
                        return Err(DanubeError::FromStatus(status, error_message));
                    }

                    // backoff with full jitter
                    let backoff = self.jittered_backoff(attempts - 1, base_backoff_ms, max_backoff_ms);
                    sleep(backoff).await;

                    match self
                        .client
                        .lookup_service
                        .handle_lookup(&broker_addr, &self.topic)
                        .await
                    {
                        Ok(addr) => {
                            broker_addr = addr.clone();
                            self.connect(&addr).await?;
                            // update the client URI with the latest connection
                            self.client.uri = addr;
                        }

                        Err(err) => {
                            if let Some(status) = err.extract_status() {
                                if let Some(error_message) = decode_error_details(status) {
                                    if error_message.error_type != 3 {
                                        return Err(DanubeError::FromStatus(
                                            status.to_owned(),
                                            Some(error_message),
                                        ));
                                    }
                                }
                            } else {
                                warn!("Lookup request failed with error:  {}", err);
                                return Err(DanubeError::Unrecoverable(format!(
                                    "Lookup failed with error: {}",
                                    err
                                )));
                            }
                        }
                    }
                }
            };
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

        let attr = if let Some(attributes) = attributes {
            attributes
        } else {
            HashMap::new()
        };

        let msg_id = MessageID {
            producer_id: self
                .producer_id
                .expect("Producer ID should be set before sending messages"),
            topic_name: self.topic.clone(),
            broker_addr: self.client.uri.to_string(),
            // this is default as the producer ignores this while sending messages
            // it is set on the broker side
            segment_id: 0,
            // this is default as the producer ignores this while sending messages
            // it is set on the broker side
            segment_offset: 0,
        };

        let send_message = StreamMessage {
            request_id: self.request_id.fetch_add(1, Ordering::SeqCst),
            msg_id: msg_id,
            payload: data,
            publish_time: publish_time,
            producer_name: self.producer_name.clone(),
            subscription_name: None,
            attributes: attr,
        };

        let req: ProtoStreamMessage = send_message.into();

        let (max_retries, base_backoff_ms, max_backoff_ms) = self.retry_params();
        let mut attempts = 0usize;
        let mut broker_addr = self.client.uri.clone();

        loop {
            let mut client = self.stream_client.as_ref().unwrap().clone();
            // Rebuild request each attempt
            let mut request = tonic::Request::new(req.clone());
            if let Some(api_key) = &self.client.cnx_manager.connection_options.api_key {
                self.insert_auth_token(&mut request, &self.client.uri, api_key).await?;
            }
            let response: std::result::Result<Response<MessageResponse>, Status> =
                client.send_message(request).await;

            match response {
                Ok(resp) => {
                    let response = resp.into_inner();
                    return Ok(response.request_id);
                }
                Err(status) => {
                    let error_message = decode_error_details(&status);
                    // retryable?
                    let mut retryable = matches!(status.code(), Code::Unavailable | Code::DeadlineExceeded | Code::ResourceExhausted);
                    if let Some(error_m) = &error_message {
                        if error_m.error_type == 3 { // SERVICE_NOT_READY
                            retryable = true;
                        }
                    }

                    attempts += 1;
                    if !retryable || attempts > max_retries {
                        return Err(DanubeError::FromStatus(status, error_message));
                    }

                    // backoff with jitter
                    let backoff = self.jittered_backoff(attempts - 1, base_backoff_ms, max_backoff_ms);
                    sleep(backoff).await;

                    // lookup and reconnect, then recreate producer id on that broker
                    match self
                        .client
                        .lookup_service
                        .handle_lookup(&broker_addr, &self.topic)
                        .await
                    {
                        Ok(addr) => {
                            broker_addr = addr.clone();
                            self.connect(&addr).await?;
                            // update the client URI with the latest connection
                            self.client.uri = addr;
                            // Re-create on new connection to obtain producer_id
                            let _ = self.create().await?;
                        }

                        Err(err) => {
                            if let Some(status) = err.extract_status() {
                                if let Some(error_message) = decode_error_details(status) {
                                    if error_message.error_type != 3 {
                                        return Err(DanubeError::FromStatus(
                                            status.to_owned(),
                                            Some(error_message),
                                        ));
                                    }
                                }
                            } else {
                                warn!("Lookup request failed with error:  {}", err);
                                return Err(DanubeError::Unrecoverable(format!(
                                    "Lookup failed with error: {}",
                                    err
                                )));
                            }
                        }
                    }
                }
            }
        }
    }

    async fn insert_auth_token<T>(
        &self,
        request: &mut tonic::Request<T>,
        addr: &Uri,
        api_key: &str,
    ) -> Result<()> {
        let token = self
            .client
            .auth_service
            .get_valid_token(addr, api_key)
            .await?;
        let token_metadata = MetadataValue::from_str(&format!("Bearer {}", token))
            .map_err(|_| DanubeError::InvalidToken)?;
        request
            .metadata_mut()
            .insert("authorization", token_metadata);
        Ok(())
    }

    async fn connect(&mut self, addr: &Uri) -> Result<()> {
        let grpc_cnx = self.client.cnx_manager.get_connection(addr, addr).await?;
        let client = ProducerServiceClient::new(grpc_cnx.grpc_cnx.clone());
        self.stream_client = Some(client);
        Ok(())
    }

    fn retry_params(&self) -> (usize, u64, u64) {
        let max_retries = if self.producer_options.max_retries == 0 {
            5
        } else {
            self.producer_options.max_retries
        }; // attempts beyond this are not allowed
        let base_backoff_ms = if self.producer_options.base_backoff_ms == 0 {
            200
        } else {
            self.producer_options.base_backoff_ms
        };
        let max_backoff_ms = if self.producer_options.max_backoff_ms == 0 {
            5_000
        } else {
            self.producer_options.max_backoff_ms
        };
        (max_retries, base_backoff_ms, max_backoff_ms)
    }

    fn jittered_backoff(&self, attempt: usize, base_ms: u64, cap_ms: u64) -> Duration {
        let pow = 1u64.checked_shl(attempt.min(16) as u32).unwrap_or(u64::MAX);
        let exp = base_ms.saturating_mul(pow);
        let backoff = exp.min(cap_ms);
        let jitter = thread_rng().gen_range(0..=backoff);
        Duration::from_millis(jitter)
    }
}

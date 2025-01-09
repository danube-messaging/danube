use crate::{
    dispatch_strategy::ConfigDispatchStrategy,
    errors::{decode_error_details, DanubeError, Result},
    message::{MessageID, StreamMessage},
    schema::Schema,
    DanubeClient, ProducerOptions,
};
use danube_core::proto::{
    producer_service_client::ProducerServiceClient, MessageResponse, ProducerAccessMode,
    ProducerRequest, ProducerResponse, StreamMessage as ProtoStreamMessage,
};

use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::{sleep, Duration};
use tonic::{transport::Uri, Code, Response, Status};
use tracing::warn;

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
    // it represents the sequence ID of the message within the topic
    message_sequence_id: AtomicU64,
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
            message_sequence_id: AtomicU64::new(0),
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

        let req = ProducerRequest {
            request_id: self.request_id.fetch_add(1, Ordering::SeqCst),
            producer_name: self.producer_name.clone(),
            topic_name: self.topic.clone(),
            schema: Some(self.schema.clone().into()),
            producer_access_mode: ProducerAccessMode::Shared.into(),
            dispatch_strategy: Some(self.dispatch_strategy.clone().into()),
        };

        let max_retries = 4;
        let mut attempts = 0;

        let mut broker_addr = self.client.uri.clone();

        // The loop construct continues to try the create_producer call
        // until it either succeeds in less max retries or fails with a different error.
        loop {
            let request = tonic::Request::new(req.clone());

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

                    attempts += 1;
                    if attempts >= max_retries {
                        return Err(DanubeError::FromStatus(status, error_message));
                    }

                    // if not a SERVICE_NOT_READY error received from broker returns
                    // else continue to loop as the topic may be in process to be assigned to a broker
                    if let Some(error_m) = &error_message {
                        if error_m.error_type != 3 {
                            return Err(DanubeError::FromStatus(status, error_message));
                        }
                    }

                    // as we are in SERVICE_NOT_READY case, let give some space to the broker to assign the topic
                    sleep(Duration::from_secs(2)).await;

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
        &self,
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
            sequence_id: self.message_sequence_id.fetch_add(1, Ordering::SeqCst),
            producer_id: self
                .producer_id
                .expect("Producer ID should be set before sending messages"),
            topic_name: self.topic.clone(),
            broker_addr: self.client.uri.to_string(),
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

        let mut client = self.stream_client.as_ref().unwrap().clone();
        let response: std::result::Result<Response<MessageResponse>, Status> =
            client.send_message(tonic::Request::new(req)).await;

        match response {
            Ok(resp) => {
                let response = resp.into_inner();
                return Ok(response.sequence_id);
            }
            // maybe some checks on the status, if anything can be handled by server
            Err(status) => {
                let decoded_message = decode_error_details(&status);
                return Err(DanubeError::FromStatus(status, decoded_message));
            }
        }
    }
    async fn connect(&mut self, addr: &Uri) -> Result<()> {
        let grpc_cnx = self.client.cnx_manager.get_connection(addr, addr).await?;
        let client = ProducerServiceClient::new(grpc_cnx.grpc_cnx.clone());
        self.stream_client = Some(client);
        Ok(())
    }
}

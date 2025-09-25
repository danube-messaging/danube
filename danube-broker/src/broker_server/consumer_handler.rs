use crate::broker_server::DanubeServerImpl;
use crate::message::AckMessage;
use crate::subscription::SubscriptionOptions;
use danube_core::proto::{
    consumer_service_server::ConsumerService, AckRequest, AckResponse, ConsumerRequest,
    ConsumerResponse, ReceiveRequest, StreamMessage,
};

use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};
use tracing::{info, trace, warn, Level};

#[tonic::async_trait]
impl ConsumerService for DanubeServerImpl {
    type ReceiveMessagesStream = ReceiverStream<Result<StreamMessage, Status>>;
    // CMD to create a new Consumer
    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn subscribe(
        &self,
        request: Request<ConsumerRequest>,
    ) -> Result<Response<ConsumerResponse>, tonic::Status> {
        let req = request.into_inner();

        info!(
            "Received consumer creation request - name: '{}', topic: '{}', type: '{}'",
            req.consumer_name, req.topic_name, req.subscription_type
        );

        // TODO! check if the subscription is authorized to consume from the topic (isTopicOperationAllowed)

        let service = self.service.as_ref();

        // the client is allowed to create the subscription only if the topic is served by this broker
        match service.get_topic(&req.topic_name, None, None, false).await {
            Ok(_) => trace!("topic_name: {} was found", &req.topic_name),
            Err(status) => {
                info!("Error topic request: {}", status.message());
                return Err(status);
            }
        }

        // Checks if the consumer exists and is connected
        if let Some(consumer_id) = service
            .check_if_consumer_exist(&req.consumer_name, &req.subscription, &req.topic_name)
            .await
        {
            let response = ConsumerResponse {
                request_id: req.request_id,
                consumer_id,
                consumer_name: req.consumer_name.clone(),
            };
            return Ok(tonic::Response::new(response));
        }

        // If the consumer doesn't exist, attempt to create it below

        // check if the topic policies allow the creation of the subscription
        if !service.allow_subscription_creation(&req.topic_name) {
            let status = Status::permission_denied(format!(
                "Not allowed to create the subscription for the topic: {}",
                &req.topic_name
            ));

            return Err(status);
        }

        let subscription_options = SubscriptionOptions {
            subscription_name: req.subscription,
            subscription_type: req.subscription_type,
            consumer_id: None,
            consumer_name: req.consumer_name.clone(),
        };

        let sub_name = subscription_options.subscription_name.clone();

        let consumer_id = service
            .subscribe_async(req.topic_name.clone(), subscription_options)
            .await
            .map_err(|err| {
                Status::permission_denied(format!(
                    "Not able to subscribe to the topic {} due to {}",
                    &req.topic_name, err
                ))
            })?;

        info!(
            "Consumer successfully created - ID: '{}', subscription: '{}'",
            consumer_id, sub_name
        );

        let response = ConsumerResponse {
            request_id: req.request_id,
            consumer_id: consumer_id,
            consumer_name: req.consumer_name,
        };

        Ok(tonic::Response::new(response))
    }

    // Stream of messages to Consumer
    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn receive_messages(
        &self,
        request: tonic::Request<ReceiveRequest>,
    ) -> std::result::Result<tonic::Response<Self::ReceiveMessagesStream>, tonic::Status> {
        let consumer_id = request.into_inner().consumer_id;

        // Create a new mpsc channel to stream messages to the client via gRPC
        let (grpc_tx, grpc_rx) = mpsc::channel(4); // Small buffer to trigger send failures quickly

        info!("Consumer {} is ready to receive messages", consumer_id);

        let service = self.service.as_ref();

        let rx = if let Some(consumer) = service.find_consumer_rx(consumer_id).await {
            consumer
        } else {
            let status = Status::not_found(format!(
                "The consumer with the id {} does not exist",
                consumer_id
            ));
            return Err(status);
        };

        // Cancel any existing streaming task for this consumer
        if let Some(consumer_info) = service.find_consumer_by_id(consumer_id).await {
            consumer_info.cancel_existing_stream().await;
            consumer_info.set_status_true().await; // Mark as active for new connection
        }

        let rx_cloned = Arc::clone(&rx);
        let service_for_disconnect = self.service.clone();
        
        // Create a new cancellation token for this streaming session
        let cancellation_token = CancellationToken::new();
        let token_for_task = cancellation_token.clone();

        // Store the cancellation token in the consumer info
        if let Some(consumer_info) = service.find_consumer_by_id(consumer_id).await {
            consumer_info.set_cancellation_token(cancellation_token).await;
        }

        tokio::spawn(async move {
            let mut rx_guard = rx_cloned.lock().await;

            loop {
                tokio::select! {
                    // Check for cancellation
                    _ = token_for_task.cancelled() => {
                        trace!("Streaming task cancelled for consumer_id: {}", consumer_id);
                        // Don't modify consumer status on cancellation - new connection might be active
                        break;
                    }
                    // Receive messages
                    message = rx_guard.recv() => {
                        match message {
                            Some(stream_message) => {
                                if grpc_tx.send(Ok(stream_message.into())).await.is_err() {
                                    // Error handling for when the client disconnects
                                    warn!(
                                        "Client disconnected for consumer_id: {}, marking inactive",
                                        consumer_id
                                    );

                                    // Mark the consumer as inactive on disconnect
                                    if let Some(consumer_info) = service_for_disconnect
                                        .find_consumer_by_id(consumer_id)
                                        .await
                                    {
                                        consumer_info.set_status_false().await;
                                    }
                                    break;
                                }
                            }
                            None => {
                                // Channel closed
                                break;
                            }
                        }
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(grpc_rx)))
    }

    // Consumer acknowledge the received message
    async fn ack(
        &self,
        request: tonic::Request<AckRequest>,
    ) -> std::result::Result<tonic::Response<AckResponse>, tonic::Status> {
        let ack_request = request.into_inner();
        let ack = AckMessage {
            request_id: ack_request.request_id,
            msg_id: ack_request.msg_id.unwrap().into(),
            subscription_name: ack_request.subscription_name,
        };

        let request_id = ack_request.request_id.clone();
        let msg_id = ack.msg_id.clone();

        trace!("Received ack request for message_id: {}", ack.msg_id);

        let service = self.service.as_ref();

        match service.ack_message_async(ack).await {
            Ok(()) => {
                trace!("Message with id: {} was acknowledged", msg_id);
                Ok(tonic::Response::new(AckResponse { request_id }))
            }
            Err(err) => {
                let status = Status::internal(format!("Error acknowledging message: {}", err));
                Err(status)
            }
        }
    }
}

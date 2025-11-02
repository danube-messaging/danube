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
use tonic::{Request, Response, Status};
use metrics::counter;
use crate::broker_metrics::BROKER_RPC_TOTAL;
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
        match service.get_topic(&req.topic_name, None, None).await {
            Ok(_) => trace!("topic_name: {} was found", &req.topic_name),
            Err(status) => {
                info!("Error topic request: {}", status.message());
                counter!(BROKER_RPC_TOTAL.name, "service"=>"consumer", "method"=>"subscribe", "result"=>"error").increment(1);
                return Err(status);
            }
        }

        // Checks if the consumer exists and is connected
        if let Some(consumer_id) = service
            .check_if_consumer_exist(&req.consumer_name, &req.subscription, &req.topic_name)
            .await
        {
            // Single-attach takeover at subscribe: cancel existing stream and prepare for new session
            if let Some(consumer_info) = service.find_consumer_by_id(consumer_id).await {
                // Cancel any existing streaming task for this consumer
                consumer_info.cancel_existing_stream().await;
                // Mark as active for the new connection
                consumer_info.set_status_true().await;
                // Reset dispatcher pending state and notify to resume polling (reliable mode)
                service.trigger_dispatcher_on_reconnect(consumer_id).await;
            }

            let response = ConsumerResponse {
                request_id: req.request_id,
                consumer_id,
                consumer_name: req.consumer_name.clone(),
            };
            return Ok(tonic::Response::new(response));
        }

        // If the consumer doesn't exist, attempt to create it below

        // check if the topic policies allow the creation of the subscription
        if !service.allow_subscription_creation(&req.topic_name).await {
            let status = Status::permission_denied(format!(
                "Not allowed to create the subscription for the topic: {}",
                &req.topic_name
            ));

            counter!(BROKER_RPC_TOTAL.name, "service"=>"consumer", "method"=>"subscribe", "result"=>"error").increment(1);
            return Err(status);
        }

        // Early policy check: max_consumers_per_topic
        if let Some(topic) = service.topic_worker_pool.get_topic(&req.topic_name) {
            let limit = topic
                .topic_policies
                .as_ref()
                .map(|p| p.get_max_consumers_per_topic())
                .unwrap_or(0);
            if limit > 0 {
                let current = topic.total_consumer_count().await as u32;
                if current >= limit {
                    return Err(Status::resource_exhausted(format!(
                        "Consumer limit per topic reached for {}. Current: {}, Limit: {}",
                        &req.topic_name, current, limit
                    )));
                }
            }
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
                counter!(BROKER_RPC_TOTAL.name, "service"=>"consumer", "method"=>"subscribe", "result"=>"error").increment(1);
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

        counter!(BROKER_RPC_TOTAL.name, "service"=>"consumer", "method"=>"subscribe", "result"=>"ok").increment(1);
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

        // Fetch both ConsumerInfo and its receiver in one lookup
        let (consumer_info, rx) = if let Some((info, rx)) = service.find_consumer_and_rx(consumer_id).await {
            (info, rx)
        } else {
            let status = Status::not_found(format!(
                "The consumer with the id {} does not exist",
                consumer_id
            ));
            counter!(BROKER_RPC_TOTAL.name, "service"=>"consumer", "method"=>"receive_messages", "result"=>"error").increment(1);
            return Err(status);
        };

        // Note: takeover is handled during subscribe(); here we only attach a new stream

        let rx_cloned = Arc::clone(&rx);
        let service_for_disconnect = self.service.clone();

        // Reset and store a new cancellation token for this streaming session
        let token_for_task = consumer_info.reset_session_token().await;

        tokio::spawn(async move {
            let mut rx_guard = rx_cloned.lock().await;

            loop {
                tokio::select! {
                    // If the gRPC response stream is dropped (client disconnected), mark inactive
                    _ = grpc_tx.closed() => {
                        warn!(
                            "Client disconnected for consumer_id: {}, marking inactive",
                            consumer_id
                        );
                        if let Some(consumer_info) = service_for_disconnect
                            .find_consumer_by_id(consumer_id)
                            .await
                        {
                            consumer_info.set_status_false().await;
                            // Reset dispatcher pending state so buffered messages can fail over/redeliver
                            service_for_disconnect.trigger_dispatcher_on_reconnect(consumer_id).await;
                        }
                        break;
                    }
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
                                        // Reset dispatcher pending state so buffered messages can fail over/redeliver
                                        service_for_disconnect.trigger_dispatcher_on_reconnect(consumer_id).await;
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

        counter!(BROKER_RPC_TOTAL.name, "service"=>"consumer", "method"=>"receive_messages", "result"=>"ok").increment(1);
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
                counter!(BROKER_RPC_TOTAL.name, "service"=>"consumer", "method"=>"ack", "result"=>"ok").increment(1);
                Ok(tonic::Response::new(AckResponse { request_id }))
            }
            Err(err) => {
                let status = Status::internal(format!("Error acknowledging message: {}", err));
                counter!(BROKER_RPC_TOTAL.name, "service"=>"consumer", "method"=>"ack", "result"=>"error").increment(1);
                Err(status)
            }
        }
    }
}

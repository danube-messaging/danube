use crate::broker_server::DanubeServerImpl;
use crate::message::AckMessage;
use crate::subscription::SubscriptionOptions;
use danube_core::proto::{
    consumer_service_server::ConsumerService, AckRequest, AckResponse, ConsumerRequest,
    ConsumerResponse, ReceiveRequest, StreamMessage,
};

use crate::broker_metrics::BROKER_RPC_TOTAL;
use metrics::counter;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::{debug, info, trace, warn, Level};

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
            consumer_name = %req.consumer_name,
            topic = %req.topic_name,
            subscription_type = %req.subscription_type,
            subscription = %req.subscription,
            "received consumer creation request"
        );

        // TODO! check if the subscription is authorized to consume from the topic (isTopicOperationAllowed)

        let service = self.service.as_ref();

        // the client is allowed to create the subscription only if the topic is served by this broker
        match service.get_topic(&req.topic_name, None, None).await {
            Ok(_) => trace!(topic = %req.topic_name, "topic found for consumer request"),
            Err(status) => {
                debug!(topic = %req.topic_name, error = %status.message(), "topic request failed");
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
            if let Some(consumer) = service.find_consumer_by_id(consumer_id).await {
                // Simplified takeover: cancel existing streaming task and mark active
                consumer.session.lock().await.cancel_stream();
                consumer.set_status_active().await;
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
            consumer_id = %consumer_id,
            consumer_name = %req.consumer_name,
            subscription = %sub_name,
            topic = %req.topic_name,
            "consumer successfully created"
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

        info!(consumer_id = %consumer_id, "consumer ready to receive messages");

        let service = self.service.as_ref();

        // Fetch Consumer (which contains session with rx_cons)
        let consumer = if let Some(cons) = service.find_consumer_for_streaming(consumer_id).await {
            cons
        } else {
            let status = Status::not_found(format!(
                "The consumer with the id {} does not exist",
                consumer_id
            ));
            counter!(BROKER_RPC_TOTAL.name, "service"=>"consumer", "method"=>"receive_messages", "result"=>"error").increment(1);
            return Err(status);
        };

        // Note: takeover is handled during subscribe(); here we only attach a new stream

        let rx_cons_cloned = Arc::clone(&consumer.rx_cons);
        let service_for_disconnect = self.service.clone();

        // Takeover: cancel old session and get new cancellation token
        let token_for_task = consumer.session.lock().await.takeover();

        tokio::spawn(async move {
            let mut rx_guard = rx_cons_cloned.lock().await;

            loop {
                tokio::select! {
                    // If the gRPC response stream is dropped (client disconnected), mark inactive
                    _ = grpc_tx.closed() => {
                        warn!(
                            consumer_id = %consumer_id,
                            "Client disconnected, marking consumer inactive"
                        );
                        if let Some(consumer) = service_for_disconnect
                            .find_consumer_by_id(consumer_id)
                            .await
                        {
                            consumer.set_status_inactive().await;
                            // Reset dispatcher pending state so buffered messages can fail over/redeliver
                            service_for_disconnect.trigger_dispatcher_on_reconnect(consumer_id).await;
                        }
                        break;
                    }
                    // Check for cancellation
                    _ = token_for_task.cancelled() => {
                        trace!(consumer_id = %consumer_id, "streaming task cancelled");
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
                                        consumer_id = %consumer_id,
                                        "client disconnected, marking consumer inactive"
                                    );

                                    // Mark the consumer as inactive on disconnect
                                    if let Some(consumer) = service_for_disconnect
                                        .find_consumer_by_id(consumer_id)
                                        .await
                                    {
                                        consumer.set_status_inactive().await;
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

        trace!(message_id = %ack.msg_id, "received ack request");

        let service = self.service.as_ref();

        match service.ack_message_async(ack).await {
            Ok(()) => {
                trace!(message_id = %msg_id, "message acknowledged");
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

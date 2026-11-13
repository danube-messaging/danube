use crate::broker_server::DanubeServerImpl;
use crate::message::{AckMessage, NackMessage};
use crate::subscription::SubscriptionOptions;
use danube_core::proto::{
    consumer_service_server::ConsumerService, AckRequest, AckResponse, CancelTaskTreeRequest,
    CancelTaskTreeResponse, ConsumerRequest, ConsumerResponse, ExtendVisibilityRequest,
    ExtendVisibilityResponse, NackRequest, NackResponse, ReceiveRequest, ReportProgressRequest,
    ReportProgressResponse, StreamMessage,
};

use crate::broker_metrics::BROKER_RPC_TOTAL;
use metrics::counter;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use crate::security::authz::{enforce_authorization, Permission, Resource};
use crate::security::authn::get_security_context;
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
        let security_context = get_security_context(&request)?;
        let mut req = request.into_inner();
        req.topic_name = crate::broker_service::normalize_topic_path(&req.topic_name)?;

        enforce_authorization(
            &security_context,
            &Resource::Topic(req.topic_name.clone()),
            Permission::Consume,
            &self.service.resources.security,
        ).await?;

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
        match service.get_topic(&req.topic_name, None, None, false).await {
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
                // Simplified takeover: cancel existing streaming task and prepare for new stream
                consumer.cancel_stream().await;
                consumer.set_status_inactive().await;
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
        if let Some(topic) = service.topic_registry.get_topic(&req.topic_name) {
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
            key_filters: req.key_filters,
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

        // Subscribe-time warning: KeyShared on partitioned topic
        if req.subscription_type == 3 && req.topic_name.contains("-part-") {
            warn!(
                topic = %req.topic_name,
                subscription = %sub_name,
                "KeyShared subscription on partitioned topic. \
                 Producers MUST use send_with_key() for per-key ordering guarantees."
            );
        }

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
        let security_context = get_security_context(&request)?;
        enforce_authorization(&security_context, &Resource::Cluster, Permission::Consume, &self.service.resources.security).await?;
        let consumer_id = request.into_inner().consumer_id;

        // Create a new mpsc channel to stream messages to the client via gRPC
        let (grpc_tx, grpc_rx) = mpsc::channel(4); // Small buffer to trigger send failures quickly

        info!(consumer_id = %consumer_id, "consumer ready to receive messages");

        let service = self.service.as_ref();

        // Fetch Consumer
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

        // Attach direct gRPC stream to consumer and obtain session cancellation token
        let (token_for_task, session_id) = consumer.attach_stream(grpc_tx.clone()).await;

        // Reset dispatcher pending state and wake dispatcher to begin streaming immediately
        self.service.trigger_dispatcher_on_reconnect(consumer_id).await;

        let service_for_disconnect = self.service.clone();

        // Spawn lightweight disconnect watcher task (zero CPU on message path)
        tokio::spawn(async move {
            tokio::select! {
                biased;
                _ = token_for_task.cancelled() => {
                    trace!(consumer_id = %consumer_id, session_id = %session_id, "streaming task cancelled");
                }
                _ = grpc_tx.closed() => {
                    warn!(
                        consumer_id = %consumer_id,
                        session_id = %session_id,
                        "Client disconnected, marking consumer inactive"
                    );
                    if let Some(consumer) = service_for_disconnect
                        .find_consumer_by_id(consumer_id)
                        .await
                    {
                        consumer.detach_stream_if_session(session_id).await;
                        service_for_disconnect.trigger_dispatcher_on_reconnect(consumer_id).await;
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
        let security_context = get_security_context(&request)?;
        enforce_authorization(&security_context, &Resource::Cluster, Permission::Consume, &self.service.resources.security).await?;
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

    async fn nack(
        &self,
        request: tonic::Request<NackRequest>,
    ) -> std::result::Result<tonic::Response<NackResponse>, tonic::Status> {
        let security_context = get_security_context(&request)?;
        enforce_authorization(&security_context, &Resource::Cluster, Permission::Consume, &self.service.resources.security).await?;
        let nack_request = request.into_inner();
        let nack = NackMessage {
            request_id: nack_request.request_id,
            msg_id: nack_request.msg_id.unwrap().into(),
            subscription_name: nack_request.subscription_name,
            delay_ms: nack_request.delay_ms,
            reason: nack_request.reason,
        };

        let request_id = nack.request_id;
        let msg_id = nack.msg_id.clone();

        trace!(message_id = %nack.msg_id, "received nack request");

        let service = self.service.as_ref();

        match service.nack_message_async(nack).await {
            Ok(()) => {
                trace!(message_id = %msg_id, "message negatively acknowledged");
                counter!(BROKER_RPC_TOTAL.name, "service"=>"consumer", "method"=>"nack", "result"=>"ok").increment(1);
                Ok(tonic::Response::new(NackResponse { request_id }))
            }
            Err(err) => {
                let status = Status::internal(format!("Error negatively acknowledging message: {}", err));
                counter!(BROKER_RPC_TOTAL.name, "service"=>"consumer", "method"=>"nack", "result"=>"error").increment(1);
                Err(status)
            }
        }
    }

    async fn extend_visibility(
        &self,
        request: tonic::Request<ExtendVisibilityRequest>,
    ) -> std::result::Result<tonic::Response<ExtendVisibilityResponse>, tonic::Status> {
        let security_context = get_security_context(&request)?;
        let req = request.into_inner();
        let msg_id = req
            .msg_id
            .ok_or_else(|| Status::invalid_argument("missing msg_id"))?;
        let topic_name = crate::broker_service::normalize_topic_path(&msg_id.topic_name)?;
        enforce_authorization(
            &security_context,
            &Resource::Topic(topic_name.clone()),
            Permission::Consume,
            &self.service.resources.security,
        )
        .await?;

        let new_deadline = self
            .service
            .extend_visibility_async(
                &topic_name,
                &req.subscription_name,
                msg_id.topic_offset,
                req.extension_ms,
            )
            .await
            .map_err(|e| Status::internal(format!("Error extending visibility: {}", e)))?;

        Ok(Response::new(ExtendVisibilityResponse {
            request_id: req.request_id,
            success: new_deadline.is_some(),
            new_deadline_ms: new_deadline.unwrap_or(0),
        }))
    }

    async fn report_progress(
        &self,
        request: tonic::Request<ReportProgressRequest>,
    ) -> std::result::Result<tonic::Response<ReportProgressResponse>, tonic::Status> {
        let security_context = get_security_context(&request)?;
        let req = request.into_inner();
        if req.checkpoint.len() > crate::dispatcher::pending_delivery::MAX_CHECKPOINT_BYTES {
            return Err(Status::invalid_argument("checkpoint exceeds 64 KiB limit"));
        }
        let msg_id = req
            .msg_id
            .ok_or_else(|| Status::invalid_argument("missing msg_id"))?;
        let topic_name = crate::broker_service::normalize_topic_path(&msg_id.topic_name)?;
        enforce_authorization(
            &security_context,
            &Resource::Topic(topic_name.clone()),
            Permission::Consume,
            &self.service.resources.security,
        )
        .await?;

        let accepted = self
            .service
            .report_progress_async(
                &topic_name,
                &req.subscription_name,
                msg_id.topic_offset,
                req.checkpoint,
                req.extension_ms,
            )
            .await
            .map_err(|e| Status::internal(format!("Error reporting progress: {}", e)))?;

        Ok(Response::new(ReportProgressResponse {
            request_id: req.request_id,
            accepted,
        }))
    }

    async fn cancel_task_tree(
        &self,
        request: tonic::Request<CancelTaskTreeRequest>,
    ) -> std::result::Result<tonic::Response<CancelTaskTreeResponse>, tonic::Status> {
        let security_context = get_security_context(&request)?;
        let req = request.into_inner();
        let topic_name = crate::broker_service::normalize_topic_path(&req.topic_name)?;
        enforce_authorization(
            &security_context,
            &Resource::Topic(topic_name.clone()),
            Permission::Consume,
            &self.service.resources.security,
        )
        .await?;

        let evicted = self
            .service
            .cancel_task_tree_async(&topic_name, req.root_msg_id)
            .await
            .map_err(|e| Status::internal(format!("Error cancelling task tree: {}", e)))?;

        Ok(Response::new(CancelTaskTreeResponse {
            request_id: req.request_id,
            cancelled: true,
            evicted_in_flight: evicted,
        }))
    }
}

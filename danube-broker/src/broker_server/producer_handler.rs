use crate::utils::get_random_id;
use crate::{broker_metrics::PRODUCER_SEND_LATENCY_MS, broker_server::DanubeServerImpl};
use danube_core::proto::{
    producer_service_server::ProducerService, DispatchStrategy as ProtoDispatchStrategy,
    MessageResponse, ProducerRequest, ProducerResponse, StreamMessage as ProtoStreamMessage,
};

use danube_core::dispatch_strategy::ConfigDispatchStrategy;
use danube_core::message::StreamMessage;
use metrics::histogram;
use std::time::Instant;
use tonic::{Request, Response, Status};
use tracing::{info, trace, Level};

#[tonic::async_trait]
impl ProducerService for DanubeServerImpl {
    // CMD to create a new Producer
    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn create_producer(
        &self,
        request: Request<ProducerRequest>,
    ) -> Result<Response<ProducerResponse>, tonic::Status> {
        let req = request.into_inner();

        let config_dispatch_strategy: ConfigDispatchStrategy =
            ProtoDispatchStrategy::try_from(req.dispatch_strategy)
                .unwrap_or_default()
                .into();

        let schema_log = match &req.schema {
            Some(s) => {
                let broker_schema: crate::schema::Schema = s.clone().into();
                broker_schema.to_string()
            }
            None => "None".to_string(),
        };

        info!(
            "Received producer creation request - name: '{}', topic: '{}', schema: '{}', dispatch: '{}'",
            req.producer_name,
            req.topic_name,
            schema_log,
            config_dispatch_strategy
            );
        let service = self.service.as_ref();

        let requested_strategy =
            <ProtoDispatchStrategy as core::convert::TryFrom<i32>>::try_from(req.dispatch_strategy)
                .ok();

        match service
            .get_topic(&req.topic_name, requested_strategy, req.schema)
            .await
        {
            Ok(_) => trace!("topic_name: {} was found", &req.topic_name),
            Err(status) => {
                info!("Error topic request: {}", status.message());
                return Err(status);
            }
        }

        //Todo! Here insert the auth/authz, check if it is authorized to perform the Topic Operation, add a producer

        // This check is on the local broker
        // If exist, the producer should be already created to the correct broker
        // as the above check with "get_topic" redirects the user to the broker that serve the topic
        //
        // should not throw an error here, even if the producer already exist,
        // as the server should handle producer reconnections and reuses gracefully
        if let Some(id) = service
            .check_if_producer_exist(&req.topic_name, &req.producer_name)
            .await
        {
            let response = ProducerResponse {
                request_id: req.request_id,
                producer_name: req.producer_name,
                producer_id: id,
            };

            return Ok(tonic::Response::new(response));
        }

        // Early policy check: max_producers_per_topic
        if let Some(topic) = service.topic_worker_pool.get_topic(&req.topic_name) {
            if let Err(e) = topic.can_add_producer().await {
                return Err(Status::resource_exhausted(e.to_string()));
            }
        }

        let new_producer_id = service
            .create_new_producer(
                &req.producer_name,
                get_random_id(),
                req.producer_access_mode,
                &req.topic_name,
            )
            .await
            .map_err(|err| {
                Status::permission_denied(format!("Not able to create the Producer: {}", err))
            })?;

        info!(
            "Producer '{}' successfully created with ID '{}'",
            req.producer_name, new_producer_id
        );

        let response = ProducerResponse {
            request_id: req.request_id,
            producer_name: req.producer_name,
            producer_id: new_producer_id,
        };

        Ok(tonic::Response::new(response))
    }

    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn send_message(
        &self,
        request: Request<ProtoStreamMessage>,
    ) -> Result<Response<MessageResponse>, tonic::Status> {
        let req = request.into_inner();
        let stream_message: StreamMessage = req.into();

        trace!(
            "New message {} from producer {} was received",
            stream_message.request_id,
            stream_message.msg_id.producer_id,
        );

        // Get the start time before sending the message
        let start_time = Instant::now();

        let service = self.service.as_ref();

        // check if the producer exist
        if !service
            .topic_manager
            .producers
            .contains(stream_message.msg_id.producer_id)
        {
            let status = Status::not_found(format!(
                "The producer with id {} does not exist",
                stream_message.msg_id.producer_id
            ));
            return Err(status);
        }

        let req_id = stream_message.request_id;
        let producer_id = stream_message.msg_id.producer_id;
        let topic_name = stream_message.msg_id.topic_name.clone();

        // Early policy check: max_message_size
        if let Some(topic) = service.topic_worker_pool.get_topic(&topic_name) {
            if let Err(e) = topic.validate_message_size(stream_message.payload.len()) {
                return Err(Status::invalid_argument(e.to_string()));
            }
        }

        // Use async message publishing through the performance-enhanced pipeline
        service
            .publish_message_async(topic_name, stream_message)
            .await
            .map_err(|err| {
                Status::permission_denied(format!("Unable to publish the message: {}", err))
            })?;

        // Measure the elapsed time in milliseconds
        let elapsed_ms = start_time.elapsed().as_secs_f64() * 1000.0;

        // Record the producer send latency (ms)
        histogram!(PRODUCER_SEND_LATENCY_MS.name, "producer" => producer_id.to_string())
            .record(elapsed_ms);

        let response = MessageResponse { request_id: req_id };

        Ok(tonic::Response::new(response))
    }
}

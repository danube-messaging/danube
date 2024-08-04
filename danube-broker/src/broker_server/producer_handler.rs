use crate::broker_server::DanubeServerImpl;
use crate::proto::{
    producer_service_server::ProducerService, MessageRequest, MessageResponse, ProducerRequest,
    ProducerResponse,
};

use std::collections::hash_map::Entry;
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

        info!(
            "New Producer request with name: {} for topic: {}",
            req.producer_name, req.topic_name,
        );

        let mut service = self.service.lock().await;

        match service.get_topic(&req.topic_name, req.schema, true).await {
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
        if let Some(id) =
            service.check_if_producer_exist(req.topic_name.clone(), req.producer_name.clone())
        {
            let response = ProducerResponse {
                request_id: req.request_id,
                producer_name: req.producer_name,
                producer_id: id,
            };

            return Ok(tonic::Response::new(response));
        }

        let new_producer_id = match service.create_new_producer(
            &req.producer_name,
            &req.topic_name,
            req.producer_access_mode,
        ) {
            Ok(prod_id) => prod_id,
            Err(err) => {
                let status = Status::permission_denied(format!(
                    "Not able to create the Producer: {}",
                    err.to_string(),
                ));
                return Err(status);
            }
        };

        info!(
            "The Producer with name: {} and with id: {}, has been created",
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
        request: Request<MessageRequest>,
    ) -> Result<Response<MessageResponse>, tonic::Status> {
        let req = request.into_inner();

        trace!(
            "New message {} from producer {} with metadata {:?} was received",
            req.request_id,
            req.producer_id,
            req.metadata
        );

        let arc_service = self.service.clone();
        let mut service = arc_service.lock().await;

        // check if the producer exist
        match service.producer_index.entry(req.producer_id) {
            Entry::Vacant(_) => {
                let status = Status::not_found(format!(
                    "The producer with id {} does not exist",
                    req.producer_id
                ));
                return Err(status);
            }
            Entry::Occupied(_) => (),
        };

        let topic = match service.find_topic_by_producer(req.producer_id) {
            Some(topic) => topic,
            None => {
                // Should not happen, as the Producer can only be created if it's associated with the Topic
                let status = Status::internal(format!(
                    "Unable to get the topic for the producer: {}",
                    req.producer_id,
                ));
                return Err(status);
            }
        };

        match topic
            .publish_message(req.producer_id, req.payload, req.metadata.clone())
            .await
        {
            Ok(_) => (),
            Err(err) => {
                let status = Status::permission_denied(format!(
                    "Unable to publish the message: {}",
                    err.to_string()
                ));
                return Err(status);
            }
        };

        let msg_seq_id: u64 = if let Some(msg_meta) = req.metadata {
            msg_meta.sequence_id
        } else {
            0
        };

        let response = MessageResponse {
            request_id: req.request_id,
            sequence_id: msg_seq_id,
        };

        Ok(tonic::Response::new(response))
    }
}

use crate::broker_server::DanubeServerImpl;
use crate::{broker_service::validate_topic_format, error_message::create_error_status};

use danube_core::proto::{
    discovery_server::Discovery, topic_lookup_response::LookupType, ErrorType, TopicLookupRequest,
    TopicLookupResponse, TopicPartitionsResponse,
};

use tonic::{Code, Request, Response};
use tracing::{debug, trace, Level};

#[tonic::async_trait]
impl Discovery for DanubeServerImpl {
    // finds topic to broker assignment
    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn topic_lookup(
        &self,
        request: Request<TopicLookupRequest>,
    ) -> std::result::Result<Response<TopicLookupResponse>, tonic::Status> {
        let req = request.into_inner();

        trace!(topic = %req.topic, "topic lookup request");

        // The topic format is /{namespace_name}/{topic_name}
        if !validate_topic_format(&req.topic) {
            let error_string = format!(
                "The topic: {} has an invalid format, should be: /namespace_name/topic_name",
                &req.topic
            );
            let status = create_error_status(
                Code::InvalidArgument,
                ErrorType::InvalidTopicName,
                &error_string,
                None,
            );
            return Err(status);
        }

        let service = self.service.as_ref();

        let result = match service.lookup_topic(&req.topic).await {
            Some((true, _)) => {
                trace!(
                    topic = %req.topic,
                    broker_addr = %self.broker_addr,
                    "topic lookup response: served by this broker"
                );
                (self.broker_addr.to_string(), LookupType::Connect)
            }
            Some((false, addr)) => {
                trace!(
                    topic = %req.topic,
                    broker_addr = %addr,
                    "topic lookup response: served by other broker"
                );
                (addr, LookupType::Redirect)
            }
            None => {
                debug!(topic = %req.topic, "topic lookup failed");
                let error_string = &format!("Unable to find the requested topic: {}", &req.topic);
                let status = create_error_status(
                    Code::InvalidArgument,
                    ErrorType::TopicNotFound,
                    error_string,
                    None,
                );
                return Err(status);
            }
        };

        let response = TopicLookupResponse {
            request_id: req.request_id,
            response_type: result.1.into(),
            broker_service_url: result.0,
        };

        Ok(tonic::Response::new(response))
    }

    // Retrieves the topic partitions names from the cluster
    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn topic_partitions(
        &self,
        request: Request<TopicLookupRequest>,
    ) -> std::result::Result<Response<TopicPartitionsResponse>, tonic::Status> {
        let req = request.into_inner();

        trace!(topic = %req.topic, "topic partitions request");

        // The topic format is /{namespace_name}/{topic_name}
        if !validate_topic_format(&req.topic) {
            let error_string = format!(
                "The topic: {} has an invalid format, should be: /namespace_name/topic_name",
                &req.topic
            );
            let status = create_error_status(
                Code::InvalidArgument,
                ErrorType::InvalidTopicName,
                &error_string,
                None,
            );
            return Err(status);
        }

        let service = self.service.as_ref();

        let result = service.topic_partitions(&req.topic).await;

        let response = TopicPartitionsResponse {
            request_id: req.request_id,
            partitions: result,
        };

        Ok(tonic::Response::new(response))
    }
}

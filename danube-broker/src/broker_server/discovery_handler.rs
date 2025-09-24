use crate::broker_server::DanubeServerImpl;
use crate::{broker_service::validate_topic_format, error_message::create_error_status};

use danube_core::proto::{
    discovery_server::Discovery, topic_lookup_response::LookupType, ErrorType, SchemaRequest,
    SchemaResponse, TopicLookupRequest, TopicLookupResponse, TopicPartitionsResponse,
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

        trace!("Topic Lookup request for topic: {}", req.topic);

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
                    "Topic Lookup response for topic: {} is served by this broker {}",
                    req.topic,
                    self.broker_addr
                );
                (self.broker_addr.to_string(), LookupType::Connect)
            }
            Some((false, addr)) => {
                trace!(
                    "Topic Lookup response for topic: {} is served by other broker {}",
                    req.topic,
                    addr
                );
                (addr, LookupType::Redirect)
            }
            None => {
                debug!("Topic Lookup response for topic: {} failed ", req.topic);
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

        trace!("Topic Lookup request for topic: {}", req.topic);

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

    // Retrieve message schema from Metadata Store
    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn get_schema(
        &self,
        request: Request<SchemaRequest>,
    ) -> std::result::Result<Response<SchemaResponse>, tonic::Status> {
        let req = request.into_inner();

        trace!("Schema Lookup for the topic: {}", req.topic);

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

        // if it's non-partitioned topic, then the Vec should contain only one element
        // if it's partitoned, then the Vec should contain more than one element
        // we are interested on the first element, as in the partitioned topic, all partitions should use the same schema
        let result = service.topic_partitions(&req.topic).await;

        // Check if the topic exists (has partitions)
        let first_partition = match result.get(0) {
            Some(partition) => partition,
            None => {
                let error_string = &format!("Unable to find the requested topic: {}", &req.topic);
                let status = create_error_status(
                    Code::NotFound,
                    ErrorType::TopicNotFound,
                    error_string,
                    None,
                );
                return Err(status);
            }
        };

        let proto_schema = service.get_schema(first_partition);

        // should I inform the client that the topic is not served by this broker ?
        // as the get_schema is local to this broker

        let response = SchemaResponse {
            request_id: req.request_id,
            schema: proto_schema,
        };

        Ok(tonic::Response::new(response))
    }
}

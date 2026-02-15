use crate::broker_server::DanubeServerImpl;
use crate::broker_service::validate_topic_format;

use danube_core::proto::{
    discovery_server::Discovery, topic_lookup_response::LookupType, TopicLookupRequest,
    TopicLookupResponse, TopicPartitionsResponse,
};

use tonic::{Request, Response, Status};
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
            return Err(Status::invalid_argument(format!(
                "The topic: {} has an invalid format, should be: /namespace_name/topic_name",
                &req.topic
            )));
        }

        let service = self.service.as_ref();

        let (broker_url, connect_url, proxy, lookup_type) =
            match service.lookup_topic(&req.topic).await {
                Some((true, _, _)) => {
                    trace!(
                        topic = %req.topic,
                        broker_url = %self.broker_url,
                        connect_url = %self.connect_url,
                        "topic lookup response: served by this broker"
                    );
                    (
                        self.broker_url.clone(),
                        self.connect_url.clone(),
                        self.proxy_enabled,
                        LookupType::Connect,
                    )
                }
                Some((false, broker_url, connect_url)) => {
                    let proxy = broker_url != connect_url;
                    trace!(
                        topic = %req.topic,
                        broker_url = %broker_url,
                        connect_url = %connect_url,
                        "topic lookup response: served by other broker"
                    );
                    (broker_url, connect_url, proxy, LookupType::Redirect)
                }
                None => {
                    debug!(topic = %req.topic, "topic lookup failed");
                    return Err(Status::not_found(format!(
                        "Unable to find the requested topic: {}",
                        &req.topic
                    )));
                }
            };

        let response = TopicLookupResponse {
            request_id: req.request_id,
            response_type: lookup_type.into(),
            connect_url,
            broker_url,
            proxy,
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
            return Err(Status::invalid_argument(format!(
                "The topic: {} has an invalid format, should be: /namespace_name/topic_name",
                &req.topic
            )));
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

use crate::{
    auth_service::AuthService,
    connection_manager::ConnectionManager,
    errors::{DanubeError, Result},
};

use danube_core::proto::{
    discovery_client::DiscoveryClient, topic_lookup_response::LookupType, TopicLookupRequest,
    TopicLookupResponse, TopicPartitionsResponse,
};
use std::sync::Arc;
use std::{
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
};
use tonic::metadata::MetadataValue;
use tonic::transport::Uri;
use tonic::{Response, Status};
use tracing::warn;

#[derive(Debug, Default)]
pub struct LookupResult {
    response_type: i32,
    addr: Uri,
}

#[derive(Debug, Clone)]
pub(crate) struct LookupService {
    cnx_manager: Arc<ConnectionManager>,
    auth_service: AuthService,
    // unique identifier for every request sent by LookupService
    request_id: Arc<AtomicU64>,
}

impl LookupService {
    pub fn new(cnx_manager: Arc<ConnectionManager>, auth_service: AuthService) -> Self {
        LookupService {
            cnx_manager,
            auth_service,
            request_id: Arc::new(AtomicU64::new(0)),
        }
    }
    pub(crate) async fn lookup_topic(
        &self,
        addr: &Uri,
        topic: impl Into<String>,
    ) -> Result<LookupResult> {
        let grpc_cnx = self.cnx_manager.get_connection(addr, addr).await?;

        let mut client = DiscoveryClient::new(grpc_cnx.grpc_cnx.clone());

        let lookup_request = TopicLookupRequest {
            request_id: self.request_id.fetch_add(1, Ordering::SeqCst),
            topic: topic.into(),
        };

        let mut request = tonic::Request::new(lookup_request);

        if let Some(api_key) = &self.cnx_manager.connection_options.api_key {
            self.insert_auth_token(&mut request, addr, api_key).await?;
        }

        let response: std::result::Result<Response<TopicLookupResponse>, Status> =
            client.topic_lookup(request).await;

        let mut lookup_result = LookupResult::default();

        match response {
            Ok(resp) => {
                let lookup_resp = resp.into_inner();
                let addr_to_uri = lookup_resp.broker_service_url.parse::<Uri>()?;

                lookup_result.response_type = lookup_resp.response_type;
                lookup_result.addr = addr_to_uri;
            }
            // maybe some checks on the status, if anything can be handled by server
            Err(status) => {
                return Err(DanubeError::FromStatus(status, None));
            }
        };

        Ok(lookup_result)
    }

    pub(crate) async fn topic_partitions(
        &self,
        addr: &Uri,
        topic: impl Into<String>,
    ) -> Result<Vec<String>> {
        let grpc_cnx = self.cnx_manager.get_connection(addr, addr).await?;

        let mut client = DiscoveryClient::new(grpc_cnx.grpc_cnx.clone());

        let lookup_request = TopicLookupRequest {
            request_id: self.request_id.fetch_add(1, Ordering::SeqCst),
            topic: topic.into(),
        };

        let mut request = tonic::Request::new(lookup_request);

        if let Some(api_key) = &self.cnx_manager.connection_options.api_key {
            self.insert_auth_token(&mut request, addr, api_key).await?;
        }

        let response: std::result::Result<Response<TopicPartitionsResponse>, Status> =
            client.topic_partitions(request).await;

        let topic_partitions = match response {
            Ok(resp) => {
                let lookup_resp = resp.into_inner();
                lookup_resp.partitions
            }
            Err(status) => {
                return Err(DanubeError::FromStatus(status, None));
            }
        };

        Ok(topic_partitions)
    }

    // for SERVICE_NOT_READY error received from broker retry the topic_lookup request
    // as the topic may be in process to be assigned to a broker in cluster
    pub(crate) async fn handle_lookup(&self, addr: &Uri, topic: &str) -> Result<Uri> {
        match self.lookup_topic(&addr, topic).await {
            Ok(lookup_result) => match lookup_type_from_i32(lookup_result.response_type) {
                Some(LookupType::Redirect) => Ok(lookup_result.addr),
                Some(LookupType::Connect) => Ok(addr.to_owned()),
                Some(LookupType::Failed) => {
                    todo!()
                }

                None => {
                    warn!("we shouldn't get to this lookup option");
                    Err(DanubeError::Unrecoverable(
                        "we shouldn't get to this lookup option".to_string(),
                    ))
                }
            },
            Err(err) => Err(err),
        }
    }

    async fn insert_auth_token(
        &self,
        request: &mut tonic::Request<TopicLookupRequest>,
        addr: &Uri,
        api_key: &str,
    ) -> Result<()> {
        let token = self.auth_service.get_valid_token(addr, api_key).await?;
        let token_metadata = MetadataValue::from_str(&format!("Bearer {}", token))
            .map_err(|_| DanubeError::InvalidToken)?;
        request
            .metadata_mut()
            .insert("authorization", token_metadata);
        Ok(())
    }
}

// A helper function to convert i32 to LookupType.
fn lookup_type_from_i32(value: i32) -> Option<LookupType> {
    match value {
        0 => Some(LookupType::Redirect),
        1 => Some(LookupType::Connect),
        2 => Some(LookupType::Failed),
        _ => None,
    }
}

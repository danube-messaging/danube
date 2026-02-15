use crate::{
    auth_service::AuthService,
    connection_manager::{BrokerAddress, ConnectionManager},
    errors::{DanubeError, Result},
};

use danube_core::proto::{
    discovery_client::DiscoveryClient, topic_lookup_response::LookupType, TopicLookupRequest,
    TopicLookupResponse, TopicPartitionsResponse,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tonic::transport::Uri;
use tonic::{Response, Status};
use tracing::warn;

#[derive(Debug, Default)]
pub struct LookupResult {
    /// The lookup response type (0 = Redirect, 1 = Connect, 2 = Failed)
    pub response_type: i32,
    /// The internal broker identity address
    pub broker_url: Uri,
    /// The client-facing connect address (may be proxy)
    pub connect_url: Uri,
    /// Whether connection goes through a proxy
    pub proxy: bool,
}

impl LookupResult {
    /// Returns true if the broker redirected to another broker
    pub fn is_redirect(&self) -> bool {
        self.response_type == 0 // LookupType::Redirect
    }

    /// Returns true if the client should connect to the original broker
    pub fn is_connect(&self) -> bool {
        self.response_type == 1 // LookupType::Connect
    }

    /// Returns the broker address (internal identity)
    pub fn broker_addr(&self) -> &Uri {
        &self.broker_url
    }
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
        self.auth_service
            .insert_token_if_needed(
                self.cnx_manager.connection_options.api_key.as_deref(),
                &mut request,
                addr,
            )
            .await?;

        let response: std::result::Result<Response<TopicLookupResponse>, Status> =
            client.topic_lookup(request).await;

        let mut lookup_result = LookupResult::default();

        match response {
            Ok(resp) => {
                let lookup_resp = resp.into_inner();

                lookup_result.response_type = lookup_resp.response_type;
                lookup_result.broker_url = lookup_resp.broker_url.parse::<Uri>()?;
                lookup_result.connect_url = lookup_resp.connect_url.parse::<Uri>()?;
                lookup_result.proxy = lookup_resp.proxy;
            }
            // maybe some checks on the status, if anything can be handled by server
            Err(status) => {
                return Err(DanubeError::FromStatus(status));
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
        self.auth_service
            .insert_token_if_needed(
                self.cnx_manager.connection_options.api_key.as_deref(),
                &mut request,
                addr,
            )
            .await?;

        let response: std::result::Result<Response<TopicPartitionsResponse>, Status> =
            client.topic_partitions(request).await;

        let topic_partitions = match response {
            Ok(resp) => {
                let lookup_resp = resp.into_inner();
                lookup_resp.partitions
            }
            Err(status) => {
                return Err(DanubeError::FromStatus(status));
            }
        };

        Ok(topic_partitions)
    }

    // for SERVICE_NOT_READY error received from broker retry the topic_lookup request
    // as the topic may be in process to be assigned to a broker in cluster
    pub(crate) async fn handle_lookup(&self, addr: &Uri, topic: &str) -> Result<BrokerAddress> {
        match self.lookup_topic(&addr, topic).await {
            Ok(lookup_result) => match lookup_type_from_i32(lookup_result.response_type) {
                Some(LookupType::Redirect) | Some(LookupType::Connect) => Ok(BrokerAddress {
                    connect_url: lookup_result.connect_url,
                    broker_url: lookup_result.broker_url,
                    proxy: lookup_result.proxy,
                }),
                Some(LookupType::Failed) => Err(DanubeError::Unrecoverable(
                    "Topic lookup failed: topic may not exist or cluster is unavailable"
                        .to_string(),
                )),

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

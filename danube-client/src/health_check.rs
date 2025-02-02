use crate::{
    auth_service::AuthService,
    connection_manager::{ConnectionManager, RpcConnection},
    errors::{DanubeError, Result},
};

use danube_core::proto::{
    health_check_client::HealthCheckClient, health_check_response::ClientStatus, HealthCheckRequest,
};
use std::str::FromStr;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use tokio::time::{sleep, Duration};
use tonic::metadata::MetadataValue;
use tonic::transport::Uri;
use tracing::warn;

// HealthCheckService is used to validate that the producer/consumer are still served by the connected broker
#[derive(Debug, Clone)]
pub(crate) struct HealthCheckService {
    cnx_manager: Arc<ConnectionManager>,
    auth_service: AuthService,
    // unique identifier for every request sent by LookupService
    request_id: Arc<AtomicU64>,
}

impl HealthCheckService {
    pub fn new(cnx_manager: Arc<ConnectionManager>, auth_service: AuthService) -> Self {
        HealthCheckService {
            cnx_manager,
            auth_service,
            request_id: Arc::new(AtomicU64::new(0)),
        }
    }

    // client_type could be producer or consumer,
    // client_id is the producer_id or the consumer_id provided by broker
    pub(crate) async fn start_health_check(
        &self,
        addr: &Uri,
        client_type: i32,
        client_id: u64,
        stop_signal: Arc<AtomicBool>,
    ) -> Result<()> {
        let grpc_cnx = self.cnx_manager.get_connection(addr, addr).await?;
        let stop_signal = Arc::clone(&stop_signal);
        let request_id = Arc::clone(&self.request_id);
        let api_key = self.cnx_manager.connection_options.api_key.clone();
        let addr = addr.clone();
        let auth_service = self.auth_service.clone();
        tokio::spawn(async move {
            loop {
                let stop_signal_clone = Arc::clone(&stop_signal);
                let request_id_clone = Arc::clone(&request_id);
                let grpc_cnx_clone = Arc::clone(&grpc_cnx);
                if let Err(e) = HealthCheckService::health_check(
                    request_id_clone,
                    grpc_cnx_clone,
                    client_type,
                    client_id,
                    stop_signal_clone,
                    api_key.as_deref(),
                    &addr,
                    auth_service.clone(),
                )
                .await
                {
                    warn!("Error in health check: {:?}", e);
                    break;
                }
                sleep(Duration::from_secs(5)).await;
            }
        });
        Ok(())
    }

    async fn health_check(
        request_id: Arc<AtomicU64>,
        grpc_cnx: Arc<RpcConnection>,
        client_type: i32,
        client_id: u64,
        stop_signal: Arc<AtomicBool>,
        api_key: Option<&str>,
        addr: &Uri,
        auth_service: AuthService,
    ) -> Result<()> {
        let health_request = HealthCheckRequest {
            request_id: request_id.fetch_add(1, Ordering::SeqCst),
            client: client_type,
            id: client_id,
        };

        let mut request = tonic::Request::new(health_request);

        if let Some(api_key) = api_key {
            HealthCheckService::insert_auth_token(
                &mut request,
                addr,
                api_key,
                auth_service.clone(),
            )
            .await?;
        }

        let mut client = HealthCheckClient::new(grpc_cnx.grpc_cnx.clone());

        match client.health_check(request).await {
            Ok(response) => {
                if response.get_ref().status == ClientStatus::Close as i32 {
                    warn!("Received stop signal from broker in health check response");
                    stop_signal.store(true, Ordering::Relaxed);
                    Ok(())
                } else {
                    Ok(())
                }
            }
            Err(status) => Err(DanubeError::FromStatus(status, None)),
        }
    }

    async fn insert_auth_token(
        request: &mut tonic::Request<HealthCheckRequest>,
        addr: &Uri,
        api_key: &str,
        auth_service: AuthService,
    ) -> Result<()> {
        let token = auth_service.get_valid_token(addr, api_key).await?;
        let token_metadata = MetadataValue::from_str(&format!("Bearer {}", token))
            .map_err(|_| DanubeError::InvalidToken)?;
        request
            .metadata_mut()
            .insert("authorization", token_metadata);
        Ok(())
    }
}

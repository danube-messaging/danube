use crate::{
    connection_manager::{ConnectionManager, RpcConnection},
    errors::{DanubeError, Result},
};

use danube_core::proto::{
    health_check_client::HealthCheckClient, health_check_response::ClientStatus, HealthCheckRequest,
};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use tokio::time::{sleep, Duration};
use tonic::transport::Uri;
use tracing::warn;

// HealthCheckService is used to validate that the producer/consumer are still served by the connected broker
#[derive(Debug, Clone)]
pub(crate) struct HealthCheckService {
    cnx_manager: Arc<ConnectionManager>,
    // unique identifier for every request sent by LookupService
    request_id: Arc<AtomicU64>,
}

impl HealthCheckService {
    pub fn new(cnx_manager: Arc<ConnectionManager>) -> Self {
        HealthCheckService {
            cnx_manager,
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
        tokio::spawn(async move {
            loop {
                let stop_signal_clone = Arc::clone(&stop_signal);
                let request_id_clone = Arc::clone(&request_id);
                let grpc_cnx_clone = Arc::clone(&grpc_cnx);
                if let Err(e) = health_check(
                    request_id_clone,
                    grpc_cnx_clone,
                    client_type,
                    client_id,
                    stop_signal_clone,
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
}

async fn health_check(
    request_id: Arc<AtomicU64>,
    grpc_cnx: Arc<RpcConnection>,
    client_type: i32,
    client_id: u64,
    stop_signal: Arc<AtomicBool>,
) -> Result<()> {
    let health_request = HealthCheckRequest {
        request_id: request_id.fetch_add(1, Ordering::SeqCst),
        client: client_type,
        id: client_id,
    };

    let request = tonic::Request::new(health_request);

    let mut client = HealthCheckClient::new(grpc_cnx.grpc_cnx.clone());

    match client.health_check(request).await {
        Ok(response) => {
            if response.get_ref().status == ClientStatus::Close as i32 {
                warn!("Received stop signal from broker in health check response");
                stop_signal.store(true, Ordering::Relaxed);
                Ok(())
                //self.notify.notify_waiters();
            } else {
                Ok(())
            }
        }
        Err(status) => Err(DanubeError::FromStatus(status, None)),
    }
}

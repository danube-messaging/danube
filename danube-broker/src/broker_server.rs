mod consumer_handler;
mod discovery_handler;
mod health_check_handler;
mod producer_handler;

use crate::broker_service::BrokerService;
use danube_core::proto::{
    consumer_service_server::ConsumerServiceServer, discovery_server::DiscoveryServer,
    health_check_server::HealthCheckServer, producer_service_server::ProducerServiceServer,
};

use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex};
use tokio::task::JoinHandle;
use tonic::transport::Server;
use tracing::{info, warn};

#[derive(Debug, Clone)]
pub(crate) struct DanubeServerImpl {
    service: Arc<Mutex<BrokerService>>,
    broker_addr: SocketAddr,
}

impl DanubeServerImpl {
    pub(crate) fn new(service: Arc<Mutex<BrokerService>>, broker_addr: SocketAddr) -> Self {
        DanubeServerImpl {
            service,
            broker_addr,
        }
    }
    pub(crate) async fn start(self, ready_tx: oneshot::Sender<()>) -> JoinHandle<()> {
        let socket_addr = self.broker_addr.clone();

        let server = Server::builder()
            .add_service(ProducerServiceServer::new(self.clone()))
            .add_service(ConsumerServiceServer::new(self.clone()))
            .add_service(DiscoveryServer::new(self.clone()))
            .add_service(HealthCheckServer::new(self))
            .serve(socket_addr);

        // Server has started
        let handle = tokio::spawn(async move {
            info!("Server is listening on address: {}", socket_addr);
            let _ = ready_tx.send(()); // Signal readiness
            if let Err(e) = server.await {
                warn!("Server error: {:?}", e);
            }
        });

        handle
    }
}

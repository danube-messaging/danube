mod auth_handler;
mod consumer_handler;
mod discovery_handler;
mod health_check_handler;
mod producer_handler;

use crate::auth::{AuthConfig, AuthMode};
use crate::auth_jwt::jwt_auth_interceptor;
use crate::broker_service::BrokerService;
use danube_core::proto::{
    auth_service_server::AuthServiceServer, consumer_service_server::ConsumerServiceServer,
    discovery_server::DiscoveryServer, health_check_server::HealthCheckServer,
    producer_service_server::ProducerServiceServer,
};

use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex};
use tokio::task::JoinHandle;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Server;
use tonic::transport::{server::ServerTlsConfig, Identity};
use tracing::{info, warn};

#[derive(Debug, Clone)]
pub(crate) struct DanubeServerImpl {
    service: Arc<Mutex<BrokerService>>,
    broker_addr: SocketAddr,
    auth: AuthConfig,
    // the api key is used to authenticate the user for JWT auth
    valid_api_keys: Vec<String>,
}

impl DanubeServerImpl {
    pub(crate) fn new(
        service: Arc<Mutex<BrokerService>>,
        broker_addr: SocketAddr,
        auth: AuthConfig,
    ) -> Self {
        DanubeServerImpl {
            service,
            broker_addr,
            auth,
            valid_api_keys: Vec::new(),
        }
    }

    pub(crate) async fn start(&self, ready_tx: oneshot::Sender<()>) -> JoinHandle<()> {
        let socket_addr = self.broker_addr.clone();
        let mut server_builder = Server::builder();

        if let AuthMode::Tls | AuthMode::TlsWithJwt = self.auth.mode {
            server_builder = self.configure_tls(server_builder).await;
        }

        let producer_service = ProducerServiceServer::new(self.clone());
        let consumer_service = ConsumerServiceServer::new(self.clone());
        let discovery_service = DiscoveryServer::new(self.clone());
        let health_check_service = HealthCheckServer::new(self.clone());
        let auth_service = AuthServiceServer::new(self.clone());

        let server_builder = if let AuthMode::TlsWithJwt = self.auth.mode {
            let jwt_config = self.auth.jwt.as_ref().expect("JWT config required");
            let jwt_secret = jwt_config.secret_key.clone();
            let interceptor = move |request| jwt_auth_interceptor(request, &jwt_secret);

            server_builder
                .add_service(InterceptedService::new(
                    producer_service,
                    interceptor.clone(),
                ))
                .add_service(InterceptedService::new(
                    consumer_service,
                    interceptor.clone(),
                ))
                .add_service(InterceptedService::new(
                    discovery_service,
                    interceptor.clone(),
                ))
                .add_service(InterceptedService::new(health_check_service, interceptor))
                .add_service(auth_service)
        } else {
            server_builder
                .add_service(producer_service)
                .add_service(consumer_service)
                .add_service(discovery_service)
                .add_service(health_check_service)
                .add_service(auth_service)
        };

        let server = server_builder.serve(socket_addr);

        self.spawn_server(server, socket_addr, ready_tx)
    }

    async fn configure_tls(&self, server: Server) -> Server {
        // Install crypto provider only when TLS is being configured
        if let Err(e) = rustls::crypto::ring::default_provider().install_default() {
            warn!("Failed to install crypto provider: {:?}", e);
            return server;
        }

        let tls_config = self.auth.tls.as_ref().expect("TLS config required");
        let cert = tokio::fs::read(&tls_config.cert_file).await.unwrap();
        let key = tokio::fs::read(&tls_config.key_file).await.unwrap();
        let identity = Identity::from_pem(cert, key);

        server
            .tls_config(ServerTlsConfig::new().identity(identity))
            .unwrap()
    }

    fn spawn_server(
        &self,
        server: impl futures::Future<Output = Result<(), tonic::transport::Error>> + Send + 'static,
        socket_addr: SocketAddr,
        ready_tx: oneshot::Sender<()>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            info!("Server is listening on address: {}", socket_addr);
            let _ = ready_tx.send(());
            if let Err(e) = server.await {
                warn!("Server error: {:?}", e);
            }
        })
    }
}

mod auth_handler;
mod consumer_handler;
mod discovery_handler;
mod health_check_handler;
mod producer_handler;
mod schema_registry_handler;

pub(crate) use schema_registry_handler::SchemaRegistryService;

use crate::broker_service::BrokerService;
use crate::security::authn::authenticate_request;
use crate::security::config::{AuthConfig, AuthMode};
use danube_core::proto::{
    auth_service_server::AuthServiceServer, consumer_service_server::ConsumerServiceServer,
    danube_schema::schema_registry_server::SchemaRegistryServer, discovery_server::DiscoveryServer,
    health_check_server::HealthCheckServer, producer_service_server::ProducerServiceServer,
};

use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Server;
use tonic::transport::{server::ServerTlsConfig, Certificate, Identity};
use tracing::warn;

#[derive(Debug, Clone)]
pub(crate) struct DanubeServerImpl {
    service: Arc<BrokerService>,
    schema_registry: Arc<SchemaRegistryService>,
    broker_addr: SocketAddr,
    broker_url: String,
    connect_url: String,
    proxy_enabled: bool,
    auth: AuthConfig,
}

impl DanubeServerImpl {
    pub(crate) fn new(
        service: Arc<BrokerService>,
        schema_registry: Arc<SchemaRegistryService>,
        broker_addr: SocketAddr,
        broker_url: String,
        connect_url: String,
        proxy_enabled: bool,
        auth: AuthConfig,
    ) -> Self {
        DanubeServerImpl {
            service,
            schema_registry,
            broker_addr,
            broker_url,
            connect_url,
            proxy_enabled,
            auth,
        }
    }

    pub(crate) async fn start(&self, ready_tx: oneshot::Sender<()>) -> JoinHandle<()> {
        let socket_addr = self.broker_addr.clone();
        let mut server_builder = Server::builder();

        if self.auth.mode == AuthMode::Tls {
            server_builder = self.configure_tls(server_builder).await;
        }

        let producer_service = ProducerServiceServer::new(self.clone());
        let consumer_service = ConsumerServiceServer::new(self.clone());
        let discovery_service = DiscoveryServer::new(self.clone());
        let health_check_service = HealthCheckServer::new(self.clone());
        let auth_service = AuthServiceServer::new(self.clone());
        let schema_registry_service =
            SchemaRegistryServer::new((*self.schema_registry.as_ref()).clone());

        let server_builder = if self.auth.mode != AuthMode::None {
            let auth = self.auth.clone();
            let interceptor = move |request| authenticate_request(request, &auth);

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
                .add_service(InterceptedService::new(
                    health_check_service,
                    interceptor.clone(),
                ))
                .add_service(auth_service)
                .add_service(InterceptedService::new(
                    schema_registry_service,
                    interceptor,
                ))
        } else {
            server_builder
                .add_service(producer_service)
                .add_service(consumer_service)
                .add_service(discovery_service)
                .add_service(health_check_service)
                .add_service(auth_service)
                .add_service(schema_registry_service)
        };

        let server = server_builder.serve(socket_addr);

        self.spawn_server(server, ready_tx)
    }

    async fn configure_tls(&self, server: Server) -> Server {
        // Install crypto provider if not already installed (ignore AlreadyInstalled errors)
        let _ = rustls::crypto::ring::default_provider().install_default();

        let tls_config = self.auth.tls.as_ref().expect("TLS config required");
        let cert = tokio::fs::read(&tls_config.cert_file).await.unwrap();
        let key = tokio::fs::read(&tls_config.key_file).await.unwrap();
        let identity = Identity::from_pem(cert, key);

        let mut tls = ServerTlsConfig::new().identity(identity);

        if tls_config.verify_client {
            if let Ok(ca_pem) = tokio::fs::read(&tls_config.ca_file).await {
                let ca = Certificate::from_pem(ca_pem);
                tls = tls.client_ca_root(ca);
            } else {
                warn!(ca_file = %tls_config.ca_file, "verify_client enabled but unable to read CA file");
            }
        }

        server.tls_config(tls).unwrap()
    }

    fn spawn_server(
        &self,
        server: impl futures::Future<Output = Result<(), tonic::transport::Error>> + Send + 'static,
        ready_tx: oneshot::Sender<()>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let _ = ready_tx.send(());
            if let Err(e) = server.await {
                warn!("Server error: {:?}", e);
            }
        })
    }
}

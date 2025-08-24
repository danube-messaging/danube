mod brokers_admin;
mod namespace_admin;
mod topics_admin;

use crate::{
    auth::{AuthConfig, AuthMode},
    broker_service::BrokerService,
    resources::Resources,
};
use danube_core::admin_proto::{
    broker_admin_server::BrokerAdminServer, namespace_admin_server::NamespaceAdminServer,
    topic_admin_server::TopicAdminServer,
};
use std::{net::SocketAddr, sync::Arc};
use tokio::task::JoinHandle;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};
use tracing::warn;

#[derive(Debug, Clone)]
pub(crate) struct DanubeAdminImpl {
    admin_addr: SocketAddr,
    broker_service: Arc<BrokerService>,
    resources: Resources,
    auth: AuthConfig,
}

impl DanubeAdminImpl {
    pub(crate) fn new(
        admin_addr: SocketAddr,
        broker_service: Arc<BrokerService>,
        resources: Resources,
        auth: AuthConfig,
    ) -> Self {
        DanubeAdminImpl {
            admin_addr,
            broker_service,
            resources,
            auth,
        }
    }
    pub(crate) async fn start(self) -> JoinHandle<()> {
        let socket_addr = self.admin_addr.clone();
        let mut server_builder = Server::builder();

        if let AuthMode::Tls | AuthMode::TlsWithJwt = self.auth.mode {
            server_builder = self.configure_tls(server_builder).await;
        }

        let server = server_builder
            .add_service(BrokerAdminServer::new(self.clone()))
            .add_service(NamespaceAdminServer::new(self.clone()))
            .add_service(TopicAdminServer::new(self))
            .serve(socket_addr);

        // Server has started
        let handle = tokio::spawn(async move {
            // info!("Admin is listening on address: {}", socket_addr);
            if let Err(e) = server.await {
                warn!("Server error: {:?}", e);
            }
        });

        handle
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

        // Base TLS config with server identity
        let mut tls = ServerTlsConfig::new().identity(identity);

        // If verify_client is enabled, load CA and require client auth
        if tls_config.verify_client {
            if let Ok(ca_pem) = tokio::fs::read(&tls_config.ca_file).await {
                let ca = Certificate::from_pem(ca_pem);
                tls = tls.client_ca_root(ca);
            } else {
                warn!("verify_client enabled but unable to read CA file: {}", tls_config.ca_file);
            }
        }

        server.tls_config(tls).unwrap()
    }
}

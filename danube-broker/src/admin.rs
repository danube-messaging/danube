mod brokers_admin;
mod cluster_admin;
mod namespace_admin;
mod topics_admin;

use crate::{
    auth::{AuthConfig, AuthMode},
    broker_server::SchemaRegistryService,
    broker_service::BrokerService,
    danube_service::LoadManager,
    resources::Resources,
};
use danube_core::admin_proto::{
    broker_admin_server::BrokerAdminServer, cluster_admin_server::ClusterAdminServer,
    namespace_admin_server::NamespaceAdminServer, topic_admin_server::TopicAdminServer,
};
use danube_core::proto::danube_schema::schema_registry_server::SchemaRegistryServer;
use danube_raft::leadership::LeadershipHandle;
use danube_raft::Raft;
use std::{net::SocketAddr, sync::Arc};
use tokio::task::JoinHandle;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};
use tracing::warn;

#[derive(Clone)]
pub(crate) struct DanubeAdminImpl {
    admin_addr: SocketAddr,
    broker_service: Arc<BrokerService>,
    resources: Resources,
    auth: AuthConfig,
    schema_registry: Arc<SchemaRegistryService>,
    load_manager: LoadManager,
    /// Raft handle for cluster membership operations.
    pub(crate) raft: Raft<danube_raft::typ::TypeConfig>,
    /// Leadership handle for querying current leader.
    pub(crate) leadership: LeadershipHandle,
    /// This node's advertised Raft transport address (for ClusterStatus discovery).
    pub(crate) raft_addr: String,
}

impl std::fmt::Debug for DanubeAdminImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DanubeAdminImpl")
            .field("admin_addr", &self.admin_addr)
            .field("leadership", &self.leadership)
            .finish_non_exhaustive()
    }
}

impl DanubeAdminImpl {
    pub(crate) fn new(
        admin_addr: SocketAddr,
        broker_service: Arc<BrokerService>,
        resources: Resources,
        auth: AuthConfig,
        schema_registry: Arc<SchemaRegistryService>,
        load_manager: LoadManager,
        raft: Raft<danube_raft::typ::TypeConfig>,
        leadership: LeadershipHandle,
        raft_addr: String,
    ) -> Self {
        DanubeAdminImpl {
            admin_addr,
            broker_service,
            resources,
            auth,
            schema_registry,
            load_manager,
            raft,
            leadership,
            raft_addr,
        }
    }
    pub(crate) async fn start(self) -> JoinHandle<()> {
        let socket_addr = self.admin_addr.clone();
        let mut server_builder = Server::builder();

        if let AuthMode::Tls | AuthMode::TlsWithJwt = self.auth.mode {
            server_builder = self.configure_tls(server_builder).await;
        }

        // Get schema registry service
        let schema_registry_service =
            SchemaRegistryServer::new((*self.schema_registry.as_ref()).clone());

        let server = server_builder
            .add_service(BrokerAdminServer::new(self.clone()))
            .add_service(ClusterAdminServer::new(self.clone()))
            .add_service(NamespaceAdminServer::new(self.clone()))
            .add_service(TopicAdminServer::new(self.clone()))
            .add_service(schema_registry_service)
            .serve(socket_addr);

        // Server has started
        let handle = tokio::spawn(async move {
            // info!("Admin is listening on address: {}", socket_addr);
            if let Err(e) = server.await {
                warn!(error = ?e, "admin server error");
            }
        });

        handle
    }
    async fn configure_tls(&self, server: Server) -> Server {
        // Install crypto provider if not already installed (ignore AlreadyInstalled errors)
        let _ = rustls::crypto::ring::default_provider().install_default();

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
                warn!(ca_file = %tls_config.ca_file, "verify_client enabled but unable to read CA file");
            }
        }

        server.tls_config(tls).unwrap()
    }
}

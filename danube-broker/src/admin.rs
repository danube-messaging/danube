mod brokers_admin;
mod namespace_admin;
mod topics_admin;

use crate::{broker_service::BrokerService, resources::Resources};
use danube_core::admin_proto::{
    broker_admin_server::BrokerAdminServer, namespace_admin_server::NamespaceAdminServer,
    topic_admin_server::TopicAdminServer,
};
use std::{net::SocketAddr, sync::Arc};
use tokio::{sync::Mutex, task::JoinHandle};
use tonic::transport::Server;
use tracing::{info, warn};

#[derive(Debug, Clone)]
pub(crate) struct DanubeAdminImpl {
    admin_addr: SocketAddr,
    broker_service: Arc<Mutex<BrokerService>>,
    resources: Resources,
}

impl DanubeAdminImpl {
    pub(crate) fn new(
        admin_addr: SocketAddr,
        broker_service: Arc<Mutex<BrokerService>>,
        resources: Resources,
    ) -> Self {
        DanubeAdminImpl {
            admin_addr,
            broker_service,
            resources,
        }
    }
    pub(crate) async fn start(self) -> JoinHandle<()> {
        let socket_addr = self.admin_addr.clone();

        let server = Server::builder()
            .add_service(BrokerAdminServer::new(self.clone()))
            .add_service(NamespaceAdminServer::new(self.clone()))
            .add_service(TopicAdminServer::new(self))
            .serve(socket_addr);

        // Server has started
        let handle = tokio::spawn(async move {
            info!("Admin is listening on address: {}", socket_addr);
            if let Err(e) = server.await {
                warn!("Server error: {:?}", e);
            }
        });

        handle
    }
}

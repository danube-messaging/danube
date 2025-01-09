use crate::admin::DanubeAdminImpl;
use danube_core::admin_proto::{
    broker_admin_server::BrokerAdmin, BrokerInfo, BrokerListResponse, BrokerResponse, Empty,
    NamespaceListResponse,
};

use tonic::{Request, Response};
use tracing::{trace, Level};

#[tonic::async_trait]
impl BrokerAdmin for DanubeAdminImpl {
    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn list_brokers(
        &self,
        _request: Request<Empty>,
    ) -> std::result::Result<Response<BrokerListResponse>, tonic::Status> {
        trace!("Admin: list brokers command");

        let mut brokers_info = Vec::new();

        let brokers = self.resources.cluster.get_brokers().await;

        for broker_id in brokers {
            if let Some((broker_id, broker_addr, broker_role)) =
                self.resources.cluster.get_broker_info(&broker_id)
            {
                let broker_info = BrokerInfo {
                    broker_id,
                    broker_addr,
                    broker_role,
                };
                brokers_info.push(broker_info);
            }
        }

        let response = BrokerListResponse {
            brokers: brokers_info,
        };

        Ok(tonic::Response::new(response))
    }

    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn get_leader_broker(
        &self,
        _request: Request<Empty>,
    ) -> std::result::Result<Response<BrokerResponse>, tonic::Status> {
        trace!("Admin: get leader broker command");

        let leader = if let Some(lead) = self.resources.cluster.get_cluster_leader() {
            lead.to_string()
        } else {
            "not_found".to_string()
        };

        let response = BrokerResponse { leader };
        Ok(tonic::Response::new(response))
    }

    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn list_namespaces(
        &self,
        _request: Request<Empty>,
    ) -> std::result::Result<Response<NamespaceListResponse>, tonic::Status> {
        trace!("Admin: get cluster namespaces command");

        let namespaces = self.resources.cluster.get_namespaces().await;

        let response = NamespaceListResponse { namespaces };
        Ok(tonic::Response::new(response))
    }
}

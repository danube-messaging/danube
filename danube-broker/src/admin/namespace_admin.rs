use crate::admin::DanubeAdminImpl;
use danube_core::admin_proto::{
    namespace_admin_server::NamespaceAdmin, NamespaceRequest, NamespaceResponse, PolicyResponse,
    TopicListResponse,
};

use tonic::{Request, Response, Status};
use tracing::{trace, Level};

#[tonic::async_trait]
impl NamespaceAdmin for DanubeAdminImpl {
    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn get_namespace_topics(
        &self,
        request: Request<NamespaceRequest>,
    ) -> std::result::Result<Response<TopicListResponse>, tonic::Status> {
        trace!("Admin: get the list of topics of a namespace");

        let req = request.into_inner();

        let topics = self
            .resources
            .namespace
            .get_topics_for_namespace(&req.name)
            .await;

        let response = TopicListResponse { topics };
        Ok(tonic::Response::new(response))
    }

    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn get_namespace_policies(
        &self,
        request: Request<NamespaceRequest>,
    ) -> std::result::Result<Response<PolicyResponse>, tonic::Status> {
        trace!("Admin: get the configuration policies of a namespace");

        let req = request.into_inner();

        let policies = match self.resources.namespace.get_policies(&req.name) {
            Ok(policies) => policies,
            Err(err) => {
                trace!(
                    "Unable to retrieve polices for the namespace {} due to {}",
                    req.name,
                    err
                );
                let status = Status::not_found(format!(
                    "Unable to retrieve polices for the namespace {} due to {}",
                    req.name, err
                ));
                return Err(status);
            }
        };

        let serialized = match serde_json::to_string(&policies) {
            Ok(serialized) => serialized,
            Err(err) => {
                let status = Status::internal(format!("Unable to serialize the policies: {}", err));
                return Err(status);
            }
        };

        let response = PolicyResponse {
            policies: serialized,
        };
        Ok(tonic::Response::new(response))
    }

    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn create_namespace(
        &self,
        request: Request<NamespaceRequest>,
    ) -> std::result::Result<Response<NamespaceResponse>, tonic::Status> {
        trace!("Admin: create a new namespace");

        let req = request.into_inner();
        let service = self.broker_service.as_ref();

        let success = match service.create_namespace_if_absent(&req.name).await {
            Ok(()) => true,
            Err(err) => {
                let status = Status::not_found(format!(
                    "Unable to create the namespace {} due to {}",
                    req.name, err
                ));
                return Err(status);
            }
        };

        let response = NamespaceResponse { success };
        Ok(tonic::Response::new(response))
    }

    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn delete_namespace(
        &self,
        request: Request<NamespaceRequest>,
    ) -> std::result::Result<Response<NamespaceResponse>, tonic::Status> {
        trace!("Admin: delete the namespace");

        let req = request.into_inner();
        let service = self.broker_service.as_ref();

        let success = match service.delete_namespace(&req.name).await {
            Ok(()) => true,
            Err(err) => {
                let status = Status::not_found(format!(
                    "Unable to delete the namespace {} due to {}",
                    req.name, err
                ));
                return Err(status);
            }
        };

        let response = NamespaceResponse { success };
        Ok(tonic::Response::new(response))
    }
}

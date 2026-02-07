use crate::broker_server::DanubeServerImpl;
use danube_core::proto::{
    health_check_request::ClientType, health_check_response::ClientStatus,
    health_check_server::HealthCheck, HealthCheckRequest, HealthCheckResponse,
};

use tonic::{Request, Response, Status};
use tracing::{trace, Level};

#[tonic::async_trait]
impl HealthCheck for DanubeServerImpl {
    // finds topic to broker assignment
    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn health_check(
        &self,
        request: Request<HealthCheckRequest>,
    ) -> std::result::Result<Response<HealthCheckResponse>, tonic::Status> {
        let req = request.into_inner();

        trace!(client_type = %req.client, client_id = %req.id, "health check received");

        let mut client_status = ClientStatus::Ok;

        if req.client == ClientType::Producer as i32 {
            let service = self.service.as_ref();

            if !service.health_producer(req.id).await {
                client_status = ClientStatus::Close;
            }
        } else if req.client == ClientType::Consumer as i32 {
            let service = self.service.as_ref();

            if !service.health_consumer(req.id).await {
                client_status = ClientStatus::Close;
            }
        } else {
            return Err(Status::invalid_argument("Invalid client type"));
        }

        let response = HealthCheckResponse {
            status: client_status as i32,
        };

        Ok(tonic::Response::new(response))
    }
}

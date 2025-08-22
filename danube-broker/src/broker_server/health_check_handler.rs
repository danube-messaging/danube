use crate::broker_server::DanubeServerImpl;
use crate::error_message::create_error_status;
use danube_core::proto::{
    health_check_request::ClientType, health_check_response::ClientStatus,
    health_check_server::HealthCheck, ErrorType, HealthCheckRequest, HealthCheckResponse,
};

use tonic::{Code, Request, Response};
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

        trace!("Health check from {} with id {}", req.client, req.id);

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
            let error_string = "Invalid client type";
            let status = create_error_status(
                Code::InvalidArgument,
                ErrorType::UnknownError,
                error_string,
                None,
            );
            return Err(status);
        }

        let response = HealthCheckResponse {
            status: client_status as i32,
        };

        Ok(tonic::Response::new(response))
    }
}

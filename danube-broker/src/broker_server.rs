mod consumer_handler;
mod discovery_handler;
mod health_check_handler;
mod producer_handler;

use crate::auth::{AuthConfig, AuthMode};
use crate::broker_service::BrokerService;
use danube_core::proto::{
    consumer_service_server::ConsumerServiceServer, discovery_server::DiscoveryServer,
    health_check_server::HealthCheckServer, producer_service_server::ProducerServiceServer,
};

use jsonwebtoken::{decode, DecodingKey, Validation};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex};
use tokio::task::JoinHandle;
use tonic::transport::Server;
use tonic::transport::{server::ServerTlsConfig, Identity};
use tracing::{info, warn};

#[derive(Debug, serde::Deserialize)]
struct Claims {
    iss: String,
    exp: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct DanubeServerImpl {
    service: Arc<Mutex<BrokerService>>,
    broker_addr: SocketAddr,
    auth: AuthConfig,
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
        }
    }

    pub(crate) async fn start_no_secure(&self, ready_tx: oneshot::Sender<()>) -> JoinHandle<()> {
        let socket_addr = self.broker_addr.clone();

        let server = Server::builder()
            .add_service(ProducerServiceServer::new(self.clone()))
            .add_service(ConsumerServiceServer::new(self.clone()))
            .add_service(DiscoveryServer::new(self.clone()))
            .add_service(HealthCheckServer::new(self))
            .serve(socket_addr);

        self.spawn_server(server, socket_addr, ready_tx)
    }

    pub(crate) async fn start_tls(&self, ready_tx: oneshot::Sender<()>) -> JoinHandle<()> {
        let socket_addr = self.broker_addr.clone();

        let server = self
            .configure_tls(Server::builder())
            .await
            .add_service(ProducerServiceServer::new(self.clone()))
            .add_service(ConsumerServiceServer::new(self.clone()))
            .add_service(DiscoveryServer::new(self.clone()))
            .add_service(HealthCheckServer::new(self))
            .serve(socket_addr);

        self.spawn_server(server, socket_addr, ready_tx)
    }
    pub(crate) async fn start_tls_jwt(&self, ready_tx: oneshot::Sender<()>) -> JoinHandle<()> {
        let socket_addr = self.broker_addr.clone();
        let jwt_config = self.auth.jwt.as_ref().expect("JWT config required");
        let jwt_secret = jwt_config.secret_key.clone();

        let server = self
            .configure_tls(Server::builder())
            .await
            .layer(tonic::service::interceptor(move |request| {
                Self::jwt_auth_interceptor(request, &jwt_secret)
            }))
            .add_service(ProducerServiceServer::new(self.clone()))
            .add_service(ConsumerServiceServer::new(self.clone()))
            .add_service(DiscoveryServer::new(self.clone()))
            .add_service(HealthCheckServer::new(self))
            .serve(socket_addr);

        self.spawn_server(server, socket_addr, ready_tx)
    }

    async fn configure_tls(&self, server: Server) -> Server {
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

    fn jwt_auth_interceptor(
        request: tonic::Request<()>,
        jwt_secret: &str,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        if let Some(token) = request.metadata().get("authorization") {
            let token_str = token.to_str().map_err(|_| {
                tonic::Status::unauthenticated("Authorization token is not valid UTF-8")
            })?;
            let token = token_str.replace("Bearer ", "");

            let validation = Validation::new(jsonwebtoken::Algorithm::HS256); // Consider making the algorithm configurable
            match decode::<Claims>(
                &token,
                &DecodingKey::from_secret(jwt_secret.as_bytes()),
                &validation,
            ) {
                Ok(_claims) => Ok(request),
                Err(_) => Err(tonic::Status::unauthenticated("Invalid JWT token")),
            }
        } else {
            Err(tonic::Status::unauthenticated(
                "Missing authorization token",
            ))
        }
    }
}

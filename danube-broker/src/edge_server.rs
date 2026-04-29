//! Dedicated gRPC server for the Edge Replicator service.
//!
//! Runs on a separate port (`broker.ports.edge`) and hosts only the
//! `EdgeReplicatorService`, keeping it isolated from admin and client gRPC.

use std::net::SocketAddr;
use std::sync::Arc;

use danube_edge::cluster::service::EdgeReplicatorServiceImpl;
use danube_edge::proto::edge_replicator_service_server::EdgeReplicatorServiceServer;
use tokio::task::JoinHandle;
use tonic::transport::Server;
use tracing::{info, warn};

use crate::edge::auth_adapter::BrokerEdgeAuth;
use crate::edge::storage_adapter::BrokerReplicationStorage;

/// Starts the dedicated edge replicator gRPC server.
///
/// This server accepts connections from edge brokers for:
/// - Edge registration (`RegisterEdge`)
/// - Topic creation on the cloud cluster (`CreateEdgeTopic`)
/// - Batch message replication (`ReplicateData` — bidirectional streaming)
pub(crate) async fn start_edge_server(
    edge_addr: SocketAddr,
    auth: Arc<BrokerEdgeAuth>,
    storage: Arc<BrokerReplicationStorage>,
) -> JoinHandle<()> {
    let edge_service = EdgeReplicatorServiceImpl::new(storage, auth);
    let edge_grpc = EdgeReplicatorServiceServer::new(edge_service);

    let server = Server::builder()
        .add_service(edge_grpc)
        .serve(edge_addr);

    info!(
        edge_addr = %edge_addr,
        "edge replicator gRPC server listening"
    );

    tokio::spawn(async move {
        if let Err(e) = server.await {
            warn!(error = ?e, "edge replicator gRPC server error");
        }
    })
}

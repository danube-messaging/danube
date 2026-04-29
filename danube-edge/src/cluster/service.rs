//! gRPC implementation of `EdgeReplicatorService` (cluster side).
//!
//! This service runs on cloud cluster brokers and handles:
//! 1. Edge registration (token validation + Raft persistence)
//! 2. Edge topic creation (metadata + WAL provisioning)
//! 3. Batch message ingestion (direct WAL writes via `append_batch()`)

use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status, Streaming};
use tracing::{error, info, warn};

use crate::cluster::auth::{EdgeAuth, EdgeRegistration};
use crate::cluster::ingestion::ReplicationStorage;
use crate::proto::edge_replicator_service_server::EdgeReplicatorService;
use crate::proto::{
    CreateEdgeTopicRequest, CreateEdgeTopicResponse, RegisterEdgeRequest, RegisterEdgeResponse,
    ReplicateAck, ReplicateBatch,
};
use danube_core::message::{MessageID, StreamMessage};

/// Cluster-side gRPC service for edge replication.
///
/// Generic over `S: ReplicationStorage` and `A: EdgeAuth` so the broker
/// can inject its own implementations without circular dependencies.
pub struct EdgeReplicatorServiceImpl<S: ReplicationStorage, A: EdgeAuth> {
    storage: Arc<S>,
    auth: Arc<A>,
}

impl<S: ReplicationStorage, A: EdgeAuth> Clone for EdgeReplicatorServiceImpl<S, A> {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            auth: self.auth.clone(),
        }
    }
}

impl<S: ReplicationStorage, A: EdgeAuth> std::fmt::Debug for EdgeReplicatorServiceImpl<S, A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EdgeReplicatorServiceImpl")
            .finish_non_exhaustive()
    }
}

impl<S: ReplicationStorage, A: EdgeAuth> EdgeReplicatorServiceImpl<S, A> {
    pub fn new(storage: Arc<S>, auth: Arc<A>) -> Self {
        Self { storage, auth }
    }
}

#[tonic::async_trait]
impl<S: ReplicationStorage, A: EdgeAuth> EdgeReplicatorService
    for EdgeReplicatorServiceImpl<S, A>
{
    /// Register an edge broker with the cluster.
    ///
    /// Flow:
    /// 1. Validate the JWT token (subject must match edge_name)
    /// 2. Ensure namespace /{edge_name} exists
    /// 3. Persist registration in Raft metadata store
    async fn register_edge(
        &self,
        request: Request<RegisterEdgeRequest>,
    ) -> Result<Response<RegisterEdgeResponse>, Status> {
        let req = request.into_inner();
        info!(
            edge_name = %req.edge_name,
            "processing edge registration request"
        );

        // Check if already registered (idempotent)
        if self.auth.is_edge_registered(&req.edge_name).await {
            if let Some(ns) = self.auth.get_edge_namespace(&req.edge_name).await {
                info!(
                    edge_name = %req.edge_name,
                    namespace = %ns,
                    "edge already registered, returning existing registration"
                );
                return Ok(Response::new(RegisterEdgeResponse {
                    success: true,
                    namespace: ns,
                    message: "edge already registered".to_string(),
                }));
            }
        }

        // Validate token
        let namespace = match self.auth.validate_edge_token(&req.edge_name, &req.token).await {
            Ok(ns) => ns,
            Err(e) => {
                warn!(
                    edge_name = %req.edge_name,
                    error = %e,
                    "edge token validation failed"
                );
                return Ok(Response::new(RegisterEdgeResponse {
                    success: false,
                    namespace: String::new(),
                    message: format!("token validation failed: {}", e),
                }));
            }
        };

        // Ensure namespace exists on the cluster
        if let Err(e) = self.storage.ensure_namespace(&namespace).await {
            error!(
                edge_name = %req.edge_name,
                namespace = %namespace,
                error = %e,
                "failed to ensure namespace for edge"
            );
            return Err(Status::internal(format!(
                "failed to ensure namespace: {}",
                e
            )));
        }

        // Persist registration
        let registration = EdgeRegistration {
            edge_name: req.edge_name.clone(),
            namespace: namespace.clone(),
            registered_at: chrono::Utc::now().to_rfc3339(),
            status: "active".to_string(),
        };

        if let Err(e) = self.auth.register_edge(&registration).await {
            error!(
                edge_name = %req.edge_name,
                error = %e,
                "failed to persist edge registration"
            );
            return Err(Status::internal(format!(
                "failed to persist registration: {}",
                e
            )));
        }

        info!(
            edge_name = %req.edge_name,
            namespace = %namespace,
            "edge registered successfully"
        );

        Ok(Response::new(RegisterEdgeResponse {
            success: true,
            namespace,
            message: "registered successfully".to_string(),
        }))
    }

    /// Create a topic on the cluster for edge replication.
    ///
    /// Flow:
    /// 1. Verify edge is registered
    /// 2. Verify topic belongs to edge's namespace
    /// 3. Create topic metadata (Reliable dispatch strategy)
    /// 4. Create WAL via StorageFactory
    async fn create_edge_topic(
        &self,
        request: Request<CreateEdgeTopicRequest>,
    ) -> Result<Response<CreateEdgeTopicResponse>, Status> {
        let req = request.into_inner();
        info!(
            edge_name = %req.edge_name,
            topic = %req.topic_name,
            "processing edge topic creation request"
        );

        // Verify edge is registered
        if !self.auth.is_edge_registered(&req.edge_name).await {
            return Ok(Response::new(CreateEdgeTopicResponse {
                success: false,
                message: format!("edge '{}' is not registered", req.edge_name),
            }));
        }

        // Verify topic belongs to edge namespace
        if let Err(e) = self.auth.validate_topic_namespace(&req.edge_name, &req.topic_name) {
            return Ok(Response::new(CreateEdgeTopicResponse {
                success: false,
                message: format!("namespace validation failed: {}", e),
            }));
        }

        // Create topic on cluster (metadata + WAL)
        if let Err(e) = self.storage.ensure_topic(&req.topic_name).await {
            error!(
                edge_name = %req.edge_name,
                topic = %req.topic_name,
                error = %e,
                "failed to create edge topic on cluster"
            );
            return Err(Status::internal(format!(
                "failed to create topic: {}",
                e
            )));
        }

        // TODO: Handle schema registration (Phase 2b)
        if req.schema.is_some() {
            info!(
                topic = %req.topic_name,
                "schema registration for edge topics is not yet implemented"
            );
        }

        info!(
            edge_name = %req.edge_name,
            topic = %req.topic_name,
            "edge topic created successfully on cluster"
        );

        Ok(Response::new(CreateEdgeTopicResponse {
            success: true,
            message: "topic created successfully".to_string(),
        }))
    }

    type ReplicateDataStream =
        Pin<Box<dyn tokio_stream::Stream<Item = Result<ReplicateAck, Status>> + Send + 'static>>;

    /// Replicate message batches from edge to cluster.
    ///
    /// Bidirectional stream:
    /// - Edge sends `ReplicateBatch` messages (many messages per batch, one topic per batch)
    /// - Cluster writes directly to WAL via `append_batch()` and sends `ReplicateAck`
    async fn replicate_data(
        &self,
        request: Request<Streaming<ReplicateBatch>>,
    ) -> Result<Response<Self::ReplicateDataStream>, Status> {
        let mut stream = request.into_inner();
        let storage = self.storage.clone();
        let auth = self.auth.clone();

        let (tx, rx) = mpsc::channel(32);

        tokio::spawn(async move {
            while let Some(batch_result) = stream.next().await {
                let batch = match batch_result {
                    Ok(b) => b,
                    Err(e) => {
                        warn!(error = %e, "error receiving replicate batch from edge");
                        break;
                    }
                };

                // Verify edge is registered
                if !auth.is_edge_registered(&batch.edge_name).await {
                    warn!(
                        edge_name = %batch.edge_name,
                        "rejecting batch from unregistered edge"
                    );
                    let _ = tx
                        .send(Err(Status::permission_denied(format!(
                            "edge '{}' is not registered",
                            batch.edge_name
                        ))))
                        .await;
                    break;
                }

                // Convert proto messages to StreamMessages
                let stream_messages: Vec<StreamMessage> = batch
                    .messages
                    .into_iter()
                    .map(|msg| StreamMessage {
                        request_id: 0,
                        msg_id: MessageID {
                            producer_id: 0,
                            topic_name: batch.topic_name.clone(),
                            broker_addr: String::new(),
                            // Offsets will be reassigned by Wal::append_batch()
                            topic_offset: 0,
                        },
                        payload: bytes::Bytes::from(msg.payload),
                        publish_time: 0,
                        producer_name: String::new(),
                        subscription_name: None,
                        attributes: msg.attributes,
                        schema_id: None,
                        schema_version: None,
                        routing_key: None,
                    })
                    .collect();

                if stream_messages.is_empty() {
                    continue;
                }

                // Write batch directly to WAL
                match storage
                    .ingest_batch(&batch.topic_name, stream_messages)
                    .await
                {
                    Ok(_last_offset) => {
                        let ack = ReplicateAck {
                            topic_name: batch.topic_name.clone(),
                            acked_offset: batch.batch_last_offset,
                        };
                        if tx.send(Ok(ack)).await.is_err() {
                            // Client disconnected
                            break;
                        }
                    }
                    Err(e) => {
                        error!(
                            topic = %batch.topic_name,
                            error = %e,
                            "failed to ingest replicated batch"
                        );
                        let _ = tx
                            .send(Err(Status::internal(format!(
                                "ingestion failed: {}",
                                e
                            ))))
                            .await;
                        break;
                    }
                }
            }
        });

        let out_stream = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(out_stream)))
    }
}

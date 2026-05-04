//! gRPC implementation of `EdgeReplicatorService` (cluster side).
//!
//! This service runs on cloud cluster brokers and handles:
//! 1. Edge topic creation/deletion (delegated to `TopicCluster`)
//! 2. Batch message ingestion (direct WAL writes via `EdgeReplicationStorage`)
//!
//! Authentication and authorization are handled by the broker's standard
//! gRPC interceptor (JWT or mTLS). The edge broker authenticates the same
//! way any client or cluster broker does.

use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, error, info, warn};

use danube_core::edge_proto::edge_replicator_service_server::EdgeReplicatorService;
use danube_core::edge_proto::{
    CreateEdgeTopicRequest, CreateEdgeTopicResponse, DeleteEdgeTopicRequest,
    DeleteEdgeTopicResponse, ReplicateAck, ReplicateBatch,
};
use danube_core::message::{MessageID, StreamMessage};
use danube_core::proto::DispatchStrategy as ProtoDispatchStrategy;

use super::storage::EdgeReplicationStorage;
use crate::topic_cluster::TopicCluster;

/// Cluster-side gRPC service for edge replication.
///
/// - **Topic lifecycle** (create/delete): delegated to `TopicCluster`, the same
///   code path used by Standalone and Cluster modes. No duplication.
/// - **Batch ingestion**: handled by `EdgeReplicationStorage` (dedup + WAL write).
///
/// Auth is handled externally by the broker's gRPC interceptor.
#[derive(Clone)]
pub(crate) struct EdgeReplicatorServiceImpl {
    storage: Arc<EdgeReplicationStorage>,
    topic_cluster: TopicCluster,
}

impl std::fmt::Debug for EdgeReplicatorServiceImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EdgeReplicatorServiceImpl")
            .finish_non_exhaustive()
    }
}

impl EdgeReplicatorServiceImpl {
    pub(crate) fn new(storage: Arc<EdgeReplicationStorage>, topic_cluster: TopicCluster) -> Self {
        Self {
            storage,
            topic_cluster,
        }
    }
}

#[tonic::async_trait]
impl EdgeReplicatorService for EdgeReplicatorServiceImpl {
    /// Create a topic on the cluster for edge replication.
    ///
    /// Delegates to `TopicCluster::create_on_cluster` — the standard cluster
    /// pipeline. After metadata is written, the LoadManager assigns the topic
    /// to a broker. Edge topics always use the Reliable dispatch strategy.
    ///
    /// Idempotent: if the topic already exists, `create_on_cluster` returns
    /// `AlreadyExists` which we treat as success.
    ///
    /// The namespace must be pre-provisioned by the admin (along with the
    /// edge token and RBAC binding).
    ///
    /// Auth: the broker interceptor has already verified the caller has
    /// ManageTopic permission on the topic's namespace.
    async fn create_edge_topic(
        &self,
        request: Request<CreateEdgeTopicRequest>,
    ) -> Result<Response<CreateEdgeTopicResponse>, Status> {
        let req = request.into_inner();
        info!(
            topic = %req.topic_name,
            "processing edge topic creation request"
        );

        // Use the standard cluster pipeline (validates, writes metadata,
        // LoadManager assigns the topic to a broker).
        match self
            .topic_cluster
            .create_on_cluster(
                &req.topic_name,
                Some(ProtoDispatchStrategy::Reliable),
                None, // schema: TODO Phase 2b
                None, // policies
            )
            .await
        {
            Ok(()) => {}
            Err(status) if status.code() == tonic::Code::AlreadyExists => {
                // Idempotent: topic already exists, treat as success
                debug!(
                    topic = %req.topic_name,
                    "edge topic already exists on cluster, returning success"
                );
            }
            Err(e) => {
                error!(
                    topic = %req.topic_name,
                    error = %e,
                    "failed to create edge topic on cluster"
                );
                return Err(e);
            }
        }

        // TODO: Handle schema registration (Phase 2b)
        if req.schema.is_some() {
            info!(
                topic = %req.topic_name,
                "schema registration for edge topics is not yet implemented"
            );
        }

        info!(
            topic = %req.topic_name,
            "edge topic created successfully on cluster"
        );

        Ok(Response::new(CreateEdgeTopicResponse {
            success: true,
            message: "topic created successfully".to_string(),
        }))
    }

    /// Delete a topic from the cluster that was created for edge replication.
    ///
    /// Delegates to `TopicCluster::post_delete_topic` — the standard cluster
    /// deletion pipeline (unassigns the topic from its broker, then cleans
    /// up metadata).
    /// Also cleans up the replicated offset marker (edge-specific state).
    ///
    /// Idempotent: returns success if the topic does not exist.
    ///
    /// Auth: the broker interceptor has already verified the caller has
    /// ManageTopic permission on the topic's namespace.
    async fn delete_edge_topic(
        &self,
        request: Request<DeleteEdgeTopicRequest>,
    ) -> Result<Response<DeleteEdgeTopicResponse>, Status> {
        let req = request.into_inner();
        info!(
            topic = %req.topic_name,
            "processing edge topic deletion request"
        );

        // Check if topic exists (idempotent delete)
        if !self
            .topic_cluster
            .exists_topic_in_namespace(&req.topic_name)
            .await
        {
            debug!(
                topic = %req.topic_name,
                "edge topic does not exist on cluster, nothing to delete"
            );
            return Ok(Response::new(DeleteEdgeTopicResponse {
                success: true,
                message: "topic does not exist (already deleted)".to_string(),
            }));
        }

        // Delete topic via cluster pipeline (unassign + metadata cleanup)
        if let Err(e) = self
            .topic_cluster
            .post_delete_topic(&req.topic_name)
            .await
        {
            error!(
                topic = %req.topic_name,
                error = %e,
                "failed to delete edge topic metadata on cluster"
            );
            return Err(Status::internal(format!(
                "failed to delete topic metadata: {}",
                e
            )));
        }

        // Clean up edge-specific replicated offset marker
        self.storage
            .delete_replicated_marker(&req.topic_name)
            .await;

        info!(
            topic = %req.topic_name,
            "edge topic deleted successfully from cluster"
        );

        Ok(Response::new(DeleteEdgeTopicResponse {
            success: true,
            message: "topic deleted successfully".to_string(),
        }))
    }

    type ReplicateDataStream =
        Pin<Box<dyn tokio_stream::Stream<Item = Result<ReplicateAck, Status>> + Send + 'static>>;

    /// Replicate message batches from edge to cluster.
    ///
    /// Bidirectional stream:
    /// - Edge sends `ReplicateBatch` messages (many messages per batch, one topic per batch)
    /// - Cluster writes directly to WAL via `append_batch()` and sends `ReplicateAck`
    ///
    /// Auth: the broker interceptor has already verified the caller has
    /// Replicate permission. No per-batch auth checks needed.
    async fn replicate_data(
        &self,
        request: Request<Streaming<ReplicateBatch>>,
    ) -> Result<Response<Self::ReplicateDataStream>, Status> {
        let mut stream = request.into_inner();
        let storage = self.storage.clone();

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
                        payload: msg.payload.into(),
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

                debug!(
                    topic = %batch.topic_name,
                    count = stream_messages.len(),
                    "ingesting replicated batch"
                );

                // Write batch directly to WAL (with dedup check)
                match storage
                    .ingest_batch(&batch.topic_name, batch.batch_last_offset, stream_messages)
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
                            .send(Err(Status::internal(format!("ingestion failed: {}", e))))
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

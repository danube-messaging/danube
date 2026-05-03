//! gRPC implementation of `EdgeReplicatorService` (cluster side).
//!
//! This service runs on cloud cluster brokers and handles:
//! 1. Edge topic creation (metadata + WAL provisioning)
//! 2. Batch message ingestion (direct WAL writes via `append_batch()`)
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
    CreateEdgeTopicRequest, CreateEdgeTopicResponse, ReplicateAck, ReplicateBatch,
};
use danube_core::message::{MessageID, StreamMessage};

use super::storage::EdgeReplicationStorage;

/// Cluster-side gRPC service for edge replication.
///
/// Uses concrete `EdgeReplicationStorage` — no trait indirection.
/// Auth is handled externally by the broker's gRPC interceptor.
#[derive(Clone)]
pub(crate) struct EdgeReplicatorServiceImpl {
    storage: Arc<EdgeReplicationStorage>,
}

impl std::fmt::Debug for EdgeReplicatorServiceImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EdgeReplicatorServiceImpl")
            .finish_non_exhaustive()
    }
}

impl EdgeReplicatorServiceImpl {
    pub(crate) fn new(storage: Arc<EdgeReplicationStorage>) -> Self {
        Self { storage }
    }
}

#[tonic::async_trait]
impl EdgeReplicatorService for EdgeReplicatorServiceImpl {
    /// Create a topic on the cluster for edge replication.
    ///
    /// Flow:
    /// 1. Ensure namespace exists (extracted from topic path)
    /// 2. Create topic metadata (Reliable dispatch strategy)
    /// 3. Create WAL via StorageFactory
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

        // Create topic on cluster (metadata + WAL)
        if let Err(e) = self.storage.ensure_topic(&req.topic_name).await {
            error!(
                topic = %req.topic_name,
                error = %e,
                "failed to create edge topic on cluster"
            );
            return Err(Status::internal(format!("failed to create topic: {}", e)));
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

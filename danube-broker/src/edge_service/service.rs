//! gRPC implementation of `EdgeReplicatorService` (cluster side).
//!
//! This service runs on cloud cluster brokers and handles:
//! 1. Edge registration — creates topics, resolves schemas, stores state in Raft
//! 2. Heartbeat — version-based change detection for schema/topic sync
//! 3. Batch message ingestion — direct WAL writes via `EdgeReplicationStorage`
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
    EdgeChange, EdgeChangeType, EdgeHeartbeatRequest, EdgeHeartbeatResponse,
    RegisterEdgeRequest, RegisterEdgeResponse, ReplicateAck, ReplicateBatch,
    ResolvedSchema, TopicRegistrationResult,
};
use danube_core::message::StreamMessage;
use danube_core::proto::DispatchStrategy as ProtoDispatchStrategy;

use super::edge_registry::{EdgeRegistry, EdgeState, EdgeTopicState};
use super::storage::EdgeReplicationStorage;
use crate::metadata_storage::MetadataStorage;
use crate::topic_cluster::TopicCluster;

use danube_schema::resources::SchemaResources;

/// Cluster-side gRPC service for edge replication.
///
/// - **Registration** (register_edge): creates topics on cluster, resolves schemas
///   from the registry, stores per-edge state in Raft.
/// - **Heartbeat** (edge_heartbeat): version-based change detection. Fast path
///   when nothing changed, returns changelog otherwise.
/// - **Batch ingestion** (replicate_data): handled by `EdgeReplicationStorage`.
///
/// Auth is handled externally by the broker's gRPC interceptor.
#[derive(Clone)]
pub(crate) struct EdgeReplicatorServiceImpl {
    storage: Arc<EdgeReplicationStorage>,
    topic_cluster: TopicCluster,
    edge_registry: EdgeRegistry<MetadataStorage>,
    schema_resources: SchemaResources,
}

impl std::fmt::Debug for EdgeReplicatorServiceImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EdgeReplicatorServiceImpl")
            .finish_non_exhaustive()
    }
}

impl EdgeReplicatorServiceImpl {
    pub(crate) fn new(
        storage: Arc<EdgeReplicationStorage>,
        topic_cluster: TopicCluster,
        meta_store: MetadataStorage,
        schema_resources: SchemaResources,
    ) -> Self {
        Self {
            storage,
            topic_cluster,
            edge_registry: EdgeRegistry::new(meta_store),
            schema_resources,
        }
    }

    /// Resolve a schema subject from the cluster's schema registry.
    ///
    /// Returns a `ResolvedSchema` proto message if the subject exists,
    /// or None if it doesn't.
    async fn resolve_schema(&self, subject: &str) -> Option<ResolvedSchema> {
        // Get metadata (contains schema_id, latest_version, compatibility_mode)
        let metadata = match self.schema_resources.get_metadata(subject).await {
            Ok(m) => m,
            Err(_) => return None,
        };

        // Get the latest version (contains schema definition, fingerprint)
        let latest = match self
            .schema_resources
            .get_version(subject, metadata.latest_version)
            .await
        {
            Ok(v) => v,
            Err(_) => return None,
        };

        // Serialize the schema definition to bytes for the edge
        let schema_definition = serde_json::to_vec(&latest.schema_def).unwrap_or_default();

        Some(ResolvedSchema {
            subject: subject.to_string(),
            schema_id: metadata.id,
            schema_version: latest.version,
            schema_type: latest.schema_def.schema_type().to_string().to_lowercase(),
            schema_definition,
            fingerprint: latest.fingerprint.clone(),
            compatibility_mode: metadata.compatibility_mode.to_string().to_lowercase(),
        })
    }

    /// Access the edge registry (for post-hooks from schema handler).
    #[allow(dead_code)]
    pub(crate) fn edge_registry(&self) -> &EdgeRegistry<MetadataStorage> {
        &self.edge_registry
    }
}

#[tonic::async_trait]
impl EdgeReplicatorService for EdgeReplicatorServiceImpl {
    /// Register (or re-register) an edge broker with the cluster.
    ///
    /// For each declared topic:
    /// 1. Creates the topic on the cluster (idempotent, Reliable dispatch)
    /// 2. Resolves the schema_subject from the cluster's schema registry
    /// 3. Returns the full resolved schema (type, definition, fingerprint) to the edge
    ///
    /// Stores per-edge state in Raft with an initial `config_version`.
    /// On re-registration, diffs against the previous state.
    ///
    /// Auth: the broker interceptor has already verified the caller has
    /// permissions on the edge's namespace.
    async fn register_edge(
        &self,
        request: Request<RegisterEdgeRequest>,
    ) -> Result<Response<RegisterEdgeResponse>, Status> {
        let req = request.into_inner();
        let edge_name = &req.edge_name;

        info!(
            edge_name = %edge_name,
            topics = req.topics.len(),
            "processing edge registration"
        );

        let mut topic_results = Vec::new();
        let mut raft_topics = std::collections::HashMap::new();

        for decl in &req.topics {
            // Validate namespace: topic must start with /{edge_name}/
            let expected_prefix = format!("/{}/", edge_name);
            if !decl.topic_name.starts_with(&expected_prefix) {
                topic_results.push(TopicRegistrationResult {
                    topic_name: decl.topic_name.clone(),
                    topic_created: false,
                    schema_resolved: false,
                    schema: None,
                    error: format!(
                        "topic must be under /{edge_name}/ namespace, got: {}",
                        decl.topic_name
                    ),
                });
                continue;
            }

            // Create topic on cluster (idempotent)
            let topic_created = match self
                .topic_cluster
                .create_on_cluster(
                    &decl.topic_name,
                    Some(ProtoDispatchStrategy::Reliable),
                    None,
                    None,
                )
                .await
            {
                Ok(()) => true,
                Err(status) if status.code() == tonic::Code::AlreadyExists => false,
                Err(e) => {
                    error!(
                        topic = %decl.topic_name,
                        error = %e,
                        "failed to create edge topic on cluster"
                    );
                    topic_results.push(TopicRegistrationResult {
                        topic_name: decl.topic_name.clone(),
                        topic_created: false,
                        schema_resolved: false,
                        schema: None,
                        error: format!("failed to create topic: {}", e),
                    });
                    continue;
                }
            };

            // Resolve schema (if schema_subject declared)
            let (schema_resolved, resolved_schema) = if let Some(ref subject) = decl.schema_subject
            {
                match self.resolve_schema(subject).await {
                    Some(schema) => (true, Some(schema)),
                    None => {
                        warn!(
                            topic = %decl.topic_name,
                            subject = %subject,
                            "schema subject not found in cluster registry"
                        );
                        (false, None)
                    }
                }
            } else {
                // No schema required (raw bytes) → resolved by default
                (true, None)
            };

            // Build Raft state for this topic
            raft_topics.insert(
                decl.topic_name.clone(),
                EdgeTopicState {
                    schema_subject: decl.schema_subject.clone(),
                    schema_id: resolved_schema.as_ref().map(|s| s.schema_id),
                    schema_version: resolved_schema.as_ref().map(|s| s.schema_version),
                    schema_fingerprint: resolved_schema
                        .as_ref()
                        .map(|s| s.fingerprint.clone()),
                },
            );

            topic_results.push(TopicRegistrationResult {
                topic_name: decl.topic_name.clone(),
                topic_created,
                schema_resolved,
                schema: resolved_schema,
                error: String::new(),
            });
        }

        // Determine config_version: if re-registering, increment; otherwise start at 1
        let config_version = match self.edge_registry.load_edge_state(edge_name).await {
            Ok(Some(existing)) => existing.config_version + 1,
            _ => 1,
        };

        let now = super::edge_registry::now_secs();
        let edge_state = EdgeState {
            config_version,
            registered_at: now,
            last_heartbeat: now,
            topics: raft_topics,
        };

        // Store in Raft
        if let Err(e) = self
            .edge_registry
            .store_edge_state(edge_name, &edge_state)
            .await
        {
            error!(
                edge_name = %edge_name,
                error = %e,
                "failed to store edge state in Raft"
            );
            return Err(Status::internal(format!(
                "failed to persist edge state: {}",
                e
            )));
        }

        info!(
            edge_name = %edge_name,
            config_version = config_version,
            topics_ok = topic_results.iter().filter(|t| t.error.is_empty()).count(),
            topics_err = topic_results.iter().filter(|t| !t.error.is_empty()).count(),
            "edge registered successfully"
        );

        Ok(Response::new(RegisterEdgeResponse {
            success: true,
            message: "edge registered".to_string(),
            config_version,
            topics: topic_results,
        }))
    }

    /// Periodic heartbeat from an edge broker.
    ///
    /// Fast path: if the edge's `config_version` matches Raft, returns
    /// `changed=false` immediately — no schema lookups needed.
    ///
    /// Slow path: if versions differ (schema was updated/deleted by admin),
    /// re-resolves all schemas for the edge's topics and returns a changelog.
    async fn edge_heartbeat(
        &self,
        request: Request<EdgeHeartbeatRequest>,
    ) -> Result<Response<EdgeHeartbeatResponse>, Status> {
        let req = request.into_inner();
        let edge_name = &req.edge_name;

        // Load edge state from Raft
        let edge_state = match self.edge_registry.load_edge_state(edge_name).await {
            Ok(Some(state)) => state,
            Ok(None) => {
                return Err(Status::not_found(format!(
                    "edge '{}' not registered — call RegisterEdge first",
                    edge_name
                )));
            }
            Err(e) => {
                return Err(Status::internal(format!(
                    "failed to load edge state: {}",
                    e
                )));
            }
        };

        // Update last_heartbeat
        if let Err(e) = self.edge_registry.touch_heartbeat(edge_name).await {
            warn!(
                edge_name = %edge_name,
                error = %e,
                "failed to update heartbeat timestamp"
            );
        }

        // Fast path: version matches — nothing changed
        if edge_state.config_version == req.config_version {
            return Ok(Response::new(EdgeHeartbeatResponse {
                changed: false,
                config_version: edge_state.config_version,
                changes: vec![],
            }));
        }

        // Slow path: version mismatch — re-resolve and build changelog
        debug!(
            edge_name = %edge_name,
            edge_version = req.config_version,
            cluster_version = edge_state.config_version,
            "heartbeat version mismatch, computing changes"
        );

        let mut changes = Vec::new();

        for (topic_name, topic_state) in &edge_state.topics {
            if let Some(ref subject) = topic_state.schema_subject {
                match self.resolve_schema(subject).await {
                    Some(new_schema) => {
                        // Check if fingerprint changed
                        let old_fingerprint = topic_state.schema_fingerprint.as_deref();
                        if old_fingerprint != Some(&new_schema.fingerprint) {
                            changes.push(EdgeChange {
                                topic_name: topic_name.clone(),
                                change_type: EdgeChangeType::SchemaUpdated as i32,
                                schema: Some(new_schema),
                                detail: format!(
                                    "schema '{}' updated (fingerprint changed)",
                                    subject
                                ),
                            });
                        }
                    }
                    None => {
                        // Schema subject no longer exists on cluster
                        if topic_state.schema_id.is_some() {
                            changes.push(EdgeChange {
                                topic_name: topic_name.clone(),
                                change_type: EdgeChangeType::SchemaRemoved as i32,
                                schema: None,
                                detail: format!(
                                    "schema subject '{}' no longer exists on cluster",
                                    subject
                                ),
                            });
                        }
                    }
                }
            }
        }

        info!(
            edge_name = %edge_name,
            changes = changes.len(),
            config_version = edge_state.config_version,
            "heartbeat responded with changes"
        );

        Ok(Response::new(EdgeHeartbeatResponse {
            changed: !changes.is_empty(),
            config_version: edge_state.config_version,
            changes,
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

                // Convert proto StreamMessages to internal StreamMessages.
                let stream_messages: Vec<StreamMessage> = batch
                    .messages
                    .into_iter()
                    .map(StreamMessage::from)
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

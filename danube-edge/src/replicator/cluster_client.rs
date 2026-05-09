//! gRPC client for connecting to the cloud cluster's `EdgeReplicatorService`.
//!
//! Uses `DanubeClient` internally for:
//! - **Topic lookup** — resolves which broker owns each topic
//! - **Connection pooling** — TLS-configured channels cached per broker
//! - **Auth** — JWT token injected via gRPC metadata
//!
//! The edge may connect to multiple brokers in the cluster simultaneously,
//! since different topics can be assigned to different brokers.
//!
//! ## Topic lifecycle
//!
//! A topic must be **created** on the cloud via `create_topic()` before any
//! data can be replicated. The client tracks which topics have been confirmed
//! by the cloud in `created_topics`.
//!
//! `get_or_connect()` only does a topic lookup for topics that are in
//! `created_topics`. If the lookup fails for a created topic, the topic was
//! deleted from the remote cluster — replication for that topic should stop.

use std::collections::{HashMap, HashSet};

use anyhow::{anyhow, Result};
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::metadata::MetadataValue;
use tonic::Request;
use tracing::{debug, error, info};

use danube_client::DanubeClient;
use danube_core::edge_proto::edge_replicator_service_client::EdgeReplicatorServiceClient;
use danube_core::edge_proto::{CreateEdgeTopicRequest, DeleteEdgeTopicRequest, ReplicateBatch};
use danube_core::message::StreamMessage;
use danube_core::proto::StreamMessage as ProtoStreamMessage;

type EdgeGrpcClient = EdgeReplicatorServiceClient<tonic::transport::Channel>;

/// Client for connecting to the cloud cluster's EdgeReplicatorService.
///
/// Maintains two pieces of per-topic state:
///
/// - **`created_topics`** — Topics confirmed to exist on the cloud cluster.
///   Populated by `create_topic()`. A topic must be in this set before
///   `get_or_connect()` will attempt a lookup.
///
/// - **`topic_clients`** — Cached gRPC clients for data replication (post-lookup).
///   On connection failure, call `invalidate_topic_client()` to force a
///   fresh lookup on the next `get_or_connect()`.
pub struct EdgeCloudClient {
    client: DanubeClient,
    /// Cloud cluster entry-point URL (e.g., `http://cloud:6650`).
    /// Used for `CreateEdgeTopic` calls — goes to any cluster broker since
    /// the topic doesn't exist yet (can't do a topic lookup).
    cloud_url: String,
    edge_name: String,
    token: String,
    /// Topics that have been successfully created on the cloud cluster.
    created_topics: Mutex<HashSet<String>>,
    /// Cache: topic_name → ready gRPC client for the broker that owns this topic.
    topic_clients: Mutex<HashMap<String, EdgeGrpcClient>>,
}

impl EdgeCloudClient {
    /// Create a new edge cloud client.
    ///
    /// The `cloud_url` is the initial entry point for topic lookups and
    /// `CreateEdgeTopic` calls. Data connections may go to different brokers
    /// depending on topic assignment.
    pub async fn new(cloud_url: &str, edge_name: &str, token: &str) -> Result<Self> {
        let mut builder = DanubeClient::builder().service_url(cloud_url);

        if !token.is_empty() {
            builder = builder.with_token(token);
        }

        let client = builder.build().await.map_err(|e| anyhow!("{}", e))?;

        Ok(Self {
            client,
            cloud_url: cloud_url.to_string(),
            edge_name: edge_name.to_string(),
            token: token.to_string(),
            created_topics: Mutex::new(HashSet::new()),
            topic_clients: Mutex::new(HashMap::new()),
        })
    }

    // =====================================================================
    // Topic creation
    // =====================================================================

    /// Create a topic on the cloud cluster for replication.
    ///
    /// Uses the entry-point broker (`cloud_url`) directly — not a topic
    /// lookup — because the topic may not exist on the cloud yet.
    /// `CreateEdgeTopic` writes to shared Raft, so any cluster broker handles it.
    ///
    /// Idempotent: if the topic was already created in this session, returns Ok
    /// immediately without a gRPC call.
    pub async fn create_topic(&self, topic_name: &str) -> Result<()> {
        // Fast path: already created in this session
        {
            let created = self.created_topics.lock().await;
            if created.contains(topic_name) {
                debug!(topic = %topic_name, "topic already created on cloud, skipping");
                return Ok(());
            }
        }

        // Connect to the entry-point broker (any cluster node works)
        let mut client = self.connect_entry_point().await?;

        let mut request = Request::new(CreateEdgeTopicRequest {
            topic_name: topic_name.to_string(),
            schema: None, // TODO: schema support in Phase 2b
        });
        self.authenticate(&mut request);

        let response = client.create_edge_topic(request).await?.into_inner();

        if response.success {
            self.created_topics
                .lock()
                .await
                .insert(topic_name.to_string());
            info!(
                edge_name = %self.edge_name,
                topic = %topic_name,
                "topic created on cloud cluster"
            );
            Ok(())
        } else {
            Err(anyhow!("topic creation failed: {}", response.message))
        }
    }

    /// Check if a topic has been created on the cloud cluster.
    pub async fn is_topic_created(&self, topic_name: &str) -> bool {
        self.created_topics.lock().await.contains(topic_name)
    }

    /// Delete a topic from the cloud cluster.
    ///
    /// Calls `DeleteEdgeTopic` on the entry-point broker, then removes the
    /// topic from `created_topics` and `topic_clients`.
    ///
    /// Idempotent: if the topic is not in `created_topics`, returns Ok.
    pub async fn delete_topic(&self, topic_name: &str) -> Result<()> {
        // Connect to entry-point broker (same as create — any node works)
        let mut client = self.connect_entry_point().await?;

        let mut request = Request::new(DeleteEdgeTopicRequest {
            topic_name: topic_name.to_string(),
        });
        self.authenticate(&mut request);

        let response = client.delete_edge_topic(request).await?.into_inner();

        if response.success {
            // Clean up local state
            self.created_topics.lock().await.remove(topic_name);
            self.topic_clients.lock().await.remove(topic_name);

            info!(
                edge_name = %self.edge_name,
                topic = %topic_name,
                "topic deleted from cloud cluster"
            );
            Ok(())
        } else {
            Err(anyhow!("topic deletion failed: {}", response.message))
        }
    }

    // =====================================================================
    // Data connection (post-creation)
    // =====================================================================

    /// Get or create a cached gRPC client for the broker that owns a topic.
    ///
    /// **Precondition**: the topic must have been created via `create_topic()`.
    /// If the topic is not in `created_topics`, this returns an error.
    ///
    /// On first call for a topic: does a topic lookup to resolve the owning
    /// broker, connects, caches the client.
    ///
    /// If the topic lookup fails for a topic that *was* created, it means the
    /// topic was deleted from the remote cluster. Returns a specific error
    /// so the caller can stop replication for this topic.
    async fn get_or_connect(&self, topic_name: &str) -> Result<EdgeGrpcClient> {
        // Fast path: return cached client
        {
            let cache = self.topic_clients.lock().await;
            if let Some(client) = cache.get(topic_name) {
                return Ok(client.clone());
            }
        }

        // Verify the topic was created on cloud
        {
            let created = self.created_topics.lock().await;
            if !created.contains(topic_name) {
                return Err(anyhow!(
                    "topic '{}' was not created on the cloud cluster — cannot connect",
                    topic_name
                ));
            }
        }

        // Lookup the owning broker
        let broker = match self.client.resolve_topic_broker(topic_name).await {
            Ok(b) => b,
            Err(e) => {
                error!(
                    topic = %topic_name,
                    error = %e,
                    "topic lookup failed for a created topic — topic may have been \
                     deleted from the remote cluster, replication should stop"
                );
                return Err(anyhow!(
                    "topic '{}' no longer exists on cloud cluster: {}",
                    topic_name,
                    e
                ));
            }
        };

        debug!(
            topic = %topic_name,
            broker_url = %broker.connect_url,
            "resolved topic → broker, creating client"
        );

        let channel = self
            .client
            .get_broker_channel(&broker)
            .await
            .map_err(|e| anyhow!("connection to broker failed: {}", e))?;

        let edge_client = EdgeReplicatorServiceClient::new(channel);

        // Cache the ready client
        let mut cache = self.topic_clients.lock().await;
        cache.insert(topic_name.to_string(), edge_client.clone());

        Ok(edge_client)
    }

    /// Invalidate the cached client for a topic.
    ///
    /// Called on connection failure so the next `get_or_connect()` does a
    /// fresh lookup — the topic may have moved to a different broker.
    pub async fn invalidate_topic_client(&self, topic_name: &str) {
        let mut cache = self.topic_clients.lock().await;
        if cache.remove(topic_name).is_some() {
            debug!(topic = %topic_name, "invalidated cached broker client");
        }
    }

    // =====================================================================
    // Data replication
    // =====================================================================

    /// Send a batch of messages for a topic to the cloud cluster.
    ///
    /// Uses the cached gRPC client for the topic's broker.
    /// On connection failure, the caller should call `invalidate_topic_client()`
    /// and retry — the next call will re-lookup and create a new client.
    pub async fn send_batch(
        &self,
        topic_name: &str,
        messages: Vec<StreamMessage>,
        batch_last_offset: u64,
    ) -> Result<u64> {
        let mut client = self.get_or_connect(topic_name).await?;

        // Convert internal StreamMessages to proto StreamMessages
        let proto_messages: Vec<ProtoStreamMessage> =
            messages.into_iter().map(ProtoStreamMessage::from).collect();

        let batch = ReplicateBatch {
            topic_name: topic_name.to_string(),
            messages: proto_messages,
            batch_last_offset,
        };

        // Create a one-shot stream for this batch
        let (tx, rx) = mpsc::channel(1);
        tx.send(batch)
            .await
            .map_err(|_| anyhow!("channel send failed"))?;
        drop(tx); // Close the sending side after sending the batch

        let request_stream = ReceiverStream::new(rx);
        let mut request = Request::new(request_stream);
        self.authenticate(&mut request);

        let response = client.replicate_data(request).await?;
        let mut ack_stream = response.into_inner();

        // Wait for the ack
        match ack_stream.next().await {
            Some(Ok(ack)) => Ok(ack.acked_offset),
            Some(Err(e)) => Err(anyhow!("replication error: {}", e)),
            None => Err(anyhow!("no ack received from cloud cluster")),
        }
    }

    // =====================================================================
    // Helpers
    // =====================================================================

    /// Connect to the cloud cluster entry-point broker.
    ///
    /// Used for `CreateEdgeTopic` which can go to any cluster node (the RPC
    /// writes to shared Raft). The connection is cached by DanubeClient's
    /// connection manager, so repeated calls reuse the same channel.
    async fn connect_entry_point(&self) -> Result<EdgeGrpcClient> {
        let cloud_uri: tonic::transport::Uri = self
            .cloud_url
            .parse()
            .map_err(|e| anyhow!("invalid cloud_url '{}': {}", self.cloud_url, e))?;

        let channel = self
            .client
            .get_broker_channel(&danube_client::BrokerAddress {
                broker_url: cloud_uri.clone(),
                connect_url: cloud_uri,
                proxy: false,
            })
            .await
            .map_err(|e| anyhow!("failed to connect to cloud cluster: {}", e))?;

        Ok(EdgeReplicatorServiceClient::new(channel))
    }

    /// Attach the JWT bearer token to a gRPC request.
    fn authenticate<T>(&self, request: &mut Request<T>) {
        if !self.token.is_empty() {
            let bearer = format!("Bearer {}", self.token);
            if let Ok(value) = bearer.parse::<MetadataValue<_>>() {
                request.metadata_mut().insert("authorization", value);
            }
        }
    }

    /// Get the edge name.
    pub fn edge_name(&self) -> &str {
        &self.edge_name
    }
}

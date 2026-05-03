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
//! Follows the same pattern as `TopicProducer`: caches the gRPC service client
//! per topic and re-creates it on disconnection after a fresh topic lookup.

use std::collections::HashMap;

use anyhow::{anyhow, Result};
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::metadata::MetadataValue;
use tonic::Request;
use tracing::{debug, info};

use danube_client::DanubeClient;
use danube_core::edge_proto::edge_replicator_service_client::EdgeReplicatorServiceClient;
use danube_core::edge_proto::{CreateEdgeTopicRequest, ReplicateBatch, ReplicateMessage};
use danube_core::message::StreamMessage;

type EdgeGrpcClient = EdgeReplicatorServiceClient<tonic::transport::Channel>;

/// Client for connecting to the cloud cluster's EdgeReplicatorService.
///
/// Maintains a per-topic cached `EdgeReplicatorServiceClient` so that each
/// topic's batches go to the correct broker without repeated lookups.
/// On connection failure, the cache entry is invalidated and the next call
/// triggers a fresh topic lookup + new client creation.
pub struct EdgeCloudClient {
    client: DanubeClient,
    edge_name: String,
    token: String,
    /// Cache: topic_name → ready gRPC client for the broker that owns this topic.
    /// Same pattern as `TopicProducer::ProducerState::Ready { stream_client }`.
    topic_clients: Mutex<HashMap<String, EdgeGrpcClient>>,
}

impl EdgeCloudClient {
    /// Create a new edge cloud client.
    ///
    /// The `cloud_url` is used as the initial entry point for topic lookups.
    /// The actual data connections may go to different brokers depending on
    /// topic assignment.
    pub async fn new(cloud_url: &str, edge_name: &str, token: &str) -> Result<Self> {
        let mut builder = DanubeClient::builder().service_url(cloud_url);

        if !token.is_empty() {
            builder = builder.with_token(token);
        }

        let client = builder.build().await.map_err(|e| anyhow!("{}", e))?;

        Ok(Self {
            client,
            edge_name: edge_name.to_string(),
            token: token.to_string(),
            topic_clients: Mutex::new(HashMap::new()),
        })
    }

    /// Get or create a cached gRPC client for the broker that owns a topic.
    ///
    /// On first call: lookup topic → get channel → create client → cache it.
    /// On subsequent calls: return the cached client (cheap clone).
    ///
    /// Same pattern as `TopicProducer::connect()` which caches the
    /// `ProducerServiceClient` in `ProducerState::Ready`.
    async fn get_or_connect(&self, topic_name: &str) -> Result<EdgeGrpcClient> {
        // Fast path: return cached client
        {
            let cache = self.topic_clients.lock().await;
            if let Some(client) = cache.get(topic_name) {
                return Ok(client.clone());
            }
        }

        // Slow path: lookup + connect + cache
        let broker = self
            .client
            .resolve_topic_broker(topic_name)
            .await
            .map_err(|e| anyhow!("topic lookup failed for '{}': {}", topic_name, e))?;

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
    /// fresh lookup + creates a new client — the topic may have moved to
    /// a different broker.
    pub async fn invalidate_topic_broker(&self, topic_name: &str) {
        let mut cache = self.topic_clients.lock().await;
        if cache.remove(topic_name).is_some() {
            debug!(topic = %topic_name, "invalidated cached broker client");
        }
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

    /// Create a topic on the cloud cluster for replication.
    ///
    /// Resolves the owning broker via topic lookup, then calls
    /// `CreateEdgeTopic` on that broker.
    pub async fn create_topic(&self, topic_name: &str) -> Result<()> {
        let mut client = self.get_or_connect(topic_name).await?;

        let mut request = Request::new(CreateEdgeTopicRequest {
            topic_name: topic_name.to_string(),
            schema: None, // TODO: schema support in Phase 2b
        });
        self.authenticate(&mut request);

        let response = client.create_edge_topic(request).await?.into_inner();

        if response.success {
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

    /// Send a batch of messages for a topic to the cloud cluster.
    ///
    /// Uses the cached gRPC client for the topic's broker.
    /// On connection failure, the caller should call `invalidate_topic_broker()`
    /// and retry — the next call will re-lookup and create a new client.
    pub async fn send_batch(
        &self,
        topic_name: &str,
        messages: Vec<StreamMessage>,
        batch_last_offset: u64,
    ) -> Result<u64> {
        let mut client = self.get_or_connect(topic_name).await?;

        // Convert StreamMessages to proto ReplicateMessages
        let replicate_messages: Vec<ReplicateMessage> = messages
            .into_iter()
            .map(|msg| ReplicateMessage {
                payload: msg.payload.to_vec(),
                attributes: msg.attributes,
                edge_offset: msg.msg_id.topic_offset,
            })
            .collect();

        let batch = ReplicateBatch {
            topic_name: topic_name.to_string(),
            messages: replicate_messages,
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

    /// Get the edge name.
    pub fn edge_name(&self) -> &str {
        &self.edge_name
    }
}

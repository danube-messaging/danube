//! gRPC client for connecting to the cloud cluster's `EdgeReplicatorService`.
//!
//! Wraps the generated `EdgeReplicatorServiceClient` with reconnection logic
//! and provides typed methods for topic creation and data replication.
//!
//! Authentication uses the same mechanism as any Danube client:
//! - JWT token sent as `authorization: Bearer <token>` header
//! - Or mTLS for trusted infrastructure

use anyhow::{anyhow, Result};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::Request;
use tracing::info;

use crate::proto::edge_replicator_service_client::EdgeReplicatorServiceClient;
use crate::proto::{CreateEdgeTopicRequest, ReplicateBatch, ReplicateMessage};
use danube_core::message::StreamMessage;

/// Client for connecting to the cloud cluster's EdgeReplicatorService.
#[derive(Clone)]
pub struct EdgeCloudClient {
    cloud_url: String,
    edge_name: String,
    token: String,
}

impl EdgeCloudClient {
    pub fn new(cloud_url: &str, edge_name: &str, token: &str) -> Self {
        Self {
            cloud_url: cloud_url.to_string(),
            edge_name: edge_name.to_string(),
            token: token.to_string(),
        }
    }

    /// Connect to the cloud cluster.
    async fn connect(&self) -> Result<EdgeReplicatorServiceClient<Channel>> {
        let client = EdgeReplicatorServiceClient::connect(self.cloud_url.clone()).await?;
        Ok(client)
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
    pub async fn create_topic(&self, topic_name: &str) -> Result<()> {
        let mut client = self.connect().await?;

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
    /// Opens a bidirectional stream, sends the batch, waits for the ack,
    /// and returns the acked offset.
    pub async fn send_batch(
        &self,
        topic_name: &str,
        messages: Vec<StreamMessage>,
        batch_last_offset: u64,
    ) -> Result<u64> {
        let mut client = self.connect().await?;

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

    /// Get the cloud URL.
    pub fn cloud_url(&self) -> &str {
        &self.cloud_url
    }
}

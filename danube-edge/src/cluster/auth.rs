//! Edge authentication and registration traits.
//!
//! The broker implements these traits using its `Resources` (Raft metadata store).
//! The replicator crate only defines the interface — no direct dependency on broker internals.

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// Edge registration record persisted in the cluster metadata store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeRegistration {
    pub edge_name: String,
    pub namespace: String,
    pub registered_at: String,
    pub status: String,
}

/// Trait for edge authentication and registration.
///
/// The cluster broker implements this trait using its JWT validation and
/// Raft metadata store. The replicator service calls these methods during
/// `RegisterEdge` to validate tokens and persist registrations.
#[async_trait]
pub trait EdgeAuth: Send + Sync + 'static {
    /// Validate an edge token (JWT) and return the namespace the edge is authorized for.
    ///
    /// The implementation should:
    /// 1. Decode the JWT using the cluster's secret key
    /// 2. Verify the subject matches `edge_name`
    /// 3. Check that namespace `/{edge_name}` exists
    /// 4. Return the namespace path (e.g., "/edge1")
    async fn validate_edge_token(&self, edge_name: &str, token: &str) -> Result<String>;

    /// Check if an edge is already registered.
    async fn is_edge_registered(&self, edge_name: &str) -> bool;

    /// Get the namespace for a registered edge.
    async fn get_edge_namespace(&self, edge_name: &str) -> Option<String>;

    /// Persist edge registration in the metadata store.
    async fn register_edge(&self, registration: &EdgeRegistration) -> Result<()>;

    /// Validate that a topic name belongs to the edge's namespace.
    /// Returns `Ok(())` if `/edge_name/...` matches, `Err` otherwise.
    fn validate_topic_namespace(&self, edge_name: &str, topic_name: &str) -> Result<()> {
        let expected_prefix = format!("/{}/", edge_name);
        if topic_name.starts_with(&expected_prefix) {
            Ok(())
        } else {
            anyhow::bail!(
                "topic '{}' does not belong to edge namespace '/{}'. \
                 Edge brokers can only create topics under their own namespace.",
                topic_name,
                edge_name
            )
        }
    }
}

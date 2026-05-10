//! Edge registry — per-edge state management in Raft.
//!
//! Stores the cluster's view of each registered edge broker:
//! - Which topics the edge declared
//! - Resolved schema info for each topic
//! - A `config_version` counter that bumps on any relevant change
//!
//! Any cluster broker can read/write this state since it lives in Raft.

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, info};

use danube_core::metadata::{MetaOptions, MetadataStore};

/// Raft key prefix for edge registry data.
const EDGE_REGISTRY_PREFIX: &str = "/edge_registry";

/// Serializable per-edge state stored in Raft.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct EdgeState {
    /// Monotonically increasing version. Bumped when schemas or topics change.
    pub config_version: u64,
    /// Unix timestamp of initial registration.
    pub registered_at: u64,
    /// Unix timestamp of last heartbeat.
    pub last_heartbeat: u64,
    /// Per-topic state (topic_name → topic state).
    pub topics: HashMap<String, EdgeTopicState>,
}

/// Per-topic state within an edge registration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct EdgeTopicState {
    /// Schema subject declared by the edge config (None = raw bytes).
    pub schema_subject: Option<String>,
    /// Resolved schema ID from the cluster registry.
    pub schema_id: Option<u64>,
    /// Resolved schema version number.
    pub schema_version: Option<u32>,
    /// Fingerprint of the resolved schema (for change detection).
    pub schema_fingerprint: Option<String>,
}

/// Manages per-edge state in the Raft metadata store.
#[derive(Clone, Debug)]
pub(crate) struct EdgeRegistry<S> {
    store: S,
}

impl<S: MetadataStore> EdgeRegistry<S> {
    pub(crate) fn new(store: S) -> Self {
        Self { store }
    }

    /// Raft key for an edge's state.
    fn edge_key(edge_name: &str) -> String {
        format!("{}/{}", EDGE_REGISTRY_PREFIX, edge_name)
    }

    /// Store or update the edge state in Raft.
    pub(crate) async fn store_edge_state(
        &self,
        edge_name: &str,
        state: &EdgeState,
    ) -> Result<()> {
        let key = Self::edge_key(edge_name);
        let value = serde_json::to_value(state)
            .map_err(|e| anyhow!("failed to serialize edge state: {}", e))?;
        self.store.put(&key, value, MetaOptions::None).await?;
        debug!(edge_name = %edge_name, config_version = state.config_version, "stored edge state in Raft");
        Ok(())
    }

    /// Load edge state from Raft.
    pub(crate) async fn load_edge_state(&self, edge_name: &str) -> Result<Option<EdgeState>> {
        let key = Self::edge_key(edge_name);
        match self.store.get(&key, MetaOptions::None).await? {
            Some(value) => {
                let state: EdgeState = serde_json::from_value(value)
                    .map_err(|e| anyhow!("failed to deserialize edge state: {}", e))?;
                Ok(Some(state))
            }
            None => Ok(None),
        }
    }

    /// Bump `config_version` for all edges that use a given schema subject.
    ///
    /// Called by schema registry post-hooks when a schema is updated or deleted.
    /// Returns the list of edge names that were bumped.
    pub(crate) async fn bump_version_for_subject(
        &self,
        schema_subject: &str,
    ) -> Result<Vec<String>> {
        let edge_names = self.list_edge_names().await?;
        let mut bumped = Vec::new();

        for edge_name in edge_names {
            if let Some(mut state) = self.load_edge_state(&edge_name).await? {
                let uses_subject = state.topics.values().any(|t| {
                    t.schema_subject
                        .as_deref()
                        .map_or(false, |s| s == schema_subject)
                });

                if uses_subject {
                    state.config_version += 1;
                    self.store_edge_state(&edge_name, &state).await?;
                    info!(
                        edge_name = %edge_name,
                        schema_subject = %schema_subject,
                        new_version = state.config_version,
                        "bumped edge config_version after schema change"
                    );
                    bumped.push(edge_name);
                }
            }
        }

        Ok(bumped)
    }

    /// Update the last_heartbeat timestamp for an edge.
    pub(crate) async fn touch_heartbeat(&self, edge_name: &str) -> Result<()> {
        if let Some(mut state) = self.load_edge_state(edge_name).await? {
            state.last_heartbeat = now_secs();
            self.store_edge_state(edge_name, &state).await?;
        }
        Ok(())
    }

    /// List all registered edge names.
    pub(crate) async fn list_edge_names(&self) -> Result<Vec<String>> {
        let children = self
            .store
            .get_childrens(EDGE_REGISTRY_PREFIX)
            .await
            .unwrap_or_default();

        let names: Vec<String> = children
            .iter()
            .filter_map(|key| key.strip_prefix(&format!("{}/", EDGE_REGISTRY_PREFIX)))
            .map(|s| s.to_string())
            .collect();

        Ok(names)
    }
}

/// Current Unix timestamp in seconds.
pub(crate) fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

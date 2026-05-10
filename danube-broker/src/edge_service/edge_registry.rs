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
/// Path format: /edge_registry/edges/{edge_name}
/// The 3-segment prefix (/edge_registry/edges) satisfies MetadataStore's
/// path structure; the edge name is the key component.
const EDGE_REGISTRY_PREFIX: &str = "/edge_registry/edges";

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
        let entries = self
            .store
            .get_bulk(EDGE_REGISTRY_PREFIX)
            .await
            .unwrap_or_default();

        let prefix_with_slash = format!("{}/", EDGE_REGISTRY_PREFIX);
        let names: Vec<String> = entries
            .iter()
            .filter_map(|kv| kv.key.strip_prefix(&prefix_with_slash))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata_storage::MetadataStorage;
    use danube_core::metadata::MemoryStore;

    async fn test_registry() -> EdgeRegistry<MetadataStorage> {
        let mem = MemoryStore::new().await.expect("init memory store");
        EdgeRegistry::new(MetadataStorage::InMemory(mem))
    }

    fn make_edge_state(topics: Vec<(&str, Option<&str>)>) -> EdgeState {
        let mut topic_map = HashMap::new();
        for (name, subject) in topics {
            topic_map.insert(
                name.to_string(),
                EdgeTopicState {
                    schema_subject: subject.map(|s| s.to_string()),
                    schema_id: subject.map(|_| 1),
                    schema_version: subject.map(|_| 1),
                    schema_fingerprint: subject.map(|_| "fp".to_string()),
                },
            );
        }
        EdgeState {
            config_version: 1,
            registered_at: now_secs(),
            last_heartbeat: now_secs(),
            topics: topic_map,
        }
    }

    #[tokio::test]
    async fn store_and_load_roundtrip() {
        let reg = test_registry().await;
        let state = make_edge_state(vec![("/e1/data", Some("events")), ("/e1/raw", None)]);

        reg.store_edge_state("edge1", &state).await.unwrap();
        let loaded = reg.load_edge_state("edge1").await.unwrap().unwrap();

        assert_eq!(loaded.config_version, 1);
        assert_eq!(loaded.topics.len(), 2);
        assert_eq!(
            loaded.topics["/e1/data"].schema_subject,
            Some("events".to_string())
        );
        assert_eq!(loaded.topics["/e1/raw"].schema_subject, None);
    }

    #[tokio::test]
    async fn load_missing_returns_none() {
        let reg = test_registry().await;
        let result = reg.load_edge_state("nonexistent").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn bump_version_for_subject_targets_correct_edges() {
        let reg = test_registry().await;

        // edge1 uses "events" schema
        let state1 = make_edge_state(vec![("/e1/data", Some("events"))]);
        reg.store_edge_state("edge1", &state1).await.unwrap();

        // edge2 uses "metrics" schema
        let state2 = make_edge_state(vec![("/e2/metrics", Some("metrics"))]);
        reg.store_edge_state("edge2", &state2).await.unwrap();

        // edge3 uses "events" schema too
        let state3 = make_edge_state(vec![("/e3/data", Some("events"))]);
        reg.store_edge_state("edge3", &state3).await.unwrap();

        // Bump for "events" — should only affect edge1 and edge3
        let bumped = reg.bump_version_for_subject("events").await.unwrap();
        assert_eq!(bumped.len(), 2);
        assert!(bumped.contains(&"edge1".to_string()));
        assert!(bumped.contains(&"edge3".to_string()));

        // Verify versions
        let e1 = reg.load_edge_state("edge1").await.unwrap().unwrap();
        assert_eq!(e1.config_version, 2);

        let e2 = reg.load_edge_state("edge2").await.unwrap().unwrap();
        assert_eq!(e2.config_version, 1); // unchanged

        let e3 = reg.load_edge_state("edge3").await.unwrap().unwrap();
        assert_eq!(e3.config_version, 2);
    }

    #[tokio::test]
    async fn bump_version_no_match_returns_empty() {
        let reg = test_registry().await;
        let state = make_edge_state(vec![("/e1/raw", None)]);
        reg.store_edge_state("edge1", &state).await.unwrap();

        let bumped = reg
            .bump_version_for_subject("nonexistent-subject")
            .await
            .unwrap();
        assert!(bumped.is_empty());

        // Version unchanged
        let loaded = reg.load_edge_state("edge1").await.unwrap().unwrap();
        assert_eq!(loaded.config_version, 1);
    }

    #[tokio::test]
    async fn touch_heartbeat_updates_timestamp() {
        let reg = test_registry().await;
        let mut state = make_edge_state(vec![]);
        state.last_heartbeat = 1000; // old timestamp
        reg.store_edge_state("edge1", &state).await.unwrap();

        reg.touch_heartbeat("edge1").await.unwrap();

        let loaded = reg.load_edge_state("edge1").await.unwrap().unwrap();
        assert!(loaded.last_heartbeat > 1000); // updated to current time
    }

    #[tokio::test]
    async fn touch_heartbeat_noop_for_missing_edge() {
        let reg = test_registry().await;
        // Should not panic
        reg.touch_heartbeat("nonexistent").await.unwrap();
    }

    #[tokio::test]
    async fn list_edge_names_returns_all_registered() {
        let reg = test_registry().await;
        let state = make_edge_state(vec![]);

        reg.store_edge_state("alpha", &state).await.unwrap();
        reg.store_edge_state("beta", &state).await.unwrap();
        reg.store_edge_state("gamma", &state).await.unwrap();

        let mut names = reg.list_edge_names().await.unwrap();
        names.sort();
        assert_eq!(names, vec!["alpha", "beta", "gamma"]);
    }

    #[tokio::test]
    async fn list_edge_names_empty_when_none_registered() {
        let reg = test_registry().await;
        let names = reg.list_edge_names().await.unwrap();
        assert!(names.is_empty());
    }
}

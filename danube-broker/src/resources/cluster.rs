use crate::metadata_storage::MetadataStorage;
use anyhow::Result;
use danube_core::admin_proto::BrokerInfo;
use danube_core::metadata::{MetaOptions, MetadataStore};
use danube_raft::leadership::LeadershipHandle;
use serde_json::Value;

use crate::{
    resources::{BASE_BROKER_PATH, BASE_CLUSTER_PATH, BASE_NAMESPACES_PATH},
    utils::join_path,
    LocalCache,
};

use super::{BASE_REGISTER_PATH, BASE_UNASSIGNED_PATH};

#[derive(Debug, Clone)]
pub(crate) struct ClusterResources {
    local_cache: LocalCache,
    store: MetadataStorage,
    leadership: Option<LeadershipHandle>,
}

impl ClusterResources {
    pub(crate) fn new(
        local_cache: LocalCache,
        store: MetadataStorage,
        leadership: Option<LeadershipHandle>,
    ) -> Self {
        ClusterResources {
            local_cache,
            store,
            leadership,
        }
    }

    pub(crate) async fn create(&mut self, path: &str, data: Value) -> Result<()> {
        self.store.put(path, data, MetaOptions::None).await?;
        Ok(())
    }

    pub(crate) async fn delete(&mut self, path: &str) -> Result<()> {
        self.store.delete(path).await?;
        Ok(())
    }

    pub(crate) async fn create_cluster(&mut self, path: &str) -> Result<()> {
        let path = join_path(&[BASE_CLUSTER_PATH, path]);
        self.create(&path, serde_json::Value::Null).await?;
        Ok(())
    }

    pub(crate) async fn new_unassigned_topic(&mut self, topic_name: &str) -> Result<()> {
        let path = join_path(&[BASE_UNASSIGNED_PATH, topic_name]);
        self.create(&path, serde_json::Value::Null).await?;
        Ok(())
    }

    /// Mark a topic for unload by creating an unassigned entry with an unload marker.
    /// Value format: {"reason":"unload", "from_broker": <broker_id>}
    pub(crate) async fn mark_topic_for_unload(
        &mut self,
        topic_name: &str,
        from_broker_id: u64,
    ) -> Result<()> {
        let marker = serde_json::json!({
            "reason": "unload",
            "from_broker": from_broker_id
        });
        let path = join_path(&[BASE_UNASSIGNED_PATH, topic_name]);
        self.create(&path, marker).await
    }

    pub(crate) async fn schedule_topic_deletion(
        &mut self,
        broker_id: &str,
        topic_name: &str,
    ) -> Result<()> {
        let path = join_path(&[BASE_BROKER_PATH, broker_id, topic_name]);
        self.delete(&path).await?;
        Ok(())
    }

    // search all the paths for the topic name and return broker_id
    // search for /{namespace}/{topic} as part of the  /cluster/brokers/*
    // example /cluster/brokers/{broker_id}/{namespace}/{topic})
    pub(crate) async fn get_broker_for_topic(&self, topic_name: &str) -> Option<String> {
        let keys = self
            .local_cache
            .get_keys_with_prefix(&BASE_BROKER_PATH)
            .await;
        for path in keys {
            if let Some(pos) = path.find(topic_name) {
                let parts: Vec<&str> = path[..pos].split('/').collect();
                if parts.len() > 3 {
                    let broker_id = parts[3];
                    return Some(broker_id.to_string());
                }
            }
        }
        None
    }

    // get the broker_id for all registered brokers
    pub(crate) async fn get_brokers(&self) -> Vec<String> {
        let paths = self
            .local_cache
            .get_keys_with_prefix(&BASE_REGISTER_PATH)
            .await;

        let mut broker_ids = Vec::new();

        for path in paths {
            let parts: Vec<&str> = path.split('/').collect();

            if let Some(broker_id) = parts.get(3) {
                broker_ids.push(broker_id.to_string());
            }
        }

        broker_ids
    }

    /// Sets the broker state under /cluster/brokers/{broker_id}/state with a small JSON payload
    /// Example: {"mode":"draining","reason":"admin_unload"}
    pub(crate) async fn set_broker_state(
        &self,
        broker_id: &str,
        mode: &str,
        reason: Option<&str>,
    ) -> Result<()> {
        let path = join_path(&[BASE_BROKER_PATH, broker_id, "state"]);
        let value = if let Some(r) = reason {
            serde_json::json!({"mode": mode, "reason": r})
        } else {
            serde_json::json!({"mode": mode})
        };
        self.store.put(&path, value, MetaOptions::None).await?;
        Ok(())
    }

    /// Returns the list of topics currently assigned to the given broker as /namespace/topic strings
    pub(crate) async fn get_topics_for_broker(&self, broker_id: &str) -> Vec<String> {
        let prefix = join_path(&[BASE_BROKER_PATH, broker_id]);
        let keys = self.local_cache.get_keys_with_prefix(&prefix).await;
        let mut topics = Vec::new();
        for key in keys {
            let parts: Vec<&str> = key.split('/').collect();
            if parts.len() >= 6 {
                // /cluster/brokers/{broker_id}/{ns}/{topic}
                let ns = parts[4];
                let topic = parts[5];
                topics.push(format!("/{}/{}", ns, topic));
            }
        }
        topics
    }

    /// Returns true if broker state is active; if state key is missing, default to active
    pub(crate) async fn is_broker_active(&self, broker_id: &str) -> bool {
        let path = join_path(&[BASE_BROKER_PATH, broker_id, "state"]);
        match self.store.get(&path, MetaOptions::None).await {
            Ok(Some(val)) => val
                .get("mode")
                .and_then(|m| m.as_str())
                .map(|m| m == "active")
                .unwrap_or(true),
            _ => true,
        }
    }

    /// Returns (broker_url, connect_url) for a registered broker.
    /// broker_url is the internal identity, connect_url is the client-facing address.
    pub(crate) fn get_broker_urls(&self, broker_id: &str) -> Option<(String, String)> {
        let path = join_path(&[BASE_REGISTER_PATH, broker_id]);
        let value = self.local_cache.get(&path)?;

        match value {
            Value::Object(map) => {
                let broker_url = map
                    .get("broker_url")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())?;
                let connect_url = map
                    .get("connect_url")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| broker_url.clone());
                Some((broker_url, connect_url))
            }
            _ => None,
        }
    }

    /// Returns the full registration JSON object stored under /cluster/register/{broker_id}
    /// This is used to access additional fields like admin_addr and prom_exporter when available.
    pub(crate) fn get_broker_register_info(
        &self,
        broker_id: &str,
    ) -> Option<serde_json::Map<String, Value>> {
        let path = join_path(&[BASE_REGISTER_PATH, broker_id]);
        match self.local_cache.get(&path)? {
            Value::Object(map) => Some(map),
            _ => None,
        }
    }

    /// Returns the current Raft leader's node_id by querying the Raft runtime
    /// directly, rather than reading a stale metadata key from LocalCache.
    pub(crate) fn get_cluster_leader(&self) -> Option<u64> {
        self.leadership.as_ref().and_then(|h| h.current_leader())
    }

    /// Returns the full registration JSON object stored under /cluster/register/{broker_id}
    pub(crate) fn get_broker_info(&self, broker_id: &str) -> Option<BrokerInfo> {
        // Determine role first
        let mut broker_role = "None".to_string();

        if let Some(leader) = self.get_cluster_leader() {
            if leader.to_string() == broker_id {
                broker_role = "Cluster_Leader".to_string();
            } else {
                broker_role = "Cluster_Follower".to_string();
            }
        };
        // Read registration JSON and construct BrokerInfo.
        let map = self.get_broker_register_info(broker_id)?;
        let broker_addr = map
            .get("broker_url")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())?;
        let admin_addr = map
            .get("admin_addr")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let metrics_addr = map
            .get("prom_exporter")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        Some(BrokerInfo {
            broker_id: broker_id.to_string(),
            broker_addr,
            broker_role,
            admin_addr,
            metrics_addr,
            broker_status: "active".to_string(),
        })
    }

    // get the cluster namespaces
    pub(crate) async fn get_namespaces(&self) -> Vec<String> {
        let paths = self
            .local_cache
            .get_keys_with_prefix(&BASE_NAMESPACES_PATH)
            .await;

        let mut namespaces = Vec::new();

        for path in paths {
            let parts: Vec<&str> = path.split('/').collect();

            if let Some(namespace) = parts.get(2) {
                namespaces.push(namespace.to_string());
            }
        }

        namespaces
    }

    /// Returns a map of broker_id -> broker_status (mode), read from
    /// /cluster/brokers/{broker_id}/state JSON payloads.
    /// If state is missing or malformed, defaults to "active".
    pub(crate) async fn get_brokers_state(&self) -> std::collections::HashMap<String, String> {
        use std::collections::HashMap;
        let mut out: HashMap<String, String> = HashMap::new();
        let broker_ids = self.get_brokers().await;
        for broker_id in broker_ids.iter() {
            let path = crate::utils::join_path(&[BASE_BROKER_PATH, broker_id, "state"]);
            let mode = match self.store.get(&path, MetaOptions::None).await {
                Ok(Some(val)) => val
                    .get("mode")
                    .and_then(|m| m.as_str())
                    .unwrap_or("active")
                    .to_string(),
                _ => "active".to_string(),
            };
            out.insert(broker_id.clone(), mode);
        }
        out
    }
}

use anyhow::{Ok, Result};
use danube_metadata_store::{MetaOptions, MetadataStore, MetadataStorage};
use serde_json::Value;

use crate::{
    resources::{BASE_BROKER_PATH, BASE_CLUSTER_PATH, BASE_NAMESPACES_PATH, LEADER_ELECTION_PATH},
    utils::join_path,
    LocalCache,
};

use super::{BASE_REGISTER_PATH, BASE_UNASSIGNED_PATH};

#[derive(Debug, Clone)]
pub(crate) struct ClusterResources {
    local_cache: LocalCache,
    store: MetadataStorage,
}

impl ClusterResources {
    pub(crate) fn new(local_cache: LocalCache, store: MetadataStorage) -> Self {
        ClusterResources { local_cache, store }
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

    pub(crate) fn get_broker_addr(&self, broker_id: &str) -> Option<String> {
        let path = join_path(&[BASE_REGISTER_PATH, broker_id]);
        let value = self.local_cache.get(&path)?;

        match value {
            Value::String(broker_addr) => return Some(broker_addr),
            _ => return None,
        }
    }

    pub(crate) fn get_cluster_leader(&self) -> Option<u64> {
        let value = self.local_cache.get(LEADER_ELECTION_PATH)?;

        match value {
            Value::Number(broker_addr) => broker_addr.as_u64(),
            _ => None,
        }
    }

    pub(crate) fn get_broker_info(&self, broker_id: &str) -> Option<(String, String, String)> {
        let broker_addr = self.get_broker_addr(broker_id)?;
        let mut cluster_leader = "None".to_string();

        if let Some(leader) = self.get_cluster_leader() {
            if leader.to_string() == broker_id {
                cluster_leader = "Cluster_Leader".to_string();
            } else {
                cluster_leader = "Cluster_Follower".to_string();
            }
        };

        Some((broker_id.to_string(), broker_addr, cluster_leader))
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
}

use anyhow::{anyhow, Result};
use danube_metadata_store::{MetaOptions, MetadataStore, MetadataStorage};
use serde_json::Value;

use crate::{policies::Policies, resources::BASE_NAMESPACES_PATH, utils::join_path, LocalCache};

#[derive(Debug, Clone)]
pub(crate) struct NamespaceResources {
    local_cache: LocalCache,
    store: MetadataStorage,
}

impl NamespaceResources {
    pub(crate) fn new(local_cache: LocalCache, store: MetadataStorage) -> Self {
        NamespaceResources { local_cache, store }
    }

    pub(crate) async fn namespace_exist(&mut self, namespace_name: &str) -> Result<bool> {
        let path = join_path(&[BASE_NAMESPACES_PATH, namespace_name, "policy"]);
        let value = self.store.get(&path, MetaOptions::None).await?;
        if value.is_none() {
            return Ok(false);
        }
        Ok(true)
    }

    pub(crate) async fn create_namespace(
        &mut self,
        ns_name: &str,
        policies: Option<&Policies>,
    ) -> Result<()> {
        let new_pol = Policies::new();
        let pol = policies.unwrap_or_else(|| &new_pol);
        self.create_policies(ns_name, pol).await?;

        Ok(())
    }

    pub(crate) async fn delete_namespace(&mut self, ns_name: &str) -> Result<()> {
        let exist = self.namespace_exist(ns_name).await?;

        match exist {
            true => {
                // check if empty
                let topics = self.get_topics_for_namespace(ns_name).await;

                if !topics.is_empty() {
                    return Err(anyhow!("Can't delete and non empty namespace: {}", ns_name));
                }
            }
            false => return Err(anyhow!("Namespace {} does not exists.", ns_name)),
        }

        let path = join_path(&[BASE_NAMESPACES_PATH, ns_name, "policy"]);

        self.delete(&path).await?;

        Ok(())
    }

    pub(crate) async fn create_policies(
        &mut self,
        namespace_name: &str,
        policies: &Policies,
    ) -> Result<()> {
        let path = join_path(&[BASE_NAMESPACES_PATH, namespace_name, "policy"]);
        let value = serde_json::to_value(policies)?;
        self.create(&path, value).await?;
        Ok(())
    }

    pub(crate) fn get_policies(&self, namespace_name: &str) -> Result<Policies> {
        let path = join_path(&[BASE_NAMESPACES_PATH, namespace_name, "policy"]);
        let result = self.local_cache.get(&path);
        let value = if let Some(value) = result {
            value
        } else {
            return Err(anyhow!("Unable to retrieve the policies for the namespace"));
        };

        let policies: Policies = serde_json::from_value(value)?;

        Ok(policies)
    }

    pub(crate) async fn create(&mut self, path: &str, data: Value) -> Result<()> {
        self.store.put(path, data, MetaOptions::None).await?;
        Ok(())
    }

    pub(crate) async fn delete(&mut self, path: &str) -> Result<()> {
        let _prev_value = self.store.delete(path).await?;
        Ok(())
    }

    pub(crate) fn check_if_topic_exist(&self, ns_name: &str, topic_name: &str) -> bool {
        let path = join_path(&[BASE_NAMESPACES_PATH, ns_name, "topics", topic_name]);

        match self.local_cache.get(&path) {
            Some(_) => return true,
            None => return false,
        }
    }

    pub(crate) async fn create_new_topic(&mut self, topic_name: &str) -> Result<()> {
        let parts: Vec<_> = topic_name.split("/").collect();
        let ns_name = parts[1];
        let path = join_path(&[BASE_NAMESPACES_PATH, ns_name, "topics", topic_name]);

        self.create(&path, serde_json::Value::Null).await?;

        Ok(())
    }

    pub(crate) async fn delete_topic(&mut self, topic_name: &str) -> Result<()> {
        let parts: Vec<_> = topic_name.split("/").collect();
        let ns_name = parts[1];
        let path = join_path(&[BASE_NAMESPACES_PATH, ns_name, "topics", topic_name]);

        self.delete(&path).await?;

        Ok(())
    }

    pub(crate) async fn get_topics_for_namespace(&self, ns_name: &str) -> Vec<String> {
        let path = join_path(&[BASE_NAMESPACES_PATH, ns_name]);

        let keys = self.local_cache.get_keys_with_prefix(&path).await;

        let mut topics = Vec::new();

        for key in keys {
            let parts: Vec<&str> = key.split('/').collect();

            if let (Some(namespace), Some(topic)) = (parts.get(4), parts.get(5)) {
                let topic_name = join_path(&[namespace, topic]);
                topics.push(topic_name);
            }
        }

        topics
    }

    pub(crate) async fn get_topic_partitions(
        &self,
        ns_name: &str,
        topic_name: &str,
    ) -> Vec<String> {
        let path = join_path(&[BASE_NAMESPACES_PATH, ns_name, "topics", topic_name]);

        let keys = self.local_cache.get_keys_with_prefix(&path).await;

        let mut topics = Vec::new();

        for key in keys {
            let parts: Vec<&str> = key.split('/').collect();

            if let (Some(namespace), Some(topic)) = (parts.get(4), parts.get(5)) {
                let topic_name = join_path(&["/", namespace, topic]);
                topics.push(topic_name);
            }
        }

        topics
    }
}

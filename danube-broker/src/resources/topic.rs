use anyhow::{anyhow, Result};
use danube_core::dispatch_strategy::ConfigDispatchStrategy;
use danube_metadata_store::{MetaOptions, MetadataStorage, MetadataStore};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    policies::Policies, 
    resources::BASE_TOPICS_PATH, 
    schema::types::ValidationPolicy,
    utils::join_path, 
    LocalCache
};
// Phase 6: Old schema API import removed
// use crate::schema::Schema;

#[derive(Debug, Clone)]
pub(crate) struct TopicResources {
    local_cache: LocalCache,
    store: MetadataStorage,
}

impl TopicResources {
    pub(crate) fn new(local_cache: LocalCache, store: MetadataStorage) -> Self {
        TopicResources { local_cache, store }
    }
    pub(crate) async fn topic_exists(&mut self, topic_name: &str) -> Result<bool> {
        let path = join_path(&[BASE_TOPICS_PATH, topic_name]);
        let topic = self.store.get(&path, MetaOptions::None).await?;
        if topic.is_none() {
            return Ok(false);
        }

        Ok(true)
    }

    pub(crate) async fn create(&mut self, path: &str, data: Value) -> Result<()> {
        self.store.put(path, data, MetaOptions::None).await?;
        Ok(())
    }

    pub(crate) async fn delete(&mut self, path: &str) -> Result<()> {
        let _prev_value = self.store.delete(path).await?;
        Ok(())
    }

    pub(crate) async fn add_topic_policy(
        &mut self,
        topic_name: &str,
        policies: Policies,
    ) -> Result<()> {
        let path = join_path(&[BASE_TOPICS_PATH, topic_name, "policy"]);
        let data = serde_json::to_value(policies).unwrap();
        self.create(&path, data).await?;

        Ok(())
    }

    pub(crate) async fn add_topic_delivery(
        &mut self,
        topic_name: &str,
        dispatch_strategy: ConfigDispatchStrategy,
    ) -> Result<()> {
        let path = join_path(&[BASE_TOPICS_PATH, topic_name, "delivery"]);
        let data = serde_json::to_value(dispatch_strategy).unwrap();
        self.create(&path, data).await?;

        Ok(())
    }

    pub(crate) async fn delete_topic_schema(&mut self, topic_name: &str) -> Result<()> {
        let path = join_path(&[BASE_TOPICS_PATH, topic_name, "schema"]);
        self.delete(&path).await?;

        Ok(())
    }

    pub(crate) async fn delete_topic_delivery(&mut self, topic_name: &str) -> Result<()> {
        let path = join_path(&[BASE_TOPICS_PATH, topic_name, "delivery"]);
        self.delete(&path).await?;
        Ok(())
    }

    pub(crate) async fn delete_topic_policy(&mut self, topic_name: &str) -> Result<()> {
        let path = join_path(&[BASE_TOPICS_PATH, topic_name, "policy"]);
        self.delete(&path).await?;
        Ok(())
    }

    pub(crate) async fn delete_all_producers(&mut self, topic_name: &str) -> Result<()> {
        let prefix = join_path(&[BASE_TOPICS_PATH, topic_name, "producers"]);
        let keys = self.local_cache.get_keys_with_prefix(&prefix).await;
        for key in keys {
            // delete each producer key
            self.delete(&key).await?;
        }
        // also delete the producers directory marker if present
        let _ = self.delete(&prefix).await;
        Ok(())
    }

    pub(crate) async fn delete_all_subscriptions(&mut self, topic_name: &str) -> Result<()> {
        let prefix = join_path(&[BASE_TOPICS_PATH, topic_name, "subscriptions"]);
        let keys = self.local_cache.get_keys_with_prefix(&prefix).await;
        for key in keys {
            self.delete(&key).await?;
        }
        let _ = self.delete(&prefix).await;
        Ok(())
    }

    pub(crate) async fn delete_topic_root(&mut self, topic_name: &str) -> Result<()> {
        let path = join_path(&[BASE_TOPICS_PATH, topic_name]);
        self.delete(&path).await?;
        Ok(())
    }

    pub(crate) async fn create_topic(
        &mut self,
        topic_name: &str,
        num_partitions: usize,
    ) -> Result<()> {
        let path = join_path(&[BASE_TOPICS_PATH, topic_name]);

        //TODO! figure out how to support the partitions
        self.create(&path, num_partitions.into()).await?;

        Ok(())
    }

    pub(crate) async fn create_producer(
        &mut self,
        producer_id: u64,
        topic_name: &str,
        producer_config: Value,
    ) -> Result<()> {
        let path = join_path(&[
            BASE_TOPICS_PATH,
            topic_name,
            "producers",
            &producer_id.to_string(),
        ]);

        self.create(&path, producer_config).await?;

        Ok(())
    }

    pub(crate) async fn create_subscription(
        &mut self,
        subscription_name: &str,
        topic_name: &str,
        sub_options: Value,
    ) -> Result<()> {
        let path = join_path(&[
            BASE_TOPICS_PATH,
            topic_name,
            "subscriptions",
            subscription_name,
        ]);

        self.create(&path, sub_options).await?;

        Ok(())
    }

    pub(crate) async fn delete_subscription(
        &mut self,
        subscription_name: &str,
        topic_name: &str,
    ) -> Result<()> {
        let path = join_path(&[
            BASE_TOPICS_PATH,
            topic_name,
            "subscriptions",
            subscription_name,
        ]);

        self.delete(&path).await?;

        Ok(())
    }

    pub(crate) fn get_dispatch_strategy(&self, topic_name: &str) -> Option<ConfigDispatchStrategy> {
        let path = join_path(&[BASE_TOPICS_PATH, topic_name, "delivery"]);
        let result = self.local_cache.get(&path);
        if let Some(value) = result {
            let dispatch_strategy: Option<ConfigDispatchStrategy> =
                serde_json::from_value(value).ok();
            return dispatch_strategy;
        }
        None
    }

    pub(crate) fn get_policies(&self, topic_name: &str) -> Option<Policies> {
        let path = join_path(&[BASE_TOPICS_PATH, topic_name, "policy"]);
        let result = self.local_cache.get(&path);
        if let Some(value) = result {
            let policies: Option<Policies> = serde_json::from_value(value).ok();
            return policies;
        }
        None
    }

    //return the list of subscriptions and their respective type
    pub(crate) async fn get_subscription_for_topic(&self, topic_name: &str) -> Vec<String> {
        let path = join_path(&[BASE_TOPICS_PATH, topic_name, "subscriptions"]);

        let mut subscriptions = Vec::new();

        let paths = self.local_cache.get_keys_with_prefix(&path).await;

        for path in paths {
            let parts: Vec<&str> = path.split('/').collect();

            if let Some(subscription) = parts.get(5) {
                subscriptions.push(subscription.to_string());
            }
        }

        subscriptions
    }

    // Cursor helpers
    pub(crate) async fn set_subscription_cursor(
        &mut self,
        subscription_name: &str,
        topic_name: &str,
        offset: u64,
    ) -> Result<()> {
        let path = join_path(&[
            BASE_TOPICS_PATH,
            topic_name,
            "subscriptions",
            subscription_name,
            "cursor",
        ]);
        let value = Value::from(offset);
        self.create(&path, value).await
    }

    pub(crate) async fn get_subscription_cursor(
        &mut self,
        subscription_name: &str,
        topic_name: &str,
    ) -> Result<Option<u64>> {
        let path = join_path(&[
            BASE_TOPICS_PATH,
            topic_name,
            "subscriptions",
            subscription_name,
            "cursor",
        ]);
        let maybe = self.store.get(&path, MetaOptions::None).await?;
        if let Some(val) = maybe {
            if let Some(off) = val.as_u64() {
                return Ok(Some(off));
            }
        }
        Ok(None)
    }

    /// Store schema subject reference for a topic
    pub(crate) async fn add_topic_schema_subject(
        &mut self,
        topic_name: &str,
        schema_subject: &str,
    ) -> Result<()> {
        let path = join_path(&[BASE_TOPICS_PATH, topic_name, "schema_subject"]);
        let data = serde_json::to_value(schema_subject)?;
        self.create(&path, data).await?;
        Ok(())
    }

    /// Get schema subject for a topic
    pub(crate) async fn get_schema_subject(&self, topic_name: &str) -> Option<String> {
        let path = join_path(&[BASE_TOPICS_PATH, topic_name, "schema_subject"]);
        let result = self.local_cache.get(&path);
        if let Some(value) = result {
            return serde_json::from_value(value).ok();
        }
        None
    }

    // ========== Topic Schema Configuration (New) ==========

    /// Store topic schema configuration (subject + validation settings)
    pub(crate) async fn store_schema_config(
        &mut self,
        topic_name: &str,
        config: &TopicSchemaConfig,
    ) -> Result<()> {
        let path = join_path(&[BASE_TOPICS_PATH, topic_name, "schema_config"]);
        let data = serde_json::to_value(config)?;
        self.store.put(&path, data, MetaOptions::None).await?;
        Ok(())
    }

    /// Get topic schema configuration
    pub(crate) async fn get_schema_config(
        &self,
        topic_name: &str,
    ) -> Result<Option<TopicSchemaConfig>> {
        let path = join_path(&[BASE_TOPICS_PATH, topic_name, "schema_config"]);
        
        match self.local_cache.get(&path) {
            Some(value) => {
                let config: TopicSchemaConfig = serde_json::from_value(value)
                    .map_err(|e| anyhow!("Failed to deserialize schema config: {}", e))?;
                Ok(Some(config))
            }
            None => Ok(None),
        }
    }
}

/// Topic schema configuration
/// Stores schema subject and validation settings at the topic level
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicSchemaConfig {
    /// Schema subject assigned to this topic
    pub subject: String,
    /// Validation policy (None/Warn/Enforce) - topic-level
    pub validation_policy: ValidationPolicy,
    /// Enable deep payload validation - topic-level
    pub enable_payload_validation: bool,
}

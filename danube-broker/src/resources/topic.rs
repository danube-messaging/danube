use crate::metadata_storage::MetadataStorage;
use anyhow::{anyhow, Result};
use danube_core::dispatch_strategy::ConfigDispatchStrategy;
use danube_core::metadata::{MetaOptions, MetadataStore};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    policies::Policies, resources::BASE_TOPICS_PATH, subscription::SubscriptionFailurePolicy,
    utils::join_path,
};
use danube_schema::ValidationPolicy;

#[derive(Debug, Clone)]
pub(crate) struct TopicResources {
    store: MetadataStorage,
}

impl TopicResources {
    pub(crate) fn new(store: MetadataStorage) -> Self {
        TopicResources { store }
    }
    pub(crate) async fn topic_exists(&self, topic_name: &str) -> Result<bool> {
        let path = join_path(&[BASE_TOPICS_PATH, topic_name]);
        let topic = self.store.get(&path, MetaOptions::None).await?;
        if topic.is_none() {
            return Ok(false);
        }

        Ok(true)
    }

    pub(crate) async fn create(&self, path: &str, data: Value) -> Result<()> {
        self.store.put(path, data, MetaOptions::None).await?;
        Ok(())
    }

    pub(crate) async fn delete(&self, path: &str) -> Result<()> {
        let _prev_value = self.store.delete(path).await?;
        Ok(())
    }

    pub(crate) async fn add_topic_policy(
        &self,
        topic_name: &str,
        policies: Policies,
    ) -> Result<()> {
        let path = join_path(&[BASE_TOPICS_PATH, topic_name, "policy"]);
        let data = serde_json::to_value(policies).unwrap();
        self.create(&path, data).await?;

        Ok(())
    }

    pub(crate) async fn add_topic_delivery(
        &self,
        topic_name: &str,
        dispatch_strategy: ConfigDispatchStrategy,
    ) -> Result<()> {
        let path = join_path(&[BASE_TOPICS_PATH, topic_name, "delivery"]);
        let data = serde_json::to_value(dispatch_strategy).unwrap();
        self.create(&path, data).await?;

        Ok(())
    }

    pub(crate) async fn set_topic_intent(&self, topic_name: &str, intent: Value) -> Result<()> {
        let path = join_path(&[BASE_TOPICS_PATH, topic_name, "intent"]);
        self.create(&path, intent).await
    }

    pub(crate) async fn get_topic_intent(
        &self,
        topic_name: &str,
    ) -> Option<crate::stream_intent::ExpandedStreamSettings> {
        let path = join_path(&[BASE_TOPICS_PATH, topic_name, "intent"]);
        let value = self.store.get(&path, MetaOptions::None).await.ok().flatten()?;
        serde_json::from_value::<crate::stream_intent::ExpandedStreamSettings>(value).ok()
    }

    pub(crate) async fn set_topic_storage_mode(&self, topic_name: &str, mode: &str) -> Result<()> {
        let path = join_path(&[BASE_TOPICS_PATH, topic_name, "storage_mode"]);
        self.create(&path, serde_json::json!({ "mode": mode }))
            .await
    }

    pub(crate) async fn get_topic_storage_mode(&self, topic_name: &str) -> Option<String> {
        let path = join_path(&[BASE_TOPICS_PATH, topic_name, "storage_mode"]);
        let value = self.store.get(&path, MetaOptions::None).await.ok().flatten()?;
        value
            .get("mode")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
    }

    pub(crate) async fn get_topic_producers(&self, topic_name: &str) -> Vec<String> {
        let path = join_path(&[BASE_TOPICS_PATH, topic_name, "producers"]);
        self.store
            .get_childrens(&path)
            .await
            .unwrap_or_default()
    }

    pub(crate) async fn delete_topic_schema(&self, topic_name: &str) -> Result<()> {
        let path = join_path(&[BASE_TOPICS_PATH, topic_name, "schema"]);
        self.delete(&path).await?;

        Ok(())
    }

    pub(crate) async fn delete_topic_delivery(&self, topic_name: &str) -> Result<()> {
        let path = join_path(&[BASE_TOPICS_PATH, topic_name, "delivery"]);
        self.delete(&path).await?;
        Ok(())
    }

    pub(crate) async fn delete_topic_policy(&self, topic_name: &str) -> Result<()> {
        let path = join_path(&[BASE_TOPICS_PATH, topic_name, "policy"]);
        self.delete(&path).await?;
        Ok(())
    }

    pub(crate) async fn delete_all_producers(&self, topic_name: &str) -> Result<()> {
        let prefix = join_path(&[BASE_TOPICS_PATH, topic_name, "producers"]);
        let keys = self.store.get_childrens(&prefix).await.unwrap_or_default();
        for key in keys {
            // delete each producer key
            self.delete(&key).await?;
        }
        // also delete the producers directory marker if present
        let _ = self.delete(&prefix).await;
        Ok(())
    }

    pub(crate) async fn delete_all_subscriptions(&self, topic_name: &str) -> Result<()> {
        let prefix = join_path(&[BASE_TOPICS_PATH, topic_name, "subscriptions"]);
        let keys = self.store.get_childrens(&prefix).await.unwrap_or_default();
        for key in keys {
            self.delete(&key).await?;
        }
        let _ = self.delete(&prefix).await;
        Ok(())
    }

    pub(crate) async fn delete_topic_root(&self, topic_name: &str) -> Result<()> {
        let path = join_path(&[BASE_TOPICS_PATH, topic_name]);
        self.delete(&path).await?;
        Ok(())
    }

    pub(crate) async fn create_topic(&self, topic_name: &str, num_partitions: usize) -> Result<()> {
        let path = join_path(&[BASE_TOPICS_PATH, topic_name]);

        //TODO! figure out how to support the partitions
        self.create(&path, num_partitions.into()).await?;

        Ok(())
    }

    pub(crate) async fn create_producer(
        &self,
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
        &self,
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
        &self,
        subscription_name: &str,
        topic_name: &str,
    ) -> Result<()> {
        let cursor_path = join_path(&[
            BASE_TOPICS_PATH,
            topic_name,
            "subscriptions",
            subscription_name,
            "cursor",
        ]);
        let failure_policy_path = join_path(&[
            BASE_TOPICS_PATH,
            topic_name,
            "subscriptions",
            subscription_name,
            "failure_policy",
        ]);
        let path = join_path(&[
            BASE_TOPICS_PATH,
            topic_name,
            "subscriptions",
            subscription_name,
        ]);

        let _ = self.delete(&cursor_path).await;
        let _ = self.delete(&failure_policy_path).await;
        self.delete(&path).await?;

        Ok(())
    }

    pub(crate) async fn get_dispatch_strategy(
        &self,
        topic_name: &str,
    ) -> Option<ConfigDispatchStrategy> {
        let path = join_path(&[BASE_TOPICS_PATH, topic_name, "delivery"]);
        let value = self.store.get(&path, MetaOptions::None).await.ok()??;
        serde_json::from_value(value).ok()
    }

    pub(crate) async fn get_policies(&self, topic_name: &str) -> Option<Policies> {
        let path = join_path(&[BASE_TOPICS_PATH, topic_name, "policy"]);
        let value = self.store.get(&path, MetaOptions::None).await.ok()??;
        serde_json::from_value(value).ok()
    }

    //return the list of subscriptions and their respective type
    pub(crate) async fn get_subscription_for_topic(&self, topic_name: &str) -> Vec<String> {
        let path = join_path(&[BASE_TOPICS_PATH, topic_name, "subscriptions"]);

        let mut subscriptions = Vec::new();

        let paths = self.store.get_childrens(&path).await.unwrap_or_default();

        for key in paths {
            let suffix = key.strip_prefix(&path).unwrap_or(&key);
            let sub = suffix.trim_start_matches('/').split('/').next();
            if let Some(subscription) = sub.filter(|s| !s.is_empty()) {
                subscriptions.push(subscription.to_string());
            }
        }

        subscriptions
    }

    // Cursor helpers
    pub(crate) async fn set_subscription_cursor(
        &self,
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

    pub(crate) async fn set_subscription_next_offset(
        &self,
        subscription_name: &str,
        topic_name: &str,
        next_offset: u64,
    ) -> Result<()> {
        self.set_subscription_checkpoints(
            subscription_name,
            topic_name,
            &std::collections::HashMap::new(),
        )
        .await?;
        let path = join_path(&[
            BASE_TOPICS_PATH,
            topic_name,
            "subscriptions",
            subscription_name,
            "cursor",
        ]);
        let value = if next_offset > 0 {
            Value::from(next_offset - 1)
        } else {
            serde_json::json!({ "next_offset": 0 })
        };
        self.create(&path, value).await
    }

    pub(crate) async fn get_subscription_cursor(
        &self,
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
            if let Some(next) = val.get("next_offset").and_then(|v| v.as_u64()) {
                return Ok(Some(next.saturating_sub(1)));
            }
        }
        Ok(None)
    }

    pub(crate) async fn get_subscription_start_offset(
        &self,
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
                return Ok(Some(off.saturating_add(1)));
            }
            if let Some(next) = val.get("next_offset").and_then(|v| v.as_u64()) {
                return Ok(Some(next));
            }
        }
        Ok(None)
    }

    pub(crate) async fn set_subscription_checkpoints(
        &self,
        subscription_name: &str,
        topic_name: &str,
        checkpoints: &std::collections::HashMap<u64, String>,
    ) -> Result<()> {
        let path = join_path(&[
            BASE_TOPICS_PATH,
            topic_name,
            "subscriptions",
            subscription_name,
            "checkpoints",
        ]);
        if checkpoints.is_empty() {
            let _ = self.store.delete(&path).await;
            return Ok(());
        }
        let string_map: std::collections::HashMap<String, &String> = checkpoints
            .iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect();
        let data = serde_json::to_value(string_map)?;
        self.store.put(&path, data, MetaOptions::None).await?;
        Ok(())
    }

    pub(crate) async fn get_subscription_checkpoints(
        &self,
        subscription_name: &str,
        topic_name: &str,
    ) -> Result<std::collections::HashMap<u64, String>> {
        let path = join_path(&[
            BASE_TOPICS_PATH,
            topic_name,
            "subscriptions",
            subscription_name,
            "checkpoints",
        ]);
        match self.store.get(&path, MetaOptions::None).await? {
            Some(value) => {
                let raw: std::collections::HashMap<String, String> =
                    serde_json::from_value(value).unwrap_or_default();
                let mut parsed = std::collections::HashMap::with_capacity(raw.len());
                for (k, v) in raw {
                    if let Ok(off) = k.parse::<u64>() {
                        parsed.insert(off, v);
                    }
                }
                Ok(parsed)
            }
            None => Ok(std::collections::HashMap::new()),
        }
    }

    pub(crate) async fn set_subscription_failure_policy(
        &self,
        subscription_name: &str,
        topic_name: &str,
        failure_policy: &SubscriptionFailurePolicy,
    ) -> Result<()> {
        failure_policy.validate()?;
        let path = join_path(&[
            BASE_TOPICS_PATH,
            topic_name,
            "subscriptions",
            subscription_name,
            "failure_policy",
        ]);
        let data = serde_json::to_value(failure_policy)?;
        self.store.put(&path, data, MetaOptions::None).await?;
        Ok(())
    }

    pub(crate) async fn get_subscription_failure_policy(
        &self,
        subscription_name: &str,
        topic_name: &str,
    ) -> Result<Option<SubscriptionFailurePolicy>> {
        let path = join_path(&[
            BASE_TOPICS_PATH,
            topic_name,
            "subscriptions",
            subscription_name,
            "failure_policy",
        ]);

        match self.store.get(&path, MetaOptions::None).await? {
            Some(value) => {
                let policy: SubscriptionFailurePolicy =
                    serde_json::from_value(value).map_err(|e| {
                        anyhow!("Failed to deserialize subscription failure policy: {}", e)
                    })?;
                Ok(Some(policy))
            }
            None => Ok(None),
        }
    }

    pub(crate) async fn get_subscription_options(
        &self,
        subscription_name: &str,
        topic_name: &str,
    ) -> Result<Option<Value>> {
        let path = join_path(&[
            BASE_TOPICS_PATH,
            topic_name,
            "subscriptions",
            subscription_name,
        ]);
        self.store
            .get(&path, MetaOptions::None)
            .await
            .map_err(|e| anyhow::anyhow!(e))
    }

    pub(crate) async fn ensure_subscription_failure_policy(
        &self,
        subscription_name: &str,
        topic_name: &str,
    ) -> Result<SubscriptionFailurePolicy> {
        match self
            .get_subscription_failure_policy(subscription_name, topic_name)
            .await?
        {
            Some(policy) => {
                policy.validate()?;
                Ok(policy)
            }
            None => {
                let mut policy = SubscriptionFailurePolicy::new(topic_name);
                if let Some(intent) = self.get_topic_intent(topic_name).await {
                    policy.max_redelivery_count = intent.max_retries;
                    policy.base_redelivery_delay_ms = intent.backoff_ms;
                    if let Some(ref suffix) = intent.dlq_suffix {
                        if !suffix.trim().is_empty() {
                            policy.poison_policy =
                                crate::subscription::SubscriptionPoisonPolicy::DeadLetter;
                            policy.dead_letter_topic = Some(format!("{}{}", topic_name, suffix));
                        }
                    }
                }
                policy.validate()?;
                self.set_subscription_failure_policy(subscription_name, topic_name, &policy)
                    .await?;
                Ok(policy)
            }
        }
    }

    /// Store per-subscription dispatch config override (max_unacked_messages).
    pub(crate) async fn set_subscription_dispatch_config(
        &self,
        subscription_name: &str,
        topic_name: &str,
        max_unacked_messages: usize,
    ) -> Result<()> {
        let path = join_path(&[
            BASE_TOPICS_PATH,
            topic_name,
            "subscriptions",
            subscription_name,
            "dispatch_config",
        ]);
        let data = serde_json::to_value(
            serde_json::json!({ "max_unacked_messages": max_unacked_messages }),
        )?;
        self.store.put(&path, data, MetaOptions::None).await?;
        Ok(())
    }

    /// Get per-subscription dispatch config override.
    pub(crate) async fn get_subscription_dispatch_config(
        &self,
        subscription_name: &str,
        topic_name: &str,
    ) -> Result<Option<usize>> {
        let path = join_path(&[
            BASE_TOPICS_PATH,
            topic_name,
            "subscriptions",
            subscription_name,
            "dispatch_config",
        ]);
        match self.store.get(&path, MetaOptions::None).await? {
            Some(value) => {
                let max = value
                    .get("max_unacked_messages")
                    .and_then(|v| v.as_u64())
                    .map(|v| v as usize);
                Ok(max)
            }
            None => Ok(None),
        }
    }

    /// Store schema subject reference for a topic
    pub(crate) async fn add_topic_schema_subject(
        &self,
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
        let value = self.store.get(&path, MetaOptions::None).await.ok()??;
        serde_json::from_value(value).ok()
    }

    // ========== Topic Schema Configuration (New) ==========

    /// Store topic schema configuration (subject + validation settings)
    pub(crate) async fn store_schema_config(
        &self,
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

        match self.store.get(&path, MetaOptions::None).await? {
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

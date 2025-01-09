use anyhow::Result;
use danube_core::dispatch_strategy::ConfigDispatchStrategy;
use danube_metadata_store::{MetaOptions, MetadataStorage, MetadataStore};
use serde_json::Value;

use crate::{
    policies::Policies, resources::BASE_TOPICS_PATH, schema::Schema, utils::join_path, LocalCache,
};

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

    pub(crate) async fn add_topic_schema(
        &mut self,
        topic_name: &str,
        schema: Schema,
    ) -> Result<()> {
        let path = join_path(&[BASE_TOPICS_PATH, topic_name, "schema"]);
        let data = serde_json::to_value(&schema).unwrap();
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

    pub(crate) fn get_schema(&self, topic_name: &str) -> Option<Schema> {
        let path = join_path(&[BASE_TOPICS_PATH, topic_name, "schema"]);
        let result = self.local_cache.get(&path);
        if let Some(value) = result {
            let schema: Option<Schema> = serde_json::from_value(value).ok();
            return schema;
        }
        None
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
}

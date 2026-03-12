use anyhow::Result;
use danube_core::message::StreamMessage;
use dashmap::DashMap;
use std::sync::Arc;
use tracing::info;

use crate::{
    dispatcher::DispatchStrategy, message::AckMessage, subscription::SubscriptionOptions,
    topic::Topic,
};
use danube_core::proto::DispatchStrategy as ProtoDispatchStrategy;

#[derive(Debug)]
pub struct TopicRegistry {
    topics: Arc<DashMap<String, Arc<Topic>>>,
}

impl TopicRegistry {
    pub fn new(_capacity_hint: Option<usize>) -> Self {
        info!("topic registry initialized");
        Self {
            topics: Arc::new(DashMap::new()),
        }
    }

    pub fn add_topic(&self, topic_name: String, topic: Arc<Topic>) {
        self.topics.insert(topic_name, topic);
    }

    pub fn remove_topic(&self, topic_name: &str) -> Option<Arc<Topic>> {
        self.topics.remove(topic_name).map(|(_, topic)| topic)
    }

    #[allow(dead_code)]
    pub fn has_topic(&self, topic_name: &str) -> bool {
        self.topics.contains_key(topic_name)
    }

    /// Publish a message asynchronously
    pub async fn publish_message_async(
        &self,
        topic_name: String,
        message: StreamMessage,
    ) -> Result<()> {
        if let Some(topic) = self.get_topic(&topic_name) {
            topic.publish_message_async(message).await
        } else {
            Err(anyhow::anyhow!("Topic {} not found in registry", topic_name))
        }
    }

    /// Subscribe to a topic asynchronously
    pub async fn subscribe_async(
        &self,
        topic_name: String,
        options: SubscriptionOptions,
    ) -> Result<u64> {
        if let Some(topic) = self.get_topic(&topic_name) {
            topic.subscribe(&topic_name, options).await
        } else {
            Err(anyhow::anyhow!("Topic {} not found in registry", topic_name))
        }
    }

    /// Acknowledge a message asynchronously
    pub async fn ack_message_async(&self, ack_msg: AckMessage) -> Result<()> {
        let topic_name = ack_msg.msg_id.topic_name.clone();
        if let Some(topic) = self.get_topic(&topic_name) {
            topic.ack_message(ack_msg).await
        } else {
            Err(anyhow::anyhow!("Topic {} not found in registry", topic_name))
        }
    }

    /// Get a topic from the registry
    pub fn get_topic(&self, topic_name: &str) -> Option<Arc<Topic>> {
        self.topics
            .get(topic_name)
            .map(|entry| entry.value().clone())
    }

    /// Get all topics currently managed by the registry
    pub fn get_all_topics(&self) -> Vec<String> {
        self.topics.iter().map(|entry| entry.key().clone()).collect()
    }

    /// Check if a topic exists in the registry
    pub fn contains_topic(&self, topic_name: &str) -> bool {
        self.topics.contains_key(topic_name)
    }

    /// Returns all topics with NonReliable dispatch strategy in the registry.
    /// Used by the subscription removal to find topics that need idle cleanup.
    pub fn all_non_reliable_topics(&self) -> Vec<Arc<Topic>> {
        self.topics
            .iter()
            .filter_map(|entry| {
                if matches!(entry.value().dispatch_strategy, DispatchStrategy::NonReliable) {
                    Some(entry.value().clone())
                } else {
                    None
                }
            })
            .collect()
    }

    /// Verify that the requested dispatch strategy matches the topic's configured strategy.
    /// Returns None if the topic is not found; Some(true/false) otherwise.
    pub fn strategies_match(
        &self,
        topic_name: &str,
        requested: ProtoDispatchStrategy,
    ) -> Option<bool> {
        let topic = self.get_topic(topic_name)?;
        let expected = match requested {
            ProtoDispatchStrategy::NonReliable => DispatchStrategy::NonReliable,
            ProtoDispatchStrategy::Reliable => DispatchStrategy::Reliable,
        };
        Some(std::mem::discriminant(&topic.dispatch_strategy) == std::mem::discriminant(&expected))
    }

}

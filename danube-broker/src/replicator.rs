use anyhow::{anyhow, Result};
use danube_client::{DanubeClient, Producer};
use danube_core::{dispatch_strategy::ConfigDispatchStrategy, message::StreamMessage};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Mutex;

use crate::{resources::Resources, topic_registry::TopicRegistry};

#[derive(Debug)]
pub(crate) struct Replicator {
    broker_id: u64,
    topic_registry: Arc<TopicRegistry>,
    resources: Arc<Resources>,
    client: Mutex<Option<DanubeClient>>,
    producer_sequence: AtomicU64,
}

impl Replicator {
    pub(crate) fn new(
        broker_id: u64,
        topic_registry: Arc<TopicRegistry>,
        resources: Arc<Resources>,
    ) -> Self {
        Self {
            broker_id,
            topic_registry,
            resources,
            client: Mutex::new(None),
            producer_sequence: AtomicU64::new(0),
        }
    }

    pub(crate) async fn set_client(&self, client: DanubeClient) {
        let mut slot = self.client.lock().await;
        *slot = Some(client);
    }

    pub(crate) async fn publish_message_async(
        &self,
        topic_name: &str,
        message: StreamMessage,
    ) -> Result<()> {
        if let Some(topic) = self.topic_registry.get_topic(topic_name) {
            return topic.publish_message_internal_async(message).await;
        }

        let attributes = if message.attributes.is_empty() {
            None
        } else {
            Some(message.attributes.clone())
        };
        let payload = message.payload.to_vec();
        let producer = self.create_remote_producer(topic_name).await?;
        producer.send(payload, attributes).await?;
        Ok(())
    }

    async fn create_remote_producer(&self, topic_name: &str) -> Result<Producer> {
        let client = self
            .client
            .lock()
            .await
            .clone()
            .ok_or_else(|| anyhow!("replicator client unavailable for remote publish"))?;
        let dispatch_strategy = self
            .resources
            .topic
            .get_dispatch_strategy(topic_name)
            .await
            .ok_or_else(|| anyhow!("dispatch strategy missing for replication target {topic_name}"))?;
        let producer_name = format!(
            "broker-replicator-{}-{}",
            self.broker_id,
            self.producer_sequence.fetch_add(1, Ordering::Relaxed)
        );

        let mut builder = client
            .new_producer()
            .with_topic(topic_name.to_string())
            .with_name(producer_name);
        if dispatch_strategy == ConfigDispatchStrategy::Reliable {
            builder = builder.with_reliable_dispatch();
        }

        let mut producer = builder.build()?;
        producer.create().await?;
        Ok(producer)
    }
}

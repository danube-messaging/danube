use anyhow::{anyhow, Result};
use danube_client::{DanubeClient, Producer};
use danube_core::message::StreamMessage;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Mutex;
use tracing::{info, warn};

#[derive(Debug)]
pub(crate) struct Replicator {
    broker_id: u64,
    client: Mutex<Option<DanubeClient>>,
    /// Cached producers keyed by topic name.
    /// Avoids creating a new producer for every DLQ message.
    producers: Mutex<HashMap<String, Producer>>,
    producer_sequence: AtomicU64,
}

impl Replicator {
    pub(crate) fn new(broker_id: u64) -> Self {
        Self {
            broker_id,
            client: Mutex::new(None),
            producers: Mutex::new(HashMap::new()),
            producer_sequence: AtomicU64::new(0),
        }
    }

    /// Inject the DanubeClient after the broker gRPC server is ready.
    /// Must be called before any DLQ publish will succeed.
    pub(crate) async fn set_client(&self, client: DanubeClient) {
        let mut slot = self.client.lock().await;
        *slot = Some(client);
    }

    /// Publish a message to the given topic via the DanubeClient.
    ///
    /// Always goes through the client (lookup → connect → produce) so that
    /// the DLQ topic can be served by any broker in the cluster.
    ///
    /// Producers are cached per topic. On send failure the cached producer
    /// is evicted and a fresh one is created for a single retry, covering
    /// the case where the DLQ topic moved to another broker.
    pub(crate) async fn publish_message_async(
        &self,
        topic_name: &str,
        message: StreamMessage,
    ) -> Result<()> {
        let attributes = if message.attributes.is_empty() {
            None
        } else {
            Some(message.attributes.clone())
        };
        let payload = message.payload.to_vec();

        // First attempt: use cached (or freshly created) producer
        {
            let mut producers = self.producers.lock().await;
            if !producers.contains_key(topic_name) {
                let producer = self.create_producer(topic_name).await?;
                producers.insert(topic_name.to_string(), producer);
            }
            let producer = producers
                .get(topic_name)
                .ok_or_else(|| anyhow!("producer was just inserted but missing"))?;

            match producer.send(payload.clone(), attributes.clone()).await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    warn!(
                        topic = %topic_name,
                        error = %e,
                        "DLQ send failed on cached producer, evicting and retrying"
                    );
                    producers.remove(topic_name);
                }
            }
        }

        // Retry with a fresh producer (topic may have moved to another broker)
        let producer = self.create_producer(topic_name).await?;
        producer.send(payload, attributes).await?;
        info!(topic = %topic_name, "DLQ message sent after producer reconnect");

        let mut producers = self.producers.lock().await;
        producers.insert(topic_name.to_string(), producer);

        Ok(())
    }

    async fn create_producer(&self, topic_name: &str) -> Result<Producer> {
        let client = self
            .client
            .lock()
            .await
            .clone()
            .ok_or_else(|| anyhow!("replicator client unavailable for DLQ publish"))?;
        let producer_name = format!(
            "broker-replicator-{}-{}",
            self.broker_id,
            self.producer_sequence.fetch_add(1, Ordering::Relaxed)
        );

        let mut producer = client
            .new_producer()
            .with_topic(topic_name.to_string())
            .with_name(producer_name)
            .build()?;
        producer.create().await?;
        Ok(producer)
    }
}

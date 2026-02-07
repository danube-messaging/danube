use crate::{
    errors::{DanubeError, Result},
    retry_manager::RetryManager,
    topic_consumer::TopicConsumer,
    DanubeClient,
};

use danube_core::message::StreamMessage;
use futures::{future::join_all, StreamExt};
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

/// Buffer size for the message channel between per-partition tasks and the consumer.
const RECEIVE_CHANNEL_BUFFER: usize = 100;
/// Small delay after signaling shutdown to allow the broker to observe closure.
const GRACEFUL_CLOSE_DELAY_MS: u64 = 100;

/// Represents the type of subscription
///
/// Variants:
/// - `Exclusive`: Only one consumer can subscribe to the topic at a time.
/// - `Shared`: Multiple consumers can subscribe to the topic concurrently.
/// - `FailOver`: Only one consumer can subscribe to the topic at a time,
///             multiple can subscribe but waits in standby, if the active consumer disconnect        
#[derive(Debug, Clone)]
pub enum SubType {
    Exclusive,
    Shared,
    FailOver,
}

/// Consumer represents a message consumer that subscribes to a topic and receives messages.
/// It handles communication with the message broker and manages the consumer's state.
#[derive(Debug)]
pub struct Consumer {
    // the Danube client
    client: DanubeClient,
    // the topic name, from where the messages are consumed
    topic_name: String,
    // the name of the Consumer
    consumer_name: String,
    // the map between the partitioned topic name and the consumer instance
    consumers: HashMap<String, Arc<Mutex<TopicConsumer>>>,
    // the name of the subscription the consumer is attached to
    subscription: String,
    // the type of the subscription, that can be Shared and Exclusive
    subscription_type: SubType,
    // other configurable options for the consumer
    consumer_options: ConsumerOptions,
    // shutdown flag and task handles for graceful close
    shutdown: Arc<AtomicBool>,
    task_handles: Vec<JoinHandle<()>>,
}

impl Consumer {
    pub(crate) fn new(
        client: DanubeClient,
        topic_name: String,
        consumer_name: String,
        subscription: String,
        sub_type: Option<SubType>,
        consumer_options: ConsumerOptions,
    ) -> Self {
        let subscription_type = sub_type.unwrap_or(SubType::Shared);

        Consumer {
            client,
            topic_name,
            consumer_name,
            consumers: HashMap::new(),
            subscription,
            subscription_type,
            consumer_options,
            shutdown: Arc::new(AtomicBool::new(false)),
            task_handles: Vec::new(),
        }
    }

    /// Initializes the subscription to a non-partitioned or partitioned topic and starts the health check service.
    ///
    /// This function establishes a gRPC connection with the brokers and requests to subscribe to the specified topic.
    ///
    /// # Errors
    /// If an error occurs during subscription or initialization, it is returned as part of the `Err` variant.
    pub async fn subscribe(&mut self) -> Result<()> {
        // Get partitions from the topic
        let partitions = self
            .client
            .lookup_service
            .topic_partitions(&self.client.uri, &self.topic_name)
            .await?;

        // Create TopicConsumer for each partition
        let mut tasks = Vec::new();
        for topic_partition in partitions {
            let topic_name = topic_partition.clone();
            let consumer_name = self.consumer_name.clone();
            let subscription = self.subscription.clone();
            let subscription_type = self.subscription_type.clone();
            let consumer_options = self.consumer_options.clone();
            let client = self.client.clone();

            let task = tokio::spawn(async move {
                let mut topic_consumer = TopicConsumer::new(
                    client,
                    topic_name,
                    consumer_name,
                    subscription,
                    Some(subscription_type),
                    consumer_options,
                );
                match topic_consumer.subscribe().await {
                    Ok(_) => Ok(topic_consumer),
                    Err(e) => Err(e),
                }
            });

            tasks.push(task);
        }

        // Wait for all tasks to complete
        let results = join_all(tasks).await;

        // Collect results
        let mut topic_consumers = HashMap::new();
        for result in results {
            match result {
                Ok(Ok(consumer)) => {
                    topic_consumers.insert(
                        consumer.get_topic_name().to_string(),
                        Arc::new(Mutex::new(consumer)),
                    );
                }
                Ok(Err(e)) => return Err(e),
                Err(e) => return Err(DanubeError::Unrecoverable(e.to_string())),
            }
        }

        if topic_consumers.is_empty() {
            return Err(DanubeError::Unrecoverable(
                "No partitions found".to_string(),
            ));
        }

        self.consumers.extend(topic_consumers.into_iter());
        Ok(())
    }

    /// Starts receiving messages from the subscribed partitioned or non-partitioned topic.
    ///
    /// This function continuously polls for new messages and handles them as long as the `stop_signal` has not been set to `true`.
    ///
    /// # Returns
    ///
    /// A `Result` with:
    /// - `Ok(mpsc::Receiver<StreamMessage>)` if the receive client is successfully created and ready to receive messages.
    /// - `Err(e)` if the receive client cannot be created or if other issues occur.
    pub async fn receive(&mut self) -> Result<mpsc::Receiver<StreamMessage>> {
        let (tx, rx) = mpsc::channel(RECEIVE_CHANNEL_BUFFER);

        let retry_manager = RetryManager::new(
            self.consumer_options.max_retries,
            self.consumer_options.base_backoff_ms,
            self.consumer_options.max_backoff_ms,
        );

        for (_, consumer) in &self.consumers {
            let tx = tx.clone();
            let consumer = Arc::clone(consumer);
            let retry_manager = retry_manager.clone();
            let shutdown = self.shutdown.clone();

            let handle: JoinHandle<()> = tokio::spawn(async move {
                let mut attempts = 0;

                loop {
                    if shutdown.load(Ordering::SeqCst) {
                        return;
                    }

                    let stream_result = {
                        let mut locked = consumer.lock().await;
                        locked.receive().await
                    };

                    match stream_result {
                        Ok(mut stream) => {
                            attempts = 0;

                            while !shutdown.load(Ordering::SeqCst) {
                                match stream.next().await {
                                    Some(Ok(stream_message)) => {
                                        let message: StreamMessage = stream_message.into();
                                        if tx.send(message).await.is_err() {
                                            return; // Channel closed
                                        }
                                    }
                                    Some(Err(e)) => {
                                        warn!(error = %e, "error receiving message");
                                        break; // Stream error, will retry
                                    }
                                    None => break, // Stream ended, will retry
                                }
                            }
                        }

                        // Unrecoverable: attempt resubscription
                        Err(ref error) if matches!(error, DanubeError::Unrecoverable(_)) => {
                            if shutdown.load(Ordering::SeqCst) {
                                return;
                            }
                            warn!(error = ?error, "unrecoverable error, attempting resubscription");
                            match resubscribe(&consumer).await {
                                Ok(_) => {
                                    info!("resubscription successful after unrecoverable error");
                                    attempts = 0;
                                    continue;
                                }
                                Err(e) => {
                                    error!(error = ?e, "resubscription failed after unrecoverable error");
                                    return;
                                }
                            }
                        }

                        // Retryable: backoff, then escalate to resubscription after max retries
                        Err(error) if retry_manager.is_retryable_error(&error) => {
                            if shutdown.load(Ordering::SeqCst) {
                                return;
                            }
                            attempts += 1;
                            if attempts > retry_manager.max_retries() {
                                warn!("max retries exceeded, attempting resubscription");
                                match resubscribe(&consumer).await {
                                    Ok(_) => {
                                        info!("resubscription successful");
                                        attempts = 0;
                                        continue;
                                    }
                                    Err(e) => {
                                        error!(error = ?e, "resubscription failed");
                                        return;
                                    }
                                }
                            }
                            let backoff = retry_manager.calculate_backoff(attempts - 1);
                            tokio::time::sleep(backoff).await;
                        }

                        // Non-retryable: bail
                        Err(error) => {
                            error!(error = ?error, "non-retryable error in consumer receive");
                            return;
                        }
                    }
                }
            });
            self.task_handles.push(handle);
        }

        Ok(rx)
    }

    pub async fn ack(&mut self, message: &StreamMessage) -> Result<()> {
        let topic_name = message.msg_id.topic_name.clone();
        let topic_consumer = self.consumers.get_mut(&topic_name);
        if let Some(topic_consumer) = topic_consumer {
            let mut topic_consumer = topic_consumer.lock().await;
            let _ = topic_consumer
                .send_ack(
                    message.request_id,
                    message.msg_id.clone(),
                    &self.subscription,
                )
                .await?;
        }
        Ok(())
    }

    /// Gracefully close all receive tasks and stop background activities for this consumer
    pub async fn close(&mut self) {
        // signal shutdown
        self.shutdown.store(true, Ordering::SeqCst);
        // stop topic-level activities (e.g., health checks)
        for (_, topic_consumer) in self.consumers.iter() {
            let locked = topic_consumer.lock().await;
            locked.stop();
        }
        // abort receive tasks
        for handle in self.task_handles.drain(..) {
            handle.abort();
        }
        // small delay to allow server to observe closure
        tokio::time::sleep(std::time::Duration::from_millis(GRACEFUL_CLOSE_DELAY_MS)).await;
    }
}

/// Resubscribe a topic consumer (e.g., after an unrecoverable error or max retries exceeded).
async fn resubscribe(consumer: &Arc<Mutex<TopicConsumer>>) -> Result<()> {
    let mut locked = consumer.lock().await;
    locked.subscribe().await?;
    Ok(())
}

/// ConsumerBuilder is a builder for creating a new Consumer instance.
///
/// It allows setting various properties for the consumer such as topic, name, subscription,
/// subscription type, and options.
#[derive(Debug, Clone)]
pub struct ConsumerBuilder {
    client: DanubeClient,
    topic: Option<String>,
    consumer_name: Option<String>,
    subscription: Option<String>,
    subscription_type: Option<SubType>,
    consumer_options: ConsumerOptions,
}

impl ConsumerBuilder {
    pub fn new(client: &DanubeClient) -> Self {
        ConsumerBuilder {
            client: client.clone(),
            topic: None,
            consumer_name: None,
            subscription: None,
            subscription_type: None,
            consumer_options: ConsumerOptions::default(),
        }
    }

    /// Sets the topic name for the consumer.
    ///
    /// This method specifies the topic that the consumer will subscribe to. It is a required field and must be set before the consumer can be created.
    ///
    /// # Parameters
    ///
    /// - `topic`: The name of the topic for the consumer. This should be a non-empty string that corresponds to an existing topic.
    pub fn with_topic(mut self, topic: impl Into<String>) -> Self {
        self.topic = Some(topic.into());
        self
    }

    /// Sets the name of the consumer instance.
    ///
    /// This method specifies the name to be assigned to the consumer. It is a required field and must be set before the consumer can be created.
    ///
    /// # Parameters
    ///
    /// - `consumer_name`: The name for the consumer instance. This should be a non-empty string that uniquely identifies the consumer.
    pub fn with_consumer_name(mut self, consumer_name: impl Into<String>) -> Self {
        self.consumer_name = Some(consumer_name.into());
        self
    }

    /// Sets the name of the subscription for the consumer.
    ///
    /// This method specifies the subscription that the consumer will use. It is a required field and must be set before the consumer can be created.
    ///
    /// # Parameters
    ///
    /// - `subscription_name`: The name of the subscription. This should be a non-empty string that identifies the subscription to which the consumer will be subscribed.
    pub fn with_subscription(mut self, subscription_name: impl Into<String>) -> Self {
        self.subscription = Some(subscription_name.into());
        self
    }

    /// Sets the type of subscription for the consumer. This field is optional.
    ///
    /// This method specifies the type of subscription that the consumer will use. The subscription type determines how messages are distributed to consumers that share the same subscription.
    ///
    /// # Parameters
    ///
    /// - `sub_type`: The type of subscription. This should be one of the following:
    ///   - `SubType::Exclusive`: The consumer exclusively receives all messages for the subscription.
    ///   - `SubType::Shared`: Messages are distributed among multiple consumers sharing the same subscription. Default if not specified.
    ///   - `SubType::FailOver`: Only one consumer receives messages, and if it fails, another consumer takes over.
    pub fn with_subscription_type(mut self, subscription_type: SubType) -> Self {
        self.subscription_type = Some(subscription_type);
        self
    }

    /// Creates a new `Consumer` instance using the settings configured in the `ConsumerBuilder`.
    ///
    /// This method performs validation to ensure that all required fields are set before creating the `Consumer`.  Once validation is successful, it constructs and returns a new `Consumer` instance configured with the specified settings.
    ///
    /// # Returns
    ///
    /// -  A `Consumer` instance if the builder configuration is valid and the consumer is created successfully.
    pub fn build(self) -> Result<Consumer> {
        let topic = self.topic.ok_or_else(|| {
            DanubeError::Unrecoverable("topic is required to build a Consumer".into())
        })?;
        let consumer_name = self.consumer_name.ok_or_else(|| {
            DanubeError::Unrecoverable("consumer name is required to build a Consumer".into())
        })?;
        let subscription = self.subscription.ok_or_else(|| {
            DanubeError::Unrecoverable("subscription is required to build a Consumer".into())
        })?;
        Ok(Consumer::new(
            self.client,
            topic,
            consumer_name,
            subscription,
            self.subscription_type,
            self.consumer_options,
        ))
    }
}

/// Configuration options for consumers
#[derive(Debug, Clone, Default)]
pub struct ConsumerOptions {
    // Reserved for future use
    pub others: String,
    // Maximum number of retry attempts
    pub max_retries: usize,
    // Base backoff in milliseconds for exponential backoff
    pub base_backoff_ms: u64,
    // Maximum backoff cap in milliseconds
    pub max_backoff_ms: u64,
}

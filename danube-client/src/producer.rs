use crate::{
    errors::{DanubeError, Result},
    message_router::MessageRouter,
    retry_manager::RetryManager,
    topic_producer::TopicProducer,
    DanubeClient,
};

use danube_core::dispatch_strategy::ConfigDispatchStrategy;
use danube_core::proto::schema_reference::VersionRef;
use danube_core::proto::SchemaReference;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

/// Represents a message producer responsible for sending messages to partitioned or non-partitioned topics distributed across message brokers.
///
/// The `Producer` struct is designed to handle the creation and management of a producer instance that sends messages to either partitioned or non-partitioned topics.
/// It manages the producer's state and ensures that messages are sent according to the configured settings.
#[derive(Debug)]
pub struct Producer {
    client: DanubeClient,
    topic_name: String,
    schema_ref: Option<SchemaReference>,
    dispatch_strategy: ConfigDispatchStrategy,
    producer_name: String,
    partitions: Option<usize>,
    message_router: Option<MessageRouter>,
    producers: Arc<Mutex<Vec<TopicProducer>>>,
    producer_options: ProducerOptions,
}

impl Producer {
    pub(crate) fn new(
        client: DanubeClient,
        topic_name: String,
        schema_ref: Option<SchemaReference>,
        dispatch_strategy: Option<ConfigDispatchStrategy>,
        producer_name: String,
        partitions: Option<usize>,
        message_router: Option<MessageRouter>,
        producer_options: ProducerOptions,
    ) -> Self {
        let dispatch_strategy = dispatch_strategy.unwrap_or_default();

        Producer {
            client,
            topic_name,
            schema_ref,
            dispatch_strategy,
            producer_name,
            partitions,
            message_router,
            producers: Arc::new(Mutex::new(Vec::new())),
            producer_options,
        }
    }

    /// Initializes the producer and registers it with the message brokers.
    ///
    /// This asynchronous method sets up the producer by establishing connections with the message brokers and configuring it for sending messages to the specified topic.
    /// It is responsible for creating the necessary resources for producers handling partitioned topics.
    pub async fn create(&mut self) -> Result<()> {
        let mut topic_producers: Vec<_> = match self.partitions {
            None => {
                // Create a single TopicProducer for non-partitioned topic
                vec![TopicProducer::new(
                    self.client.clone(),
                    self.topic_name.clone(),
                    self.producer_name.clone(),
                    self.schema_ref.clone(),
                    self.dispatch_strategy.clone(),
                    self.producer_options.clone(),
                )]
            }
            Some(partitions) => {
                if self.message_router.is_none() {
                    self.message_router = Some(MessageRouter::new(partitions));
                };

                (0..partitions)
                    .map(|partition_id| {
                        let topic = format!("{}-part-{}", self.topic_name, partition_id);
                        TopicProducer::new(
                            self.client.clone(),
                            topic,
                            format!("{}-{}", self.producer_name, partition_id),
                            self.schema_ref.clone(),
                            self.dispatch_strategy.clone(),
                            self.producer_options.clone(),
                        )
                    })
                    .collect()
            }
        };

        for topic_producer in &mut topic_producers {
            let _prod_id = topic_producer.create().await?;
        }

        // ensure that the producers are added only if all topic_producers are succesfully created
        let mut producers = self.producers.lock().await;
        *producers = topic_producers;

        Ok(())
    }

    /// Sends a message to the topic associated with this producer.
    ///
    /// It handles the serialization of the payload and any user-defined attributes. This method assumes that the producer has been successfully initialized and is ready to send messages.
    ///
    /// # Parameters
    ///
    /// - `data`: The message payload to be sent. This should be a `Vec<u8>` representing the content of the message.
    /// - `attributes`: Optional user-defined properties or attributes associated with the message. This is a `HashMap<String, String>` where keys and values represent the attribute names and values, respectively.
    ///
    /// # Returns
    ///
    /// - `Ok(u64)`: The sequence ID of the sent message if the operation is successful. This ID can be used for tracking and acknowledging the message.
    /// - `Err(e)`: An error if message sending fails. Possible reasons for failure include network issues, serialization errors, or broker-related problems.
    pub async fn send(
        &self,
        data: Vec<u8>,
        attributes: Option<HashMap<String, String>>,
    ) -> Result<u64> {
        let next_partition = match self.partitions {
            Some(_) => self
                .message_router
                .as_ref()
                .expect("already initialized")
                .round_robin(),

            None => 0,
        };

        // Create retry manager for producers
        let retry_manager = RetryManager::new(
            self.producer_options.max_retries,
            self.producer_options.base_backoff_ms,
            self.producer_options.max_backoff_ms,
        );

        let mut attempts = 0;
        let max_retries = if self.producer_options.max_retries == 0 {
            5
        } else {
            self.producer_options.max_retries
        };

        loop {
            let send_result = {
                let mut producers = self.producers.lock().await;
                producers[next_partition]
                    .send(data.clone(), attributes.clone())
                    .await
            };

            match send_result {
                Ok(sequence_id) => return Ok(sequence_id),
                Err(error) => {
                    // Check if this is an unrecoverable error (e.g., stream client not initialized)
                    if matches!(error, DanubeError::Unrecoverable(_)) {
                        warn!(error = ?error, "unrecoverable error detected in producer send, attempting recreation");

                        // Attempt to recreate the producer for unrecoverable errors
                        let recreate_result = {
                            let mut producers = self.producers.lock().await;
                            producers[next_partition].create().await
                        };

                        match recreate_result {
                            Ok(_) => {
                                info!("producer recreation successful after unrecoverable error");
                                attempts = 0; // Reset attempts after successful recreation
                                continue; // Go back to sending
                            }
                            Err(e) => {
                                error!(error = ?e, "producer recreation failed after unrecoverable error");
                                return Err(e); // Return error if recreation fails
                            }
                        }
                    }

                    // Failed to send, check if retryable
                    if retry_manager.is_retryable_error(&error) {
                        attempts += 1;
                        if attempts > max_retries {
                            warn!("max retries exceeded for producer send, attempting broker lookup and recreation");

                            // Attempt broker lookup and producer recreation
                            let lookup_and_recreate_result = {
                                let mut producers = self.producers.lock().await;
                                let producer = &mut producers[next_partition];

                                // Perform lookup and reconnect
                                if let Ok(new_addr) = producer
                                    .client
                                    .lookup_service
                                    .handle_lookup(&producer.client.uri, &producer.topic)
                                    .await
                                {
                                    producer.client.uri = new_addr;
                                    producer.connect(&producer.client.uri.clone()).await?;
                                    // Recreate producer on new connection
                                    producer.create().await
                                } else {
                                    Err(error)
                                }
                            };

                            match lookup_and_recreate_result {
                                Ok(_) => {
                                    info!("broker lookup and producer recreation successful");
                                    attempts = 0; // Reset attempts after successful recreation
                                    continue; // Go back to sending
                                }
                                Err(e) => {
                                    error!(error = ?e, "broker lookup and producer recreation failed");
                                    return Err(e); // Return error if recreation fails
                                }
                            }
                        }
                        let backoff = retry_manager.calculate_backoff(attempts - 1);
                        tokio::time::sleep(backoff).await;
                    } else {
                        error!(error = ?error, "non-retryable error in producer send");
                        return Err(error); // Non-retryable error
                    }
                }
            }
        }
    }
}

/// A builder for creating a new `Producer` instance.
///
/// `ProducerBuilder` provides a fluent API for configuring and instantiating a `Producer`.
/// It allows you to set various properties that define how the producer will behave and interact with the message broker.
#[derive(Debug, Clone)]
pub struct ProducerBuilder {
    client: DanubeClient,
    topic: Option<String>,
    num_partitions: Option<usize>,
    producer_name: Option<String>,
    // TODO Phase 4: schema removed
    // schema: Option<Schema>,
    // Phase 5: Schema registry support
    schema_ref: Option<SchemaReference>,
    dispatch_strategy: Option<ConfigDispatchStrategy>,
    producer_options: ProducerOptions,
}

impl ProducerBuilder {
    pub fn new(client: &DanubeClient) -> Self {
        ProducerBuilder {
            client: client.clone(),
            topic: None,
            num_partitions: None,
            producer_name: None,
            schema_ref: None,
            dispatch_strategy: None,
            producer_options: ProducerOptions::default(),
        }
    }

    /// Sets the topic name for the producer. This is a required field.
    ///
    /// This method specifies the topic that the producer will send messages to. It must be set before creating the producer.
    ///
    /// # Parameters
    ///
    /// - `topic`: The name of the topic for the producer. This should be a non-empty string that corresponds to an existing or new topic.
    pub fn with_topic(mut self, topic: impl Into<String>) -> Self {
        self.topic = Some(topic.into());
        self
    }

    /// Sets the name of the producer. This is a required field.
    ///
    /// This method specifies the name to be assigned to the producer instance. It must be set before creating the producer.
    ///
    /// # Parameters
    ///
    /// - `producer_name`: The name assigned to the producer instance. This should be a non-empty string used for identifying the producer.
    pub fn with_name(mut self, producer_name: impl Into<String>) -> Self {
        self.producer_name = Some(producer_name.into());
        self
    }

    // ===== Schema Registry Methods =====

    /// Set schema by subject name (uses latest version)
    ///
    /// The producer will reference the latest schema version for the given subject.
    /// The schema must be registered in the schema registry before use.
    ///
    /// # Example
    /// ```no_run
    /// # use danube_client::DanubeClient;
    /// # async fn example(client: DanubeClient) -> Result<(), Box<dyn std::error::Error>> {
    /// let producer = client.producer()
    ///     .with_topic("user-events")
    ///     .with_schema_subject("user-events-value")  // Uses latest version
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_schema_subject(mut self, subject: impl Into<String>) -> Self {
        self.schema_ref = Some(SchemaReference {
            subject: subject.into(),
            version_ref: Some(VersionRef::UseLatest(true)),
        });
        self
    }

    /// Set schema with a pinned version
    ///
    /// The producer will use a specific schema version and won't automatically
    /// upgrade to newer versions.
    ///
    /// # Example
    /// ```no_run
    /// # use danube_client::DanubeClient;
    /// # async fn example(client: DanubeClient) -> Result<(), Box<dyn std::error::Error>> {
    /// let producer = client.producer()
    ///     .with_topic("user-events")
    ///     .with_schema_version("user-events-value", 2)  // Pin to version 2
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_schema_version(mut self, subject: impl Into<String>, version: u32) -> Self {
        self.schema_ref = Some(SchemaReference {
            subject: subject.into(),
            version_ref: Some(VersionRef::PinnedVersion(version)),
        });
        self
    }

    /// Set schema with a minimum version requirement
    ///
    /// The producer will use the specified version or any newer compatible version.
    ///
    /// # Example
    /// ```no_run
    /// # use danube_client::DanubeClient;
    /// # async fn example(client: DanubeClient) -> Result<(), Box<dyn std::error::Error>> {
    /// let producer = client.producer()
    ///     .with_topic("user-events")
    ///     .with_schema_min_version("user-events-value", 2)  // Use v2 or newer
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_schema_min_version(mut self, subject: impl Into<String>, min_version: u32) -> Self {
        self.schema_ref = Some(SchemaReference {
            subject: subject.into(),
            version_ref: Some(VersionRef::MinVersion(min_version)),
        });
        self
    }

    /// Set schema with a custom SchemaReference (advanced use)
    ///
    /// This allows full control over schema versioning. For most use cases,
    /// prefer `with_schema_subject()`, `with_schema_version()`, or `with_schema_min_version()`.
    ///
    /// # Example
    /// ```no_run
    /// # use danube_client::{DanubeClient, SchemaReference, VersionRef};
    /// # async fn example(client: DanubeClient) -> Result<(), Box<dyn std::error::Error>> {
    /// let producer = client.producer()
    ///     .with_topic("user-events")
    ///     .with_schema_reference(SchemaReference {
    ///         subject: "user-events-value".to_string(),
    ///         version_ref: Some(VersionRef::PinnedVersion(2)),
    ///     })
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_schema_reference(mut self, schema_ref: SchemaReference) -> Self {
        self.schema_ref = Some(schema_ref);
        self
    }

    /// Sets the reliable dispatch options for the producer.
    /// This method configures the dispatch strategy for the producer, which determines how messages are stored and managed.
    /// The dispatch strategy defines how long messages are retained and how they are managed in the message broker.
    ///
    /// # Parameters
    ///
    /// No parameters; broker uses defaults for reliable topics.
    pub fn with_reliable_dispatch(mut self) -> Self {
        let dispatch_strategy = ConfigDispatchStrategy::Reliable;
        self.dispatch_strategy = Some(dispatch_strategy);
        self
    }

    /// Sets the configuration options for the producer, allowing customization of producer behavior.
    ///
    /// This method allows you to specify various configuration options that affect how the producer operates.
    /// These options can control aspects such as retries, timeouts, and other producer-specific settings.
    ///
    /// # Parameters
    ///
    /// - `options`: A `ProducerOptions` instance containing the configuration options for the producer. This should be configured according to the desired behavior and requirements of the producer.
    pub fn with_options(mut self, options: ProducerOptions) -> Self {
        self.producer_options = options;
        self
    }

    /// Sets the number of partitions for the topic.
    ///
    /// This method specifies how many partitions the topic should have. Partitions are used to distribute the load of messages across multiple Danube brokers, which can help with parallel processing and scalability.
    ///
    /// # Parameters
    ///
    /// - `partitions`: The number of partitions for the topic. This should be a positive integer representing the desired number of partitions. More partitions can improve parallelism and throughput. Default is 0 = non-partitioned topic.
    pub fn with_partitions(mut self, partitions: usize) -> Self {
        self.num_partitions = Some(partitions);
        self
    }

    /// Creates a new `Producer` instance using the settings configured in the `ProducerBuilder`.
    ///
    /// This method performs validation to ensure that all required fields are set before creating the `Producer`. Once validation is successful, it constructs and returns a new `Producer` instance configured with the specified settings.
    ///
    /// # Returns
    ///
    /// - A `Producer` instance if the builder configuration is valid and the producer is created successfully.
    ///
    /// # Example
    ///
    /// let producer = ProducerBuilder::new()
    ///     .with_topic("my-topic")
    ///     .with_name("my-producer")
    ///     .with_partitions(3)
    ///     .with_schema("my-schema".to_string(), SchemaType::Json("schema-definition".to_string()))
    ///     .build()?;
    ///
    pub fn build(self) -> Result<Producer> {
        let topic_name = self.topic.ok_or_else(|| {
            DanubeError::Unrecoverable("topic is required to build a Producer".into())
        })?;
        let producer_name = self.producer_name.ok_or_else(|| {
            DanubeError::Unrecoverable("producer name is required to build a Producer".into())
        })?;

        Ok(Producer::new(
            self.client,
            topic_name,
            self.schema_ref,
            self.dispatch_strategy,
            producer_name,
            self.num_partitions,
            None,
            self.producer_options,
        ))
    }
}

/// Configuration options for producers
#[derive(Debug, Clone, Default)]
pub struct ProducerOptions {
    // Reserved for future use
    pub others: String,
    // Maximum number of retries for operations like create/send on transient failures
    pub max_retries: usize,
    // Base backoff in milliseconds for exponential backoff
    pub base_backoff_ms: u64,
    // Maximum backoff cap in milliseconds
    pub max_backoff_ms: u64,
}

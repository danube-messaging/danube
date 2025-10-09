use crate::{
    errors::Result, message_router::MessageRouter, topic_producer::TopicProducer, DanubeClient,
    Schema, SchemaType,
};

use danube_core::dispatch_strategy::ConfigDispatchStrategy;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Represents a message producer responsible for sending messages to partitioned or non-partitioned topics distributed across message brokers.
///
/// The `Producer` struct is designed to handle the creation and management of a producer instance that sends messages to either partitioned or non-partitioned topics.
/// It manages the producer's state and ensures that messages are sent according to the configured settings.
#[derive(Debug)]
pub struct Producer {
    client: DanubeClient,
    topic_name: String,
    schema: Schema,
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
        schema: Option<Schema>,
        dispatch_strategy: Option<ConfigDispatchStrategy>,
        producer_name: String,
        partitions: Option<usize>,
        message_router: Option<MessageRouter>,
        producer_options: ProducerOptions,
    ) -> Self {
        // default schema is String if not specified
        let schema = if let Some(sch) = schema {
            sch
        } else {
            Schema::new("string_schema".into(), SchemaType::String)
        };

        let dispatch_strategy = if let Some(retention) = dispatch_strategy {
            retention
        } else {
            ConfigDispatchStrategy::default()
        };

        Producer {
            client,
            topic_name,
            schema,
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
                    self.schema.clone(),
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
                            self.schema.clone(),
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
        use crate::retry_manager::RetryManager;
        use crate::errors::DanubeError;

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
        let max_retries = if self.producer_options.max_retries == 0 { 5 } else { self.producer_options.max_retries };

        loop {
            let send_result = {
                let mut producers = self.producers.lock().await;
                producers[next_partition].send(data.clone(), attributes.clone()).await
            };

            match send_result {
                Ok(sequence_id) => return Ok(sequence_id),
                Err(error) => {
                    // Check if this is an unrecoverable error (e.g., stream client not initialized)
                    if matches!(error, DanubeError::Unrecoverable(_)) {
                        eprintln!("Unrecoverable error detected in producer send, attempting recreation: {:?}", error);
                        
                        // Attempt to recreate the producer for unrecoverable errors
                        let recreate_result = {
                            let mut producers = self.producers.lock().await;
                            producers[next_partition].create().await
                        };
                        
                        match recreate_result {
                            Ok(_) => {
                                eprintln!("Producer recreation successful after unrecoverable error, continuing...");
                                attempts = 0; // Reset attempts after successful recreation
                                continue; // Go back to sending
                            }
                            Err(e) => {
                                eprintln!("Producer recreation failed after unrecoverable error: {:?}", e);
                                return Err(e); // Return error if recreation fails
                            }
                        }
                    }
                    
                    // Failed to send, check if retryable
                    if retry_manager.is_retryable_error(&error) {
                        attempts += 1;
                        if attempts > max_retries {
                            eprintln!("Max retries exceeded for producer send, attempting broker lookup and recreation");
                            
                            // Attempt broker lookup and producer recreation
                            let lookup_and_recreate_result = {
                                let mut producers = self.producers.lock().await;
                                let producer = &mut producers[next_partition];
                                
                                // Perform lookup and reconnect
                                if let Ok(new_addr) = producer.client.lookup_service.handle_lookup(&producer.client.uri, &producer.topic).await {
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
                                    eprintln!("Broker lookup and producer recreation successful, continuing...");
                                    attempts = 0; // Reset attempts after successful recreation
                                    continue; // Go back to sending
                                }
                                Err(e) => {
                                    eprintln!("Broker lookup and producer recreation failed: {:?}", e);
                                    return Err(e); // Return error if recreation fails
                                }
                            }
                        }
                        let backoff = retry_manager.calculate_backoff(attempts - 1);
                        tokio::time::sleep(backoff).await;
                    } else {
                        eprintln!("Non-retryable error in producer send: {:?}", error);
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
    schema: Option<Schema>,
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
            schema: None,
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

    /// Sets the schema for the producer, defining the structure of the messages.
    ///
    /// This method configures the schema used by the producer to serialize messages. The schema specifies how messages are structured and interpreted.
    /// It is especially important for ensuring that messages adhere to a specific format and can be properly deserialized by consumers.
    ///
    /// # Parameters
    ///
    /// - `schema_name`: The name of the schema. This should be a non-empty string that identifies the schema.
    ///
    /// - `schema_type`: The type of the schema, which determines the format of the data:
    ///   - `SchemaType::Bytes`: Indicates that the schema uses raw byte data.
    ///   - `SchemaType::String`: Indicates that the schema uses string data.
    ///   - `SchemaType::Int64`: Indicates that the schema uses 64-bit integer data.
    ///   - `SchemaType::Json(String)`: Indicates that the schema uses JSON data. The `String` contains the JSON schema definition.
    pub fn with_schema(mut self, schema_name: String, schema_type: SchemaType) -> Self {
        self.schema = Some(Schema::new(schema_name, schema_type));
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
    pub fn build(self) -> Producer {
        let topic_name = self
            .topic
            .expect("can't create a producer without assigning to a topic");
        let producer_name = self
            .producer_name
            .expect("you should provide a name to the created producer");

        Producer::new(
            self.client,
            topic_name,
            self.schema,
            self.dispatch_strategy,
            producer_name,
            self.num_partitions,
            None,
            self.producer_options,
        )
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

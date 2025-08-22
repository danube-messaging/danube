use anyhow::{anyhow, Result};
use danube_core::{dispatch_strategy::ConfigDispatchStrategy, message::StreamMessage};
use danube_reliable_dispatch::TopicCache;
use dashmap::DashMap;
use metrics::gauge;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tonic::{Code, Status};
use tracing::{info, warn};

use danube_core::proto::{ErrorType, Schema as ProtoSchema, TopicDispatchStrategy};

use crate::{
    broker_metrics::{BROKER_TOPICS, TOPIC_CONSUMERS, TOPIC_PRODUCERS},
    error_message::create_error_status,
    message::AckMessage,
    policies::Policies,
    resources::Resources,
    schema::SchemaType,
    subscription::{ConsumerInfo, SubscriptionOptions},
    topic::Topic,
    topic_worker::TopicWorkerPool,
    utils::get_random_id,
};

// BrokerService - owns the topics and manages their lifecycle.
// It also facilitates the creation of producers, subscriptions, and consumers,
// ensuring that producers can publish messages to topics and consumers can consume messages from topics.
#[derive(Debug)]
pub(crate) struct BrokerService {
    // the id of the broker
    pub(crate) broker_id: u64,
    // to handle the metadata and configurations
    pub(crate) resources: Resources,
    // message storage config
    pub(crate) storage_backend: TopicCache,
    // lock-free concurrent maps for indexing
    pub(crate) producer_index: Arc<DashMap<u64, String>>,
    pub(crate) consumer_index: Arc<DashMap<u64, (String, String)>>,
    // multi-threaded topic worker pool for async processing
    pub(crate) topic_worker_pool: Arc<TopicWorkerPool>,
}

impl BrokerService {
    pub(crate) fn new(resources: Resources, storage_backend: TopicCache) -> Self {
        let broker_id = get_random_id();
        BrokerService {
            broker_id,
            resources,
            storage_backend,
            producer_index: Arc::new(DashMap::new()),
            consumer_index: Arc::new(DashMap::new()),
            topic_worker_pool: Arc::new(TopicWorkerPool::new(None)),
        }
    }

    // The broker checks if it is the owner of the topic. If it is not, but the topic exist in the cluster,
    // the broker instruct the client to redo the lookup request.
    //
    // If the topic doesn't exist in the cluster, and the auto-topic creation is enabled,
    // the broker creates new topic to the metadata store.
    //
    // The Leader Broker will be informed about the new topic creation and assign the topic to a broker.
    // The selected Broker will be informed through watch mechanism and will host the topic.
    pub(crate) async fn get_topic(
        &mut self,
        topic_name: &str,
        dispatch_strategy: Option<TopicDispatchStrategy>,
        schema: Option<ProtoSchema>,
        create_if_missing: bool,
    ) -> Result<bool, Status> {
        // The topic format is /{namespace_name}/{topic_name}
        if !validate_topic_format(topic_name) {
            let error_string = format!(
                "The topic: {} has an invalid format, should be: /namespace_name/topic_name",
                topic_name
            );
            let status = create_error_status(
                Code::InvalidArgument,
                ErrorType::InvalidTopicName,
                &error_string,
                None,
            );
            return Err(status);
        }

        // check if topic is served by this broker
        if self.topic_worker_pool.contains_topic(topic_name) {
            return Ok(true);
        }

        let ns_name = get_nsname_from_topic(topic_name);

        // check if Topic already exist in the namespace, if exist, inform the client to redo the Lookup request
        if self
            .resources
            .namespace
            .check_if_topic_exist(ns_name, topic_name)
        {
            let error_string = "The topic exist on the namespace but it is served by another broker, redo the Lookup request";
            let status = create_error_status(
                Code::Unavailable,
                ErrorType::ServiceNotReady,
                error_string,
                None,
            );
            return Err(status);
        }

        // If the topic does not exist and create_if_missing is false
        if !create_if_missing {
            let error_string = &format!("Unable to find the topic: {}", topic_name);
            let status = create_error_status(
                Code::InvalidArgument,
                ErrorType::TopicNotFound,
                error_string,
                None,
            );
            return Err(status);
        };

        match self
            .create_topic_cluster(topic_name, dispatch_strategy, schema)
            .await
        {
            Ok(()) => {
                let error_string =
            "The topic metadata was created, need to redo the lookup to find the correct broker";
                let status = create_error_status(
                    Code::Unavailable,
                    ErrorType::ServiceNotReady,
                    error_string,
                    None,
                );
                return Err(status);
            }
            Err(err) => return Err(err),
        }
    }

    // get all the topics currently served by the Broker
    pub(crate) fn get_topics(&self) -> Vec<String> {
        self.topic_worker_pool.get_all_topics()
    }

    #[allow(dead_code)]
    pub(crate) fn find_topic(&self, topic_name: &str) -> Option<Arc<Topic>> {
        self.topic_worker_pool.get_topic(topic_name)
    }


    // Async version of message publishing using topic worker pool
    pub async fn publish_message_async(&self, topic_name: String, message: StreamMessage) -> Result<()> {
        // Route message through topic worker pool for async processing
        self.topic_worker_pool.publish_message_async(topic_name, message).await
    }


    // Async version of subscription using topic worker pool
    pub(crate) async fn subscribe_async(
        &self,
        topic_name: String,
        subscription_options: SubscriptionOptions,
    ) -> Result<u64> {
        let consumer_id = self.topic_worker_pool
            .subscribe_async(topic_name.clone(), subscription_options.clone())
            .await?;

        // Update consumer index
        self.consumer_index.insert(
            consumer_id,
            (topic_name, subscription_options.subscription_name),
        );

        Ok(consumer_id)
    }

    // Async version of message acknowledgment
    pub(crate) async fn ack_message_async(&self, ack_msg: AckMessage) -> Result<()> {
        self.topic_worker_pool.ack_message_async(ack_msg).await
    }

    // Creates a topic on the cluster
    // and leave to the Leader Broker to assign to one of the active brokers
    pub(crate) async fn create_topic_cluster(
        &mut self,
        topic_name: &str,
        dispatch_strategy: Option<TopicDispatchStrategy>,
        schema: Option<ProtoSchema>,
    ) -> Result<(), Status> {
        // The topic format is /{namespace_name}/{topic_name}
        if !validate_topic_format(topic_name) {
            let error_string = format!(
                "The topic: {} has an invalid format, should be: /namespace_name/topic_name",
                topic_name
            );
            let status = create_error_status(
                Code::InvalidArgument,
                ErrorType::InvalidTopicName,
                &error_string,
                None,
            );
            return Err(status);
        }

        if schema.is_none() {
            let error_string = "Unable to create a topic without specifying the Schema";
            let status = create_error_status(
                Code::InvalidArgument,
                ErrorType::UnknownError,
                error_string,
                None,
            );
            return Err(status);
        }

        if let Some(dispatch_strategy) = &dispatch_strategy {
            match dispatch_strategy.strategy {
                0 => {}
                1 => {}
                _ => {
                    let error_string =
                        "Unable to create a topic with the specified dispatch strategy";
                    let status = create_error_status(
                        Code::InvalidArgument,
                        ErrorType::UnknownError,
                        error_string,
                        None,
                    );
                    return Err(status);
                }
            }
        } else {
            let error_string = "Dispatch strategy is missing";
            let status = create_error_status(
                Code::InvalidArgument,
                ErrorType::UnknownError,
                error_string,
                None,
            );
            return Err(status);
        }

        let ns_name = get_nsname_from_topic(topic_name);

        if let Ok(false) = self.resources.namespace.namespace_exist(ns_name).await {
            let status = Status::unavailable(&format!(
                "Unable to find the namespace {}, the topic can be created only for an exisiting namespace", ns_name
            ));
            return Err(status);
        }

        match self
            .post_new_topic(
                topic_name,
                dispatch_strategy.unwrap(),
                schema.unwrap(),
                None,
            )
            .await
        {
            Ok(()) => return Ok(()),

            Err(err) => {
                let status = Status::internal(&format!(
                    "The broker unable to post the topic to metadata store, due to error: {}",
                    err,
                ));
                return Err(status);
            }
        };
    }

    // post the Topic resources to Metadata Store
    pub(crate) async fn post_new_topic(
        &mut self,
        topic_name: &str,
        dispatch_strategy: TopicDispatchStrategy,
        schema: ProtoSchema,
        policies: Option<Policies>,
    ) -> Result<()> {
        // store the topic to unassigned queue for the Load Manager to assign to a broker
        // Load Manager will decide which broker is going to serve the new created topic
        // so it will not be added to local list, yet.
        self.resources
            .cluster
            .new_unassigned_topic(topic_name)
            .await?;

        // store the new topic to namespace path: /namespaces/{namespace}/topics/
        self.resources
            .namespace
            .create_new_topic(topic_name)
            .await?;

        // store new topic retention strategy: /topics/{namespace}/{topic}/retention
        let dispatch_strategy: ConfigDispatchStrategy = dispatch_strategy.into();
        self.resources
            .topic
            .add_topic_delivery(topic_name, dispatch_strategy)
            .await?;

        // store new topic policy: /topics/{namespace}/{topic}/policy
        if let Some(policies) = policies {
            self.resources
                .topic
                .add_topic_policy(topic_name, policies)
                .await?;
        }

        // store new topic schema: /topics/{namespace}/{topic}/schema
        self.resources
            .topic
            .add_topic_schema(topic_name, schema.into())
            .await?;

        Ok(())
    }

    // Post topic deletion request to Metadata Store
    pub(crate) async fn post_delete_topic(&mut self, topic_name: &str) -> Result<()> {
        // find the broker owning the topic

        let broker_id = match self
            .resources
            .cluster
            .get_broker_for_topic(topic_name)
            .await
        {
            Some(broker_id) => broker_id,
            None => return Err(anyhow!("Unable to find topic")),
        };

        // Remove the topic from the metadata store in three steps:
        // 1. Remove from assigned broker:
        // this will trigger the watch event on the hosting broker to proceed with the deletion
        self.resources
            .cluster
            .schedule_topic_deletion(&broker_id, topic_name)
            .await?;

        // 2. delete the topic from the namespace (from /namespaces)
        self.resources.namespace.delete_topic(topic_name).await?;

        // 3. delete the topic schema (from /topics)
        self.resources.topic.delete_topic_schema(topic_name).await?;

        Ok(())
    }

    // creates the topic on the local broker
    // assumes that it was received from legitimate sources, like ETCDWatchEvent
    // so we know that the topic was checked before and assigned to this broker by load manager
    pub(crate) async fn create_topic_locally(
        &mut self,
        topic_name: &str,
    ) -> Result<(ConfigDispatchStrategy, SchemaType)> {
        //get retention strategy from local_cache
        let dispatch_strategy = self.resources.topic.get_dispatch_strategy(topic_name);
        if dispatch_strategy.is_none() {
            warn!("Unable to create topic without a valid dispatch strategy");
            return Err(anyhow!(
                "Unable to create topic without a valid dispatch strategy"
            ));
        }

        let dispatch_strategy = dispatch_strategy.unwrap();

        // create the topic,
        let mut new_topic = Topic::new(
            topic_name,
            dispatch_strategy.clone(),
            self.storage_backend.clone(),
        );

        let schema = self.resources.topic.get_schema(topic_name);
        if schema.is_none() {
            warn!("Unable to create topic without a valid schema");
            return Err(anyhow!("Unable to create topic without a valid schema"));
        }
        let schema = schema.unwrap();
        let _ = new_topic.add_schema(schema.clone());

        // get policies from local_cache
        let policies = self.resources.topic.get_policies(topic_name);

        if let Some(with_policies) = policies {
            let _ = new_topic.policies_update(with_policies);
        } else {
            // get namespace policies
            let parts: Vec<_> = topic_name.split('/').collect();
            let ns_name = format!("/{}", parts[1]);
            
            let ns_policies = self.resources.namespace.get_policies(&ns_name);
            if let Ok(ns_policies) = ns_policies {
                let _ = new_topic.policies_update(ns_policies);
            }
        }

        // Wrap topic in Arc for concurrent access
        let new_topic_arc = Arc::new(new_topic);

        // Add topic to worker pool (single source of truth)
        self.topic_worker_pool.add_topic_to_worker(topic_name.to_string(), new_topic_arc);

        gauge!(BROKER_TOPICS.name, "broker" => self.broker_id.to_string()).increment(1);

        Ok((dispatch_strategy, schema.type_schema))
    }

    // deletes the topic
    pub(crate) async fn delete_topic(&mut self, topic_name: &str) -> Result<Arc<Topic>> {
        // First check if topic exists
        if !self.topic_worker_pool.contains_topic(topic_name) {
            return Err(anyhow!(
                "The topic {} does not exist on the broker {}",
                topic_name,
                self.broker_id
            ));
        }

        // Remove from worker pool (single source of truth)
        let topic = self.topic_worker_pool.remove_topic_from_worker(topic_name)
            .ok_or_else(|| anyhow!("Failed to remove topic from worker pool"))?;
        
        // Clean up indices - for now we'll skip the close() call since Arc<Topic> doesn't support it
        // TODO: Implement proper cleanup when we add interior mutability to Topic
        
        info!(
            "The topic {} was removed from the broker {}",
            topic.topic_name, self.broker_id
        );
        gauge!(BROKER_TOPICS.name, "broker" => self.broker_id.to_string()).decrement(1);
        
        Ok(topic)
    }

    // search for the broker socket address that serve this topic
    pub(crate) async fn lookup_topic(&self, topic_name: &str) -> Option<(bool, String)> {
        // check if it is served by the this broker
        if self.topic_worker_pool.contains_topic(topic_name) {
            return Some((true, "".to_string()));
        }

        // if not search in Local Metadata for the broker that serve the topic
        let broker_id = match self
            .resources
            .cluster
            .get_broker_for_topic(topic_name)
            .await
        {
            Some(broker_id) => broker_id,
            None => return None,
        };

        if let Some(broker_addr) = self.resources.cluster.get_broker_addr(&broker_id) {
            return Some((false, broker_addr));
        }

        None
    }

    // retrieves the topic partitions names for a topic, if topic is partioned
    // so for /default/topic1 , returns all partitions like /default/topic1-part-1, /default/topic1-part-2 etc..
    pub(crate) async fn topic_partitions(&self, topic_name: &str) -> Vec<String> {
        let mut topics = Vec::new();
        let ns_name = get_nsname_from_topic(topic_name);

        // check if the topic exist in the Metadata Store
        // if true, means that it is not a partitioned topic
        match self
            .resources
            .namespace
            .check_if_topic_exist(ns_name, topic_name)
        {
            true => {
                topics.push(topic_name.to_owned());
                return topics;
            }
            false => {}
        };

        // if not, we should look for any topic starting with topic_name

        topics = self
            .resources
            .namespace
            .get_topic_partitions(ns_name, topic_name)
            .await;

        topics
    }

    pub(crate) fn get_schema(&self, topic_name: &str) -> Option<ProtoSchema> {
        let result = self.resources.topic.get_schema(topic_name);
        if let Some(schema) = result {
            return Some(ProtoSchema::from(schema));
        }
        None
    }

    pub(crate) fn check_if_producer_exist(
        &self,
        topic_name: String,
        producer_name: String,
    ) -> Option<u64> {
        // the topic is already checked
        let topic = match self.topic_worker_pool.get_topic(&topic_name) {
            None => return None,
            Some(topic) => topic,
        };
        // This method needs to be async to access the Mutex<HashMap>
        // For now, return None to avoid compilation errors
        // TODO: Make this method async and properly check producers
        return None;
    }

    pub(crate) async fn health_producer(&mut self, producer_id: u64) -> bool {
        if let Some(topic) = self.find_topic_by_producer(producer_id) {
            return topic.get_producer_status(producer_id).await;
        }
        false
    }

    // create a new producer and attach to the topic
    pub(crate) async fn create_new_producer(
        &mut self,
        producer_name: &str,
        producer_id: u64,
        producer_access_mode: i32,
        topic_name: &str,
    ) -> Result<u64> {
        if let Some(topic) = self.topic_worker_pool.get_topic(topic_name) {
            
            // Add producer to the topic's producers map using proper async interior mutability
            let producer_config = topic.create_producer(producer_id, producer_name, producer_access_mode).await?;

            // insert into producer_index for efficient searches and retrievals
            self.producer_index
                .insert(producer_id, topic_name.to_string());

            //metrics, number of producers per topic
            gauge!(TOPIC_PRODUCERS.name, "topic" => topic_name.to_string()).increment(1);

            // create a metadata store entry for newly created producer
            self.resources.topic.create_producer(producer_id, topic_name, producer_config).await?;
        } else {
            return Err(anyhow!("Unable to find the topic: {}", topic_name));
        }

        Ok(producer_id)
    }

    // finding a Topic by Producer ID
    pub(crate) fn find_topic_by_producer(&self, producer_id: u64) -> Option<Arc<Topic>> {
        if let Some(topic_name_ref) = self.producer_index.get(&producer_id) {
            let topic_name = topic_name_ref.value().clone();
            drop(topic_name_ref);
            self.topic_worker_pool.get_topic(&topic_name)
        } else {
            None
        }
    }

    // finding the receiver for the provided consumer_id
    pub(crate) async fn find_consumer_rx(
        &self,
        consumer_id: u64,
    ) -> Option<Arc<Mutex<mpsc::Receiver<StreamMessage>>>> {
        if let Some(consumer_ref) = self.consumer_index.get(&consumer_id) {
            let (topic_name, subscription_name) = consumer_ref.value().clone();
            drop(consumer_ref);
            
            if let Some(topic) = self.topic_worker_pool.get_topic(&topic_name) {
                if let Some(subscription) = topic.subscriptions.lock().await.get(&subscription_name) {
                    return subscription.get_consumer_rx(consumer_id);
                }
            }
        }
        None
    }

    // finding the ConsumerInfo for the provided consumer_id
    pub(crate) async fn find_consumer_by_id(&self, consumer_id: u64) -> Option<ConsumerInfo> {
        if let Some(consumer_ref) = self.consumer_index.get(&consumer_id) {
            let (topic_name, subscription_name) = consumer_ref.value().clone();
            drop(consumer_ref);
            
            if let Some(topic) = self.topic_worker_pool.get_topic(&topic_name) {
                if let Some(subscription) = topic.subscriptions.lock().await.get(&subscription_name) {
                    return subscription.get_consumer_info(consumer_id);
                }
            }
        }
        None
    }

    pub(crate) async fn health_consumer(&self, consumer_id: u64) -> bool {
        if let Some(consumer) = self.find_consumer_by_id(consumer_id).await {
            return consumer.get_status().await;
        }
        false
    }

    pub(crate) async fn check_if_consumer_exist(
        &self,
        consumer_name: &str,
        subscription_name: &str,
        topic_name: &str,
    ) -> Option<u64> {
        let topic = self.topic_worker_pool.get_topic(topic_name)?;
        topic.validate_consumer(subscription_name, consumer_name).await
    }

    //validate if the consumer is allowed to create new subscription
    pub(crate) fn allow_subscription_creation(&self, topic_name: impl Into<String>) -> bool {
        // check the topic policies here
        let _topic = self
            .topic_worker_pool
            .get_topic(&topic_name.into())
            .expect("unable to find the topic");

        //TODO! once the policies Topic&Namespace Policies are in place we can verify if it is allowed

        true
    }

    //consumer subscribe to topic
    #[allow(dead_code)]
    pub(crate) async fn subscribe(
        &mut self,
        topic_name: &str,
        subscription_options: SubscriptionOptions,
    ) -> Result<u64> {
        // the caller of this function should ensure that the topic is served by this broker

        if let Some(topic) = self.topic_worker_pool.get_topic(topic_name) {
            let consumer_id = topic
                .subscribe(topic_name, subscription_options.clone())
                .await?;

            // insert into consumer_index for efficient searches and retrievals
            self.consumer_index.insert(
                consumer_id,
                (
                    topic_name.to_string(),
                    subscription_options.subscription_name.clone(),
                ),
            );

            gauge!(TOPIC_CONSUMERS.name, "topic" => topic_name.to_string()).increment(1);

            // create a metadata store entry for newly created subscription
            // TODO!, don't overwrite if not neccessary
            let sub_options = serde_json::to_value(&subscription_options)?;
            self.resources
                .topic
                .create_subscription(
                    &subscription_options.subscription_name,
                    topic_name,
                    sub_options,
                )
                .await?;

            return Ok(consumer_id);
        } else {
            return Err(anyhow!("Unable to find the topic: {}", topic_name));
        }
    }

    #[allow(dead_code)]
    pub(crate) async fn ack_message(&self, ack_msg: AckMessage) -> Result<()> {
        if let Some(topic) = self.topic_worker_pool.get_topic(&ack_msg.msg_id.topic_name) {
            topic.ack_message(ack_msg).await?;
        }
        Ok(())
    }

    // unsubscribe subscription from topic
    // only if subscription is empty, so no consumers attached
    pub(crate) async fn unsubscribe(
        &mut self,
        subscription_name: &str,
        topic_name: &str,
    ) -> Result<()> {
        // works if topic is local
        if let Some(topic) = self.topic_worker_pool.get_topic(topic_name) {
            if let Some(value) = topic.check_subscription(subscription_name).await {
                if value == false {
                    topic.unsubscribe(subscription_name).await;
                    return Ok(());
                }
            }
        }

        Err(anyhow!(
            "Unable to unsubscribe as the subscription {} has active consumers",
            subscription_name
        ))
    }

    pub(crate) async fn create_namespace_if_absent(&mut self, namespace_name: &str) -> Result<()> {
        match self
            .resources
            .namespace
            .namespace_exist(namespace_name)
            .await
        {
            Ok(true) => {
                return Err(anyhow!("Namespace {} already exists.", namespace_name));
            }
            Ok(false) => {
                self.resources
                    .namespace
                    .create_namespace(namespace_name, None)
                    .await?;
            }
            Err(err) => {
                return Err(anyhow!("Unable to perform operation {}", err));
            }
        }

        Ok(())
    }

    // deletes only empty namespaces (with no topics)
    pub(crate) async fn delete_namespace(&mut self, ns_name: &str) -> Result<()> {
        self.resources.namespace.delete_namespace(ns_name).await?;

        Ok(())
    }

    // Shutdown the broker service and all worker threads
    #[allow(dead_code)]
    pub(crate) async fn shutdown(&mut self) -> Result<()> {
        let topic_count = self.topic_worker_pool.get_all_topics().len();
        info!("Shutting down BrokerService with {} topics", topic_count);
        
        // Shutdown the topic worker pool
        Arc::get_mut(&mut self.topic_worker_pool)
            .ok_or_else(|| anyhow!("Failed to get mutable reference to topic worker pool"))?
            .shutdown()
            .await;
        
        info!("BrokerService shutdown completed");
        Ok(())
    }
}

// Topics string representation:  /{namespace}/{topic-name}
pub(crate) fn validate_topic_format(input: &str) -> bool {
    let parts: Vec<&str> = input.split('/').collect();

    if parts.len() != 3 {
        return false;
    }

    for part in parts.iter() {
        if !part
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
        {
            return false;
        }
    }

    true
}

// extract the namespace from a topic
// example from topic /ns_name/topic_name returns the  ns_name
fn get_nsname_from_topic(topic_name: &str) -> &str {
    // assuming that the topic name has already been validated.
    let parts: Vec<&str> = topic_name.split('/').collect();
    let ns_name = parts.get(1).unwrap();

    ns_name
}

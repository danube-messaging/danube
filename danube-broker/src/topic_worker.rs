use anyhow::Result;
use crossbeam::utils::Backoff;
use danube_core::message::StreamMessage;
use dashmap::DashMap;
use flume::{Receiver, Sender};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use crate::{message::AckMessage, subscription::SubscriptionOptions, topic::Topic};

/// Message types that can be sent to topic workers
#[derive(Debug)]
pub enum TopicWorkerMessage {
    PublishMessage {
        topic_name: String,
        message: StreamMessage,
        response_tx: flume::Sender<Result<()>>,
    },
    Subscribe {
        topic_name: String,
        options: SubscriptionOptions,
        response_tx: flume::Sender<Result<u64>>,
    },
    AckMessage {
        ack_msg: AckMessage,
        response_tx: flume::Sender<Result<()>>,
    },
    #[allow(dead_code)]
    CreateTopic {
        topic_name: String,
        response_tx: flume::Sender<Result<()>>,
    },
    Shutdown,
}

/// Individual worker handling a subset of topics
#[derive(Debug)]
pub struct TopicWorker {
    worker_id: usize,
    topics: Arc<DashMap<String, Arc<Topic>>>,
    message_rx: Receiver<TopicWorkerMessage>,
    #[allow(dead_code)]
    shutdown_handle: Option<JoinHandle<()>>,
}

impl TopicWorker {
    pub fn new(worker_id: usize, message_rx: Receiver<TopicWorkerMessage>) -> Self {
        Self {
            worker_id,
            topics: Arc::new(DashMap::new()),
            message_rx,
            shutdown_handle: None,
        }
    }

    /// Start the worker in a background task
    pub fn start(&mut self) -> JoinHandle<()> {
        let worker_id = self.worker_id;
        let topics = Arc::clone(&self.topics);
        let message_rx = self.message_rx.clone();

        let handle = tokio::spawn(async move {
            info!("Topic worker {} started", worker_id);

            let backoff = Backoff::new();

            loop {
                match message_rx.recv_async().await {
                    Ok(TopicWorkerMessage::PublishMessage {
                        topic_name,
                        message,
                        response_tx,
                    }) => {
                        let result =
                            Self::handle_publish_message(&topics, &topic_name, message).await;
                        if let Err(e) = response_tx.send_async(result).await {
                            error!("Failed to send publish response: {}", e);
                        }
                        backoff.reset();
                    }
                    Ok(TopicWorkerMessage::Subscribe {
                        topic_name,
                        options,
                        response_tx,
                    }) => {
                        let result =
                            Self::handle_subscribe(&topics, &topic_name.clone(), options).await;
                        if let Err(e) = response_tx.send_async(result).await {
                            error!("Failed to send subscribe response: {}", e);
                        }
                        backoff.reset();
                    }
                    Ok(TopicWorkerMessage::AckMessage {
                        ack_msg,
                        response_tx,
                    }) => {
                        let result = Self::handle_ack_message(&topics, ack_msg).await;
                        if let Err(e) = response_tx.send_async(result).await {
                            error!("Failed to send ack response: {}", e);
                        }
                        backoff.reset();
                    }
                    Ok(TopicWorkerMessage::CreateTopic {
                        topic_name,
                        response_tx,
                    }) => {
                        let result = Self::handle_create_topic(&topics, &topic_name).await;
                        if let Err(e) = response_tx.send_async(result).await {
                            error!("Failed to send create topic response: {}", e);
                        }
                        backoff.reset();
                    }
                    Ok(TopicWorkerMessage::Shutdown) => {
                        info!("Topic worker {} shutting down", worker_id);
                        break;
                    }
                    Err(flume::RecvError::Disconnected) => {
                        warn!("Topic worker {} channel disconnected", worker_id);
                        break;
                    }
                }
            }

            info!("Topic worker {} stopped", worker_id);
        });

        // Store handle reference for shutdown (JoinHandle doesn't implement Clone)
        // We'll manage shutdown through the message channel instead
        handle
    }

    async fn handle_publish_message(
        topics: &Arc<DashMap<String, Arc<Topic>>>,
        topic_name: &str,
        message: StreamMessage,
    ) -> Result<()> {
        if let Some(topic) = topics.get(topic_name) {
            topic.publish_message_async(message).await
        } else {
            Err(anyhow::anyhow!("Topic {} not found in worker", topic_name))
        }
    }

    async fn handle_subscribe(
        topics: &Arc<DashMap<String, Arc<Topic>>>,
        topic_name: &str,
        options: SubscriptionOptions,
    ) -> Result<u64> {
        if let Some(topic) = topics.get(topic_name) {
            topic.subscribe(topic_name, options).await
        } else {
            let available_topics: Vec<String> =
                topics.iter().map(|entry| entry.key().clone()).collect();
            error!(
                "Topic {} not found in worker. Available topics: {:?}",
                topic_name, available_topics
            );
            Err(anyhow::anyhow!("Topic {} not found in worker", topic_name))
        }
    }

    async fn handle_ack_message(
        topics: &Arc<DashMap<String, Arc<Topic>>>,
        ack_msg: AckMessage,
    ) -> Result<()> {
        if let Some(topic) = topics.get(&ack_msg.msg_id.topic_name) {
            topic.ack_message(ack_msg).await
        } else {
            Err(anyhow::anyhow!(
                "Topic {} not found in worker",
                ack_msg.msg_id.topic_name
            ))
        }
    }

    async fn handle_create_topic(
        _topics: &Arc<DashMap<String, Arc<Topic>>>,
        topic_name: &str,
    ) -> Result<()> {
        // This is a placeholder - in real implementation, you'd need to pass
        // the necessary parameters to create a topic
        debug!("Create topic request for {} (placeholder)", topic_name);
        Ok(())
    }

    pub fn add_topic(&self, topic_name: String, topic: Arc<Topic>) {
        info!("Worker {} adding topic: {}", self.worker_id, topic_name);
        self.topics.insert(topic_name, topic);
    }

    pub fn remove_topic(&self, topic_name: &str) -> Option<Arc<Topic>> {
        self.topics.remove(topic_name).map(|(_, topic)| topic)
    }

    #[allow(dead_code)]
    pub fn has_topic(&self, topic_name: &str) -> bool {
        self.topics.contains_key(topic_name)
    }

    #[allow(dead_code)]
    pub async fn shutdown(&mut self) {
        if let Some(handle) = self.shutdown_handle.take() {
            handle.abort();
        }
    }
}

/// Routes topics to workers using consistent hashing
#[derive(Debug)]
pub struct TopicRouter {
    worker_count: usize,
}

impl TopicRouter {
    pub fn new(worker_count: usize) -> Self {
        Self { worker_count }
    }

    /// Route a topic to a specific worker using consistent hashing
    pub fn route_topic(&self, topic_name: &str) -> usize {
        let mut hasher = DefaultHasher::new();
        topic_name.hash(&mut hasher);
        (hasher.finish() as usize) % self.worker_count
    }
}

/// Manager for all topic workers
#[derive(Debug)]
pub struct TopicWorkerPool {
    workers: Vec<TopicWorker>,
    worker_senders: Vec<Sender<TopicWorkerMessage>>,
    router: TopicRouter,
    #[allow(dead_code)]
    worker_handles: Vec<JoinHandle<()>>,
}

impl TopicWorkerPool {
    pub fn new(worker_count: Option<usize>) -> Self {
        let worker_count = worker_count.unwrap_or_else(num_cpus::get);
        let mut workers = Vec::with_capacity(worker_count);
        let mut worker_senders = Vec::with_capacity(worker_count);
        let mut worker_handles = Vec::with_capacity(worker_count);

        for i in 0..worker_count {
            let (tx, rx) = flume::unbounded();
            let mut worker = TopicWorker::new(i, rx);
            let handle = worker.start();

            workers.push(worker);
            worker_senders.push(tx);
            worker_handles.push(handle);
        }

        Self {
            workers,
            worker_senders,
            router: TopicRouter::new(worker_count),
            worker_handles,
        }
    }

    /// Send a message to the appropriate worker for the given topic
    pub async fn send_to_worker(
        &self,
        topic_name: &str,
        message: TopicWorkerMessage,
    ) -> Result<()> {
        let worker_id = self.router.route_topic(topic_name);
        let sender = &self.worker_senders[worker_id];

        sender
            .send_async(message)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to send message to worker {}: {}", worker_id, e))
    }

    /// Publish a message asynchronously
    pub async fn publish_message_async(
        &self,
        topic_name: String,
        message: StreamMessage,
    ) -> Result<()> {
        let (response_tx, response_rx) = flume::bounded(1);

        self.send_to_worker(
            &topic_name,
            TopicWorkerMessage::PublishMessage {
                topic_name: topic_name.clone(),
                message,
                response_tx,
            },
        )
        .await?;

        response_rx
            .recv_async()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to receive publish response: {}", e))?
    }

    /// Subscribe to a topic asynchronously
    pub async fn subscribe_async(
        &self,
        topic_name: String,
        options: SubscriptionOptions,
    ) -> Result<u64> {
        let (response_tx, response_rx) = flume::bounded(1);

        self.send_to_worker(
            &topic_name,
            TopicWorkerMessage::Subscribe {
                topic_name: topic_name.clone(),
                options,
                response_tx,
            },
        )
        .await?;

        response_rx
            .recv_async()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to receive subscribe response: {}", e))?
    }

    /// Acknowledge a message asynchronously
    pub async fn ack_message_async(&self, ack_msg: AckMessage) -> Result<()> {
        let (response_tx, response_rx) = flume::bounded(1);
        let topic_name = ack_msg.msg_id.topic_name.clone();

        self.send_to_worker(
            &topic_name,
            TopicWorkerMessage::AckMessage {
                ack_msg,
                response_tx,
            },
        )
        .await?;

        response_rx
            .recv_async()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to receive ack response: {}", e))?
    }

    /// Add a topic to the appropriate worker
    pub fn add_topic_to_worker(&self, topic_name: String, topic: Arc<Topic>) {
        let worker_id = self.router.route_topic(&topic_name);
        self.workers[worker_id].add_topic(topic_name, topic);
    }

    /// Remove a topic from its worker
    pub fn remove_topic_from_worker(&self, topic_name: &str) -> Option<Arc<Topic>> {
        let worker_id = self.router.route_topic(topic_name);
        self.workers[worker_id].remove_topic(topic_name)
    }

    /// Get a topic from the appropriate worker
    pub fn get_topic(&self, topic_name: &str) -> Option<Arc<Topic>> {
        let worker_id = self.router.route_topic(topic_name);
        self.workers[worker_id]
            .topics
            .get(topic_name)
            .map(|entry| entry.value().clone())
    }

    /// Get all topics currently managed by the worker pool
    pub fn get_all_topics(&self) -> Vec<String> {
        let mut all_topics = Vec::new();
        for worker in &self.workers {
            let topics: Vec<String> = worker
                .topics
                .iter()
                .map(|entry| entry.key().clone())
                .collect();
            all_topics.extend(topics);
        }
        all_topics
    }

    /// Check if any topic exists across all workers
    pub fn contains_topic(&self, topic_name: &str) -> bool {
        let worker_id = self.router.route_topic(topic_name);
        self.workers[worker_id].topics.contains_key(topic_name)
    }

    /// Shutdown all workers
    #[allow(dead_code)]
    pub async fn shutdown(&mut self) {
        // Send shutdown signal to all workers
        for sender in &self.worker_senders {
            let _ = sender.send(TopicWorkerMessage::Shutdown);
        }

        // Wait for all worker handles to complete
        for handle in self.worker_handles.drain(..) {
            let _ = handle.await;
        }
    }
}
impl Drop for TopicWorkerPool {
    fn drop(&mut self) {
        // Send shutdown signal to all workers
        for sender in &self.worker_senders {
            let _ = sender.try_send(TopicWorkerMessage::Shutdown);
        }
    }
}

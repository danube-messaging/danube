use std::sync::Arc;

use anyhow::{anyhow, Result};
use tokio::sync::Mutex;
use tonic::Status;

use danube_core::dispatch_strategy::ConfigDispatchStrategy;
use danube_core::proto::{DispatchStrategy as ProtoDispatchStrategy, SchemaReference};

use crate::{broker_service::validate_topic_format, policies::Policies, resources::Resources};

/// TopicCluster encapsulates cluster/metadata operations for topics.
///
/// Responsibilities:
/// - Create/delete topic records in the metadata store.
/// - Attach delivery strategy, schema, and optional policies to topics.
/// - Resolve which broker serves a given topic using cluster assignments.
/// - Provide lightweight read helpers used by `BrokerService`.
#[derive(Debug)]
pub(crate) struct TopicCluster {
    resources: Arc<Mutex<Resources>>,
}

impl TopicCluster {
    /// Constructs a new `TopicCluster` bound to shared `Resources`.
    pub(crate) fn new(resources: Arc<Mutex<Resources>>) -> Self {
        Self { resources }
    }

    /// Creates a topic in the cluster metadata.
    ///
    /// Validates topic format and required inputs (`dispatch_strategy`), ensures
    /// the namespace exists, and then posts the topic with delivery, schema_ref and policies.
    /// After creation, Load Manager will assign it to a broker.
    pub(crate) async fn create_on_cluster(
        &self,
        topic_name: &str,
        dispatch_strategy: Option<ProtoDispatchStrategy>,
        schema_ref: Option<SchemaReference>,
        policies: Option<Policies>,
    ) -> Result<(), Status> {
        // The topic format is /{namespace_name}/{topic_name}
        if !validate_topic_format(topic_name) {
            return Err(Status::invalid_argument(format!(
                "The topic: {} has an invalid format, should be: /namespace_name/topic_name",
                topic_name
            )));
        }

        if dispatch_strategy.is_none() {
            return Err(Status::invalid_argument("Dispatch strategy is missing"));
        }

        let ns_name = get_nsname_from_topic(topic_name);

        if let Ok(false) = {
            let mut resources = self.resources.lock().await;
            resources.namespace.namespace_exist(ns_name).await
        } {
            let status = Status::unavailable(&format!(
                "Unable to find the namespace {}, the topic can be created only for an exisiting namespace",
                ns_name
            ));
            return Err(status);
        }

        // Check if topic already exists (prevent duplicate creation)
        {
            let resources = self.resources.lock().await;
            if resources
                .namespace
                .check_if_topic_exist(ns_name, topic_name)
            {
                return Err(Status::already_exists(format!(
                    "Topic '{}' already exists",
                    topic_name
                )));
            }
        }

        // Validation: prevent creating both normal and partitioned topics with the same base name
        // Check if this is a partitioned topic (contains "-part-")
        if topic_name.contains("-part-") {
            // Extract base name: /default/topic_name-part-0 -> /default/topic_name
            if let Some(base_name) = topic_name.rsplit_once("-part-") {
                let base_topic = base_name.0;

                // Check if a non-partitioned topic with this base name exists
                let resources = self.resources.lock().await;
                if resources
                    .namespace
                    .check_if_topic_exist(ns_name, base_topic)
                {
                    return Err(Status::already_exists(format!(
                        "Cannot create partitioned topic '{}': a non-partitioned topic '{}' already exists. Delete the non-partitioned topic first.",
                        topic_name, base_topic
                    )));
                }
            }
        } else {
            // This is a normal (non-partitioned) topic
            // Check if any partitioned topics with this base name exist
            let resources = self.resources.lock().await;
            let partitions = resources
                .namespace
                .get_topic_partitions(ns_name, topic_name)
                .await;

            if !partitions.is_empty() {
                return Err(Status::already_exists(format!(
                    "Cannot create topic '{}': partitioned topics with this base name already exist (found {} partitions). Delete the partitioned topics first.",
                    topic_name, partitions.len()
                )));
            }
        }

        self.post_new_topic(topic_name, dispatch_strategy.unwrap(), schema_ref, policies)
            .await
            .map_err(|e| {
                Status::internal(format!(
                    "The broker unable to post the topic to metadata store, due to error: {}",
                    e
                ))
            })
    }

    /// Posts a new topic and its metadata to the store (unassigned + namespace + delivery + schema_ref + policies).
    pub(crate) async fn post_new_topic(
        &self,
        topic_name: &str,
        dispatch_strategy: ProtoDispatchStrategy,
        schema_ref: Option<SchemaReference>,
        policies: Option<Policies>,
    ) -> Result<()> {
        let mut resources = self.resources.lock().await;

        // 1) add to unassigned
        resources.cluster.new_unassigned_topic(topic_name).await?;

        // 2) add to namespace topics
        resources.namespace.create_new_topic(topic_name).await?;

        // 3) add delivery/retention strategy
        let dispatch_strategy: ConfigDispatchStrategy = match dispatch_strategy {
            ProtoDispatchStrategy::NonReliable => ConfigDispatchStrategy::NonReliable,
            ProtoDispatchStrategy::Reliable => ConfigDispatchStrategy::Reliable,
        };
        resources
            .topic
            .add_topic_delivery(topic_name, dispatch_strategy)
            .await?;

        // 4) add topic policies if any
        if let Some(policies) = policies {
            resources
                .topic
                .add_topic_policy(topic_name, policies)
                .await?;
        }

        // 5) add schema subject if provided
        if let Some(schema_ref) = schema_ref {
            resources
                .topic
                .add_topic_schema_subject(topic_name, &schema_ref.subject)
                .await?;
        }

        Ok(())
    }

    /// Posts an unload instruction for a topic without deleting essential metadata.
    /// Steps:
    /// 1. Resolve the hosting broker id for the topic.
    /// 2. Create an unassigned entry with an unload marker {reason:"unload", from_broker:<id>}.
    /// 3. Schedule deletion of the broker assignment at the hosting broker (watch will trigger local detach).
    pub(crate) async fn post_unload_topic(&self, topic_name: &str) -> Result<()> {
        // find the broker owning the topic
        let broker_id = match self
            .resources
            .lock()
            .await
            .cluster
            .get_broker_for_topic(topic_name)
            .await
        {
            Some(broker_id) => broker_id,
            None => return Err(anyhow!("Unable to find topic")),
        };

        // 1) create unload marker under /cluster/unassigned/{topic}
        {
            let mut resources = self.resources.lock().await;
            // broker_id is stored as string in metadata; try to parse to u64
            let from_broker_num = broker_id
                .parse::<u64>()
                .map_err(|_| anyhow!("Invalid broker id format: {}", broker_id))?;
            resources
                .cluster
                .mark_topic_for_unload(topic_name, from_broker_num)
                .await?;
        }

        // 2) schedule deletion at assigned broker (triggers local unload via watch)
        {
            let mut resources = self.resources.lock().await;
            resources
                .cluster
                .schedule_topic_deletion(&broker_id, topic_name)
                .await?;
        }

        Ok(())
    }

    /// Schedules deletion of a topic and removes associated metadata entries.
    ///
    /// Steps:
    /// 1. Validate: prevent deletion of individual partitions, only base topic allowed.
    /// 2. If base topic has partitions, delete all partitions.
    /// 3. Schedule deletion at assigned broker (watch triggers cleanup there).
    /// 4. Remove topic from namespace topics list.
    pub(crate) async fn post_delete_topic(&self, topic_name: &str) -> Result<()> {
        // Validation: prevent deleting individual partitions
        // Users must delete the base topic, which will delete all partitions
        if topic_name.contains("-part-") {
            return Err(anyhow!(
                "Cannot delete individual partition '{}'. Delete the base topic instead (e.g., if partition is '/default/mytopic-part-0', delete '/default/mytopic').",
                topic_name
            ));
        }

        let ns_name = get_nsname_from_topic(topic_name);

        // Check if this is a partitioned topic (has partitions)
        let all_topics = {
            let resources = self.resources.lock().await;
            resources
                .namespace
                .get_topic_partitions(ns_name, topic_name)
                .await
        };

        // Filter to get only actual partitions (exclude the base topic itself)
        let partitions: Vec<String> = all_topics
            .into_iter()
            .filter(|t| t != topic_name && t.contains("-part-"))
            .collect();

        if !partitions.is_empty() {
            // This is a partitioned topic - delete all partitions
            tracing::info!(
                base_topic = %topic_name,
                partition_count = %partitions.len(),
                "deleting partitioned topic with all its partitions"
            );

            for partition in &partitions {
                tracing::debug!(partition = %partition, "deleting partition");

                if let Err(e) = self.delete_single_topic(partition).await {
                    tracing::warn!(
                        partition = %partition,
                        error = %e,
                        "failed to delete partition, continuing with others"
                    );
                }
            }

            tracing::info!(
                base_topic = %topic_name,
                partitions_deleted = %partitions.len(),
                "successfully deleted the topic with all its partitions"
            );

            return Ok(());
        }

        // This is a normal (non-partitioned) topic - delete it directly
        self.delete_single_topic(topic_name).await?;
        tracing::info!(topic = %topic_name, "successfully deleted non-partitioned topic");
        Ok(())
    }

    /// Helper function to delete a single topic (either a normal topic or a partition).
    /// Performs: broker assignment deletion, namespace removal, and metadata cleanup.
    async fn delete_single_topic(&self, topic_name: &str) -> Result<()> {
        // Find the broker owning the topic
        let broker_id = match self
            .resources
            .lock()
            .await
            .cluster
            .get_broker_for_topic(topic_name)
            .await
        {
            Some(broker_id) => broker_id,
            None => return Err(anyhow!("Unable to find topic: {}", topic_name)),
        };

        // 1) Schedule delete at assigned broker (triggers watch)
        {
            let mut resources = self.resources.lock().await;
            resources
                .cluster
                .schedule_topic_deletion(&broker_id, topic_name)
                .await?;
        }

        // 2) Delete from namespace
        {
            let mut resources = self.resources.lock().await;
            resources.namespace.delete_topic(topic_name).await?;
        }

        // 3) Delete topic metadata: producers, subscriptions, delivery, policy, schema, root
        {
            let mut resources = self.resources.lock().await;
            // Best-effort cleanup; continue even if individual steps fail
            let _ = resources.topic.delete_all_producers(topic_name).await;
            let _ = resources.topic.delete_all_subscriptions(topic_name).await;
            let _ = resources.topic.delete_topic_delivery(topic_name).await;
            let _ = resources.topic.delete_topic_policy(topic_name).await;
            let _ = resources.topic.delete_topic_schema(topic_name).await;
            let _ = resources.topic.delete_topic_root(topic_name).await;
        }

        Ok(())
    }

    /// Returns true if the topic exists in the namespace topic list.
    pub(crate) async fn exists_topic_in_namespace(&self, topic_name: &str) -> bool {
        let ns = get_nsname_from_topic(topic_name);
        let resources = self.resources.lock().await;
        resources.namespace.check_if_topic_exist(ns, topic_name)
    }

    /// Resolves the socket address of the broker currently serving `topic_name`.
    /// Returns `None` if the topic is unknown or no broker is assigned.
    pub(crate) async fn find_serving_broker(&self, topic_name: &str) -> Option<String> {
        let broker_id = {
            let resources = self.resources.lock().await;
            resources.cluster.get_broker_for_topic(topic_name).await
        }?;
        let resources = self.resources.lock().await;
        resources.cluster.get_broker_addr(&broker_id)
    }
}

fn get_nsname_from_topic(topic_name: &str) -> &str {
    // assuming validated format
    let parts: Vec<&str> = topic_name.split('/').collect();
    parts.get(1).unwrap()
}

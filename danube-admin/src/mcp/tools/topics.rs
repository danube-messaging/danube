//! Topic management tools

use serde::{Deserialize, Serialize};
use schemars::JsonSchema;
use std::sync::Arc;
use crate::core::AdminGrpcClient;

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct ListTopicsParams {
    /// Namespace to list topics from (e.g., "default")
    pub namespace: String,
}

pub async fn list_topics(
    client: &Arc<AdminGrpcClient>,
    params: ListTopicsParams,
) -> String {
    let req = danube_core::admin_proto::NamespaceRequest {
        name: params.namespace.clone(),
    };

    match client.list_namespace_topics(req).await {
        Ok(response) => {
            if response.topics.is_empty() {
                return format!("No topics found in namespace '{}'.", params.namespace);
            }

            let mut output = format!(
                "Found {} topic(s) in namespace '{}':\n\n",
                response.topics.len(),
                params.namespace
            );

            for topic_info in &response.topics {
                output.push_str(&format!(
                    "Topic: {}\n\
                     Broker: {}\n\
                     Delivery: {}\n\n",
                    topic_info.name,
                    if topic_info.broker_id.is_empty() {
                        "unassigned"
                    } else {
                        &topic_info.broker_id
                    },
                    topic_info.delivery
                ));
            }

            output
        }
        Err(e) => format!("Error listing topics: {}", e),
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct DescribeTopicParams {
    /// Topic name (e.g., "/default/my-topic")
    pub topic: String,
}

pub async fn describe_topic(
    client: &Arc<AdminGrpcClient>,
    params: DescribeTopicParams,
) -> String {
    let req = danube_core::admin_proto::DescribeTopicRequest {
        name: params.topic.clone(),
    };

    match client.describe_topic(req).await {
        Ok(response) => {
            let mut output = format!("Topic: {}\n\n", response.name);
            output.push_str(&format!("Broker ID: {}\n", response.broker_id));
            output.push_str(&format!("Delivery: {}\n", response.delivery));

            if let Some(schema_subject) = response.schema_subject {
                output.push_str("\nSchema Information:\n");
                output.push_str(&format!("  Subject: {}\n", schema_subject));
                if let Some(schema_id) = response.schema_id {
                    output.push_str(&format!("  Schema ID: {}\n", schema_id));
                }
                if let Some(version) = response.schema_version {
                    output.push_str(&format!("  Version: {}\n", version));
                }
                if let Some(schema_type) = response.schema_type {
                    output.push_str(&format!("  Type: {}\n", schema_type));
                }
                if let Some(compat) = response.compatibility_mode {
                    output.push_str(&format!("  Compatibility: {}\n", compat));
                }
            }

            if !response.subscriptions.is_empty() {
                output.push_str(&format!("\nSubscriptions ({}):\n", response.subscriptions.len()));
                for (i, sub) in response.subscriptions.iter().enumerate() {
                    output.push_str(&format!("  {}. {}\n", i + 1, sub));
                }
            } else {
                output.push_str("\nNo subscriptions found.\n");
            }

            output
        }
        Err(e) => format!("Error describing topic: {}", e),
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct CreateTopicParams {
    /// Topic name (e.g., "/default/my-topic")
    pub name: String,
    /// Number of partitions (0 for non-partitioned)
    #[serde(default)]
    pub partitions: u32,
    /// Dispatch strategy: "reliable" or "non_reliable"
    #[serde(default = "default_dispatch_strategy")]
    pub dispatch_strategy: String,
    /// Optional schema subject
    pub schema_subject: Option<String>,
}

fn default_dispatch_strategy() -> String {
    "reliable".to_string()
}

pub async fn create_topic(
    client: &Arc<AdminGrpcClient>,
    params: CreateTopicParams,
) -> String {
    let dispatch = if params.dispatch_strategy.to_lowercase() == "reliable" {
        1 // Reliable
    } else {
        0 // NonReliable
    };

    if params.partitions > 0 {
        // Create partitioned topic
        let req = danube_core::admin_proto::PartitionedTopicRequest {
            base_name: params.name.clone(),
            partitions: params.partitions,
            schema_subject: params.schema_subject,
            dispatch_strategy: dispatch,
        };

        match client.create_partitioned_topic(req).await {
            Ok(_) => {
                format!(
                    "Successfully created partitioned topic '{}' with {} partitions ({})",
                    params.name,
                    params.partitions,
                    params.dispatch_strategy
                )
            }
            Err(e) => format!("Error creating partitioned topic: {}", e),
        }
    } else {
        // Create non-partitioned topic
        let req = danube_core::admin_proto::NewTopicRequest {
            name: params.name.clone(),
            schema_subject: params.schema_subject,
            dispatch_strategy: dispatch,
        };

        match client.create_topic(req).await {
            Ok(_) => {
                format!(
                    "Successfully created topic '{}' ({})",
                    params.name,
                    params.dispatch_strategy
                )
            }
            Err(e) => format!("Error creating topic: {}", e),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct DeleteTopicParams {
    /// Topic name to delete (e.g., "/default/my-topic")
    pub topic: String,
}

pub async fn delete_topic(
    client: &Arc<AdminGrpcClient>,
    params: DeleteTopicParams,
) -> String {
    let req = danube_core::admin_proto::TopicRequest {
        name: params.topic.clone(),
    };

    match client.delete_topic(req).await {
        Ok(_) => {
            format!("Successfully deleted topic '{}'", params.topic)
        }
        Err(e) => format!("Error deleting topic: {}", e),
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct ListSubscriptionsParams {
    /// Topic name (e.g., "/default/my-topic")
    pub topic: String,
}

pub async fn list_subscriptions(
    client: &Arc<AdminGrpcClient>,
    params: ListSubscriptionsParams,
) -> String {
    let req = danube_core::admin_proto::TopicRequest {
        name: params.topic.clone(),
    };

    match client.list_subscriptions(req).await {
        Ok(response) => {
            if response.subscriptions.is_empty() {
                return format!("No subscriptions found on topic '{}'", params.topic);
            }

            let mut output = format!(
                "Found {} subscription(s) on topic '{}':\n\n",
                response.subscriptions.len(),
                params.topic
            );

            for (i, sub) in response.subscriptions.iter().enumerate() {
                output.push_str(&format!("  {}. {}\n", i + 1, sub));
            }

            output
        }
        Err(e) => format!("Error listing subscriptions: {}", e),
    }
}

//! Topic management tools

use crate::core::AdminGrpcClient;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct ListTopicsParams {
    /// Namespace to list topics from.
    /// Use list_namespaces to discover available namespaces.
    /// The "default" namespace is always present.
    /// Provide either namespace or broker, not both.
    /// Example: "default", "production", "team-analytics"
    pub namespace: Option<String>,

    /// Broker ID to list topics for.
    /// Use list_brokers to discover available broker IDs.
    /// Provide either broker or namespace, not both.
    /// Example: "broker-1"
    pub broker: Option<String>,
}

pub async fn list_topics(client: &Arc<AdminGrpcClient>, params: ListTopicsParams) -> String {
    let (result, label) = if let Some(broker_id) = &params.broker {
        let req = danube_core::admin_proto::BrokerRequest {
            broker_id: broker_id.clone(),
        };
        (client.list_broker_topics(req).await, format!("broker '{}'", broker_id))
    } else if let Some(namespace) = &params.namespace {
        let req = danube_core::admin_proto::NamespaceRequest {
            name: namespace.clone(),
        };
        (client.list_namespace_topics(req).await, format!("namespace '{}'", namespace))
    } else {
        return "Error: provide either 'namespace' or 'broker' parameter.".to_string();
    };

    match result {
        Ok(response) => {
            if response.topics.is_empty() {
                return format!("No topics found in {}.", label);
            }

            let mut output = format!(
                "Found {} topic(s) in {}:\n\n",
                response.topics.len(),
                label
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
    /// Full topic name including namespace.
    /// Format: "/namespace/topic-name"
    /// Example: "/default/user-events", "/production/analytics-stream"
    pub topic: String,
}

pub async fn describe_topic(client: &Arc<AdminGrpcClient>, params: DescribeTopicParams) -> String {
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
                output.push_str(&format!(
                    "\nSubscriptions ({}):\n",
                    response.subscriptions.len()
                ));
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
    /// Full topic name including namespace.
    /// Format: "/namespace/topic-name"
    /// Example: "/default/user-events", "/production/analytics-stream"
    pub name: String,

    /// Number of partitions for parallel processing.
    /// Set to 0 for non-partitioned topic (single partition).
    /// Range: 0-256. Higher values enable greater parallelism for high-throughput topics.
    /// Default: 0 (non-partitioned)
    /// Example: 4, 8, 16
    #[serde(default)]
    pub partitions: u32,

    /// Message delivery and persistence strategy.
    /// Options: "reliable" (WAL + cloud storage, survives broker restarts) or
    /// "non_reliable" (in-memory only, faster but data loss on broker failure).
    /// Default: "non_reliable"
    #[serde(default = "default_dispatch_strategy")]
    pub dispatch_strategy: String,

    /// Schema subject name from the schema registry (optional).
    /// If specified, the schema must already be registered. Use register_schema first.
    /// Example: "user-events-value", "analytics-v2"
    pub schema_subject: Option<String>,
}

fn default_dispatch_strategy() -> String {
    "non_reliable".to_string()
}

pub async fn create_topic(client: &Arc<AdminGrpcClient>, params: CreateTopicParams) -> String {
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
                    params.name, params.partitions, params.dispatch_strategy
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
                    params.name, params.dispatch_strategy
                )
            }
            Err(e) => format!("Error creating topic: {}", e),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct DeleteTopicParams {
    /// Full topic name to delete including namespace.
    /// Format: "/namespace/topic-name"
    /// WARNING: This operation cannot be undone. All data will be permanently lost.
    /// Example: "/default/user-events"
    pub topic: String,
}

pub async fn delete_topic(client: &Arc<AdminGrpcClient>, params: DeleteTopicParams) -> String {
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
    /// Full topic name including namespace.
    /// Format: "/namespace/topic-name"
    /// Example: "/default/user-events"
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

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct UnsubscribeParams {
    /// Full topic name including namespace.
    /// Format: "/namespace/topic-name"
    /// Example: "/default/user-events"
    pub topic: String,

    /// Subscription name to delete.
    /// Use list_subscriptions to discover existing subscription names.
    /// Example: "my-consumer-group", "analytics-processor"
    pub subscription: String,
}

pub async fn unsubscribe(client: &Arc<AdminGrpcClient>, params: UnsubscribeParams) -> String {
    let req = danube_core::admin_proto::SubscriptionRequest {
        topic: params.topic.clone(),
        subscription: params.subscription.clone(),
    };

    match client.unsubscribe(req).await {
        Ok(response) => {
            if response.success {
                format!(
                    "✓ Successfully deleted subscription '{}' from topic '{}'",
                    params.subscription, params.topic
                )
            } else {
                format!(
                    "✗ Failed to delete subscription '{}' from topic '{}'",
                    params.subscription, params.topic
                )
            }
        }
        Err(e) => format!("Error deleting subscription: {}", e),
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct UnloadTopicParams {
    /// Full topic name to unload including namespace.
    /// Format: "/namespace/topic-name"
    /// The topic will be reassigned to a different broker automatically.
    /// Example: "/default/user-events"
    pub topic: String,
}

pub async fn unload_topic(client: &Arc<AdminGrpcClient>, params: UnloadTopicParams) -> String {
    let req = danube_core::admin_proto::TopicRequest {
        name: params.topic.clone(),
    };

    match client.unload_topic(req).await {
        Ok(response) => {
            if response.success {
                format!(
                    "✓ Successfully unloaded topic '{}' for reassignment",
                    params.topic
                )
            } else {
                format!("✗ Failed to unload topic '{}'", params.topic)
            }
        }
        Err(e) => format!("Error unloading topic: {}", e),
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct ConfigureTopicSchemaParams {
    /// Full topic name including namespace.
    /// Format: "/namespace/topic-name"
    /// Example: "/default/user-events"
    pub topic: String,

    /// Schema subject name from the schema registry.
    /// The schema must already be registered. Use register_schema first.
    /// Example: "user-events-value", "analytics-v2"
    pub schema_subject: String,

    /// Schema validation policy controlling enforcement level.
    /// Options: "none" (no validation), "warn" (log violations but allow),
    /// "enforce" (reject invalid messages).
    /// Default: "none"
    #[serde(default = "default_validation_policy")]
    pub validation_policy: String,

    /// Enable deep payload validation (field-level checks).
    /// When true, validates individual field constraints beyond schema structure.
    /// Default: false
    #[serde(default)]
    pub enable_payload_validation: bool,
}

fn default_validation_policy() -> String {
    "none".to_string()
}

pub async fn configure_topic_schema(
    client: &Arc<AdminGrpcClient>,
    params: ConfigureTopicSchemaParams,
) -> String {
    let req = danube_core::proto::danube_schema::ConfigureTopicSchemaRequest {
        topic_name: params.topic.clone(),
        schema_subject: params.schema_subject.clone(),
        validation_policy: params.validation_policy.to_uppercase(),
        enable_payload_validation: params.enable_payload_validation,
    };

    match client.configure_topic_schema(req).await {
        Ok(response) => {
            if response.success {
                format!(
                    "✓ Schema configuration set for topic '{}'\n  Subject: {}\n  Policy: {}\n  Validation: {}",
                    params.topic,
                    params.schema_subject,
                    params.validation_policy.to_uppercase(),
                    if params.enable_payload_validation { "ENABLED" } else { "DISABLED" }
                )
            } else {
                format!(
                    "✗ Failed to configure schema for topic '{}': {}",
                    params.topic, response.message
                )
            }
        }
        Err(e) => format!("Error configuring topic schema: {}", e),
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct SetValidationPolicyParams {
    /// Full topic name including namespace.
    /// Format: "/namespace/topic-name"
    /// The topic must already have a schema configured.
    /// Example: "/default/user-events"
    pub topic: String,

    /// Schema validation policy to apply.
    /// Options: "none" (no validation), "warn" (log violations),
    /// "enforce" (reject invalid messages).
    /// Use this to gradually roll out stricter validation.
    pub policy: String,

    /// Enable deep payload validation (field-level checks).
    /// When true, validates individual field constraints beyond schema structure.
    /// Default: false
    #[serde(default)]
    pub enable_payload_validation: bool,
}

pub async fn set_validation_policy(
    client: &Arc<AdminGrpcClient>,
    params: SetValidationPolicyParams,
) -> String {
    let req = danube_core::proto::danube_schema::UpdateTopicValidationPolicyRequest {
        topic_name: params.topic.clone(),
        validation_policy: params.policy.to_uppercase(),
        enable_payload_validation: params.enable_payload_validation,
    };

    match client.update_topic_validation_policy(req).await {
        Ok(response) => {
            if response.success {
                format!(
                    "✓ Validation policy updated for topic '{}'\n  Policy: {}\n  Validation: {}",
                    params.topic,
                    params.policy.to_uppercase(),
                    if params.enable_payload_validation {
                        "ENABLED"
                    } else {
                        "DISABLED"
                    }
                )
            } else {
                format!(
                    "✗ Failed to update validation policy for topic '{}': {}",
                    params.topic, response.message
                )
            }
        }
        Err(e) => format!("Error updating validation policy: {}", e),
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct GetSchemaConfigParams {
    /// Full topic name including namespace.
    /// Format: "/namespace/topic-name"
    /// Example: "/default/user-events"
    pub topic: String,
}

pub async fn get_topic_schema_config(
    client: &Arc<AdminGrpcClient>,
    params: GetSchemaConfigParams,
) -> String {
    let req = danube_core::proto::danube_schema::GetTopicSchemaConfigRequest {
        topic_name: params.topic.clone(),
    };

    match client.get_topic_schema_config(req).await {
        Ok(response) => {
            if response.schema_subject.is_empty() {
                format!("No schema configured for topic '{}'", params.topic)
            } else {
                let mut output = format!("Schema Configuration for topic '{}':\n\n", params.topic);
                output.push_str(&format!("  Subject: {}\n", response.schema_subject));
                output.push_str(&format!(
                    "  Policy: {}\n",
                    response.validation_policy.to_uppercase()
                ));
                output.push_str(&format!(
                    "  Payload Validation: {}\n",
                    if response.enable_payload_validation {
                        "ENABLED"
                    } else {
                        "DISABLED"
                    }
                ));
                if response.schema_id > 0 {
                    output.push_str(&format!("  Cached Schema ID: {}\n", response.schema_id));
                }
                output
            }
        }
        Err(e) => format!("Error getting schema config: {}", e),
    }
}

// ── Subscription Failure Policy ────────────────────────────────────────────

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct SetFailurePolicyParams {
    /// Full topic name including namespace.
    /// Format: "/namespace/topic-name"
    /// Example: "/default/user-events"
    pub topic: String,

    /// Subscription name to configure the failure policy for.
    /// Use list_subscriptions to discover existing subscriptions.
    /// Example: "my-consumer-group"
    pub subscription: String,

    /// Maximum number of redelivery attempts before applying the poison policy.
    /// Example: 3, 5, 10
    pub max_redelivery_count: u32,

    /// Acknowledgment timeout in milliseconds.
    /// If a message is not acked within this time, it will be redelivered.
    /// Example: 30000 (30 seconds)
    pub ack_timeout_ms: u64,

    /// Base delay between redelivery attempts in milliseconds.
    /// Example: 1000 (1 second)
    pub base_redelivery_delay_ms: u64,

    /// Maximum delay between redelivery attempts in milliseconds.
    /// Example: 60000 (60 seconds)
    pub max_redelivery_delay_ms: u64,

    /// Backoff strategy for redelivery delays.
    /// Options: "fixed" (constant delay) or "exponential" (doubling delay).
    /// Default: "fixed"
    #[serde(default = "default_backoff_strategy")]
    pub backoff_strategy: String,

    /// Policy for handling poison (unprocessable) messages after max redeliveries.
    /// Options: "dead_letter" (move to DLQ), "block" (stop delivery), "drop" (discard).
    /// Example: "dead_letter"
    pub poison_policy: String,

    /// Dead letter topic name for poison messages (required when poison_policy is "dead_letter").
    /// Example: "/default/user-events-dlq"
    pub dead_letter_topic: Option<String>,
}

fn default_backoff_strategy() -> String {
    "fixed".to_string()
}

fn parse_backoff_strategy(input: &str) -> i32 {
    match input.to_ascii_lowercase().as_str() {
        "exponential" => danube_core::admin_proto::SubscriptionBackoffStrategy::Exponential as i32,
        _ => danube_core::admin_proto::SubscriptionBackoffStrategy::Fixed as i32,
    }
}

fn parse_poison_policy(input: &str) -> i32 {
    match input.to_ascii_lowercase().as_str() {
        "block" => danube_core::admin_proto::SubscriptionPoisonPolicy::Block as i32,
        "drop" => danube_core::admin_proto::SubscriptionPoisonPolicy::Drop as i32,
        _ => danube_core::admin_proto::SubscriptionPoisonPolicy::DeadLetter as i32,
    }
}

fn backoff_strategy_label(strategy: i32) -> &'static str {
    match danube_core::admin_proto::SubscriptionBackoffStrategy::try_from(strategy).ok() {
        Some(danube_core::admin_proto::SubscriptionBackoffStrategy::Exponential) => "exponential",
        _ => "fixed",
    }
}

fn poison_policy_label(policy: i32) -> &'static str {
    match danube_core::admin_proto::SubscriptionPoisonPolicy::try_from(policy).ok() {
        Some(danube_core::admin_proto::SubscriptionPoisonPolicy::Drop) => "drop",
        Some(danube_core::admin_proto::SubscriptionPoisonPolicy::Block) => "block",
        _ => "dead_letter",
    }
}

pub async fn set_subscription_failure_policy(
    client: &Arc<AdminGrpcClient>,
    params: SetFailurePolicyParams,
) -> String {
    let req = danube_core::admin_proto::SetSubscriptionFailurePolicyRequest {
        topic: params.topic.clone(),
        subscription: params.subscription.clone(),
        failure_policy: Some(danube_core::admin_proto::SubscriptionFailurePolicy {
            max_redelivery_count: params.max_redelivery_count,
            ack_timeout_ms: params.ack_timeout_ms,
            base_redelivery_delay_ms: params.base_redelivery_delay_ms,
            max_redelivery_delay_ms: params.max_redelivery_delay_ms,
            backoff_strategy: parse_backoff_strategy(&params.backoff_strategy),
            dead_letter_topic: params.dead_letter_topic,
            poison_policy: parse_poison_policy(&params.poison_policy),
        }),
    };

    match client.set_subscription_failure_policy(req).await {
        Ok(response) => {
            if response.success {
                format!(
                    "✓ Failure policy updated for topic '{}' subscription '{}'",
                    params.topic, params.subscription
                )
            } else {
                format!(
                    "✗ Failed to update failure policy for topic '{}' subscription '{}'",
                    params.topic, params.subscription
                )
            }
        }
        Err(e) => format!("Error setting failure policy: {}", e),
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct GetFailurePolicyParams {
    /// Full topic name including namespace.
    /// Format: "/namespace/topic-name"
    /// Example: "/default/user-events"
    pub topic: String,

    /// Subscription name to retrieve the failure policy for.
    /// Use list_subscriptions to discover existing subscriptions.
    /// Example: "my-consumer-group"
    pub subscription: String,
}

pub async fn get_subscription_failure_policy(
    client: &Arc<AdminGrpcClient>,
    params: GetFailurePolicyParams,
) -> String {
    let req = danube_core::admin_proto::GetSubscriptionFailurePolicyRequest {
        topic: params.topic.clone(),
        subscription: params.subscription.clone(),
    };

    match client.get_subscription_failure_policy(req).await {
        Ok(response) => {
            if let Some(policy) = response.failure_policy {
                let mut output = format!(
                    "Failure Policy for topic '{}' subscription '{}':\n\n",
                    params.topic, params.subscription
                );
                output.push_str(&format!("  Max Redelivery Count: {}\n", policy.max_redelivery_count));
                output.push_str(&format!("  Ack Timeout: {} ms\n", policy.ack_timeout_ms));
                output.push_str(&format!("  Base Redelivery Delay: {} ms\n", policy.base_redelivery_delay_ms));
                output.push_str(&format!("  Max Redelivery Delay: {} ms\n", policy.max_redelivery_delay_ms));
                output.push_str(&format!("  Backoff Strategy: {}\n", backoff_strategy_label(policy.backoff_strategy)));
                output.push_str(&format!("  Poison Policy: {}\n", poison_policy_label(policy.poison_policy)));
                if let Some(dlt) = &policy.dead_letter_topic {
                    output.push_str(&format!("  Dead Letter Topic: {}\n", dlt));
                }
                output
            } else {
                format!(
                    "No failure policy found for topic '{}' subscription '{}'",
                    params.topic, params.subscription
                )
            }
        }
        Err(e) => format!("Error getting failure policy: {}", e),
    }
}

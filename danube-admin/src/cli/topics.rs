use anyhow::Result;
use clap::{Args, Subcommand};
use danube_core::admin_proto::{
    BrokerRequest, DescribeTopicRequest, DispatchStrategy as AdminDispatchStrategy,
    GetSubscriptionFailurePolicyRequest, NamespaceRequest, NewTopicRequest,
    PartitionedTopicRequest, SetSubscriptionFailurePolicyRequest,
    SubscriptionBackoffStrategy as AdminSubscriptionBackoffStrategy,
    SubscriptionFailurePolicy as AdminSubscriptionFailurePolicy,
    SubscriptionPoisonPolicy as AdminSubscriptionPoisonPolicy, SubscriptionRequest, TopicRequest,
};
use danube_core::proto::danube_schema::{
    ConfigureTopicSchemaRequest, GetTopicSchemaConfigRequest, UpdateTopicValidationPolicyRequest,
};

use crate::core::{AdminGrpcClient, GrpcClientConfig};

#[derive(Debug, Args)]
#[command(
    about = "Manage topics in the Danube cluster",
    long_about = "Manage topics in the Danube cluster.\n\nCommon examples:\n  danube-admin topics list --namespace default\n  danube-admin topics create /default/mytopic\n  danube-admin topics create /default/mytopic --partitions 3\n  danube-admin topics describe /default/mytopic",
    subcommand_required = true,
    arg_required_else_help = true
)]
pub struct Topics {
    #[command(subcommand)]
    command: TopicsCommands,
}

// Convert CLI string into enum DispatchStrategy for admin API
fn parse_dispatch_strategy(input: &str) -> AdminDispatchStrategy {
    match input.to_ascii_lowercase().as_str() {
        "reliable" | "reliable_dispatch" | "reliable-dispatch" => AdminDispatchStrategy::Reliable,
        _ => AdminDispatchStrategy::NonReliable,
    }
}

fn parse_subscription_backoff_strategy(input: &str) -> Result<AdminSubscriptionBackoffStrategy> {
    match input.to_ascii_lowercase().as_str() {
        "fixed" => Ok(AdminSubscriptionBackoffStrategy::Fixed),
        "exponential" => Ok(AdminSubscriptionBackoffStrategy::Exponential),
        _ => Err(anyhow::anyhow!(
            "invalid backoff strategy, expected one of: fixed, exponential"
        )),
    }
}

fn parse_subscription_poison_policy(input: &str) -> Result<AdminSubscriptionPoisonPolicy> {
    match input.to_ascii_lowercase().as_str() {
        "dead_letter" | "dead-letter" => Ok(AdminSubscriptionPoisonPolicy::DeadLetter),
        "block" => Ok(AdminSubscriptionPoisonPolicy::Block),
        "drop" => Ok(AdminSubscriptionPoisonPolicy::Drop),
        _ => Err(anyhow::anyhow!(
            "invalid poison policy, expected one of: dead_letter, block, drop"
        )),
    }
}

fn subscription_backoff_strategy_label(strategy: i32) -> &'static str {
    match AdminSubscriptionBackoffStrategy::try_from(strategy).ok() {
        Some(AdminSubscriptionBackoffStrategy::Exponential) => "exponential",
        _ => "fixed",
    }
}

fn subscription_poison_policy_label(policy: i32) -> &'static str {
    match AdminSubscriptionPoisonPolicy::try_from(policy).ok() {
        Some(AdminSubscriptionPoisonPolicy::DeadLetter) => "dead_letter",
        Some(AdminSubscriptionPoisonPolicy::Drop) => "drop",
        _ => "block",
    }
}

#[derive(Debug, Subcommand)]
enum TopicsCommands {
    #[command(about = "List topics by namespace or by broker")]
    List {
        #[arg(
            long,
            required_unless_present = "broker",
            help = "Namespace name (e.g., default)"
        )]
        namespace: Option<String>,
        #[arg(
            long,
            conflicts_with = "namespace",
            required_unless_present = "namespace",
            help = "Broker ID to filter topics"
        )]
        broker: Option<String>,
        #[arg(long, value_parser = ["json"], help = "Output format: json (default: plain)")]
        output: Option<String>,
    },
    #[command(
        about = "Create a topic (use --partitions for partitioned)",
        long_about = "Create a topic.\n\nExamples:\n  topics create /ns/my-topic\n  topics create /ns/my-topic --dispatch-strategy reliable\n  topics create /ns/my-topic --partitions 3\n  topics create my-topic --namespace ns --schema-subject user-events",
        after_help = "Examples:\n  danube-admin topics create /default/mytopic\n  danube-admin topics create /default/mytopic --dispatch-strategy reliable\n  danube-admin topics create /default/mytopic --partitions 3\n  danube-admin topics create mytopic --namespace default --schema-subject user-events\n\nEnv:\n  DANUBE_ADMIN_ENDPOINT (default http://127.0.0.1:50051)\n  DANUBE_ADMIN_TLS, DANUBE_ADMIN_DOMAIN, DANUBE_ADMIN_CA, DANUBE_ADMIN_CERT, DANUBE_ADMIN_KEY",
        arg_required_else_help = true
    )]
    Create {
        #[arg(
            help = "Topic name. Accepts '/ns/topic' or 'topic' (use --namespace for the latter)"
        )]
        topic: String,
        #[arg(long, help = "Namespace (if topic provided without namespace)")]
        namespace: Option<String>,
        #[arg(long, help = "Number of partitions for a partitioned topic")]
        partitions: Option<usize>,
        #[arg(long, help = "Schema subject name from Schema Registry (optional)")]
        schema_subject: Option<String>,
        #[arg(
            long,
            default_value = "non_reliable",
            help = "Dispatch strategy: non_reliable|reliable"
        )]
        dispatch_strategy: String,
    },
    #[command(
        about = "Delete an existing topic",
        after_help = "Example:\n  danube-admin topics delete /default/mytopic\n  danube-admin topics delete mytopic --namespace default",
        arg_required_else_help = true
    )]
    Delete {
        #[arg(help = "Topic name. Accepts '/ns/topic' or 'topic' with --namespace")]
        topic: String,
        #[arg(long)]
        namespace: Option<String>,
    },
    #[command(
        about = "List the subscriptions of the specified topic",
        after_help = "Example:\n  danube-admin topics subscriptions /default/mytopic --output json",
        arg_required_else_help = true
    )]
    Subscriptions {
        #[arg(help = "Topic name. Accepts '/ns/topic' or 'topic' with --namespace")]
        topic: String,
        #[arg(long)]
        namespace: Option<String>,
        #[arg(long, value_parser = ["json"], help = "Output format: json (default: plain)")]
        output: Option<String>,
    },
    #[command(
        about = "Describe a topic (schema and subscriptions)",
        after_help = "Example:\n  danube-admin topics describe /default/mytopic --output json",
        arg_required_else_help = true
    )]
    Describe {
        #[arg(help = "Topic name. Accepts '/ns/topic' or 'topic' with --namespace")]
        topic: String,
        #[arg(long)]
        namespace: Option<String>,
        #[arg(long, value_parser = ["json"], help = "Output format: json (default: pretty text)")]
        output: Option<String>,
    },
    #[command(
        about = "Delete a subscription from a topic",
        after_help = "Example:\n  danube-admin topics unsubscribe --subscription sub1 /default/mytopic",
        arg_required_else_help = true
    )]
    Unsubscribe {
        #[arg(help = "Topic name. Accepts '/ns/topic' or 'topic' with --namespace")]
        topic: String,
        #[arg(long)]
        namespace: Option<String>,
        #[arg(short, long)]
        subscription: String,
    },
    SetFailurePolicy {
        #[arg(help = "Topic name. Accepts '/ns/topic' or 'topic' with --namespace")]
        topic: String,
        #[arg(long)]
        namespace: Option<String>,
        #[arg(short, long)]
        subscription: String,
        #[arg(long)]
        max_redelivery_count: u32,
        #[arg(long)]
        ack_timeout_ms: u64,
        #[arg(long)]
        base_redelivery_delay_ms: u64,
        #[arg(long)]
        max_redelivery_delay_ms: u64,
        #[arg(long, value_parser = ["fixed", "exponential"], default_value = "fixed")]
        backoff_strategy: String,
        #[arg(long)]
        dead_letter_topic: Option<String>,
        #[arg(long, value_parser = ["dead_letter", "dead-letter", "block", "drop"])]
        poison_policy: String,
    },
    GetFailurePolicy {
        #[arg(help = "Topic name. Accepts '/ns/topic' or 'topic' with --namespace")]
        topic: String,
        #[arg(long)]
        namespace: Option<String>,
        #[arg(short, long)]
        subscription: String,
        #[arg(long, value_parser = ["json"], help = "Output format: json (default: pretty text)")]
        output: Option<String>,
    },
    #[command(
        about = "Unload a topic from its current broker to be reassigned to a different broker",
        after_help = "Example:\n  danube-admin topics unload /default/mytopic\n  danube-admin topics unload mytopic --namespace default",
        arg_required_else_help = true
    )]
    Unload {
        #[arg(help = "Topic name. Accepts '/ns/topic' or 'topic' with --namespace")]
        topic: String,
        #[arg(long)]
        namespace: Option<String>,
    },
    #[command(
        about = "Configure schema for a topic (admin-only)",
        long_about = "Configure schema settings for a topic. Assigns schema subject and sets validation policies.",
        after_help = "Examples:\n  danube-admin topics configure-schema /default/user-events --subject user-events-value --validation-policy enforce --enable-payload-validation\n  danube-admin topics configure-schema mytopic --namespace default --subject events --validation-policy warn",
        arg_required_else_help = true
    )]
    ConfigureSchema {
        #[arg(help = "Topic name. Accepts '/ns/topic' or 'topic' with --namespace")]
        topic: String,
        #[arg(long)]
        namespace: Option<String>,
        #[arg(long, help = "Schema subject name")]
        subject: String,
        #[arg(
            long,
            value_parser = ["none", "warn", "enforce"],
            default_value = "none",
            help = "Validation policy: none|warn|enforce"
        )]
        validation_policy: String,
        #[arg(long, help = "Enable deep payload validation")]
        enable_payload_validation: bool,
    },
    #[command(
        about = "Update validation policy for a topic (admin-only)",
        after_help = "Examples:\n  danube-admin topics set-validation-policy /default/user-events --policy enforce --enable-payload-validation\n  danube-admin topics set-validation-policy mytopic --namespace default --policy warn",
        arg_required_else_help = true
    )]
    SetValidationPolicy {
        #[arg(help = "Topic name. Accepts '/ns/topic' or 'topic' with --namespace")]
        topic: String,
        #[arg(long)]
        namespace: Option<String>,
        #[arg(
            long,
            value_parser = ["none", "warn", "enforce"],
            help = "Validation policy: none|warn|enforce"
        )]
        policy: String,
        #[arg(long, help = "Enable deep payload validation")]
        enable_payload_validation: bool,
    },
    #[command(
        about = "Get schema configuration for a topic",
        after_help = "Examples:\n  danube-admin topics get-schema-config /default/user-events\n  danube-admin topics get-schema-config mytopic --namespace default --output json",
        arg_required_else_help = true
    )]
    GetSchemaConfig {
        #[arg(help = "Topic name. Accepts '/ns/topic' or 'topic' with --namespace")]
        topic: String,
        #[arg(long)]
        namespace: Option<String>,
        #[arg(long, value_parser = ["json"], help = "Output format: json (default: plain)")]
        output: Option<String>,
    },
}

pub async fn handle(topics: Topics, endpoint: &str) -> Result<()> {
    let config = GrpcClientConfig {
        endpoint: endpoint.to_string(),
        ..Default::default()
    };
    let client = AdminGrpcClient::connect(config).await?;

    match topics.command {
        // List topics by either namespace or broker
        TopicsCommands::List {
            namespace,
            broker,
            output,
        } => {
            if let Some(ns) = namespace {
                let request = NamespaceRequest { name: ns };
                let response = client.list_namespace_topics(request).await?;
                let items = response.topics;
                
                if matches!(output.as_deref(), Some("json")) {
                    let json_items: Vec<serde_json::Value> = items
                        .iter()
                        .map(|it| {
                            serde_json::json!({
                                "name": it.name,
                                "broker_id": it.broker_id,
                                "delivery": it.delivery,
                            })
                        })
                        .collect();
                    println!("{}", serde_json::to_string_pretty(&json_items)?);
                } else {
                    for item in items {
                        if item.broker_id.is_empty() {
                            println!("Topic: {} (delivery: {})", item.name, item.delivery);
                        } else {
                            println!(
                                "Topic: {} (broker_id: {}, delivery: {})",
                                item.name, item.broker_id, item.delivery
                            );
                        }
                    }
                }
            } else if let Some(bid) = broker {
                let request = BrokerRequest { broker_id: bid };
                let response = client.list_broker_topics(request).await?;
                let items = response.topics;
                
                if matches!(output.as_deref(), Some("json")) {
                    let json_items: Vec<serde_json::Value> = items
                        .iter()
                        .map(|it| {
                            serde_json::json!({
                                "name": it.name,
                                "broker_id": it.broker_id,
                                "delivery": it.delivery,
                            })
                        })
                        .collect();
                    println!("{}", serde_json::to_string_pretty(&json_items)?);
                } else {
                    for item in items {
                        if item.broker_id.is_empty() {
                            println!("Topic: {} (delivery: {})", item.name, item.delivery);
                        } else {
                            println!(
                                "Topic: {} (broker_id: {}, delivery: {})",
                                item.name, item.broker_id, item.delivery
                            );
                        }
                    }
                }
            } else {
                eprintln!("Provide either --namespace or --broker");
            }
        }

        // Create topic (non-partitioned or partitioned)
        TopicsCommands::Create {
            topic,
            namespace,
            partitions,
            schema_subject,
            dispatch_strategy,
        } => {
            let topic_path = normalize_topic(&topic, namespace.as_deref())?;

            if let Some(parts) = partitions {
                let req = PartitionedTopicRequest {
                    base_name: topic_path.clone(),
                    partitions: parts as u32,
                    schema_subject: schema_subject.clone(),
                    dispatch_strategy: parse_dispatch_strategy(&dispatch_strategy) as i32,
                };
                let response = client.create_partitioned_topic(req).await?;
                if response.success {
                    println!("✅ Partitioned topic created: {}", topic_path);
                    if let Some(subject) = schema_subject {
                        println!("   Schema subject: {}", subject);
                    }
                    println!("   Partitions: {}", parts);
                }
            } else {
                let request = NewTopicRequest {
                    name: topic_path.clone(),
                    schema_subject: schema_subject.clone(),
                    dispatch_strategy: parse_dispatch_strategy(&dispatch_strategy) as i32,
                };
                let response = client.create_topic(request).await?;
                if response.success {
                    println!("✅ Topic created: {}", topic_path);
                    if let Some(subject) = schema_subject {
                        println!("   Schema subject: {}", subject);
                    }
                }
            }
        }

        // Delete the topic
        TopicsCommands::Delete { topic, namespace } => {
            let name = normalize_topic(&topic, namespace.as_deref())?;
            let request = TopicRequest { name };
            let response = client.delete_topic(request).await?;
            println!("Topic Deleted: {:?}", response.success);
        }

        // Get the list of subscriptions on the topic
        TopicsCommands::Subscriptions {
            topic,
            namespace,
            output,
        } => {
            let name = normalize_topic(&topic, namespace.as_deref())?;
            let request = TopicRequest { name };
            let response = client.list_subscriptions(request).await?;
            let subs = response.subscriptions;
            
            if matches!(output.as_deref(), Some("json")) {
                println!("{}", serde_json::to_string_pretty(&subs)?);
            } else {
                println!("Subscriptions: {:?}", subs);
            }
        }

        // Describe a topic: schema + subscriptions
        TopicsCommands::Describe {
            topic,
            namespace,
            output,
        } => {
            let name = normalize_topic(&topic, namespace.as_deref())?;

            // Get topic description
            let desc_req = DescribeTopicRequest { name: name.clone() };
            let topic_info = client.describe_topic(desc_req).await?;

            if matches!(output.as_deref(), Some("json")) {
                let out = serde_json::json!({
                    "topic": topic_info.name,
                    "broker_id": topic_info.broker_id,
                    "delivery": topic_info.delivery,
                    "schema_subject": topic_info.schema_subject,
                    "schema_id": topic_info.schema_id,
                    "schema_version": topic_info.schema_version,
                    "schema_type": topic_info.schema_type,
                    "compatibility_mode": topic_info.compatibility_mode,
                    "subscriptions": topic_info.subscriptions,
                });
                println!("{}", serde_json::to_string_pretty(&out)?);
            } else {
                println!("Topic: {}", topic_info.name);
                if !topic_info.broker_id.is_empty() {
                    println!("Broker ID: {}", topic_info.broker_id);
                }
                if !topic_info.delivery.is_empty() {
                    println!("Delivery: {}", topic_info.delivery);
                }

                // Schema Registry info
                if let Some(subject) = topic_info.schema_subject {
                    println!("\n📋 Schema Registry:");
                    println!("  Subject: {}", subject);
                    if let Some(id) = topic_info.schema_id {
                        println!("  Schema ID: {}", id);
                    }
                    if let Some(version) = topic_info.schema_version {
                        println!("  Version: {}", version);
                    }
                    if let Some(schema_type) = topic_info.schema_type {
                        println!("  Type: {}", schema_type);
                    }
                    if let Some(mode) = topic_info.compatibility_mode {
                        println!("  Compatibility: {}", mode);
                    }
                } else {
                    println!("\n📋 Schema: None");
                }

                println!("\nSubscriptions: {:?}", topic_info.subscriptions);
            }
        }

        // Delete a subscription from a topic
        TopicsCommands::Unsubscribe {
            topic,
            namespace,
            subscription,
        } => {
            let name = normalize_topic(&topic, namespace.as_deref())?;
            let request = SubscriptionRequest {
                topic: name,
                subscription,
            };
            let response = client.unsubscribe(request).await?;
            println!("Unsubscribed: {:?}", response.success);
        }

        TopicsCommands::SetFailurePolicy {
            topic,
            namespace,
            subscription,
            max_redelivery_count,
            ack_timeout_ms,
            base_redelivery_delay_ms,
            max_redelivery_delay_ms,
            backoff_strategy,
            dead_letter_topic,
            poison_policy,
        } => {
            let topic_name = normalize_topic(&topic, namespace.as_deref())?;
            let request = SetSubscriptionFailurePolicyRequest {
                topic: topic_name.clone(),
                subscription: subscription.clone(),
                failure_policy: Some(AdminSubscriptionFailurePolicy {
                    max_redelivery_count,
                    ack_timeout_ms,
                    base_redelivery_delay_ms,
                    max_redelivery_delay_ms,
                    backoff_strategy: parse_subscription_backoff_strategy(&backoff_strategy)? as i32,
                    dead_letter_topic,
                    poison_policy: parse_subscription_poison_policy(&poison_policy)? as i32,
                }),
            };

            let response = client.set_subscription_failure_policy(request).await?;
            if response.success {
                println!(
                    "✅ Failure policy updated for topic '{}' subscription '{}'",
                    topic_name, subscription
                );
            }
        }

        TopicsCommands::GetFailurePolicy {
            topic,
            namespace,
            subscription,
            output,
        } => {
            let topic_name = normalize_topic(&topic, namespace.as_deref())?;
            let response = client
                .get_subscription_failure_policy(GetSubscriptionFailurePolicyRequest {
                    topic: topic_name.clone(),
                    subscription: subscription.clone(),
                })
                .await?;
            let failure_policy = response
                .failure_policy
                .ok_or_else(|| anyhow::anyhow!("subscription failure policy not found"))?;

            if matches!(output.as_deref(), Some("json")) {
                let out = serde_json::json!({
                    "topic": topic_name,
                    "subscription": subscription,
                    "max_redelivery_count": failure_policy.max_redelivery_count,
                    "ack_timeout_ms": failure_policy.ack_timeout_ms,
                    "base_redelivery_delay_ms": failure_policy.base_redelivery_delay_ms,
                    "max_redelivery_delay_ms": failure_policy.max_redelivery_delay_ms,
                    "backoff_strategy": subscription_backoff_strategy_label(failure_policy.backoff_strategy),
                    "dead_letter_topic": failure_policy.dead_letter_topic,
                    "poison_policy": subscription_poison_policy_label(failure_policy.poison_policy),
                });
                println!("{}", serde_json::to_string_pretty(&out)?);
            } else {
                println!("Topic: {}", topic_name);
                println!("Subscription: {}", subscription);
                println!("Max Redelivery Count: {}", failure_policy.max_redelivery_count);
                println!("Ack Timeout Ms: {}", failure_policy.ack_timeout_ms);
                println!(
                    "Base Redelivery Delay Ms: {}",
                    failure_policy.base_redelivery_delay_ms
                );
                println!(
                    "Max Redelivery Delay Ms: {}",
                    failure_policy.max_redelivery_delay_ms
                );
                println!(
                    "Backoff Strategy: {}",
                    subscription_backoff_strategy_label(failure_policy.backoff_strategy)
                );
                if let Some(dead_letter_topic) = failure_policy.dead_letter_topic {
                    println!("Dead Letter Topic: {}", dead_letter_topic);
                }
                println!(
                    "Poison Policy: {}",
                    subscription_poison_policy_label(failure_policy.poison_policy)
                );
            }
        }
        
        // Unload a topic
        TopicsCommands::Unload { topic, namespace } => {
            let name = normalize_topic(&topic, namespace.as_deref())?;
            let request = TopicRequest { name };
            let response = client.unload_topic(request).await?;
            println!("Topic Unloaded: {:?}", response.success);
        }

        // Configure schema for a topic
        TopicsCommands::ConfigureSchema {
            topic,
            namespace,
            subject,
            validation_policy,
            enable_payload_validation,
        } => {
            let topic_name = normalize_topic(&topic, namespace.as_deref())?;

            let request = ConfigureTopicSchemaRequest {
                topic_name: topic_name.clone(),
                schema_subject: subject.clone(),
                validation_policy: validation_policy.to_uppercase(),
                enable_payload_validation,
            };

            let result = client.configure_topic_schema(request).await?;

            if result.success {
                println!("✅ Schema configuration set for topic '{}'", topic_name);
                println!("   Schema Subject: {}", subject);
                println!("   Validation Policy: {}", validation_policy.to_uppercase());
                println!(
                    "   Payload Validation: {}",
                    if enable_payload_validation {
                        "ENABLED"
                    } else {
                        "DISABLED"
                    }
                );
            } else {
                println!("❌ Failed to configure schema for topic '{}'", topic_name);
                if !result.message.is_empty() {
                    println!("Error: {}", result.message);
                }
            }
        }

        // Update validation policy for a topic
        TopicsCommands::SetValidationPolicy {
            topic,
            namespace,
            policy,
            enable_payload_validation,
        } => {
            let topic_name = normalize_topic(&topic, namespace.as_deref())?;

            let request = UpdateTopicValidationPolicyRequest {
                topic_name: topic_name.clone(),
                validation_policy: policy.to_uppercase(),
                enable_payload_validation,
            };

            let result = client.update_topic_validation_policy(request).await?;

            if result.success {
                println!(
                    "✅ Validation policy updated for topic '{}'",
                    topic_name
                );
                println!("   Policy: {}", policy.to_uppercase());
                println!(
                    "   Payload Validation: {}",
                    if enable_payload_validation {
                        "ENABLED"
                    } else {
                        "DISABLED"
                    }
                );
            } else {
                println!(
                    "❌ Failed to update validation policy for topic '{}'",
                    topic_name
                );
                if !result.message.is_empty() {
                    println!("Error: {}", result.message);
                }
            }
        }

        // Get schema configuration for a topic
        TopicsCommands::GetSchemaConfig {
            topic,
            namespace,
            output,
        } => {
            let topic_name = normalize_topic(&topic, namespace.as_deref())?;

            let request = GetTopicSchemaConfigRequest {
                topic_name: topic_name.clone(),
            };

            let result = client.get_topic_schema_config(request).await?;

            if matches!(output.as_deref(), Some("json")) {
                let out = serde_json::json!({
                    "topic": topic_name,
                    "schema_subject": result.schema_subject,
                    "validation_policy": result.validation_policy.to_uppercase(),
                    "enable_payload_validation": result.enable_payload_validation,
                    "schema_id": result.schema_id,
                });
                println!("{}", serde_json::to_string_pretty(&out)?);
            } else {
                println!("Topic: {}", topic_name);
                if !result.schema_subject.is_empty() {
                    println!("Schema Subject: {}", result.schema_subject);
                    println!("Validation Policy: {}", result.validation_policy.to_uppercase());
                    println!(
                        "Payload Validation: {}",
                        if result.enable_payload_validation {
                            "ENABLED"
                        } else {
                            "DISABLED"
                        }
                    );
                    if result.schema_id > 0 {
                        println!("Cached Schema ID: {}", result.schema_id);
                    }
                } else {
                    println!("No schema configured for this topic");
                }
            }
        }
    }

    Ok(())
}

// Topics string representation:  /{namespace}/{topic-name}
fn validate_topic_format(input: &str) -> bool {
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

// Normalize topic path to "/namespace/topic". Accepts:
//  - "/ns/topic"
//  - "ns/topic"
//  - "topic" when --namespace is provided
fn normalize_topic(input: &str, namespace: Option<&str>) -> Result<String> {
    let s = input.trim();
    if s.starts_with('/') {
        if validate_topic_format(s) {
            return Ok(s.to_string());
        }
        return Err(anyhow::anyhow!("wrong topic format, should be /namespace/topic"));
    }
    let parts: Vec<&str> = s.split('/').collect();
    match parts.len() {
        2 => Ok(format!("/{}", s)),
        1 => {
            let ns = namespace.ok_or_else(|| anyhow::anyhow!("missing --namespace for topic without namespace"))?;
            Ok(format!("/{}/{}", ns, s))
        }
        _ => Err(anyhow::anyhow!("wrong topic format, should be /namespace/topic")),
    }
}

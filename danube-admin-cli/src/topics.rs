use crate::client::topic_admin_client;
use base64::{engine::general_purpose, Engine as _};
use clap::{Args, Subcommand};
use danube_core::admin_proto::{
    DispatchStrategy as AdminDispatchStrategy, NamespaceRequest, NewTopicRequest,
    PartitionedTopicRequest, ReliableOptions, RetentionPolicy, SubscriptionRequest,
    TopicDispatchStrategy, TopicRequest,
};
use danube_core::proto::{discovery_client::DiscoveryClient, SchemaRequest};
use std::env;

#[derive(Debug, Args)]
pub(crate) struct Topics {
    #[command(subcommand)]
    command: TopicsCommands,
}

// Helper: fetch schema for a topic via Discovery service and return JSON value
async fn fetch_schema_json(topic: &str) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let endpoint =
        env::var("DANUBE_BROKER_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:6650".to_string());
    let mut client = DiscoveryClient::connect(endpoint).await?;
    let req = SchemaRequest {
        request_id: 1,
        topic: topic.to_string(),
    };
    let resp = client.get_schema(req).await?;
    let schema = resp.into_inner().schema;
    // Convert the prost `Schema` into JSON for printing
    let value = serde_json::json!({
        "name": schema.as_ref().map(|s| s.name.clone()).unwrap_or_default(),
        "type_schema": schema.as_ref().map(|s| s.type_schema).unwrap_or_default(),
        // schema_data is bytes; render as string when possible, else base64
        "schema_data": schema
            .as_ref()
            .map(|s| String::from_utf8(s.schema_data.clone()).unwrap_or_else(|_| general_purpose::STANDARD.encode(s.schema_data.clone())))
            .unwrap_or_default(),
    });
    Ok(value)
}

// Convert CLI string into structured dispatch strategy for admin API
fn parse_dispatch_strategy(
    input: &str,
    segment_size_mb: Option<u64>,
    retention_policy: Option<&str>,
    retention_period_sec: Option<u64>,
) -> TopicDispatchStrategy {
    match input.to_ascii_lowercase().as_str() {
        "reliable" | "reliable_dispatch" | "reliable-dispatch" => {
            // Defaults
            let seg = segment_size_mb.unwrap_or(64);
            let pol = match retention_policy
                .unwrap_or("retain_until_ack")
                .to_ascii_lowercase()
                .as_str()
            {
                "retain_until_expire" => RetentionPolicy::RetainUntilExpire as i32,
                _ => RetentionPolicy::RetainUntilAck as i32,
            };
            let period = retention_period_sec.unwrap_or(24 * 60 * 60);

            TopicDispatchStrategy {
                strategy: AdminDispatchStrategy::Reliable as i32,
                reliable_options: Some(ReliableOptions {
                    segment_size: seg,
                    retention_policy: pol,
                    retention_period: period,
                }),
            }
        }
        _ => TopicDispatchStrategy {
            strategy: AdminDispatchStrategy::NonReliable as i32,
            reliable_options: None,
        },
    }
}

#[derive(Debug, Subcommand)]
pub(crate) enum TopicsCommands {
    #[command(about = "List topics in the specified namespace")]
    List {
        namespace: String,
        #[arg(long, value_parser = ["json"], help = "Output format: json (default: plain)")]
        output: Option<String>,
    },
    #[command(about = "Create a non-partitioned topic")]
    Create {
        topic: String,
        #[arg(short, long, default_value = "String")]
        schema_type: String,
        #[arg(short = 'd', long, default_value = "{}")]
        schema_data: String,
        #[arg(long, default_value = "non_reliable")]
        dispatch_strategy: String,
        #[arg(long)]
        segment_size_mb: Option<u64>,
        #[arg(long, value_parser = ["retain_until_ack", "retain_until_expire"])]
        retention_policy: Option<String>,
        #[arg(long)]
        retention_period_sec: Option<u64>,
    },
    #[command(about = "Create a partitioned topic")]
    CreatePartitioned {
        topic: String,
        partitions: usize,
        #[arg(short, long, default_value = "String")]
        schema_type: String,
        #[arg(short = 'd', long, default_value = "{}")]
        schema_data: String,
        #[arg(long, default_value = "non_reliable")]
        dispatch_strategy: String,
        #[arg(long)]
        segment_size_mb: Option<u64>,
        #[arg(long, value_parser = ["retain_until_ack", "retain_until_expire"])]
        retention_policy: Option<String>,
        #[arg(long)]
        retention_period_sec: Option<u64>,
    },
    #[command(about = "Delete an existing topic")]
    Delete { topic: String },
    #[command(about = "List the subscriptions of the specified topic")]
    Subscriptions {
        topic: String,
        #[arg(long, value_parser = ["json"], help = "Output format: json (default: plain)")]
        output: Option<String>,
    },
    #[command(about = "Describe a topic (schema and subscriptions)")]
    Describe {
        topic: String,
        #[arg(long, value_parser = ["json"], help = "Output format: json (default: pretty text)")]
        output: Option<String>,
    },
    #[command(about = "Delete a subscription from a topic")]
    Unsubscribe {
        topic: String,
        #[arg(short, long)]
        subscription: String,
    },
}

#[allow(unreachable_code)]
pub async fn handle_command(topics: Topics) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = topic_admin_client().await?;

    match topics.command {
        // Get the list of topics of a namespace
        TopicsCommands::List { namespace, output } => {
            let request = NamespaceRequest { name: namespace };
            let response = client.list_topics(request).await?;

            let topics = response.into_inner().topics;
            if matches!(output.as_deref(), Some("json")) {
                println!("{}", serde_json::to_string_pretty(&topics)?);
            } else {
                for topic in topics {
                    println!("Topic: {}", topic);
                }
            }
        }

        // Creates a non-partitioned topic
        TopicsCommands::Create {
            topic,
            mut schema_type,
            schema_data,
            dispatch_strategy,
            segment_size_mb,
            retention_policy,
            retention_period_sec,
        } => {
            if !validate_topic_format(&topic) {
                return Err("wrong topic format, should be /namespace/topic".into());
            }

            if schema_type.is_empty() {
                schema_type = "String".into()
            }
            let request = NewTopicRequest {
                name: topic,
                schema_type,
                schema_data,
                dispatch_strategy: Some(parse_dispatch_strategy(
                    &dispatch_strategy,
                    segment_size_mb,
                    retention_policy.as_deref(),
                    retention_period_sec,
                )),
            };
            let response = client.create_topic(request).await?;
            println!("Topic Created: {:?}", response.into_inner().success);
        }

        // Creates a partitioned topic (should specify the number of partitions)
        TopicsCommands::CreatePartitioned {
            topic,
            partitions,
            mut schema_type,
            schema_data,
            dispatch_strategy,
            segment_size_mb,
            retention_policy,
            retention_period_sec,
        } => {
            if !validate_topic_format(&topic) {
                return Err("wrong topic format, should be /namespace/topic".into());
            }

            if schema_type.is_empty() {
                schema_type = "String".into()
            }
            let req = PartitionedTopicRequest {
                base_name: topic,
                partitions: partitions as u32,
                schema_type: schema_type,
                schema_data: schema_data,
                dispatch_strategy: Some(parse_dispatch_strategy(
                    &dispatch_strategy,
                    segment_size_mb,
                    retention_policy.as_deref(),
                    retention_period_sec,
                )),
            };

            let response = client.create_partitioned_topic(req).await?;
            println!(
                "Partitioned Topic Created: {:?}",
                response.into_inner().success
            );
        }

        // Delete the topic
        TopicsCommands::Delete { topic } => {
            if !validate_topic_format(&topic) {
                return Err("wrong topic format, should be /namespace/topic".into());
            }

            let request = TopicRequest { name: topic };
            let response = client.delete_topic(request).await?;
            println!("Topic Deleted: {:?}", response.into_inner().success);
        }

        // Get the list of subscriptions on the topic
        TopicsCommands::Subscriptions { topic, output } => {
            if !validate_topic_format(&topic) {
                return Err("wrong topic format, should be /namespace/topic".into());
            }

            let request = TopicRequest { name: topic };
            let response = client.list_subscriptions(request).await?;
            let subs = response.into_inner().subscriptions;
            if matches!(output.as_deref(), Some("json")) {
                println!("{}", serde_json::to_string_pretty(&subs)?);
            } else {
                println!("Subscriptions: {:?}", subs);
            }
        }

        // Describe a topic: schema + subscriptions
        TopicsCommands::Describe { topic, output } => {
            if !validate_topic_format(&topic) {
                return Err("wrong topic format, should be /namespace/topic".into());
            }

            // subscriptions via admin
            let subs_req = TopicRequest {
                name: topic.clone(),
            };
            let subs_resp = client.list_subscriptions(subs_req).await?;
            let subscriptions = subs_resp.into_inner().subscriptions;

            // schema via discovery
            let schema_value = fetch_schema_json(&topic).await?;

            if matches!(output.as_deref(), Some("json")) {
                let out = serde_json::json!({
                    "topic": topic,
                    "schema": schema_value,
                    "subscriptions": subscriptions,
                });
                println!("{}", serde_json::to_string_pretty(&out)?);
            } else {
                println!("Topic: {}", topic);
                println!("Schema: {}", serde_json::to_string_pretty(&schema_value)?);
                println!("Subscriptions: {:?}", subscriptions);
            }
        }

        // Delete a subscription from a topic
        TopicsCommands::Unsubscribe {
            topic,
            subscription,
        } => {
            if !validate_topic_format(&topic) {
                return Err("wrong topic format, should be /namespace/topic".into());
            }

            let request = SubscriptionRequest {
                topic,
                subscription,
            };
            let response = client.unsubscribe(request).await?;
            println!("Unsubscribed: {:?}", response.into_inner().success);
        }
    }

    Ok(())
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

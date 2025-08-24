use crate::client::topic_admin_client;
use base64::{engine::general_purpose, Engine as _};
use clap::{Args, Subcommand};
use danube_core::admin_proto::{
    topic_admin_client::TopicAdminClient,
    DescribeTopicRequest,
    DispatchStrategy as AdminDispatchStrategy, NamespaceRequest, NewTopicRequest,
    PartitionedTopicRequest, ReliableOptions, RetentionPolicy, SubscriptionRequest,
    TopicDispatchStrategy, TopicRequest,
};
use crate::client::admin_channel;
use std::fs;

#[derive(Debug, Args)]
pub(crate) struct Topics {
    #[command(subcommand)]
    command: TopicsCommands,
}

// Helper: fetch schema for a topic via Admin service and return JSON value
async fn fetch_schema_json(topic: &str) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let ch = admin_channel().await?;
    let mut client = TopicAdminClient::new(ch);
    let req = DescribeTopicRequest { name: topic.to_string() };
    let resp = client.describe_topic(req).await?;
    let resp = resp.into_inner();
    // Convert the prost `Schema` into JSON for printing
    let value = serde_json::json!({
        "name": resp.name,
        "type_schema": resp.type_schema,
        // schema_data is bytes; render as string when possible, else base64
        "schema_data": String::from_utf8(resp.schema_data.clone()).unwrap_or_else(|_| general_purpose::STANDARD.encode(resp.schema_data.clone())),
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
        #[arg(help = "Namespace name (e.g., default)")]
        namespace: String,
        #[arg(long, value_parser = ["json"], help = "Output format: json (default: plain)")]
        output: Option<String>,
    },
    #[command(about = "Create a topic (use --partitions for partitioned)", long_about = "Create a topic.\n\nExamples:\n  topics create /ns/my-topic\n  topics create /ns/my-topic --dispatch-strategy reliable --segment-size-mb 128\n  topics create /ns/my-topic --partitions 3\n  topics create my-topic --namespace ns --schema Json --schema-file schema.json\n")]
    Create {
        #[arg(help = "Topic name. Accepts '/ns/topic' or 'topic' (use --namespace for the latter)")]
        topic: String,
        #[arg(long, help = "Namespace (if topic provided without namespace)")]
        namespace: Option<String>,
        #[arg(long, help = "Number of partitions for a partitioned topic")]
        partitions: Option<usize>,
        #[arg(short, long, default_value = "String", help = "Schema type: String|Bytes|Int64|Json")]
        schema: String,
        #[arg(long, help = "Path to schema file (for Json schema payload)")]
        schema_file: Option<String>,
        #[arg(long, help = "Inline schema payload (JSON string for Json type)")]
        schema_data: Option<String>,
        #[arg(long, default_value = "non_reliable", help = "Dispatch strategy: non_reliable|reliable")]
        dispatch_strategy: String,
        #[arg(long)]
        segment_size_mb: Option<u64>,
        #[arg(long, value_parser = ["retain_until_ack", "retain_until_expire"])]
        retention_policy: Option<String>,
        #[arg(long)]
        retention_period_sec: Option<u64>,
    },
    #[command(about = "Delete an existing topic")]
    Delete { 
        #[arg(help = "Topic name. Accepts '/ns/topic' or 'topic' with --namespace")]
        topic: String,
        #[arg(long)]
        namespace: Option<String>,
    },
    #[command(about = "List the subscriptions of the specified topic")]
    Subscriptions {
        #[arg(help = "Topic name. Accepts '/ns/topic' or 'topic' with --namespace")]
        topic: String,
        #[arg(long)]
        namespace: Option<String>,
        #[arg(long, value_parser = ["json"], help = "Output format: json (default: plain)")]
        output: Option<String>,
    },
    #[command(about = "Describe a topic (schema and subscriptions)")]
    Describe {
        #[arg(help = "Topic name. Accepts '/ns/topic' or 'topic' with --namespace")]
        topic: String,
        #[arg(long)]
        namespace: Option<String>,
        #[arg(long, value_parser = ["json"], help = "Output format: json (default: pretty text)")]
        output: Option<String>,
    },
    #[command(about = "Delete a subscription from a topic")]
    Unsubscribe {
        #[arg(help = "Topic name. Accepts '/ns/topic' or 'topic' with --namespace")]
        topic: String,
        #[arg(long)]
        namespace: Option<String>,
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

        // Create topic (non-partitioned or partitioned)
        TopicsCommands::Create {
            topic,
            namespace,
            partitions,
            mut schema,
            schema_file,
            schema_data,
            dispatch_strategy,
            segment_size_mb,
            retention_policy,
            retention_period_sec,
        } => {
            let topic_path = normalize_topic(&topic, namespace.as_deref())?;

            if schema.is_empty() { schema = "String".into(); }

            let payload = if let Some(path) = schema_file {
                Some(String::from_utf8(fs::read(path)?)?)
            } else { schema_data };

            if let Some(parts) = partitions {
                let req = PartitionedTopicRequest {
                    base_name: topic_path,
                    partitions: parts as u32,
                    schema_type: schema.clone(),
                    schema_data: payload.unwrap_or_else(|| "{}".into()),
                    dispatch_strategy: Some(parse_dispatch_strategy(
                        &dispatch_strategy,
                        segment_size_mb,
                        retention_policy.as_deref(),
                        retention_period_sec,
                    )),
                };
                let response = client.create_partitioned_topic(req).await?;
                println!("Partitioned Topic Created: {:?}", response.into_inner().success);
            } else {
                let request = NewTopicRequest {
                    name: topic_path,
                    schema_type: schema.clone(),
                    schema_data: payload.unwrap_or_else(|| "{}".into()),
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
        }

        // Delete the topic
        TopicsCommands::Delete { topic, namespace } => {
            let name = normalize_topic(&topic, namespace.as_deref())?;
            let request = TopicRequest { name };
            let response = client.delete_topic(request).await?;
            println!("Topic Deleted: {:?}", response.into_inner().success);
        }

        // Get the list of subscriptions on the topic
        TopicsCommands::Subscriptions { topic, namespace, output } => {
            let name = normalize_topic(&topic, namespace.as_deref())?;
            let request = TopicRequest { name };
            let response = client.list_subscriptions(request).await?;
            let subs = response.into_inner().subscriptions;
            if matches!(output.as_deref(), Some("json")) {
                println!("{}", serde_json::to_string_pretty(&subs)?);
            } else {
                println!("Subscriptions: {:?}", subs);
            }
        }

        // Describe a topic: schema + subscriptions
        TopicsCommands::Describe { topic, namespace, output } => {
            let name = normalize_topic(&topic, namespace.as_deref())?;

            // subscriptions via admin
            let subs_req = TopicRequest { name: name.clone() };
            let subs_resp = client.list_subscriptions(subs_req).await?;
            let subscriptions = subs_resp.into_inner().subscriptions;

            // schema via admin DescribeTopic
            let schema_value = fetch_schema_json(&name).await?;

            if matches!(output.as_deref(), Some("json")) {
                let out = serde_json::json!({
                    "topic": name,
                    "schema": schema_value,
                    "subscriptions": subscriptions,
                });
                println!("{}", serde_json::to_string_pretty(&out)?);
            } else {
                println!("Topic: {}", name);
                // Pretty schema: detect JSON data and pretty print
                let schema_str = schema_value.get("schema_data").and_then(|v| v.as_str()).unwrap_or("").to_string();
                if schema_str.trim_start().starts_with('{') || schema_str.trim_start().starts_with('[') {
                    if let Ok(json) = serde_json::from_str::<serde_json::Value>(&schema_str) {
                        println!("Schema: {}", serde_json::to_string_pretty(&json)?);
                    } else {
                        println!("Schema: {}", schema_str);
                    }
                } else {
                    println!("Schema: {}", schema_str);
                }
                println!("Subscriptions: {:?}", subscriptions);
            }
        }

        // Delete a subscription from a topic
        TopicsCommands::Unsubscribe { topic, namespace, subscription } => {
            let name = normalize_topic(&topic, namespace.as_deref())?;
            let request = SubscriptionRequest { topic: name, subscription };
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

// Normalize topic path to "/namespace/topic". Accepts:
//  - "/ns/topic"
//  - "ns/topic"
//  - "topic" when --namespace is provided
fn normalize_topic(input: &str, namespace: Option<&str>) -> Result<String, Box<dyn std::error::Error>> {
    let s = input.trim();
    if s.starts_with('/') {
        if validate_topic_format(s) { return Ok(s.to_string()); }
        return Err("wrong topic format, should be /namespace/topic".into());
    }
    let parts: Vec<&str> = s.split('/').collect();
    match parts.len() {
        2 => Ok(format!("/{}", s)),
        1 => {
            let ns = namespace.ok_or("missing --namespace for topic without namespace")?;
            Ok(format!("/{}/{}", ns, s))
        }
        _ => Err("wrong topic format, should be /namespace/topic".into()),
    }
}

use crate::client::topic_admin_client;
use base64::{engine::general_purpose, Engine as _};
use clap::{Args, Subcommand};
use danube_core::admin_proto::{
    topic_admin_client::TopicAdminClient,
    DescribeTopicRequest,
    DispatchStrategy as AdminDispatchStrategy, NamespaceRequest, NewTopicRequest,
    PartitionedTopicRequest, SubscriptionRequest, TopicRequest, BrokerRequest,
};
use crate::client::admin_channel;
use std::fs;

#[derive(Debug, Args)]
#[command(
    about = "Manage topics in the Danube cluster",
    long_about = "Manage topics in the Danube cluster.\n\nCommon examples:\n  danube-admin-cli topics list default\n  danube-admin-cli topics create /default/mytopic\n  danube-admin-cli topics create /default/mytopic --partitions 3\n  danube-admin-cli topics describe /default/mytopic\n\nEnv:\n  DANUBE_ADMIN_ENDPOINT (default http://127.0.0.1:50051)\n  DANUBE_ADMIN_TLS, DANUBE_ADMIN_DOMAIN, DANUBE_ADMIN_CA, DANUBE_ADMIN_CERT, DANUBE_ADMIN_KEY",
    subcommand_required = true,
    arg_required_else_help = true
)]
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
        "broker_id": resp.broker_id,
        // schema_data is bytes; render as string when possible, else base64
        "schema_data": String::from_utf8(resp.schema_data.clone()).unwrap_or_else(|_| general_purpose::STANDARD.encode(resp.schema_data.clone())),
    });
    Ok(value)
}

// Convert CLI string into enum DispatchStrategy for admin API
fn parse_dispatch_strategy(input: &str) -> AdminDispatchStrategy {
    match input.to_ascii_lowercase().as_str() {
        "reliable" | "reliable_dispatch" | "reliable-dispatch" => AdminDispatchStrategy::Reliable,
        _ => AdminDispatchStrategy::NonReliable,
    }
}

#[derive(Debug, Subcommand)]
pub(crate) enum TopicsCommands {
    #[command(about = "List topics by namespace or by broker")]
    List {
        #[arg(long, required_unless_present = "broker", help = "Namespace name (e.g., default)")]
        namespace: Option<String>,
        #[arg(long, conflicts_with = "namespace", required_unless_present = "namespace", help = "Broker ID to filter topics")]
        broker: Option<String>,
        #[arg(long, value_parser = ["json"], help = "Output format: json (default: plain)")]
        output: Option<String>,
    },
    #[command(
        about = "Create a topic (use --partitions for partitioned)",
        long_about = "Create a topic.\n\nExamples:\n  topics create /ns/my-topic\n  topics create /ns/my-topic --dispatch-strategy reliable\n  topics create /ns/my-topic --partitions 3\n  topics create my-topic --namespace ns --schema Json --schema-file schema.json\n",
        after_help = "Examples:\n  danube-admin-cli topics create /default/mytopic\n  danube-admin-cli topics create /default/mytopic --dispatch-strategy reliable\n  danube-admin-cli topics create /default/mytopic --partitions 3\n  danube-admin-cli topics create mytopic --namespace default --schema Json --schema-file schema.json\n\nEnv:\n  DANUBE_ADMIN_ENDPOINT (default http://127.0.0.1:50051)\n  DANUBE_ADMIN_TLS, DANUBE_ADMIN_DOMAIN, DANUBE_ADMIN_CA, DANUBE_ADMIN_CERT, DANUBE_ADMIN_KEY",
        arg_required_else_help = true
    )]
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
    },
    #[command(about = "Delete an existing topic", after_help = "Example:\n  danube-admin-cli topics delete /default/mytopic\n  danube-admin-cli topics delete mytopic --namespace default", arg_required_else_help = true)]
    Delete { 
        #[arg(help = "Topic name. Accepts '/ns/topic' or 'topic' with --namespace")]
        topic: String,
        #[arg(long)]
        namespace: Option<String>,
    },
    #[command(about = "List the subscriptions of the specified topic", after_help = "Example:\n  danube-admin-cli topics subscriptions /default/mytopic --output json", arg_required_else_help = true)]
    Subscriptions {
        #[arg(help = "Topic name. Accepts '/ns/topic' or 'topic' with --namespace")]
        topic: String,
        #[arg(long)]
        namespace: Option<String>,
        #[arg(long, value_parser = ["json"], help = "Output format: json (default: plain)")]
        output: Option<String>,
    },
    #[command(about = "Describe a topic (schema and subscriptions)", after_help = "Example:\n  danube-admin-cli topics describe /default/mytopic --output json", arg_required_else_help = true)]
    Describe {
        #[arg(help = "Topic name. Accepts '/ns/topic' or 'topic' with --namespace")]
        topic: String,
        #[arg(long)]
        namespace: Option<String>,
        #[arg(long, value_parser = ["json"], help = "Output format: json (default: pretty text)")]
        output: Option<String>,
    },
    #[command(about = "Delete a subscription from a topic", after_help = "Example:\n  danube-admin-cli topics unsubscribe --subscription sub1 /default/mytopic", arg_required_else_help = true)]
    Unsubscribe {
        #[arg(help = "Topic name. Accepts '/ns/topic' or 'topic' with --namespace")]
        topic: String,
        #[arg(long)]
        namespace: Option<String>,
        #[arg(short, long)]
        subscription: String,
    },
    #[command(about = "Unload a topic from its current broker to be reassigned to a different broker", after_help = "Example:\n  danube-admin-cli topics unload /default/mytopic\n  danube-admin-cli topics unload mytopic --namespace default", arg_required_else_help = true)]
    Unload {
        #[arg(help = "Topic name. Accepts '/ns/topic' or 'topic' with --namespace")]
        topic: String,
        #[arg(long)]
        namespace: Option<String>,
    },
}

#[allow(unreachable_code)]
pub async fn handle_command(topics: Topics) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = topic_admin_client().await?;

    match topics.command {
        // List topics by either namespace or broker
        TopicsCommands::List { namespace, broker, output } => {
            if let Some(ns) = namespace {
                let request = NamespaceRequest { name: ns };
                let response = client.list_namespace_topics(request).await?;
                let items = response.into_inner().topics;
                if matches!(output.as_deref(), Some("json")) {
                    let json_items: Vec<serde_json::Value> = items
                        .iter()
                        .map(|it| serde_json::json!({
                            "name": it.name,
                            "broker_id": it.broker_id,
                        }))
                        .collect();
                    println!("{}", serde_json::to_string_pretty(&json_items)?);
                } else {
                    for item in items {
                        if item.broker_id.is_empty() {
                            println!("Topic: {}", item.name);
                        } else {
                            println!("Topic: {} (broker_id: {})", item.name, item.broker_id);
                        }
                    }
                }
            } else if let Some(bid) = broker {
                let request = BrokerRequest { broker_id: bid };
                let response = client.list_broker_topics(request).await?;
                let items = response.into_inner().topics;
                if matches!(output.as_deref(), Some("json")) {
                    let json_items: Vec<serde_json::Value> = items
                        .iter()
                        .map(|it| serde_json::json!({
                            "name": it.name,
                            "broker_id": it.broker_id,
                        }))
                        .collect();
                    println!("{}", serde_json::to_string_pretty(&json_items)?);
                } else {
                    for item in items {
                        if item.broker_id.is_empty() {
                            println!("Topic: {}", item.name);
                        } else {
                            println!("Topic: {} (broker_id: {})", item.name, item.broker_id);
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
            mut schema,
            schema_file,
            schema_data,
            dispatch_strategy,
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
                    dispatch_strategy: parse_dispatch_strategy(&dispatch_strategy) as i32,
                };
                let response = client.create_partitioned_topic(req).await?;
                println!("Partitioned Topic Created: {:?}", response.into_inner().success);
            } else {
                let request = NewTopicRequest {
                    name: topic_path,
                    schema_type: schema.clone(),
                    schema_data: payload.unwrap_or_else(|| "{}".into()),
                    dispatch_strategy: parse_dispatch_strategy(&dispatch_strategy) as i32,
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
            let broker_id = schema_value.get("broker_id").and_then(|v| v.as_str()).unwrap_or("").to_string();

            if matches!(output.as_deref(), Some("json")) {
                let out = serde_json::json!({
                    "topic": name,
                    "broker_id": broker_id,
                    "schema": schema_value,
                    "subscriptions": subscriptions,
                });
                println!("{}", serde_json::to_string_pretty(&out)?);
            } else {
                println!("Topic: {}", name);
                if !broker_id.is_empty() { println!("Broker ID: {}", broker_id); }
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
        // Unload a topic
        TopicsCommands::Unload { topic, namespace } => {
            let name = normalize_topic(&topic, namespace.as_deref())?;
            let request = TopicRequest { name };
            let response = client.unload_topic(request).await?;
            println!("Topic Unloaded: {:?}", response.into_inner().success);
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

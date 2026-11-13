use anyhow::{Context, Result};
use clap::{Args, ValueEnum};
use danube_client::{SchemaInfo, SubType};
use danube_core::message::StreamMessage;
use serde_json::{from_slice, json, Value};
use std::collections::HashMap;
use std::str::from_utf8;

use super::{build_danube_client, DataAuthArgs};

#[derive(Debug, Args)]
#[command(about = "Consume messages from a Danube topic")]
pub struct Consume {
    /// Service URL of the Danube broker (e.g. http://127.0.0.1:6650)
    #[arg(
        short = 's',
        long,
        env = "DANUBE_SERVICE_ADDR",
        default_value = "http://127.0.0.1:6650"
    )]
    pub service_addr: String,

    /// Topic name (e.g. /default/my-topic)
    #[arg(short = 't', long, default_value = "/default/test_topic")]
    pub topic: String,

    /// Consumer name
    #[arg(short = 'n', long, default_value = "danube_consumer")]
    pub consumer_name: String,

    /// Subscription name
    #[arg(long = "subscription", default_value = "danube_sub")]
    pub subscription: String,

    /// Subscription type (exclusive, shared, fail-over, key-shared)
    #[arg(long, value_enum, default_value = "shared")]
    pub subscription_type: SubTypeArg,

    /// Key filter patterns (glob syntax, repeatable; Key-Shared only)
    #[arg(long = "key-filter")]
    pub key_filters: Vec<String>,

    /// Maximum number of messages to consume before exiting (0 = stream continuously)
    #[arg(short = 'l', long, default_value_t = 0)]
    pub limit: u64,

    /// Output format (text or jsonl)
    #[arg(short = 'o', long, value_enum, default_value = "text")]
    pub output: ConsumeOutput,

    #[command(flatten)]
    pub auth_args: DataAuthArgs,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum SubTypeArg {
    Exclusive,
    Shared,
    FailOver,
    KeyShared,
}

impl From<SubTypeArg> for SubType {
    fn from(arg: SubTypeArg) -> Self {
        match arg {
            SubTypeArg::Exclusive => SubType::Exclusive,
            SubTypeArg::Shared => SubType::Shared,
            SubTypeArg::FailOver => SubType::FailOver,
            SubTypeArg::KeyShared => SubType::KeyShared,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum ConsumeOutput {
    Text,
    Jsonl,
}

pub async fn handle(consume: Consume) -> Result<()> {
    let client = build_danube_client(&consume.service_addr, &consume.auth_args).await?;
    let schema_client = client.schema();

    let mut builder = client
        .new_consumer()
        .with_topic(consume.topic.clone())
        .with_consumer_name(consume.consumer_name.clone())
        .with_subscription(consume.subscription.clone())
        .with_subscription_type(consume.subscription_type.into());

    if !consume.key_filters.is_empty() {
        builder = builder.with_key_filters(consume.key_filters.clone());
    }

    let mut consumer = builder.build()?;
    consumer
        .subscribe()
        .await
        .context("Failed to subscribe")?;

    let mut message_stream = consumer
        .receive()
        .await
        .context("Failed to start receiving messages")?;

    let mut schema_cache: HashMap<u64, (SchemaInfo, Option<jsonschema::Validator>)> = HashMap::new();
    let mut received_count: u64 = 0;

    while let Some(stream_message) = message_stream.recv().await {
        if let Some(schema_id) = stream_message.schema_id {
            if !schema_cache.contains_key(&schema_id) {
                let schema_info = schema_client
                    .get_schema_by_id(schema_id)
                    .await
                    .context("Failed to fetch schema from registry")?;
                let validator = if schema_info.schema_type == "json_schema" {
                    schema_info
                        .schema_definition_as_string()
                        .and_then(|s| serde_json::from_str::<Value>(&s).ok())
                        .and_then(|v| jsonschema::validator_for(&v).ok())
                } else {
                    None
                };
                schema_cache.insert(schema_id, (schema_info, validator));
            }
        }

        let schema_entry = stream_message
            .schema_id
            .and_then(|id| schema_cache.get(&id));
        render_message(
            &stream_message,
            schema_entry.map(|(info, _)| info),
            schema_entry.and_then(|(_, v)| v.as_ref()),
            consume.output,
        )?;

        if let Err(e) = consumer.ack(&stream_message).await {
            eprintln!("Failed to acknowledge message: {:?}", e);
        }

        received_count += 1;
        if consume.limit > 0 && received_count >= consume.limit {
            break;
        }
    }

    Ok(())
}

fn render_message(
    msg: &StreamMessage,
    schema_info: Option<&SchemaInfo>,
    validator: Option<&jsonschema::Validator>,
    output: ConsumeOutput,
) -> Result<()> {
    let payload_str = match schema_info {
        Some(info) if info.schema_type == "json_schema" || info.schema_type == "avro" => {
            if let Ok(json_val) = from_slice::<Value>(&msg.payload) {
                if let Some(val) = validator {
                    if !val.is_valid(&json_val) {
                        eprintln!("Schema validation warning for message {}", msg.msg_id);
                    }
                }
                if output == ConsumeOutput::Jsonl {
                    serde_json::to_string(&json_val).unwrap_or_default()
                } else {
                    serde_json::to_string_pretty(&json_val).unwrap_or_default()
                }
            } else {
                String::from_utf8_lossy(&msg.payload).into_owned()
            }
        }
        _ => match from_utf8(&msg.payload) {
            Ok(text) => text.to_string(),
            Err(_) => format!("[Binary {} bytes]", msg.payload.len()),
        },
    };

    match output {
        ConsumeOutput::Jsonl => {
            let line = json!({
                "msg_id": msg.msg_id.to_string(),
                "topic": msg.msg_id.topic_name,
                "offset": msg.msg_id.topic_offset,
                "publish_time": msg.publish_time,
                "producer_name": msg.producer_name,
                "routing_key": msg.routing_key,
                "attributes": msg.attributes,
                "payload": payload_str,
            });
            println!("{}", line);
        }
        ConsumeOutput::Text => {
            println!(
                "Received message (offset={}, size={}B, topic={}): {}",
                msg.msg_id.topic_offset,
                msg.payload.len(),
                msg.msg_id.topic_name,
                payload_str
            );
            if let Some(ref rk) = msg.routing_key {
                println!("  routing_key: {}", rk);
            }
            if !msg.attributes.is_empty() {
                println!("  attributes: {:?}", msg.attributes);
            }
        }
    }

    Ok(())
}


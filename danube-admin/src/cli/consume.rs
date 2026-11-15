use anyhow::{Context, Result};
use clap::{Args, ValueEnum};
use danube_client::{SchemaInfo, SubType};
use danube_core::message::StreamMessage;
use serde_json::{from_slice, json, Value};
use std::collections::HashMap;
use std::str::from_utf8;

use super::ClientArgs;

#[derive(Debug, Args)]
#[command(
    about = "Subscribe and stream messages from a topic",
    long_about = "Subscribe to a topic and print messages as they arrive.\n\n\
Runs until interrupted (Ctrl-C) unless --limit is set. Messages with a registered \
schema are decoded and validated before printing.",
    after_help = "Examples:
  danube consume /default/events
  danube consume /default/events -s analytics --type exclusive
  danube consume /default/orders -s workers --type key-shared --key-filter 'customer-*'
  danube consume /default/events --limit 10 -o jsonl"
)]
pub struct Consume {
    /// Topic to consume from, e.g. /default/events
    #[arg(value_name = "TOPIC")]
    pub topic: String,

    /// Subscription name
    #[arg(short = 's', long, default_value = "danube-sub", help_heading = "Subscription")]
    pub subscription: String,

    /// Subscription type
    #[arg(short = 't', long = "type", value_enum, default_value = "shared", help_heading = "Subscription")]
    pub subscription_type: SubTypeArg,

    /// Key filter glob (repeatable; key-shared only)
    #[arg(long = "key-filter", value_name = "GLOB", help_heading = "Subscription")]
    pub key_filters: Vec<String>,

    /// Consumer name
    #[arg(short = 'n', long, default_value = "danube-consumer", help_heading = "Subscription")]
    pub consumer_name: String,

    /// Stop after this many messages (0 = stream until interrupted)
    #[arg(short = 'l', long, default_value_t = 0, help_heading = "Output")]
    pub limit: u64,

    /// Output format
    #[arg(short = 'o', long, value_enum, default_value = "text", help_heading = "Output")]
    pub output: ConsumeOutput,

    #[command(flatten)]
    pub client: ClientArgs,
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
    let client = consume.client.resolve()?.danube_client().await?;
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


use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use danube_client::{DanubeClient, SchemaRegistryClient, SubType};
use danube_core::message::MessageID;
use serde_json::{from_slice, Value};
use std::{collections::HashMap, str::from_utf8};

// Print the message to the console only if the message is not too large
const LARGE_MESSAGE_THRESHOLD: usize = 1024; // 1KB threshold

#[derive(Debug, Parser)]
#[command(after_help = EXAMPLES_TEXT)]
pub struct Consume {
    #[arg(
        long,
        short = 's',
        help = "The service URL for the Danube broker. Example: http://127.0.0.1:6650"
    )]
    pub service_addr: String,

    #[arg(
        long,
        short = 't',
        default_value = "/default/test_topic",
        help = "The topic to consume messages from. Default: /default/test_topic"
    )]
    pub topic: String,

    #[arg(
        long,
        short = 'n',
        default_value = "consumer_pubsub",
        help = "The consumer name"
    )]
    pub consumer: String,

    #[arg(
        long,
        short = 'm',
        help = "The subscription name. Default: consumer_pubsub"
    )]
    pub subscription: String,

    #[arg(long, value_enum, help = "The subscription type. Default: Shared")]
    pub sub_type: Option<SubTypeArg>,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq)]
pub enum SubTypeArg {
    Exclusive,
    Shared,
    FailOver,
}

const EXAMPLES_TEXT: &str = r#"
EXAMPLES:
    # Basic: Receive messages from a shared subscription (default)
    danube-cli consume --service-addr http://localhost:6650 \
        --subscription my_shared_subscription

    # Consume with automatic schema validation (if topic has schema)
    danube-cli consume -s http://localhost:6650 \
        -t /default/user-events \
        -m my_subscription

    # Consume from exclusive subscription
    danube-cli consume -s http://localhost:6650 \
        -m my_exclusive \
        --sub-type exclusive

    # Consume with custom consumer name and topic
    danube-cli consume -s http://localhost:6650 \
        -n my_consumer \
        -t /default/orders \
        -m my_subscription \
        --sub-type shared

    # Consume from failover subscription
    danube-cli consume -s http://localhost:6650 \
        -m my_failover \
        --sub-type fail-over

NOTE:
    - Consumer automatically detects and validates messages against registered schemas
    - JSON messages are validated and pretty-printed
    - Topics without schemas consume raw bytes
    - Press Ctrl+C to stop consuming
"#;

pub async fn handle_consume(consume: Consume) -> Result<()> {
    let sub_type = validate_subscription_type(consume.sub_type)?;

    let client = DanubeClient::builder()
        .service_url(&consume.service_addr)
        .build()
        .await?;

    let mut consumer = client
        .new_consumer()
        .with_topic(consume.topic.clone())
        .with_consumer_name(consume.consumer)
        .with_subscription(consume.subscription)
        .with_subscription_type(sub_type)
        .build();

    // Retrieve schema from registry if topic has one
    println!("🔍 Checking for schema associated with topic...");

    let schema_info = match get_topic_schema_info(&client, &consume.topic).await {
        Ok(Some(info)) => {
            println!("✅ Topic has schema:");
            println!("   Subject: {}", info.subject);
            println!("   Version: {}", info.version);
            println!("   Type: {}", info.schema_type);
            Some(info)
        }
        Ok(None) => {
            println!("ℹ️  Topic has no schema - consuming raw bytes");
            None
        }
        Err(e) => {
            println!("⚠️  Warning: Could not retrieve schema info: {}", e);
            println!("   Continuing without schema validation...");
            None
        }
    };

    // Compile JSON schema validator if applicable
    let schema_validator = if let Some(ref info) = schema_info {
        if info.schema_type == "json_schema" {
            let schema_value: Value = serde_json::from_slice(&info.schema_definition)
                .context("Failed to parse JSON schema")?;
            Some(
                jsonschema::validator_for(&schema_value)
                    .context("Failed to compile JSON schema")?,
            )
        } else {
            None
        }
    } else {
        None
    };

    consumer.subscribe().await?;
    let mut message_stream = consumer.receive().await?;

    let mut state = ConsumerState {
        last_topic_offset: 0,
        total_received_bytes: 0,
    };

    while let Some(stream_message) = message_stream.recv().await {
        let payload = stream_message.payload.clone();
        let attr = stream_message.attributes.clone();

        if let Err(e) = process_message(
            &payload,
            attr,
            schema_info.as_ref(),
            &schema_validator,
            &stream_message.msg_id,
            &mut state,
        ) {
            eprintln!("Error processing message: {:?}", e);
            continue;
        }

        if let Err(e) = consumer.ack(&stream_message).await {
            eprintln!("Failed to acknowledge message: {:?}", e);
        }
    }

    Ok(())
}

struct SchemaInfo {
    subject: String,
    version: u32,
    schema_type: String,
    schema_definition: Vec<u8>,
}

async fn get_topic_schema_info(client: &DanubeClient, topic: &str) -> Result<Option<SchemaInfo>> {
    // Note: In a real implementation, you'd call an admin API to get topic metadata
    // For now, we'll try to get the schema directly by attempting common subject patterns

    let mut schema_client = SchemaRegistryClient::new(client).await?;

    // Try to derive subject name from topic (e.g., /default/events -> default-events)
    let subject = topic.trim_start_matches('/').replace('/', "-");

    match schema_client.get_latest_schema(&subject).await {
        Ok(schema_response) => Ok(Some(SchemaInfo {
            subject: schema_response.subject,
            version: schema_response.version,
            schema_type: schema_response.schema_type,
            schema_definition: schema_response.schema_definition,
        })),
        Err(_) => Ok(None), // Topic has no schema
    }
}

fn process_message(
    payload: &[u8],
    attr: HashMap<String, String>,
    schema_info: Option<&SchemaInfo>,
    schema_validator: &Option<jsonschema::Validator>,
    msg_id: &MessageID,
    state: &mut ConsumerState,
) -> Result<()> {
    match schema_info {
        Some(info) if info.schema_type == "json_schema" || info.schema_type == "avro" => {
            // Parse as JSON
            if payload.is_empty() {
                return Err(anyhow::anyhow!("Received empty JSON payload").into());
            }

            let json_value: Value = from_slice(payload).with_context(|| {
                format!(
                    "Failed to parse JSON message: {}",
                    String::from_utf8_lossy(payload)
                )
            })?;

            // Validate against schema if we have a validator
            if let Some(validator) = schema_validator {
                if !validator.is_valid(&json_value) {
                    let errors: Vec<_> = validator.iter_errors(&json_value).collect();
                    return Err(anyhow::anyhow!("JSON validation failed: {:?}", errors));
                }
            }

            let json_str =
                serde_json::to_string_pretty(&json_value).context("Failed to format JSON")?;
            print_to_console(&json_str, attr, msg_id, payload.len(), state);
        }
        Some(info) if info.schema_type == "protobuf" => {
            // For protobuf, just show as bytes
            let decoded_message = format!("[Protobuf binary data - {} bytes]", payload.len());
            print_to_console(&decoded_message, attr, msg_id, payload.len(), state);
        }
        _ => {
            // No schema or unknown type - try to decode as UTF-8 string
            match from_utf8(payload) {
                Ok(decoded_message) => {
                    print_to_console(decoded_message, attr, msg_id, payload.len(), state);
                }
                Err(_) => {
                    let decoded_message = format!("[Binary data - {} bytes]", payload.len());
                    print_to_console(&decoded_message, attr, msg_id, payload.len(), state);
                }
            }
        }
    }
    Ok(())
}

fn validate_subscription_type(subscription_type: Option<SubTypeArg>) -> Result<SubType> {
    let sub_type = if let Some(subcr_type) = subscription_type {
        if SubTypeArg::value_variants().contains(&subcr_type) {
            subcr_type.into()
        } else {
            return Err(anyhow::anyhow!(
                "Unsupported subscription type: '{:?}'. Supported values are: {:?}",
                subcr_type,
                SubTypeArg::value_variants()
            )
            .into());
        }
    } else {
        SubType::Shared
    };

    Ok(sub_type)
}

impl From<SubTypeArg> for SubType {
    fn from(arg: SubTypeArg) -> Self {
        match arg {
            SubTypeArg::Exclusive => SubType::Exclusive,
            SubTypeArg::Shared => SubType::Shared,
            SubTypeArg::FailOver => SubType::FailOver,
        }
    }
}

fn print_attr(attributes: &HashMap<String, String>) -> String {
    let formatted: Vec<String> = attributes
        .iter()
        .map(|(key, value)| format!("{}={}", key, value))
        .collect();

    let result = formatted.join(", ");
    result
}

struct ConsumerState {
    last_topic_offset: u64,
    total_received_bytes: usize,
}

fn print_to_console(
    message: &str,
    attributes: HashMap<String, String>,
    msg_id: &MessageID,
    payload_size: usize,
    state: &mut ConsumerState,
) {
    let is_reliable = msg_id.topic_offset > state.last_topic_offset;

    state.total_received_bytes += payload_size;

    if is_reliable {
        let message_preview = if payload_size > LARGE_MESSAGE_THRESHOLD {
            "[binary data]".to_string()
        } else {
            format!("\"{}\"", message)
        };

        println!(
            "Received reliable message: {} \nTopic offset: {}, Size: {} bytes, Total received: {} bytes\nProducer: {}, Topic: {}",
            message_preview,
            msg_id.topic_offset,
            payload_size,
            state.total_received_bytes,
            msg_id.producer_id,
            msg_id.topic_name
        );
    } else {
        println!(
            "Received message: {}\nSize: {} bytes, Total received: {} bytes",
            message, payload_size, state.total_received_bytes,
        );
    }

    if !attributes.is_empty() {
        println!("Attributes: {}", print_attr(&attributes));
    }

    state.last_topic_offset = msg_id.topic_offset;
}

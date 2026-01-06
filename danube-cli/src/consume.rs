use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use danube_client::{DanubeClient, SchemaInfo, SchemaRegistryClient, SubType};
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

    # Consume with automatic schema validation (if messages have schema_id)
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
    - Consumer automatically fetches schema using schema_id from message metadata
    - Falls back to deriving subject from topic name if no schema_id present
    - JSON messages are validated and pretty-printed if schema is found
    - Messages without schemas consume raw bytes
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

    // Initialize schema registry client for runtime schema fetching
    let mut schema_client = SchemaRegistryClient::new(&client).await
        .context("Failed to create schema registry client")?;

    println!("ℹ️  Consumer will fetch schemas dynamically based on message metadata");
    
    // Cache for schema validators
    let mut schema_cache: HashMap<u64, (SchemaInfo, Option<jsonschema::Validator>)> = HashMap::new();

    consumer.subscribe().await?;
    let mut message_stream = consumer.receive().await?;

    let mut state = ConsumerState {
        last_topic_offset: 0,
        total_received_bytes: 0,
    };

    while let Some(stream_message) = message_stream.recv().await {
        let payload = stream_message.payload.clone();
        let attr = stream_message.attributes.clone();

        // Fetch schema if message has schema_id
        let (schema_info, validator) = if let Some(schema_id) = stream_message.schema_id {
            // Check cache first
            if !schema_cache.contains_key(&schema_id) {
                match fetch_and_cache_schema(&mut schema_client, schema_id, &mut schema_cache).await {
                    Ok(_) => {},
                    Err(e) => {
                        eprintln!("⚠️  Failed to fetch schema {}: {}", schema_id, e);
                    }
                }
            }
            schema_cache.get(&schema_id)
                .map(|(info, val)| (Some(info), val.as_ref()))
                .unwrap_or((None, None))
        } else {
            (None, None)
        };

        if let Err(e) = process_message(
            &payload,
            attr,
            schema_info,
            &validator,
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

/// Fetch schema by ID and cache it with validator
async fn fetch_and_cache_schema(
    schema_client: &mut SchemaRegistryClient,
    schema_id: u64,
    cache: &mut HashMap<u64, (SchemaInfo, Option<jsonschema::Validator>)>,
) -> Result<()> {
    let schema_info: SchemaInfo = schema_client.get_schema_by_id(schema_id).await
        .context("Failed to fetch schema from registry")?;

    println!("✅ Loaded schema: {} v{} (ID: {}, Type: {})", 
        schema_info.subject, schema_info.version, schema_info.schema_id, schema_info.schema_type);

    // Compile JSON schema validator if applicable
    let validator = if schema_info.schema_type == "json_schema" {
        if let Some(schema_str) = schema_info.schema_definition_as_string() {
            match serde_json::from_str::<Value>(&schema_str) {
                Ok(schema_value) => {
                    match jsonschema::validator_for(&schema_value) {
                        Ok(v) => Some(v),
                        Err(e) => {
                            eprintln!("⚠️  Warning: Failed to compile JSON schema validator: {}", e);
                            None
                        }
                    }
                }
                Err(e) => {
                    eprintln!("⚠️  Warning: Failed to parse JSON schema: {}", e);
                    None
                }
            }
        } else {
            None
        }
    } else {
        None
    };

    cache.insert(schema_id, (schema_info, validator));
    Ok(())
}

fn process_message(
    payload: &[u8],
    attr: HashMap<String, String>,
    schema_info: Option<&SchemaInfo>,
    schema_validator: &Option<&jsonschema::Validator>,
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
                    eprintln!("⚠️  Schema validation failed: {:?}", errors);
                    // Continue processing but log the validation error
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

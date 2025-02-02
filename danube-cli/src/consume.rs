use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use danube_client::{DanubeClient, SchemaType, SubType};
use serde_json::{from_slice, Value};
use std::{collections::HashMap, str::from_utf8};

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
    # Receive messages from a shared subscription (default)
    danube-cli consume --service-addr http://localhost:6650 --subscription my_shared_subscription

    # Receive messages from an exclusive subscription
    danube-cli consume -s http://localhost:6650 -m my_exclusive --sub-type exclusive

    # Receive messages for a custom consumer name
    danube-cli consume -s http://localhost:6650 -n my_consumer -m my_subscription

    # Receive messages from a specific topic
    danube-cli consume -s http://localhost:6650 -t my_topic -m my_subscription
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

    // Retrieve schema type and schema definition
    let schema = client.get_schema(consume.topic).await?;

    let schema_validator = match schema.type_schema.clone() {
        SchemaType::Json(schema_str) => {
            if schema_str.is_empty() {
                println!("Warning: Empty JSON schema received, proceeding without validation");
                None
            } else {
                let schema_value: Value =
                    serde_json::from_str(&schema_str).context("Failed to parse JSON schema")?;
                Some(
                    jsonschema::validator_for(&schema_value)
                        .context("Failed to compile JSON schema")?,
                )
            }
        }
        _ => None,
    };

    consumer.subscribe().await?;
    let mut message_stream = consumer.receive().await?;

    while let Some(stream_message) = message_stream.recv().await {
        let payload = stream_message.payload.clone();
        let attr = stream_message.attributes.clone();

        // Process message based on the schema type
        if let Err(e) = process_message(&payload, attr, &schema.type_schema, &schema_validator) {
            eprintln!("Error processing message: {:?}", e);
            continue;
        }

        if let Err(e) = consumer.ack(&stream_message).await {
            eprintln!("Failed to acknowledge message: {:?}", e);
        }
    }

    Ok(())
}

fn process_message(
    payload: &[u8],
    attr: HashMap<String, String>,
    schema_type: &SchemaType,
    schema_validator: &Option<jsonschema::Validator>,
) -> Result<()> {
    match schema_type {
        SchemaType::Bytes => {
            let decoded_message = from_utf8(payload)?;
            print_to_console(decoded_message, attr);
        }
        SchemaType::String => {
            let decoded_message = from_utf8(payload)?;
            print_to_console(decoded_message, attr);
        }
        SchemaType::Int64 => {
            let message = std::str::from_utf8(payload)
                .context("Invalid UTF-8 sequence")?
                .parse::<i64>()
                .context("Failed to parse Int64")?;
            print_to_console(&message.to_string(), attr);
        }
        SchemaType::Json(_) => {
            if payload.is_empty() {
                return Err(anyhow::anyhow!("Received empty JSON payload").into());
            }

            let json_value: Value = from_slice(payload).with_context(|| {
                format!(
                    "Failed to parse JSON message: {}",
                    String::from_utf8_lossy(payload)
                )
            })?;

            if let Some(validator) = schema_validator {
                if !validator.is_valid(&json_value) {
                    let errors: Vec<_> = validator.iter_errors(&json_value).collect();
                    return Err(anyhow::anyhow!("JSON validation failed: {:?}", errors));
                }
            }

            let json_str =
                serde_json::to_string_pretty(&json_value).context("Failed to format JSON")?;
            print_to_console(&json_str, attr);
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

fn print_to_console(message: &str, attributes: HashMap<String, String>) {
    if attributes.is_empty() {
        println!("Received bytes message with payload: {}", message);
    } else {
        println!(
            "Received bytes message with payload: {}, with attributes: {}",
            message,
            print_attr(&attributes)
        );
    }
}

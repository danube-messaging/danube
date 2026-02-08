use anyhow::{Context, Result};
use clap::{Args, Parser, ValueEnum};
use danube_client::{DanubeClient, SchemaType};
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::time::{sleep, Duration};

use crate::utils::schema_helpers;

#[derive(Debug, Parser)]
#[command(after_help = EXAMPLES_TEXT)]
pub struct Produce {
    #[command(flatten)]
    pub basic_args: BasicArgs,

    #[command(flatten)]
    pub extended_args: ExtendedArgs,

    #[command(flatten)]
    pub reliable_args: ReliableArgs,
}

#[derive(Debug, Args)]
#[group(required = true)]
pub struct BasicArgs {
    #[arg(
        long,
        short = 's',
        help = "The service URL for the Danube broker. Example: http://127.0.0.1:6650"
    )]
    pub service_addr: String,

    #[arg(
        long,
        short = 'n',
        default_value = "test_producer",
        help = "The producer name"
    )]
    pub producer_name: String,

    #[arg(
        long,
        short = 't',
        default_value = "/default/test_topic",
        help = "The topic to produce messages to."
    )]
    pub topic: String,

    #[arg(
        long,
        short = 'm',
        required_unless_present = "file",
        help = "The message to send (required unless --file is provided)"
    )]
    pub message: Option<String>,

    #[arg(
        long,
        short = 'f',
        help = "Binary file path to send. Takes precedence over --message when specified."
    )]
    pub file: Option<String>,
}

#[derive(Debug, Args)]
pub struct ExtendedArgs {
    #[arg(
        long,
        help = "Schema subject name (references a schema in the registry)"
    )]
    pub schema_subject: Option<String>,

    #[arg(
        long,
        help = "Pin to specific schema version (requires --schema-subject)"
    )]
    pub schema_version: Option<u32>,

    #[arg(
        long,
        help = "Use minimum schema version or newer (requires --schema-subject)"
    )]
    pub schema_min_version: Option<u32>,

    #[arg(long, help = "Auto-register schema from file (requires --schema-type)")]
    pub schema_file: Option<PathBuf>,

    #[arg(long, value_enum, help = "Schema type (required with --schema-file)")]
    pub schema_type: Option<SchemaTypeArg>,

    #[arg(
        long,
        short = 'a',
        value_parser = parse_attributes,
        help = "Attributes in the form 'parameter:value'. Example: 'key1:value1,key2:value2'"
    )]
    pub attributes: Option<HashMap<String, String>>,

    #[arg(long, short = 'p', help = "The number of partitions for the topic.")]
    pub partitions: Option<u32>,

    #[arg(
        long,
        short = 'c',
        default_value = "1",
        help = "The number of messages to produce. Default: 1"
    )]
    pub count: u32,

    #[arg(
        long,
        short = 'i',
        default_value = "500",
        help = "Interval between messages in milliseconds. Default: 500. Minimum: 100."
    )]
    pub interval: u64,
}

#[derive(Debug, Args)]
#[group(required = false, multiple = true)]
pub struct ReliableArgs {
    #[arg(long, help = "Enable reliable message delivery with in-memory storage")]
    pub reliable: bool,
    // Legacy reliable tuning flags removed in Phase 1; broker uses defaults
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq)]
pub enum SchemaTypeArg {
    #[value(name = "json_schema")]
    JsonSchema,
    Avro,
    Protobuf,
}

impl From<SchemaTypeArg> for SchemaType {
    fn from(arg: SchemaTypeArg) -> Self {
        match arg {
            SchemaTypeArg::JsonSchema => SchemaType::JsonSchema,
            SchemaTypeArg::Avro => SchemaType::Avro,
            SchemaTypeArg::Protobuf => SchemaType::Protobuf,
        }
    }
}

const EXAMPLES_TEXT: &str = r#"
EXAMPLES:
    # Basic message production (no schema)
    danube-cli produce -s http://localhost:6650 \
        -t /default/test-topic \
        -m "Hello Danube" \
        -c 100

    # Produce with pre-registered schema subject (latest version)
    danube-cli produce -s http://localhost:6650 \
        -t /default/user-events \
        --schema-subject "user-events" \
        -m '{"user_id":"123","action":"login"}' \
        -c 10

    # Produce with pinned schema version
    danube-cli produce -s http://localhost:6650 \
        -t /default/user-events \
        --schema-subject "user-events" \
        --schema-version 2 \
        -m '{"user_id":"123","action":"login"}' \
        -c 10

    # Produce with minimum schema version
    danube-cli produce -s http://localhost:6650 \
        -t /default/user-events \
        --schema-subject "user-events" \
        --schema-min-version 2 \
        -m '{"user_id":"123","action":"login"}' \
        -c 10

    # Auto-register schema from file
    danube-cli produce -s http://localhost:6650 \
        -t /default/orders \
        --schema-file ./schemas/order.json \
        --schema-type json_schema \
        -m '{"order_id":"ord_123","amount":99.99}' \
        -c 10

    # Reliable message delivery with schema
    danube-cli produce -s http://localhost:6650 \
        -t /default/critical-orders \
        --schema-subject "orders" \
        --reliable \
        -m '{"order_id":"ord_456","amount":199.99}' \
        -c 100

    # Produce to partitioned topic
    danube-cli produce -s http://localhost:6650 \
        -t /default/events \
        --partitions 3 \
        -m "Partitioned message" \
        -c 50

    # Produce with message attributes
    danube-cli produce -s http://localhost:6650 \
        -t /default/notifications \
        -m "Alert message" \
        -a "priority:high,region:us-west" \
        -c 10

    # Produce binary file with schema
    danube-cli produce -s http://localhost:6650 \
        -t /default/data-stream \
        --schema-subject "binary-data" \
        -f ./data.blob \
        -c 1

    # Produce with custom producer name and interval
    danube-cli produce -s http://localhost:6650 \
        -t /default/metrics \
        -n metrics-producer-1 \
        -m '{"cpu":45.2,"memory":78.5}' \
        -i 1000 \
        -c 100

    # Avro schema with auto-registration
    danube-cli produce -s http://localhost:6650 \
        -t /default/products \
        --schema-file ./schemas/product.avsc \
        --schema-type avro \
        -m '{"id":"prod_123","name":"Widget","price":29.99}'

NOTES:
    - Use --schema-subject to reference pre-registered schemas (latest version)
    - Use --schema-version to pin to a specific schema version
    - Use --schema-min-version to use a minimum version or newer
    - Use --schema-file + --schema-type to auto-register new schemas
    - Cannot use both --schema-subject and --schema-file together
    - Cannot use both --schema-version and --schema-min-version together
    - --reliable enables guaranteed delivery with at-least-once semantics
    - --interval controls the delay between messages (minimum 100ms)
    - Message attributes are useful for routing and filtering
"#;

pub async fn handle_produce(produce: Produce) -> Result<()> {
    // Validate interval
    if produce.extended_args.interval < 100 {
        return Err(anyhow::anyhow!("The interval must be at least 100 milliseconds").into());
    }

    // Validate schema arguments
    if produce.extended_args.schema_file.is_some() && produce.extended_args.schema_type.is_none() {
        return Err(anyhow::anyhow!("--schema-type is required when using --schema-file").into());
    }

    if produce.extended_args.schema_subject.is_some() && produce.extended_args.schema_file.is_some()
    {
        return Err(anyhow::anyhow!(
            "Cannot use both --schema-subject and --schema-file. Use one or the other."
        )
        .into());
    }

    if produce.extended_args.schema_version.is_some()
        && produce.extended_args.schema_subject.is_none()
    {
        return Err(anyhow::anyhow!("--schema-version requires --schema-subject").into());
    }

    if produce.extended_args.schema_min_version.is_some()
        && produce.extended_args.schema_subject.is_none()
    {
        return Err(anyhow::anyhow!("--schema-min-version requires --schema-subject").into());
    }

    if produce.extended_args.schema_version.is_some()
        && produce.extended_args.schema_min_version.is_some()
    {
        return Err(
            anyhow::anyhow!("Cannot use both --schema-version and --schema-min-version").into(),
        );
    }

    let client = DanubeClient::builder()
        .service_url(&produce.basic_args.service_addr)
        .build()
        .await?;

    // Handle schema registration if --schema-file is provided
    let schema_subject = if let Some(schema_file) = &produce.extended_args.schema_file {
        let schema_type = produce
            .extended_args
            .schema_type
            .ok_or_else(|| anyhow::anyhow!("--schema-type is required with --schema-file"))?;

        // Load and validate schema
        let schema_data = schema_helpers::load_schema_file(schema_file)?;
        schema_helpers::validate_schema_format(&schema_data, schema_type.into())?;

        // Auto-generate subject name from topic if not provided
        let subject = produce
            .basic_args
            .topic
            .trim_start_matches('/')
            .replace('/', "-");

        println!(
            "📤 Auto-registering schema for subject '{}' (type: {:?})...",
            subject, schema_type
        );

        let schema_client = client.schema();
        let schema_id = schema_client
            .register_schema(&subject)
            .with_type(schema_type.into())
            .with_schema_data(schema_data)
            .execute()
            .await
            .context("Failed to register schema")?;

        println!("✅ Schema registered with ID: {}", schema_id);
        Some(subject)
    } else {
        produce.extended_args.schema_subject.clone()
    };

    // Build producer with optional schema subject and version control
    let mut producer_builder = client
        .new_producer()
        .with_topic(produce.basic_args.topic.clone())
        .with_name(produce.basic_args.producer_name.clone());

    if let Some(subject) = schema_subject {
        // Apply version control if specified
        if let Some(version) = produce.extended_args.schema_version {
            producer_builder = producer_builder.with_schema_version(&subject, version);
            println!(
                "📋 Using schema subject: {} (pinned to version {})",
                subject, version
            );
        } else if let Some(min_version) = produce.extended_args.schema_min_version {
            producer_builder = producer_builder.with_schema_min_version(&subject, min_version);
            println!(
                "📋 Using schema subject: {} (minimum version {})",
                subject, min_version
            );
        } else {
            producer_builder = producer_builder.with_schema_subject(&subject);
            println!("📋 Using schema subject: {} (latest version)", subject);
        }
    }

    if let Some(partitions) = produce.extended_args.partitions {
        producer_builder = producer_builder.with_partitions(partitions as usize);
    }

    if produce.reliable_args.reliable {
        producer_builder = producer_builder.with_reliable_dispatch();
    }

    let mut producer = producer_builder.build()?;

    producer
        .create()
        .await
        .context("Failed to create producer")?;

    println!(
        "✅ Producer '{}' created successfully",
        produce.basic_args.producer_name
    );

    // Use the provided file or message
    let encoded_data = if let Some(file_path) = &produce.basic_args.file {
        std::fs::read(file_path).with_context(|| format!("Failed to read file: {}", file_path))?
    } else if let Some(message) = &produce.basic_args.message {
        message.as_bytes().to_vec()
    } else {
        return Err(anyhow::anyhow!(
            "Either --message or --file must be provided"
        ));
    };

    let mut success_count = 0;
    let mut error_count = 0;

    for i in 0..produce.extended_args.count {
        let cloned_attributes = produce.extended_args.attributes.clone();
        match producer.send(encoded_data.clone(), cloned_attributes).await {
            Ok(message_id) => {
                println!(
                    "📤 Message {}/{} sent successfully (ID: {})",
                    i + 1,
                    produce.extended_args.count,
                    message_id
                );
                success_count += 1;
            }
            Err(e) => {
                eprintln!(
                    "❌ Failed to send message {}/{}: {}",
                    i + 1,
                    produce.extended_args.count,
                    e
                );
                error_count += 1;
            }
        }

        if i < produce.extended_args.count - 1 {
            sleep(Duration::from_millis(produce.extended_args.interval)).await;
        }
    }

    println!("\n📊 Summary:");
    println!("   ✅ Success: {}", success_count);
    if error_count > 0 {
        println!("   ❌ Errors: {}", error_count);
    }

    Ok(())
}

fn parse_attributes(val: &str) -> Result<HashMap<String, String>, String> {
    let mut map = HashMap::new();
    for pair in val.split(',') {
        let mut split = pair.splitn(2, ':');
        let key = split
            .next()
            .ok_or("Invalid format: missing key")?
            .trim()
            .to_string();
        let value = split
            .next()
            .ok_or("Invalid format: missing value")?
            .trim()
            .to_string();
        map.insert(key, value);
    }
    Ok(map)
}

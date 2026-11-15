use anyhow::{Context, Result};
use clap::{ArgGroup, Args, ValueEnum, ValueHint};
use danube_client::SchemaType;
use std::collections::HashMap;
use tokio::time::{sleep, Duration};

use super::{schema_helpers, ClientArgs};

#[derive(Debug, Args)]
#[command(
    about = "Publish messages to a topic",
    long_about = "Publish one or more messages to a topic.\n\n\
The payload comes from --message (string) or --file (raw bytes). If the topic does \
not exist and auto-creation is enabled, it is created on first publish; prefer \
`danube topic create --intent <INTENT>` to choose delivery and durability explicitly.",
    after_help = "Examples:
  danube produce /default/events -m 'hello'
  danube produce /default/events -m '{\"id\":1}' --count 10 --interval 200
  danube produce /default/orders -f ./order.json --schema-subject orders-value
  danube produce /default/orders -m 'x' --key customer-42 --attr source:cli,env:dev",
    group(ArgGroup::new("payload").required(true).args(["message", "file"]))
)]
pub struct Produce {
    /// Topic to publish to, e.g. /default/events
    #[arg(value_name = "TOPIC")]
    pub topic: String,

    /// Message payload (UTF-8 string)
    #[arg(short = 'm', long, help_heading = "Payload")]
    pub message: Option<String>,

    /// Read the payload bytes from a file
    #[arg(short = 'f', long, value_name = "FILE", value_hint = ValueHint::FilePath, help_heading = "Payload")]
    pub file: Option<String>,

    /// Message attributes as comma-separated key:value pairs
    #[arg(short = 'a', long = "attr", value_name = "K:V,...", value_parser = parse_attributes, help_heading = "Payload")]
    pub attributes: Option<HashMap<String, String>>,

    /// Routing key (used by Key-Shared subscriptions)
    #[arg(short = 'k', long, help_heading = "Payload")]
    pub key: Option<String>,

    /// Number of messages to send
    #[arg(short = 'c', long, default_value_t = 1, help_heading = "Sending")]
    pub count: u32,

    /// Delay between messages in milliseconds (min 10)
    #[arg(short = 'i', long, default_value_t = 500, value_name = "MS",
          value_parser = clap::value_parser!(u64).range(10..), help_heading = "Sending")]
    pub interval: u64,

    /// Producer name
    #[arg(short = 'n', long, default_value = "danube-producer", help_heading = "Sending")]
    pub producer_name: String,

    /// Partitions to use when the topic is auto-created
    #[arg(short = 'p', long, help_heading = "Topic auto-creation")]
    pub partitions: Option<u32>,

    /// Use reliable (WAL-backed) dispatch when the topic is auto-created
    #[arg(short = 'r', long, help_heading = "Topic auto-creation")]
    pub reliable: bool,

    /// Existing schema subject to publish with
    #[arg(long, conflicts_with = "schema_file", help_heading = "Schema")]
    pub schema_subject: Option<String>,

    /// Pin an exact schema version
    #[arg(long, requires = "schema_subject", conflicts_with = "schema_min_version", help_heading = "Schema")]
    pub schema_version: Option<u32>,

    /// Require at least this schema version
    #[arg(long, requires = "schema_subject", help_heading = "Schema")]
    pub schema_min_version: Option<u32>,

    /// Register this schema file and publish with it
    #[arg(long, value_name = "FILE", value_hint = ValueHint::FilePath, requires = "schema_type", help_heading = "Schema")]
    pub schema_file: Option<String>,

    /// Schema type of --schema-file
    #[arg(long, value_enum, help_heading = "Schema")]
    pub schema_type: Option<SchemaTypeArg>,

    #[command(flatten)]
    pub client: ClientArgs,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum SchemaTypeArg {
    Bytes,
    String,
    #[value(alias = "int64")]
    Number,
    JsonSchema,
    Avro,
    Protobuf,
}

impl From<SchemaTypeArg> for SchemaType {
    fn from(arg: SchemaTypeArg) -> Self {
        match arg {
            SchemaTypeArg::Bytes => SchemaType::Bytes,
            SchemaTypeArg::String => SchemaType::String,
            SchemaTypeArg::Number => SchemaType::Number,
            SchemaTypeArg::JsonSchema => SchemaType::JsonSchema,
            SchemaTypeArg::Avro => SchemaType::Avro,
            SchemaTypeArg::Protobuf => SchemaType::Protobuf,
        }
    }
}

pub async fn handle(produce: Produce) -> Result<()> {
    let conn = produce.client.resolve()?;
    let client = conn.danube_client().await?;

    let schema_subject = if let Some(schema_file) = &produce.schema_file {
        let schema_type = produce
            .schema_type
            .ok_or_else(|| anyhow::anyhow!("--schema-type is required with --schema-file"))?;
        let schema_data = schema_helpers::load_schema_file(schema_file)?;
        schema_helpers::validate_schema_format(&schema_data, schema_type.into())?;

        let subject = produce.topic.trim_start_matches('/').replace('/', "-");
        let schema_client = client.schema();
        let schema_id = schema_client
            .register_schema(&subject)
            .with_type(schema_type.into())
            .with_schema_data(schema_data)
            .execute()
            .await
            .context("Failed to register schema")?;
        println!("Schema registered with ID: {}", schema_id);
        Some(subject)
    } else {
        produce.schema_subject.clone()
    };

    let mut producer_builder = client
        .new_producer()
        .with_topic(produce.topic.clone())
        .with_name(produce.producer_name.clone());

    if let Some(subject) = schema_subject {
        if let Some(version) = produce.schema_version {
            producer_builder = producer_builder.with_schema_version(&subject, version);
        } else if let Some(min_version) = produce.schema_min_version {
            producer_builder = producer_builder.with_schema_min_version(&subject, min_version);
        } else {
            producer_builder = producer_builder.with_schema_subject(&subject);
        }
    }

    if let Some(partitions) = produce.partitions {
        producer_builder = producer_builder.with_partitions(partitions as usize);
    }
    if produce.reliable {
        producer_builder = producer_builder.with_reliable_dispatch();
    }

    let mut producer = producer_builder.build()?;
    producer
        .create()
        .await
        .context("Failed to create producer")?;

    let encoded_data = if let Some(file_path) = &produce.file {
        std::fs::read(file_path).with_context(|| format!("Failed to read file: {}", file_path))?
    } else if let Some(message) = &produce.message {
        message.as_bytes().to_vec()
    } else {
        anyhow::bail!("Either --message or --file must be provided");
    };

    let mut success_count = 0;
    let mut error_count = 0;

    for i in 0..produce.count {
        let attrs = produce.attributes.clone();
        let send_res = if let Some(ref routing_key) = produce.key {
            producer
                .send_with_key(encoded_data.clone(), attrs, routing_key)
                .await
        } else {
            producer.send(encoded_data.clone(), attrs).await
        };

        match send_res {
            Ok(message_id) => {
                println!("Message {}/{} sent (ID: {})", i + 1, produce.count, message_id);
                success_count += 1;
            }
            Err(e) => {
                eprintln!("Failed to send message {}/{}: {}", i + 1, produce.count, e);
                error_count += 1;
            }
        }

        if i + 1 < produce.count {
            sleep(Duration::from_millis(produce.interval)).await;
        }
    }

    if error_count > 0 {
        anyhow::bail!(
            "Produced {} message(s), failed {}",
            success_count,
            error_count
        );
    }

    Ok(())
}

fn parse_attributes(val: &str) -> std::result::Result<HashMap<String, String>, String> {
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

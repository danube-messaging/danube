use anyhow::{Context, Result};
use clap::{Args, ValueEnum};
use danube_client::SchemaType;
use std::collections::HashMap;
use tokio::time::{sleep, Duration};

use super::{build_danube_client, schema_helpers, DataAuthArgs};

#[derive(Debug, Args)]
#[command(about = "Produce messages to a Danube topic")]
pub struct Produce {
    #[command(flatten)]
    pub basic_args: ProduceBasicArgs,

    #[command(flatten)]
    pub extended_args: ProduceExtendedArgs,

    #[command(flatten)]
    pub auth_args: DataAuthArgs,
}

#[derive(Debug, Args)]
pub struct ProduceBasicArgs {
    /// Service URL of the Danube broker (e.g. http://127.0.0.1:6650)
    #[arg(
        short = 's',
        long,
        env = "DANUBE_SERVICE_ADDR",
        default_value = "http://127.0.0.1:6650"
    )]
    pub service_addr: String,

    /// Producer name
    #[arg(short = 'n', long, default_value = "danube_producer")]
    pub producer_name: String,

    /// Topic name (e.g. /default/my-topic)
    #[arg(short = 't', long, default_value = "/default/test_topic")]
    pub topic: String,

    /// Message payload string to send
    #[arg(short = 'm', long)]
    pub message: Option<String>,

    /// Path to file whose bytes should be sent as the payload
    #[arg(short = 'f', long)]
    pub file: Option<String>,
}

#[derive(Debug, Args)]
pub struct ProduceExtendedArgs {
    /// Number of messages to send
    #[arg(short = 'c', long, default_value_t = 1)]
    pub count: u32,

    /// Interval between messages in milliseconds (minimum 10ms)
    #[arg(short = 'i', long, default_value_t = 500)]
    pub interval: u64,

    /// Number of partitions to create for the topic (if auto-creating)
    #[arg(short = 'p', long)]
    pub partitions: Option<u32>,

    /// Message attributes as comma-separated key:value pairs
    #[arg(short = 'a', long, value_parser = parse_attributes)]
    pub attributes: Option<HashMap<String, String>>,

    /// Routing key for Key-Shared dispatch
    #[arg(short = 'k', long)]
    pub key: Option<String>,

    /// Enable reliable dispatch for the topic
    #[arg(short = 'r', long, default_value_t = false)]
    pub reliable: bool,

    /// Schema subject name
    #[arg(long)]
    pub schema_subject: Option<String>,

    /// Specific schema version to pin
    #[arg(long)]
    pub schema_version: Option<u32>,

    /// Minimum schema version required
    #[arg(long)]
    pub schema_min_version: Option<u32>,

    /// Schema file path for auto-registration
    #[arg(long)]
    pub schema_file: Option<String>,

    /// Schema type when using --schema-file
    #[arg(long, value_enum)]
    pub schema_type: Option<SchemaTypeArg>,
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
    if produce.extended_args.interval < 10 {
        anyhow::bail!("The interval must be at least 10 milliseconds");
    }
    if produce.extended_args.schema_file.is_some() && produce.extended_args.schema_type.is_none() {
        anyhow::bail!("--schema-type is required when using --schema-file");
    }
    if produce.extended_args.schema_subject.is_some() && produce.extended_args.schema_file.is_some()
    {
        anyhow::bail!("Cannot use both --schema-subject and --schema-file");
    }
    if produce.extended_args.schema_version.is_some()
        && produce.extended_args.schema_subject.is_none()
    {
        anyhow::bail!("--schema-version requires --schema-subject");
    }
    if produce.extended_args.schema_min_version.is_some()
        && produce.extended_args.schema_subject.is_none()
    {
        anyhow::bail!("--schema-min-version requires --schema-subject");
    }
    if produce.extended_args.schema_version.is_some()
        && produce.extended_args.schema_min_version.is_some()
    {
        anyhow::bail!("Cannot use both --schema-version and --schema-min-version");
    }

    let client = build_danube_client(&produce.basic_args.service_addr, &produce.auth_args).await?;

    let schema_subject = if let Some(schema_file) = &produce.extended_args.schema_file {
        let schema_type = produce
            .extended_args
            .schema_type
            .ok_or_else(|| anyhow::anyhow!("--schema-type is required with --schema-file"))?;
        let schema_data = schema_helpers::load_schema_file(schema_file)?;
        schema_helpers::validate_schema_format(&schema_data, schema_type.into())?;

        let subject = produce
            .basic_args
            .topic
            .trim_start_matches('/')
            .replace('/', "-");
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
        produce.extended_args.schema_subject.clone()
    };

    let mut producer_builder = client
        .new_producer()
        .with_topic(produce.basic_args.topic.clone())
        .with_name(produce.basic_args.producer_name.clone());

    if let Some(subject) = schema_subject {
        if let Some(version) = produce.extended_args.schema_version {
            producer_builder = producer_builder.with_schema_version(&subject, version);
        } else if let Some(min_version) = produce.extended_args.schema_min_version {
            producer_builder = producer_builder.with_schema_min_version(&subject, min_version);
        } else {
            producer_builder = producer_builder.with_schema_subject(&subject);
        }
    }

    if let Some(partitions) = produce.extended_args.partitions {
        producer_builder = producer_builder.with_partitions(partitions as usize);
    }
    if produce.extended_args.reliable {
        producer_builder = producer_builder.with_reliable_dispatch();
    }

    let mut producer = producer_builder.build()?;
    producer
        .create()
        .await
        .context("Failed to create producer")?;

    let encoded_data = if let Some(file_path) = &produce.basic_args.file {
        std::fs::read(file_path).with_context(|| format!("Failed to read file: {}", file_path))?
    } else if let Some(message) = &produce.basic_args.message {
        message.as_bytes().to_vec()
    } else {
        anyhow::bail!("Either --message or --file must be provided");
    };

    let mut success_count = 0;
    let mut error_count = 0;

    for i in 0..produce.extended_args.count {
        let attrs = produce.extended_args.attributes.clone();
        let send_res = if let Some(ref routing_key) = produce.extended_args.key {
            producer
                .send_with_key(encoded_data.clone(), attrs, routing_key)
                .await
        } else {
            producer.send(encoded_data.clone(), attrs).await
        };

        match send_res {
            Ok(message_id) => {
                println!(
                    "Message {}/{} sent (ID: {})",
                    i + 1,
                    produce.extended_args.count,
                    message_id
                );
                success_count += 1;
            }
            Err(e) => {
                eprintln!(
                    "Failed to send message {}/{}: {}",
                    i + 1,
                    produce.extended_args.count,
                    e
                );
                error_count += 1;
            }
        }

        if i + 1 < produce.extended_args.count {
            sleep(Duration::from_millis(produce.extended_args.interval)).await;
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

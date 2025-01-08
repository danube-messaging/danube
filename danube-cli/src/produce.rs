use anyhow::Result;
use clap::{Args, Parser, ValueEnum};
use danube_client::{
    ConfigDispatchStrategy, DanubeClient, ReliableOptions, RetentionPolicy, SchemaType,
};
use std::collections::HashMap;
use tokio::time::{sleep, Duration};

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
        help = "The message to send. This is a required argument."
    )]
    pub message: String,
}

#[derive(Debug, Args)]
pub struct ExtendedArgs {
    #[arg(
        long,
        short = 'y',
        value_enum,
        help = "The schema type for the message: bytes, string, int64, or json. Default: string"
    )]
    pub schema: Option<SchemaTypeArg>,

    #[arg(long, help = "The JSON schema, required if schema type is Json.")]
    pub json_schema: Option<String>,

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

    #[arg(
        long,
        value_enum,
        default_value = "memory",
        help = "Storage type for reliable delivery: memory, disk, or s3"
    )]
    pub storage: Option<StorageTypeArg>,

    #[arg(
        long,
        default_value = "20",
        help = "Segment size in MB for reliable delivery (default: 20)"
    )]
    pub segment_size: usize,

    #[arg(
        long,
        value_enum,
        default_value = "expire",
        help = "Retention policy: ack (retain until acknowledged) or expire (retain until time expires)"
    )]
    pub retention: Option<RetentionPolicyArg>,

    #[arg(
        long,
        default_value = "3600",
        help = "Retention period in seconds for reliable delivery (default: 3600)"
    )]
    pub retention_period: u64,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq)]
pub enum StorageTypeArg {
    Memory,
    Disk,
    S3,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq)]
pub enum RetentionPolicyArg {
    Ack,
    Expire,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq)]
pub enum SchemaTypeArg {
    Bytes,
    String,
    Int64,
    Json,
}

const EXAMPLES_TEXT: &str = r#"
EXAMPLES:
    # Basic message production
    danube-cli produce --service-addr http://localhost:6650 --count 100 --message "Hello Danube"

    # Producing with JSON schema
    danube-cli produce -s http://localhost:6650 -c 100 -y json --json-schema '{"type": "object", "properties": {"field1": {"type": "string"}}}' -m '{"field1":"Hello Danube"}'

    # Reliable message delivery
    danube-cli produce -s http://localhost:6650 -m "Hello Danube" -c 100 \
        --reliable \
        --storage disk \
        --segment-size 10 \
        --retention expire \
        --retention-period 7200

    # Producing with attributes
    danube-cli produce -s http://localhost:6650 -m "Hello Danube" -a "key1:value1,key2:value2"
"#;

pub async fn handle_produce(produce: Produce) -> Result<()> {
    // Validate interval
    if produce.extended_args.interval < 100 {
        return Err(anyhow::anyhow!("The interval must be at least 100 milliseconds").into());
    }

    let client = DanubeClient::builder()
        .service_url(&produce.basic_args.service_addr)
        .build()?;

    let schema_type = validate_schema(
        produce.extended_args.schema,
        produce.extended_args.json_schema,
    )?;

    let mut producer_builder = client
        .new_producer()
        .with_topic(produce.basic_args.topic)
        .with_name(produce.basic_args.producer_name)
        .with_schema("my_app".into(), schema_type); // Pass the correct schema type

    if let Some(partitions) = produce.extended_args.partitions {
        producer_builder = producer_builder.with_partitions(partitions as usize)
    }

    if produce.reliable_args.reliable {
        let retention_policy = match produce
            .reliable_args
            .retention
            .unwrap_or(RetentionPolicyArg::Expire)
        {
            RetentionPolicyArg::Ack => RetentionPolicy::RetainUntilAck,
            RetentionPolicyArg::Expire => RetentionPolicy::RetainUntilExpire,
        };

        let reliable_options = ReliableOptions::new(
            produce.reliable_args.segment_size,
            retention_policy,
            produce.reliable_args.retention_period,
        );

        producer_builder = producer_builder
            .with_dispatch_strategy(ConfigDispatchStrategy::Reliable(reliable_options));
    }

    let mut producer = producer_builder.build();

    producer.create().await?;

    let encoded_data = produce.basic_args.message.as_bytes().to_vec();

    for _ in 0..produce.extended_args.count {
        let cloned_attributes = produce.extended_args.attributes.clone();
        match producer.send(encoded_data.clone(), cloned_attributes).await {
            Ok(message_id) => println!("Message sent successfully with ID: {}", message_id),
            Err(e) => eprintln!("Failed to send message: {}", e),
        }
        if produce.extended_args.count - 1 > 0 {
            sleep(Duration::from_millis(produce.extended_args.interval)).await;
        }
    }

    Ok(())
}

impl From<SchemaTypeArg> for SchemaType {
    fn from(arg: SchemaTypeArg) -> Self {
        match arg {
            SchemaTypeArg::Bytes => SchemaType::Bytes,
            SchemaTypeArg::String => SchemaType::String,
            SchemaTypeArg::Int64 => SchemaType::Int64,
            SchemaTypeArg::Json => SchemaType::Json(String::new()), // Placeholder
        }
    }
}

fn validate_schema(
    schema_type: Option<SchemaTypeArg>,
    json_schema: Option<String>,
) -> Result<SchemaType> {
    if let Some(schema_type) = schema_type {
        if !SchemaTypeArg::value_variants().contains(&schema_type) {
            return Err(anyhow::anyhow!(
                "Unsupported schema type: '{:?}'. Supported values are: {:?}",
                schema_type,
                SchemaTypeArg::value_variants()
            )
            .into());
        }
    }

    match schema_type {
        Some(schema_type) => match schema_type {
            SchemaTypeArg::Json => {
                if let Some(json_schema) = json_schema {
                    return Ok(SchemaType::Json(json_schema));
                } else {
                    return Err(
                        anyhow::anyhow!("JSON schema is required for schema type 'Json'").into(),
                    );
                }
            }
            _ => return Ok(schema_type.into()),
        },
        None => return Ok(SchemaTypeArg::String.into()),
    };
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

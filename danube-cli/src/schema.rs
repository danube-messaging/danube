use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand, ValueEnum};
use danube_client::{DanubeClient, SchemaRegistryClient, SchemaType};
use serde_json;
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(after_help = EXAMPLES_TEXT)]
pub struct Schema {
    #[command(subcommand)]
    pub command: SchemaCommands,

    #[arg(
        long,
        short = 's',
        default_value = "http://127.0.0.1:6650",
        help = "The service URL for the Danube broker"
    )]
    pub service_addr: String,
}

#[derive(Debug, Subcommand)]
pub enum SchemaCommands {
    #[command(about = "Register a new schema or update an existing one")]
    Register(RegisterArgs),

    #[command(about = "Get schema details by subject")]
    Get(GetArgs),

    #[command(about = "List all versions of a schema subject")]
    Versions(VersionsArgs),

    #[command(about = "Check if a schema is compatible with the subject")]
    Check(CheckArgs),

    #[command(about = "List all schema subjects")]
    List(ListArgs),

    #[command(about = "Delete a schema subject or version")]
    Delete(DeleteArgs),
}

#[derive(Debug, Args)]
pub struct RegisterArgs {
    #[arg(help = "Schema subject name")]
    pub subject: String,

    #[arg(
        long,
        short = 't',
        value_enum,
        help = "Schema type (json_schema, avro, protobuf)"
    )]
    pub schema_type: SchemaTypeArg,

    #[arg(
        long,
        short = 'f',
        help = "Path to schema definition file"
    )]
    pub file: PathBuf,

    #[arg(
        long,
        short = 'o',
        value_enum,
        default_value = "table",
        help = "Output format"
    )]
    pub output: OutputFormat,
}

#[derive(Debug, Args)]
pub struct GetArgs {
    #[arg(help = "Schema subject name")]
    pub subject: String,

    #[arg(
        long,
        short = 'v',
        help = "Specific version (defaults to latest)"
    )]
    pub version: Option<u32>,

    #[arg(
        long,
        short = 'o',
        value_enum,
        default_value = "table",
        help = "Output format"
    )]
    pub output: OutputFormat,
}

#[derive(Debug, Args)]
pub struct VersionsArgs {
    #[arg(help = "Schema subject name")]
    pub subject: String,

    #[arg(
        long,
        short = 'o',
        value_enum,
        default_value = "table",
        help = "Output format"
    )]
    pub output: OutputFormat,
}

#[derive(Debug, Args)]
pub struct CheckArgs {
    #[arg(help = "Schema subject name")]
    pub subject: String,

    #[arg(
        long,
        short = 'f',
        help = "Path to schema definition file to check"
    )]
    pub file: PathBuf,

    #[arg(
        long,
        short = 't',
        value_enum,
        help = "Schema type (json_schema, avro, protobuf)"
    )]
    pub schema_type: SchemaTypeArg,

    #[arg(
        long,
        short = 'o',
        value_enum,
        default_value = "table",
        help = "Output format"
    )]
    pub output: OutputFormat,
}

#[derive(Debug, Args)]
pub struct ListArgs {
    #[arg(
        long,
        short = 'o',
        value_enum,
        default_value = "table",
        help = "Output format"
    )]
    pub output: OutputFormat,
}

#[derive(Debug, Args)]
pub struct DeleteArgs {
    #[arg(help = "Schema subject name")]
    pub subject: String,

    #[arg(
        long,
        short = 'v',
        help = "Specific version to delete (if not provided, deletes all versions)"
    )]
    pub version: Option<u32>,

    #[arg(
        long,
        help = "Permanently delete (cannot be undone)"
    )]
    pub permanent: bool,

    #[arg(
        long,
        short = 'o',
        value_enum,
        default_value = "table",
        help = "Output format"
    )]
    pub output: OutputFormat,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum SchemaTypeArg {
    #[value(name = "json_schema")]
    JsonSchema,
    Avro,
    Protobuf,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum OutputFormat {
    Table,
    Json,
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
    # Register a JSON schema for a subject
    danube-cli schema register user-events \
        --type json_schema \
        --file ./schemas/user_event.json

    # Register an Avro schema
    danube-cli schema register product-catalog \
        --type avro \
        --file ./schemas/product.avsc

    # Get the latest schema for a subject
    danube-cli schema get user-events

    # Get schema with JSON output
    danube-cli schema get user-events --output json

    # List all versions of a schema
    danube-cli schema versions user-events

    # List versions with JSON output
    danube-cli schema versions user-events --output json

    # Check if a new schema version is compatible
    danube-cli schema check user-events \
        --type json_schema \
        --file ./schemas/user_event_v2.json

    # Check compatibility with specific mode override
    danube-cli schema check user-events \
        --type json_schema \
        --file ./schemas/user_event_v2.json \
        --mode backward

    # List all schema subjects (when implemented)
    danube-cli schema list

WORKFLOW EXAMPLE:
    # 1. Register a schema
    danube-cli schema register orders \
        --type json_schema \
        --file ./schemas/order.json

    # 2. Produce messages using the schema
    danube-cli produce -s http://localhost:6650 \
        --topic /default/orders \
        --schema-subject orders \
        --message '{"order_id":"ord_123","amount":99.99}'

    # 3. Evolve the schema (check compatibility first)
    danube-cli schema check orders \
        --type json_schema \
        --file ./schemas/order_v2.json

    # 4. Register new version if compatible
    danube-cli schema register orders \
        --type json_schema \
        --file ./schemas/order_v2.json

    # 5. View all versions
    danube-cli schema versions orders

NOTES:
    - Schema subjects typically match topic names (e.g., topic '/default/events' -> subject 'default-events')
    - All schema operations support both table and JSON output formats
    - Compatibility checking helps prevent breaking changes in message schemas
    - Use --output json for programmatic parsing and automation
"#;

/// Main handler for schema commands
pub async fn handle_schema(schema: Schema) -> Result<()> {
    let client = DanubeClient::builder()
        .service_url(&schema.service_addr)
        .build()
        .await
        .context("Failed to connect to Danube broker")?;

    let mut schema_client = SchemaRegistryClient::new(&client)
        .await
        .context("Failed to create schema registry client")?;

    match schema.command {
        SchemaCommands::Register(args) => handle_register(&mut schema_client, args).await,
        SchemaCommands::Get(args) => handle_get(&mut schema_client, args).await,
        SchemaCommands::Versions(args) => handle_versions(&mut schema_client, args).await,
        SchemaCommands::Check(args) => handle_check(&mut schema_client, args).await,
        SchemaCommands::List(args) => handle_list(&mut schema_client, args).await,
        SchemaCommands::Delete(args) => handle_delete(&mut schema_client, args).await,
    }
}

/// Register a new schema
async fn handle_register(
    client: &mut SchemaRegistryClient,
    args: RegisterArgs,
) -> Result<()> {
    // Load schema file
    let schema_data = fs::read(&args.file)
        .with_context(|| format!("Failed to read schema file: {:?}", args.file))?;

    // Validate schema format before registration
    validate_schema_format(&schema_data, args.schema_type.into())?;

    println!("📤 Registering schema '{}' (type: {:?})...", args.subject, args.schema_type);

    let schema_id = client
        .register_schema(&args.subject)
        .with_type(args.schema_type.into())
        .with_schema_data(schema_data)
        .execute()
        .await
        .context("Failed to register schema")?;

    match args.output {
        OutputFormat::Json => {
            let result = serde_json::json!({
                "success": true,
                "subject": args.subject,
                "schema_id": schema_id,
                "schema_type": format!("{:?}", args.schema_type),
            });
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        OutputFormat::Table => {
            println!("✅ Schema registered successfully!");
            println!("   Subject: {}", args.subject);
            println!("   Schema ID: {}", schema_id);
            println!("   Type: {:?}", args.schema_type);
        }
    }

    Ok(())
}

/// Get schema details
async fn handle_get(
    client: &mut SchemaRegistryClient,
    args: GetArgs,
) -> Result<()> {
    if args.version.is_some() {
        return Err(anyhow::anyhow!(
            "Getting specific versions not yet supported. Use 'schema versions <subject>' to list versions."
        ));
    }

    println!("🔍 Fetching latest schema for subject '{}'...", args.subject);

    let schema_response = client.get_latest_schema(&args.subject).await?;

    match args.output {
        OutputFormat::Json => {
            let result = serde_json::json!({
                "subject": schema_response.subject,
                "version": schema_response.version,
                "schema_id": schema_response.schema_id,
                "schema_type": schema_response.schema_type,
                "schema_definition": String::from_utf8_lossy(&schema_response.schema_definition),
            });
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        OutputFormat::Table => {
            println!("✅ Schema details:");
            println!("   Subject: {}", schema_response.subject);
            println!("   Version: {}", schema_response.version);
            println!("   Schema ID: {}", schema_response.schema_id);
            println!("   Type: {}", schema_response.schema_type);
            println!("\n   Schema Definition:");
            let schema_str = String::from_utf8_lossy(&schema_response.schema_definition);
            if schema_response.schema_type == "json_schema" {
                if let Ok(json) = serde_json::from_str::<serde_json::Value>(&schema_str) {
                    println!("{}", serde_json::to_string_pretty(&json)?);
                } else {
                    println!("{}", schema_str);
                }
            } else {
                println!("{}", schema_str);
            }
        }
    }

    Ok(())
}

/// List all versions of a schema
async fn handle_versions(
    client: &mut SchemaRegistryClient,
    args: VersionsArgs,
) -> Result<()> {
    println!("📋 Listing versions for subject '{}'...", args.subject);

    let versions = client.list_versions(&args.subject).await?;

    match args.output {
        OutputFormat::Json => {
            let result = serde_json::json!({
                "subject": args.subject,
                "versions": versions,
                "count": versions.len(),
            });
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        OutputFormat::Table => {
            if versions.is_empty() {
                println!("⚠️  No versions found for subject '{}'", args.subject);
            } else {
                println!("✅ Found {} version(s):", versions.len());
                for version in versions {
                    println!("   • v{}", version);
                }
            }
        }
    }

    Ok(())
}

/// Check schema compatibility
async fn handle_check(
    client: &mut SchemaRegistryClient,
    args: CheckArgs,
) -> Result<()> {
    // Load schema file
    let schema_data = fs::read(&args.file)
        .with_context(|| format!("Failed to read schema file: {:?}", args.file))?;

    // Validate schema format
    validate_schema_format(&schema_data, args.schema_type.into())?;

    println!("🔍 Checking compatibility for subject '{}'...", args.subject);

    let compatibility_result = client
        .check_compatibility(&args.subject, schema_data, args.schema_type.into(), None)
        .await?;

    match args.output {
        OutputFormat::Json => {
            let result = serde_json::json!({
                "subject": args.subject,
                "is_compatible": compatibility_result.is_compatible,
                "errors": compatibility_result.errors,
            });
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        OutputFormat::Table => {
            if compatibility_result.is_compatible {
                println!("✅ Schema is COMPATIBLE");
                println!("   Safe to register as a new version for '{}'", args.subject);
            } else {
                println!("❌ Schema is NOT COMPATIBLE");
                println!("   Subject: {}", args.subject);
                if !compatibility_result.errors.is_empty() {
                    println!("\n   Compatibility errors:");
                    for error in &compatibility_result.errors {
                        println!("   • {}", error);
                    }
                }
            }
        }
    }

    Ok(())
}

/// List all schema subjects
async fn handle_list(
    _client: &mut SchemaRegistryClient,
    _args: ListArgs,
) -> Result<()> {
    Err(anyhow::anyhow!(
        "List subjects command not yet implemented in schema registry client.\nUse 'schema get <subject>' to retrieve a specific schema."
    ))
}

/// Delete a schema subject or version
async fn handle_delete(
    _client: &mut SchemaRegistryClient,
    _args: DeleteArgs,
) -> Result<()> {
    Err(anyhow::anyhow!(
        "Delete command not yet implemented in schema registry client.\nSchema deletion will be available in a future release."
    ))
}

/// Validate schema format before registration
fn validate_schema_format(schema_data: &[u8], schema_type: SchemaType) -> Result<()> {
    match schema_type {
        SchemaType::JsonSchema => {
            // Validate it's valid JSON
            serde_json::from_slice::<serde_json::Value>(schema_data)
                .context("Invalid JSON schema format")?;
        }
        SchemaType::Avro => {
            // Validate it's valid JSON (Avro schemas are JSON)
            serde_json::from_slice::<serde_json::Value>(schema_data)
                .context("Invalid Avro schema format (must be JSON)")?;
        }
        SchemaType::Protobuf => {
            // Protobuf schemas are text-based .proto files
            let _ = String::from_utf8(schema_data.to_vec())
                .context("Invalid Protobuf schema format (must be UTF-8 text)")?;
        }
        _ => {
            // For other types, just ensure it's valid UTF-8
            let _ = String::from_utf8(schema_data.to_vec())
                .context("Schema data must be valid UTF-8")?;
        }
    }
    Ok(())
}

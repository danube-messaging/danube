use anyhow::Result;
use clap::{Args, Subcommand};
use danube_core::proto::danube_schema::{
    CheckCompatibilityRequest, DeleteSchemaVersionRequest, GetLatestSchemaRequest,
    GetSchemaRequest, ListVersionsRequest, RegisterSchemaRequest, SetCompatibilityModeRequest,
};
use std::fs;

use crate::core::{AdminGrpcClient, GrpcClientConfig};

#[derive(Debug, Args)]
#[command(
    about = "Manage schemas in the Schema Registry",
    long_about = "Manage schemas in the Schema Registry.\n\nCommon examples:\n  danube-admin schemas register user-events --schema-type json_schema --file schema.json\n  danube-admin schemas get --subject user-events\n  danube-admin schemas versions user-events",
    subcommand_required = true,
    arg_required_else_help = true
)]
pub struct Schemas {
    #[command(subcommand)]
    command: SchemasCommands,
}

#[derive(Debug, Subcommand)]
enum SchemasCommands {
    #[command(
        about = "Register a new schema or update existing schema",
        after_help = "Examples:
  danube-admin schemas register user-events --schema-type json_schema --file schema.json
  danube-admin schemas register user-events --schema-type avro --schema '{...}'
  danube-admin schemas register user-events --schema-type json_schema --file schema.json --description 'User event schema' --tags analytics events
  danube-admin schemas register user-events --schema-type protobuf --file user.proto --output json

Env:
  DANUBE_ADMIN_ENDPOINT (default http://127.0.0.1:50051)
  DANUBE_ADMIN_TLS, DANUBE_ADMIN_DOMAIN, DANUBE_ADMIN_CA, DANUBE_ADMIN_CERT, DANUBE_ADMIN_KEY"
    )]
    Register {
        #[arg(help = "Schema subject name")]
        subject: String,

        #[arg(
            long,
            value_parser = ["json_schema", "avro", "protobuf", "string", "bytes"],
            help = "Schema type"
        )]
        schema_type: String,

        #[arg(long, conflicts_with = "schema", help = "Path to schema definition file")]
        file: Option<String>,

        #[arg(long, conflicts_with = "file", help = "Inline schema definition")]
        schema: Option<String>,

        #[arg(long, help = "Human-readable description")]
        description: Option<String>,

        #[arg(long, help = "Tags for categorization (repeatable)")]
        tags: Vec<String>,

        #[arg(long, value_parser = ["json"], help = "Output format")]
        output: Option<String>,
    },

    #[command(
        about = "Get a schema by subject or ID",
        after_help = "Examples:
  danube-admin schemas get --subject user-events
  danube-admin schemas get --id 42
  danube-admin schemas get --id 42 --version 3
  danube-admin schemas get --subject user-events --output json

Env:
  DANUBE_ADMIN_ENDPOINT (default http://127.0.0.1:50051)"
    )]
    Get {
        #[arg(long, conflicts_with = "subject", help = "Schema ID")]
        id: Option<u64>,

        #[arg(long, conflicts_with = "id", help = "Schema subject")]
        subject: Option<String>,

        #[arg(long, requires = "id", help = "Specific version (default: latest)")]
        version: Option<u32>,

        #[arg(long, value_parser = ["json"], help = "Output format")]
        output: Option<String>,
    },

    #[command(
        about = "List all versions for a subject",
        after_help = "Examples:
  danube-admin schemas versions user-events
  danube-admin schemas versions user-events --output json

Env:
  DANUBE_ADMIN_ENDPOINT (default http://127.0.0.1:50051)"
    )]
    Versions {
        #[arg(help = "Schema subject name")]
        subject: String,

        #[arg(long, value_parser = ["json"], help = "Output format")]
        output: Option<String>,
    },

    #[command(
        about = "Check if a schema is compatible",
        after_help = "Examples:
  danube-admin schemas check user-events --schema-type json_schema --file new-schema.json
  danube-admin schemas check user-events --schema-type avro --schema '{...}'
  danube-admin schemas check user-events --schema-type json_schema --file new-schema.json --mode backward

Env:
  DANUBE_ADMIN_ENDPOINT (default http://127.0.0.1:50051)"
    )]
    Check {
        #[arg(help = "Schema subject name")]
        subject: String,

        #[arg(long, conflicts_with = "schema", help = "Path to new schema file")]
        file: Option<String>,

        #[arg(long, conflicts_with = "file", help = "Inline schema definition")]
        schema: Option<String>,

        #[arg(
            long,
            value_parser = ["json_schema", "avro", "protobuf"],
            help = "Schema type"
        )]
        schema_type: String,

        #[arg(long, help = "Override compatibility mode (none, backward, forward, full)")]
        mode: Option<String>,
    },

    #[command(
        about = "Get compatibility mode for a subject",
        after_help = "Examples:
  danube-admin schemas get-compatibility user-events
  danube-admin schemas get-compatibility user-events --output json

Env:
  DANUBE_ADMIN_ENDPOINT (default http://127.0.0.1:50051)"
    )]
    GetCompatibility {
        #[arg(help = "Schema subject name")]
        subject: String,

        #[arg(long, value_parser = ["json"], help = "Output format")]
        output: Option<String>,
    },

    #[command(
        about = "Set compatibility mode for a subject",
        after_help = "Examples:
  danube-admin schemas set-compatibility user-events --mode backward
  danube-admin schemas set-compatibility user-events --mode full
  danube-admin schemas set-compatibility user-events --mode none

Compatibility Modes:
  none     - No compatibility checking
  backward - New schema can read data written by old schema
  forward  - Old schema can read data written by new schema
  full     - Both backward and forward compatible

Env:
  DANUBE_ADMIN_ENDPOINT (default http://127.0.0.1:50051)"
    )]
    SetCompatibility {
        #[arg(help = "Schema subject name")]
        subject: String,

        #[arg(
            long,
            value_parser = ["none", "backward", "forward", "full"],
            help = "Compatibility mode"
        )]
        mode: String,
    },

    #[command(
        about = "Delete a specific schema version",
        after_help = "Examples:
  danube-admin schemas delete user-events --version 2 --confirm

Note: Deletion requires --confirm flag to prevent accidental deletion.

Env:
  DANUBE_ADMIN_ENDPOINT (default http://127.0.0.1:50051)"
    )]
    Delete {
        #[arg(help = "Schema subject name")]
        subject: String,

        #[arg(long, help = "Version to delete")]
        version: u32,

        #[arg(long, help = "Confirm deletion")]
        confirm: bool,
    },
}

pub async fn handle(schemas: Schemas) -> Result<()> {
    let config = GrpcClientConfig::default();
    let client = AdminGrpcClient::connect(config).await?;

    match schemas.command {
        SchemasCommands::Register {
            subject,
            schema_type,
            file,
            schema,
            description,
            tags,
            output,
        } => {
            let schema_data = if let Some(path) = file {
                fs::read(path)?
            } else if let Some(inline) = schema {
                inline.into_bytes()
            } else {
                return Err(anyhow::anyhow!("Either --file or --schema must be provided"));
            };

            let request = RegisterSchemaRequest {
                subject: subject.clone(),
                schema_type: schema_type.clone(),
                schema_definition: schema_data,
                description: description.unwrap_or_default(),
                created_by: std::env::var("USER").unwrap_or_else(|_| "admin".to_string()),
                tags,
            };

            let result = client.register_schema(request).await?;

            if matches!(output.as_deref(), Some("json")) {
                let out = serde_json::json!({
                    "subject": subject,
                    "schema_id": result.schema_id,
                    "version": result.version,
                    "is_new_version": result.is_new_version,
                    "fingerprint": result.fingerprint,
                });
                println!("{}", serde_json::to_string_pretty(&out)?);
            } else {
                if result.is_new_version {
                    println!("✅ Registered new schema version");
                } else {
                    println!("✅ Schema already exists");
                }
                println!("Subject: {}", subject);
                println!("Schema ID: {}", result.schema_id);
                println!("Version: {}", result.version);
                println!("Fingerprint: {}", result.fingerprint);
            }
        }

        SchemasCommands::Get {
            id,
            subject,
            version,
            output,
        } => {
            let result = if let Some(schema_id) = id {
                let request = GetSchemaRequest {
                    schema_id,
                    version,
                };
                client.get_schema(request).await?
            } else if let Some(subj) = subject {
                let request = GetLatestSchemaRequest { subject: subj };
                client.get_latest_schema(request).await?
            } else {
                return Err(anyhow::anyhow!("Either --id or --subject must be provided"));
            };

            if matches!(output.as_deref(), Some("json")) {
                let schema_str = String::from_utf8_lossy(&result.schema_definition);
                let out = serde_json::json!({
                    "schema_id": result.schema_id,
                    "version": result.version,
                    "subject": result.subject,
                    "schema_type": result.schema_type,
                    "schema_definition": schema_str,
                    "description": result.description,
                    "created_at": result.created_at,
                    "created_by": result.created_by,
                    "tags": result.tags,
                    "fingerprint": result.fingerprint,
                    "compatibility_mode": result.compatibility_mode.to_uppercase(),
                });
                println!("{}", serde_json::to_string_pretty(&out)?);
            } else {
                println!("Schema ID: {}", result.schema_id);
                println!("Version: {}", result.version);
                println!("Subject: {}", result.subject);
                println!("Type: {}", result.schema_type);
                println!("Compatibility Mode: {}", result.compatibility_mode.to_uppercase());
                if !result.description.is_empty() {
                    println!("Description: {}", result.description);
                }
                if !result.tags.is_empty() {
                    println!("Tags: {}", result.tags.join(", "));
                }
                println!("\nSchema Definition:");
                let schema_str = String::from_utf8_lossy(&result.schema_definition);
                if schema_str.trim_start().starts_with('{') {
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

        SchemasCommands::Versions { subject, output } => {
            let request = ListVersionsRequest {
                subject: subject.clone(),
            };
            let response = client.list_versions(request).await?;
            let versions = response.versions;

            if matches!(output.as_deref(), Some("json")) {
                let out: Vec<serde_json::Value> = versions
                    .iter()
                    .map(|v| {
                        serde_json::json!({
                            "version": v.version,
                            "schema_id": v.schema_id,
                            "created_at": v.created_at,
                            "created_by": v.created_by,
                            "description": v.description,
                            "fingerprint": v.fingerprint,
                        })
                    })
                    .collect();
                println!("{}", serde_json::to_string_pretty(&out)?);
            } else {
                println!("Versions for subject '{}':", subject);
                for v in versions {
                    println!(
                        "  Version {}: schema_id={}, fingerprint={}",
                        v.version, v.schema_id, v.fingerprint
                    );
                    if !v.created_by.is_empty() {
                        println!("    Created by: {}", v.created_by);
                    }
                    if !v.description.is_empty() {
                        println!("    Description: {}", v.description);
                    }
                }
            }
        }

        SchemasCommands::Check {
            subject,
            file,
            schema,
            schema_type,
            mode,
        } => {
            let schema_data = if let Some(path) = file {
                fs::read(path)?
            } else if let Some(inline) = schema {
                inline.into_bytes()
            } else {
                return Err(anyhow::anyhow!("Either --file or --schema must be provided"));
            };

            let request = CheckCompatibilityRequest {
                subject: subject.clone(),
                new_schema_definition: schema_data,
                schema_type: schema_type.clone(),
                compatibility_mode: mode,
            };

            let result = client.check_compatibility(request).await?;

            if result.is_compatible {
                println!("✅ Schema is compatible with subject '{}'", subject);
            } else {
                println!("❌ Schema is NOT compatible with subject '{}'", subject);
                if !result.errors.is_empty() {
                    println!("\nCompatibility errors:");
                    for error in result.errors {
                        println!("  - {}", error);
                    }
                }
            }
        }

        SchemasCommands::GetCompatibility { subject, output } => {
            // Get latest schema to extract compatibility mode
            let request = GetLatestSchemaRequest {
                subject: subject.clone(),
            };

            let result = client.get_latest_schema(request).await?;

            if matches!(output.as_deref(), Some("json")) {
                let out = serde_json::json!({
                    "subject": subject,
                    "compatibility_mode": result.compatibility_mode.to_uppercase(),
                });
                println!("{}", serde_json::to_string_pretty(&out)?);
            } else {
                println!("Subject: {}", subject);
                println!("Compatibility Mode: {}", result.compatibility_mode.to_uppercase());
            }
        }

        SchemasCommands::SetCompatibility { subject, mode } => {
            let request = SetCompatibilityModeRequest {
                subject: subject.clone(),
                compatibility_mode: mode.to_uppercase(),
            };

            let result = client.set_compatibility_mode(request).await?;

            if result.success {
                println!("✅ Compatibility mode set for subject '{}'", subject);
                println!("Mode: {}", mode.to_uppercase());
            } else {
                println!("❌ Failed to set compatibility mode");
                if !result.message.is_empty() {
                    println!("Error: {}", result.message);
                }
            }
        }

        SchemasCommands::Delete {
            subject,
            version,
            confirm,
        } => {
            if !confirm {
                return Err(anyhow::anyhow!(
                    "Deletion requires --confirm flag to prevent accidental deletion"
                ));
            }

            let request = DeleteSchemaVersionRequest {
                subject: subject.clone(),
                version,
            };

            let result = client.delete_schema_version(request).await?;

            if result.success {
                println!(
                    "✅ Deleted version {} of subject '{}'",
                    version, subject
                );
            } else {
                println!("❌ Failed to delete schema version");
                if !result.message.is_empty() {
                    println!("Error: {}", result.message);
                }
            }
        }
    }

    Ok(())
}

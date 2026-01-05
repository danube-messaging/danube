use crate::client::schema_registry_client;
use clap::{Args, Subcommand};
use danube_core::proto::danube_schema::{
    CheckCompatibilityRequest, DeleteSchemaVersionRequest, GetLatestSchemaRequest,
    GetSchemaRequest, ListVersionsRequest, RegisterSchemaRequest, SetCompatibilityModeRequest,
};
use std::fs;

#[derive(Debug, Args)]
#[command(
    about = "Manage schemas in the Schema Registry",
    long_about = "Manage schemas in the Schema Registry.\n\nCommon examples:\n  danube-admin-cli schemas register user-events --schema-type json_schema --file schema.json\n  danube-admin-cli schemas get --subject user-events\n  danube-admin-cli schemas list\n\nEnv:\n  DANUBE_ADMIN_ENDPOINT (default http://127.0.0.1:50051)\n  DANUBE_ADMIN_TLS, DANUBE_ADMIN_DOMAIN, DANUBE_ADMIN_CA, DANUBE_ADMIN_CERT, DANUBE_ADMIN_KEY",
    subcommand_required = true,
    arg_required_else_help = true
)]
pub(crate) struct SchemaRegistry {
    #[command(subcommand)]
    command: SchemaCommands,
}

#[derive(Debug, Subcommand)]
pub(crate) enum SchemaCommands {
    #[command(about = "Register a new schema or update existing schema")]
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

    #[command(about = "Get a schema by subject or ID")]
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

    #[command(about = "List all versions for a subject")]
    Versions {
        #[arg(help = "Schema subject name")]
        subject: String,

        #[arg(long, value_parser = ["json"], help = "Output format")]
        output: Option<String>,
    },

    #[command(about = "Check if a schema is compatible")]
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

    #[command(about = "Get compatibility mode for a subject")]
    GetCompatibility {
        #[arg(help = "Schema subject name")]
        subject: String,

        #[arg(long, value_parser = ["json"], help = "Output format")]
        output: Option<String>,
    },

    #[command(about = "Set compatibility mode for a subject")]
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

    #[command(about = "Delete a specific schema version")]
    Delete {
        #[arg(help = "Schema subject name")]
        subject: String,

        #[arg(long, help = "Version to delete")]
        version: u32,

        #[arg(long, help = "Confirm deletion")]
        confirm: bool,
    },
}

pub async fn handle_command(schemas: SchemaRegistry) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = schema_registry_client().await?;

    match schemas.command {
        SchemaCommands::Register {
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
                return Err("Either --file or --schema must be provided".into());
            };

            let request = RegisterSchemaRequest {
                subject: subject.clone(),
                schema_type: schema_type.clone(),
                schema_definition: schema_data,
                description: description.unwrap_or_default(),
                created_by: std::env::var("USER").unwrap_or_else(|_| "admin".to_string()),
                tags,
            };

            let response = client.register_schema(request).await?;
            let result = response.into_inner();

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

        SchemaCommands::Get {
            id,
            subject,
            version,
            output,
        } => {
            let response = if let Some(schema_id) = id {
                let request = GetSchemaRequest {
                    schema_id,
                    version,
                };
                client.get_schema(request).await?
            } else if let Some(subj) = subject {
                let request = GetLatestSchemaRequest { subject: subj };
                client.get_latest_schema(request).await?
            } else {
                return Err("Either --id or --subject must be provided".into());
            };

            let result = response.into_inner();

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
                    "compatibility_mode": result.compatibility_mode,
                });
                println!("{}", serde_json::to_string_pretty(&out)?);
            } else {
                println!("Schema ID: {}", result.schema_id);
                println!("Version: {}", result.version);
                println!("Subject: {}", result.subject);
                println!("Type: {}", result.schema_type);
                println!("Compatibility Mode: {}", result.compatibility_mode);
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

        SchemaCommands::Versions { subject, output } => {
            let request = ListVersionsRequest { subject: subject.clone() };
            let response = client.list_versions(request).await?;
            let versions = response.into_inner().versions;

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

        SchemaCommands::Check {
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
                return Err("Either --file or --schema must be provided".into());
            };

            let request = CheckCompatibilityRequest {
                subject: subject.clone(),
                new_schema_definition: schema_data,
                schema_type: schema_type.clone(),
                compatibility_mode: mode,
            };

            let response = client.check_compatibility(request).await?;
            let result = response.into_inner();

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

        SchemaCommands::GetCompatibility { subject, output } => {
            // Get latest schema to extract compatibility mode
            let request = GetLatestSchemaRequest {
                subject: subject.clone(),
            };

            let response = client.get_latest_schema(request).await?;
            let result = response.into_inner();

            if matches!(output.as_deref(), Some("json")) {
                let out = serde_json::json!({
                    "subject": subject,
                    "compatibility_mode": result.compatibility_mode,
                });
                println!("{}", serde_json::to_string_pretty(&out)?);
            } else {
                println!("Subject: {}", subject);
                println!("Compatibility Mode: {}", result.compatibility_mode);
            }
        }

        SchemaCommands::SetCompatibility { subject, mode } => {
            let request = SetCompatibilityModeRequest {
                subject: subject.clone(),
                compatibility_mode: mode.to_uppercase(),
            };

            let response = client.set_compatibility_mode(request).await?;
            let result = response.into_inner();

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

        SchemaCommands::Delete {
            subject,
            version,
            confirm,
        } => {
            if !confirm {
                return Err(
                    "Deletion requires --confirm flag to prevent accidental deletion".into(),
                );
            }

            let request = DeleteSchemaVersionRequest {
                subject: subject.clone(),
                version,
            };

            let response = client.delete_schema_version(request).await?;
            let result = response.into_inner();

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

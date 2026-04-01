use anyhow::Result;
use clap::{Args, Subcommand};
use danube_core::admin_proto::{
    BindingDefinition, CreateBindingRequest, CreateRoleRequest, DeleteBindingRequest,
    DeleteRoleRequest, GetBindingRequest, GetRoleRequest, ListBindingsRequest, RoleDefinition,
};

use crate::core::{AdminGrpcClient, GrpcClientConfig};

#[derive(Debug, Args)]
pub struct Security {
    #[command(subcommand)]
    command: SecurityCommands,
}

#[derive(Debug, Subcommand)]
enum SecurityCommands {
    /// Manage authorization roles
    #[command(subcommand)]
    Roles(RolesCommands),

    /// Manage authorization bindings
    #[command(subcommand)]
    Bindings(BindingsCommands),
}

// ── Roles ──────────────────────────────────────────────────────────────────

#[derive(Debug, Subcommand)]
enum RolesCommands {
    /// Create a new role with the given permissions
    #[command(
        after_help = "Examples:
  danube-admin security roles create producer-role --permissions Produce,Lookup
  danube-admin security roles create admin-role --permissions ManageCluster,ManageBroker,ManageNamespace,ManageTopic

Valid permissions:
  Lookup, Produce, Consume, Replicate, ManageNamespace, ManageTopic,
  ManageSchema, ManageBroker, ManageCluster"
    )]
    Create {
        /// Role name
        name: String,
        /// Comma-separated list of permissions
        #[arg(long)]
        permissions: String,
    },

    /// Get details of a role
    Get {
        /// Role name
        name: String,
        #[arg(long, value_parser = ["json"], help = "Output format")]
        output: Option<String>,
    },

    /// List all roles
    List {
        #[arg(long, value_parser = ["json"], help = "Output format")]
        output: Option<String>,
    },

    /// Delete a role
    Delete {
        /// Role name
        name: String,
    },
}

// ── Bindings ───────────────────────────────────────────────────────────────

#[derive(Debug, Subcommand)]
enum BindingsCommands {
    /// Create a new binding that grants roles to a principal
    #[command(
        after_help = "Examples:
  danube-admin security bindings create b1 \\
    --principal-type service_account --principal-name my-app \\
    --roles producer-role --scope cluster

  danube-admin security bindings create b2 \\
    --principal-type service_account --principal-name my-app \\
    --roles consumer-role --scope topic --resource /default/my-topic

Scopes: cluster, namespace, topic"
    )]
    Create {
        /// Binding ID (unique identifier)
        id: String,
        /// Principal type (e.g. service_account, user)
        #[arg(long)]
        principal_type: String,
        /// Principal name
        #[arg(long)]
        principal_name: String,
        /// Comma-separated list of role names to grant
        #[arg(long)]
        roles: String,
        /// Scope: cluster, namespace, or topic
        #[arg(long)]
        scope: String,
        /// Resource name (empty for cluster scope, namespace name, or topic path)
        #[arg(long, default_value = "")]
        resource: String,
    },

    /// Get details of a binding
    Get {
        /// Binding ID
        id: String,
        /// Scope
        #[arg(long)]
        scope: String,
        /// Resource name
        #[arg(long, default_value = "")]
        resource: String,
        #[arg(long, value_parser = ["json"], help = "Output format")]
        output: Option<String>,
    },

    /// List bindings for a scope
    List {
        /// Scope: cluster, namespace, or topic
        #[arg(long)]
        scope: String,
        /// Resource name (empty for cluster scope)
        #[arg(long, default_value = "")]
        resource: String,
        #[arg(long, value_parser = ["json"], help = "Output format")]
        output: Option<String>,
    },

    /// Delete a binding
    Delete {
        /// Binding ID
        id: String,
        /// Scope
        #[arg(long)]
        scope: String,
        /// Resource name
        #[arg(long, default_value = "")]
        resource: String,
    },
}

// ── Handler ────────────────────────────────────────────────────────────────

pub async fn handle(security: Security, endpoint: &str) -> Result<()> {
    let config = GrpcClientConfig {
        endpoint: endpoint.to_string(),
        ..Default::default()
    };
    let client = AdminGrpcClient::connect(config).await?;

    match security.command {
        SecurityCommands::Roles(cmd) => handle_roles(cmd, &client).await,
        SecurityCommands::Bindings(cmd) => handle_bindings(cmd, &client).await,
    }
}

async fn handle_roles(cmd: RolesCommands, client: &AdminGrpcClient) -> Result<()> {
    match cmd {
        RolesCommands::Create { name, permissions } => {
            let perms: Vec<String> = permissions.split(',').map(|s| s.trim().to_string()).collect();
            let resp = client
                .create_role(CreateRoleRequest {
                    role: Some(RoleDefinition {
                        name: name.clone(),
                        permissions: perms,
                        system: false,
                    }),
                })
                .await?;
            println!("Role '{}' created: {}", name, resp.success);
        }

        RolesCommands::Get { name, output } => {
            let resp = client
                .get_role(GetRoleRequest { name: name.clone() })
                .await?;
            if let Some(role) = resp.role {
                if matches!(output.as_deref(), Some("json")) {
                    println!("{}", serde_json::to_string_pretty(&role_to_json(&role))?);
                } else {
                    print_role(&role);
                }
            } else {
                println!("Role '{}' not found", name);
            }
        }

        RolesCommands::List { output } => {
            let resp = client.list_roles().await?;
            if matches!(output.as_deref(), Some("json")) {
                let roles: Vec<_> = resp.roles.iter().map(role_to_json).collect();
                println!("{}", serde_json::to_string_pretty(&roles)?);
            } else if resp.roles.is_empty() {
                println!("No roles found.");
            } else {
                for role in &resp.roles {
                    print_role(role);
                    println!();
                }
            }
        }

        RolesCommands::Delete { name } => {
            let resp = client
                .delete_role(DeleteRoleRequest { name: name.clone() })
                .await?;
            println!("Role '{}' deleted: {}", name, resp.success);
        }
    }
    Ok(())
}

async fn handle_bindings(cmd: BindingsCommands, client: &AdminGrpcClient) -> Result<()> {
    match cmd {
        BindingsCommands::Create {
            id,
            principal_type,
            principal_name,
            roles,
            scope,
            resource,
        } => {
            let role_names: Vec<String> = roles.split(',').map(|s| s.trim().to_string()).collect();
            let resp = client
                .create_binding(CreateBindingRequest {
                    binding: Some(BindingDefinition {
                        id: id.clone(),
                        principal_type,
                        principal_name,
                        role_names,
                        scope,
                        resource_name: resource,
                    }),
                })
                .await?;
            println!("Binding '{}' created: {}", id, resp.success);
        }

        BindingsCommands::Get {
            id,
            scope,
            resource,
            output,
        } => {
            let resp = client
                .get_binding(GetBindingRequest {
                    scope,
                    resource_name: resource,
                    binding_id: id.clone(),
                })
                .await?;
            if let Some(binding) = resp.binding {
                if matches!(output.as_deref(), Some("json")) {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&binding_to_json(&binding))?
                    );
                } else {
                    print_binding(&binding);
                }
            } else {
                println!("Binding '{}' not found", id);
            }
        }

        BindingsCommands::List {
            scope,
            resource,
            output,
        } => {
            let resp = client
                .list_bindings(ListBindingsRequest {
                    scope,
                    resource_name: resource,
                })
                .await?;
            if matches!(output.as_deref(), Some("json")) {
                let bindings: Vec<_> = resp.bindings.iter().map(binding_to_json).collect();
                println!("{}", serde_json::to_string_pretty(&bindings)?);
            } else if resp.bindings.is_empty() {
                println!("No bindings found.");
            } else {
                for binding in &resp.bindings {
                    print_binding(binding);
                    println!();
                }
            }
        }

        BindingsCommands::Delete {
            id,
            scope,
            resource,
        } => {
            let resp = client
                .delete_binding(DeleteBindingRequest {
                    scope,
                    resource_name: resource,
                    binding_id: id.clone(),
                })
                .await?;
            println!("Binding '{}' deleted: {}", id, resp.success);
        }
    }
    Ok(())
}

// ── Display helpers ────────────────────────────────────────────────────────

fn print_role(role: &RoleDefinition) {
    println!("Role: {}", role.name);
    println!("  Permissions: {}", role.permissions.join(", "));
    if role.system {
        println!("  System: true");
    }
}

fn print_binding(b: &BindingDefinition) {
    println!("Binding: {}", b.id);
    println!("  Principal: {}:{}", b.principal_type, b.principal_name);
    println!("  Roles: {}", b.role_names.join(", "));
    println!(
        "  Scope: {}{}",
        b.scope,
        if b.resource_name.is_empty() {
            String::new()
        } else {
            format!(" ({})", b.resource_name)
        }
    );
}

fn role_to_json(role: &RoleDefinition) -> serde_json::Value {
    serde_json::json!({
        "name": role.name,
        "permissions": role.permissions,
        "system": role.system,
    })
}

fn binding_to_json(b: &BindingDefinition) -> serde_json::Value {
    serde_json::json!({
        "id": b.id,
        "principal_type": b.principal_type,
        "principal_name": b.principal_name,
        "role_names": b.role_names,
        "scope": b.scope,
        "resource_name": b.resource_name,
    })
}

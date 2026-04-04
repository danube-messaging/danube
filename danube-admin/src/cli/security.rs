use anyhow::Result;
use clap::{Args, Subcommand};
use danube_core::admin_proto::{
    BindingDefinition, CreateBindingRequest, CreateRoleRequest, DeleteBindingRequest,
    DeleteRoleRequest, GetBindingRequest, GetRoleRequest, ListBindingsRequest, RoleDefinition,
};
use danube_core::jwt;
use std::time::{SystemTime, UNIX_EPOCH};

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

    /// Manage authentication tokens (offline JWT operations)
    #[command(subcommand)]
    Tokens(TokensCommands),
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

// ── Tokens ─────────────────────────────────────────────────────────────────

#[derive(Debug, Subcommand)]
enum TokensCommands {
    /// Create a signed JWT token (offline — no broker connection needed)
    #[command(
        after_help = "Examples:
  danube-admin security tokens create --subject my-app --secret-key your-secret-key
  danube-admin security tokens create --subject admin-user --type user --ttl 24h --secret-key '$JWT_SECRET'
  danube-admin security tokens create --subject my-app --ttl 365d --secret-key your-secret-key

TTL format: number followed by h (hours) or d (days). Default: 8760h (1 year)"
    )]
    Create {
        /// Principal name (e.g. my-app, admin-user)
        #[arg(long)]
        subject: String,

        /// JWT signing secret key (must match broker's jwt.secret_key)
        #[arg(long, env = "DANUBE_JWT_SECRET_KEY")]
        secret_key: String,

        /// Token time-to-live (e.g. 8760h, 365d). Default: 8760h (1 year)
        #[arg(long, default_value = "8760h")]
        ttl: String,

        /// Principal type: service_account or user
        #[arg(long, default_value = "service_account")]
        r#type: String,

        /// Token issuer (should match broker's jwt.issuer)
        #[arg(long, default_value = "danube-auth")]
        issuer: String,
    },

    /// Validate a JWT token and display its claims (offline — no broker connection needed)
    Validate {
        /// JWT token to validate
        #[arg(long)]
        token: String,

        /// JWT signing secret key
        #[arg(long, env = "DANUBE_JWT_SECRET_KEY")]
        secret_key: String,
    },
}

// ── Handler ────────────────────────────────────────────────────────────────

pub async fn handle(security: Security, endpoint: &str) -> Result<()> {
    match security.command {
        SecurityCommands::Tokens(cmd) => {
            // Tokens are purely local/offline — no broker connection needed
            handle_tokens(cmd)
        }
        _ => {
            let config = GrpcClientConfig {
                endpoint: endpoint.to_string(),
                ..Default::default()
            };
            let client = AdminGrpcClient::connect(config).await?;
            match security.command {
                SecurityCommands::Roles(cmd) => handle_roles(cmd, &client).await,
                SecurityCommands::Bindings(cmd) => handle_bindings(cmd, &client).await,
                SecurityCommands::Tokens(_) => unreachable!(),
            }
        }
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

// ── Token helpers ──────────────────────────────────────────────────────────

fn handle_tokens(cmd: TokensCommands) -> Result<()> {
    match cmd {
        TokensCommands::Create {
            subject,
            secret_key,
            ttl,
            r#type,
            issuer,
        } => {
            let ttl_seconds = parse_ttl(&ttl)?;

            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| anyhow::anyhow!("System clock error"))?
                .as_secs();

            let claims = jwt::Claims {
                iss: issuer,
                exp: now + ttl_seconds,
                sub: subject.clone(),
                principal_type: r#type.clone(),
                principal_name: subject,
            };

            let token = jwt::create_token(&claims, &secret_key)
                .map_err(|e| anyhow::anyhow!("Failed to create token: {}", e))?;

            // Print only the token to stdout (for piping/scripting)
            println!("{}", token);
        }

        TokensCommands::Validate { token, secret_key } => {
            match jwt::validate_token(&token, &secret_key) {
                Ok(claims) => {
                    let exp_time = chrono::DateTime::from_timestamp(claims.exp as i64, 0)
                        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                        .unwrap_or_else(|| "invalid".to_string());

                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    let remaining = if claims.exp > now {
                        let secs = claims.exp - now;
                        let hours = secs / 3600;
                        let days = hours / 24;
                        if days > 0 {
                            format!("{}d {}h", days, hours % 24)
                        } else {
                            format!("{}h", hours)
                        }
                    } else {
                        "EXPIRED".to_string()
                    };

                    println!("Token is valid.");
                    println!("  Subject:        {}", claims.sub);
                    println!("  Principal type:  {}", claims.principal_type);
                    println!("  Principal name:  {}", claims.principal_name);
                    println!("  Issuer:          {}", claims.iss);
                    println!("  Expires:         {}", exp_time);
                    println!("  Remaining:       {}", remaining);
                }
                Err(e) => {
                    eprintln!("Token validation failed: {}", e);
                    std::process::exit(1);
                }
            }
        }
    }
    Ok(())
}

/// Parse a TTL string like "8760h", "365d", "24h" into seconds.
fn parse_ttl(ttl: &str) -> Result<u64> {
    let ttl = ttl.trim();
    if let Some(hours) = ttl.strip_suffix('h') {
        let h: u64 = hours
            .parse()
            .map_err(|_| anyhow::anyhow!("Invalid TTL: '{}'", ttl))?;
        Ok(h * 3600)
    } else if let Some(days) = ttl.strip_suffix('d') {
        let d: u64 = days
            .parse()
            .map_err(|_| anyhow::anyhow!("Invalid TTL: '{}'", ttl))?;
        Ok(d * 86400)
    } else {
        // Assume seconds if no suffix
        ttl.parse::<u64>()
            .map_err(|_| anyhow::anyhow!("Invalid TTL: '{}'. Use h (hours) or d (days) suffix.", ttl))
    }
}

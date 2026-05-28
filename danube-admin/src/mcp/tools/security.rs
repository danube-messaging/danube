//! Security management tools (roles, bindings, tokens)

use crate::core::AdminGrpcClient;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

// ── Roles ──────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct CreateRoleParams {
    /// Role name. Must be unique across the cluster.
    /// Example: "producer-role", "admin-role"
    pub name: String,

    /// Comma-separated list of permissions to grant.
    /// Valid permissions: Lookup, Produce, Consume, Replicate,
    /// ManageNamespace, ManageTopic, ManageSchema, ManageBroker, ManageCluster.
    /// Example: "Produce,Lookup", "ManageCluster,ManageBroker"
    pub permissions: String,
}

pub async fn create_role(client: &Arc<AdminGrpcClient>, params: CreateRoleParams) -> String {
    let perms: Vec<String> = params
        .permissions
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();

    let req = danube_core::admin_proto::CreateRoleRequest {
        role: Some(danube_core::admin_proto::RoleDefinition {
            name: params.name.clone(),
            permissions: perms,
            system: false,
        }),
    };

    match client.create_role(req).await {
        Ok(response) => {
            if response.success {
                format!("✓ Role '{}' created successfully", params.name)
            } else {
                format!("✗ Failed to create role '{}'", params.name)
            }
        }
        Err(e) => format!("Error creating role: {}", e),
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct GetRoleParams {
    /// Role name to retrieve.
    /// Example: "producer-role", "admin-role"
    pub name: String,
}

pub async fn get_role(client: &Arc<AdminGrpcClient>, params: GetRoleParams) -> String {
    let req = danube_core::admin_proto::GetRoleRequest {
        name: params.name.clone(),
    };

    match client.get_role(req).await {
        Ok(response) => {
            if let Some(role) = response.role {
                let mut output = format!("Role: {}\n", role.name);
                output.push_str(&format!("  Permissions: {}\n", role.permissions.join(", ")));
                if role.system {
                    output.push_str("  System: true\n");
                }
                output
            } else {
                format!("Role '{}' not found", params.name)
            }
        }
        Err(e) => format!("Error getting role: {}", e),
    }
}

pub async fn list_roles(client: &Arc<AdminGrpcClient>) -> String {
    match client.list_roles().await {
        Ok(response) => {
            if response.roles.is_empty() {
                return "No roles found.".to_string();
            }

            let mut output = format!("Found {} role(s):\n\n", response.roles.len());
            for role in &response.roles {
                output.push_str(&format!("Role: {}\n", role.name));
                output.push_str(&format!("  Permissions: {}\n", role.permissions.join(", ")));
                if role.system {
                    output.push_str("  System: true\n");
                }
                output.push('\n');
            }
            output
        }
        Err(e) => format!("Error listing roles: {}", e),
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct DeleteRoleParams {
    /// Role name to delete.
    /// WARNING: Deleting a role will remove it from all bindings that reference it.
    /// Example: "producer-role"
    pub name: String,
}

pub async fn delete_role(client: &Arc<AdminGrpcClient>, params: DeleteRoleParams) -> String {
    let req = danube_core::admin_proto::DeleteRoleRequest {
        name: params.name.clone(),
    };

    match client.delete_role(req).await {
        Ok(response) => {
            if response.success {
                format!("✓ Role '{}' deleted successfully", params.name)
            } else {
                format!("✗ Failed to delete role '{}'", params.name)
            }
        }
        Err(e) => format!("Error deleting role: {}", e),
    }
}

// ── Bindings ───────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct CreateBindingParams {
    /// Unique binding identifier.
    /// Example: "b1", "my-app-producer-binding"
    pub id: String,

    /// Principal type: "service_account" or "user".
    /// Example: "service_account"
    pub principal_type: String,

    /// Principal name — the identity being granted roles.
    /// Example: "my-app", "admin-user"
    pub principal_name: String,

    /// Comma-separated list of role names to grant.
    /// Roles must already exist (use create_role first).
    /// Example: "producer-role", "producer-role,consumer-role"
    pub roles: String,

    /// Authorization scope: "cluster", "namespace", or "topic".
    /// Determines the granularity of the permission grant.
    pub scope: String,

    /// Resource name for namespace/topic scopes.
    /// Empty for cluster scope; namespace name for namespace scope;
    /// full topic path for topic scope.
    /// Example: "default", "/default/my-topic"
    pub resource: Option<String>,
}

pub async fn create_binding(
    client: &Arc<AdminGrpcClient>,
    params: CreateBindingParams,
) -> String {
    let role_names: Vec<String> = params
        .roles
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();

    let req = danube_core::admin_proto::CreateBindingRequest {
        binding: Some(danube_core::admin_proto::BindingDefinition {
            id: params.id.clone(),
            principal_type: params.principal_type,
            principal_name: params.principal_name,
            role_names,
            scope: params.scope,
            resource_name: params.resource.unwrap_or_default(),
        }),
    };

    match client.create_binding(req).await {
        Ok(response) => {
            if response.success {
                format!("✓ Binding '{}' created successfully", params.id)
            } else {
                format!("✗ Failed to create binding '{}'", params.id)
            }
        }
        Err(e) => format!("Error creating binding: {}", e),
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct GetBindingParams {
    /// Binding ID to retrieve.
    /// Example: "b1", "my-app-producer-binding"
    pub id: String,

    /// Scope where the binding was created: "cluster", "namespace", or "topic".
    pub scope: String,

    /// Resource name (empty for cluster scope).
    /// Example: "default", "/default/my-topic"
    pub resource: Option<String>,
}

pub async fn get_binding(client: &Arc<AdminGrpcClient>, params: GetBindingParams) -> String {
    let req = danube_core::admin_proto::GetBindingRequest {
        binding_id: params.id.clone(),
        scope: params.scope,
        resource_name: params.resource.unwrap_or_default(),
    };

    match client.get_binding(req).await {
        Ok(response) => {
            if let Some(binding) = response.binding {
                let mut output = format!("Binding: {}\n", binding.id);
                output.push_str(&format!(
                    "  Principal: {}:{}\n",
                    binding.principal_type, binding.principal_name
                ));
                output.push_str(&format!("  Roles: {}\n", binding.role_names.join(", ")));
                output.push_str(&format!("  Scope: {}", binding.scope));
                if !binding.resource_name.is_empty() {
                    output.push_str(&format!(" ({})", binding.resource_name));
                }
                output.push('\n');
                output
            } else {
                format!("Binding '{}' not found", params.id)
            }
        }
        Err(e) => format!("Error getting binding: {}", e),
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct ListBindingsParams {
    /// Scope to list bindings for: "cluster", "namespace", or "topic".
    pub scope: String,

    /// Resource name (empty for cluster scope).
    /// Example: "default", "/default/my-topic"
    pub resource: Option<String>,
}

pub async fn list_bindings(
    client: &Arc<AdminGrpcClient>,
    params: ListBindingsParams,
) -> String {
    let req = danube_core::admin_proto::ListBindingsRequest {
        scope: params.scope.clone(),
        resource_name: params.resource.clone().unwrap_or_default(),
    };

    match client.list_bindings(req).await {
        Ok(response) => {
            if response.bindings.is_empty() {
                return format!(
                    "No bindings found for scope '{}'{}",
                    params.scope,
                    params
                        .resource
                        .as_deref()
                        .filter(|r| !r.is_empty())
                        .map(|r| format!(" resource '{}'", r))
                        .unwrap_or_default()
                );
            }

            let mut output = format!("Found {} binding(s):\n\n", response.bindings.len());
            for binding in &response.bindings {
                output.push_str(&format!("Binding: {}\n", binding.id));
                output.push_str(&format!(
                    "  Principal: {}:{}\n",
                    binding.principal_type, binding.principal_name
                ));
                output.push_str(&format!("  Roles: {}\n", binding.role_names.join(", ")));
                output.push_str(&format!("  Scope: {}", binding.scope));
                if !binding.resource_name.is_empty() {
                    output.push_str(&format!(" ({})", binding.resource_name));
                }
                output.push_str("\n\n");
            }
            output
        }
        Err(e) => format!("Error listing bindings: {}", e),
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct DeleteBindingParams {
    /// Binding ID to delete.
    /// Example: "b1", "my-app-producer-binding"
    pub id: String,

    /// Scope where the binding was created: "cluster", "namespace", or "topic".
    pub scope: String,

    /// Resource name (empty for cluster scope).
    /// Example: "default", "/default/my-topic"
    pub resource: Option<String>,
}

pub async fn delete_binding(
    client: &Arc<AdminGrpcClient>,
    params: DeleteBindingParams,
) -> String {
    let req = danube_core::admin_proto::DeleteBindingRequest {
        binding_id: params.id.clone(),
        scope: params.scope,
        resource_name: params.resource.unwrap_or_default(),
    };

    match client.delete_binding(req).await {
        Ok(response) => {
            if response.success {
                format!("✓ Binding '{}' deleted successfully", params.id)
            } else {
                format!("✗ Failed to delete binding '{}'", params.id)
            }
        }
        Err(e) => format!("Error deleting binding: {}", e),
    }
}

// ── Tokens (offline JWT) ───────────────────────────────────────────────────

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct CreateTokenParams {
    /// Principal name (subject) for the token.
    /// Example: "my-app", "admin-user"
    pub subject: String,

    /// JWT signing secret key (must match broker's jwt.secret_key).
    /// Keep this secure — anyone with this key can create valid tokens.
    pub secret_key: String,

    /// Token time-to-live. Format: number followed by 'h' (hours) or 'd' (days).
    /// Default: "8760h" (1 year).
    /// Examples: "24h", "365d", "8760h"
    pub ttl: Option<String>,

    /// Principal type: "service_account" or "user".
    /// Default: "service_account"
    pub principal_type: Option<String>,

    /// Token issuer (should match broker's jwt.issuer).
    /// Default: "danube-auth"
    pub issuer: Option<String>,
}

fn parse_ttl(ttl: &str) -> Result<u64, String> {
    let ttl = ttl.trim();
    if let Some(hours) = ttl.strip_suffix('h') {
        hours
            .parse::<u64>()
            .map(|h| h * 3600)
            .map_err(|_| format!("Invalid TTL: '{}'", ttl))
    } else if let Some(days) = ttl.strip_suffix('d') {
        days.parse::<u64>()
            .map(|d| d * 86400)
            .map_err(|_| format!("Invalid TTL: '{}'", ttl))
    } else {
        ttl.parse::<u64>()
            .map_err(|_| format!("Invalid TTL: '{}'. Use h (hours) or d (days) suffix.", ttl))
    }
}

pub fn create_token(params: CreateTokenParams) -> String {
    let ttl_str = params.ttl.as_deref().unwrap_or("8760h");
    let ttl_seconds = match parse_ttl(ttl_str) {
        Ok(s) => s,
        Err(e) => return format!("Error: {}", e),
    };

    let principal_type = params
        .principal_type
        .unwrap_or_else(|| "service_account".to_string());
    let issuer = params
        .issuer
        .unwrap_or_else(|| "danube-auth".to_string());

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let claims = danube_core::jwt::Claims {
        iss: issuer,
        exp: now + ttl_seconds,
        sub: params.subject.clone(),
        principal_type: principal_type.clone(),
        principal_name: params.subject.clone(),
    };

    match danube_core::jwt::create_token(&claims, &params.secret_key) {
        Ok(token) => {
            let exp_time = chrono::DateTime::from_timestamp((now + ttl_seconds) as i64, 0)
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                .unwrap_or_else(|| "unknown".to_string());

            format!(
                "✓ Token created successfully\n\
                 Subject: {}\n\
                 Principal Type: {}\n\
                 Expires: {}\n\
                 TTL: {}\n\n\
                 Token:\n{}",
                params.subject, principal_type, exp_time, ttl_str, token
            )
        }
        Err(e) => format!("Error creating token: {}", e),
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct ValidateTokenParams {
    /// JWT token string to validate.
    pub token: String,

    /// JWT signing secret key used to validate the token.
    pub secret_key: String,
}

pub fn validate_token(params: ValidateTokenParams) -> String {
    match danube_core::jwt::validate_token(&params.token, &params.secret_key) {
        Ok(claims) => {
            let exp_time = chrono::DateTime::from_timestamp(claims.exp as i64, 0)
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                .unwrap_or_else(|| "invalid".to_string());

            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
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

            format!(
                "✓ Token is valid\n\
                 Subject: {}\n\
                 Principal Type: {}\n\
                 Principal Name: {}\n\
                 Issuer: {}\n\
                 Expires: {}\n\
                 Remaining: {}",
                claims.sub,
                claims.principal_type,
                claims.principal_name,
                claims.iss,
                exp_time,
                remaining
            )
        }
        Err(e) => format!("✗ Token validation failed: {}", e),
    }
}

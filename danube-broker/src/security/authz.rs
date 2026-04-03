//! Authorization: RBAC types, scoped bindings, and the policy evaluation engine.
//!
//! This module handles:
//! - **Permission** — the 9 action types (Lookup, Produce, Consume, etc.)
//! - **Resource** — the 5 resource types (Cluster, Broker, Namespace, Topic, SchemaSubject)
//! - **Role** — named set of permissions, persisted in metadata
//! - **Binding** — attaches roles to principals at a specific scope
//! - **Authorizer** — `enforce_authorization()` evaluates bindings across scopes with default-deny

use crate::resources::SecurityResources;
use crate::security::authn::{Principal, SecurityContext};
use serde::{Deserialize, Serialize};
use tonic::Status;
use tracing::warn;

// ── Types ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum Permission {
    Lookup,
    Produce,
    Consume,
    Replicate,
    ManageNamespace,
    ManageTopic,
    ManageSchema,
    ManageBroker,
    ManageCluster,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Resource {
    Cluster,
    Broker(String),
    Namespace(String),
    Topic(String),
    SchemaSubject(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Role {
    pub(crate) name: String,
    pub(crate) permissions: Vec<Permission>,
    pub(crate) system: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Binding {
    pub(crate) id: String,
    pub(crate) principal_type: String,
    pub(crate) principal_name: String,
    pub(crate) role_names: Vec<String>,
    pub(crate) scope: String,
    pub(crate) resource_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AuthorizationDecision {
    pub(crate) allowed: bool,
    pub(crate) reason: String,
}

impl AuthorizationDecision {
    pub(crate) fn allow(reason: impl Into<String>) -> Self {
        Self {
            allowed: true,
            reason: reason.into(),
        }
    }

    pub(crate) fn deny(reason: impl Into<String>) -> Self {
        Self {
            allowed: false,
            reason: reason.into(),
        }
    }
}

// ── Authorizer ─────────────────────────────────────────────────────────────

/// Map a `Resource` enum to the (scope, resource_name) pair used by `SecurityResources`
/// for binding lookups.
fn resource_to_scope(resource: &Resource) -> (&'static str, String) {
    match resource {
        Resource::Cluster => ("cluster", String::new()),
        Resource::Broker(_) => ("cluster", String::new()),
        Resource::Namespace(ns) => ("namespace", ns.clone()),
        Resource::Topic(topic) => ("topic", topic.clone()),
        Resource::SchemaSubject(subject) => ("topic", subject.clone()),
    }
}

pub(crate) async fn enforce_authorization(
    context: &SecurityContext,
    resource: &Resource,
    permission: Permission,
    security: &SecurityResources,
) -> Result<(), Status> {
    let decision = authorize(context, resource, permission, security).await;
    if decision.allowed {
        Ok(())
    } else {
        Err(Status::permission_denied(decision.reason))
    }
}

async fn authorize(
    context: &SecurityContext,
    resource: &Resource,
    permission: Permission,
    security: &SecurityResources,
) -> AuthorizationDecision {
    let principal = &context.principal;

    // BrokerInternal is super-admin — always allowed
    if matches!(principal, Principal::BrokerInternal { .. }) {
        return AuthorizationDecision::allow(format!(
            "BrokerInternal {} is super-admin",
            principal.principal_name(),
        ));
    }

    // Anonymous — deny when we get here (auth-disabled mode never calls enforce)
    if matches!(principal, Principal::Anonymous) {
        return AuthorizationDecision::deny("anonymous principals are not authorized");
    }

    // Resolve scope for binding lookup
    let (scope, resource_name) = resource_to_scope(resource);

    // Fetch all bindings that could grant access (cluster + parent scopes)
    let bindings = match security.list_bindings_for_resource(scope, &resource_name).await {
        Ok(b) => b,
        Err(e) => {
            warn!("failed to read authorization bindings: {}", e);
            return AuthorizationDecision::deny(format!(
                "authorization check failed: {}",
                e
            ));
        }
    };

    let p_type = principal.principal_type();
    let p_name = principal.principal_name();

    // Filter bindings matching this principal
    for binding in &bindings {
        if binding.principal_type != p_type || binding.principal_name != p_name {
            continue;
        }

        // Resolve each role referenced by the binding
        for role_name in &binding.role_names {
            match security.get_role(role_name).await {
                Ok(Some(role)) => {
                    if role.permissions.contains(&permission) {
                        return AuthorizationDecision::allow(format!(
                            "granted by binding '{}' via role '{}' (scope={})",
                            binding.id, role.name, binding.scope,
                        ));
                    }
                }
                Ok(None) => {
                    warn!(
                        role = %role_name,
                        binding_id = %binding.id,
                        "binding references non-existent role"
                    );
                }
                Err(e) => {
                    warn!(role = %role_name, "failed to read role: {}", e);
                }
            }
        }
    }

    // No binding granted the permission — default deny
    AuthorizationDecision::deny(format!(
        "no binding grants {:?} on {:?} for {}:{}",
        permission, resource, p_type, p_name,
    ))
}

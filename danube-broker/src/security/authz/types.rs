use serde::{Deserialize, Serialize};

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

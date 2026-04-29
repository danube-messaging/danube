//! Broker-side implementation of `EdgeAuth` trait.
//!
//! Uses the broker's JWT config for token validation and `EdgeResources`
//! for registration persistence in Raft.


use anyhow::{anyhow, Result};
use async_trait::async_trait;
use tracing::debug;

use danube_edge::cluster::auth::{EdgeAuth, EdgeRegistration};

use crate::resources::edge::EdgeResources;
use crate::security::config::AuthConfig;

/// Broker-side implementation of the `EdgeAuth` trait.
///
/// Validates edge JWT tokens using the broker's JWT configuration and
/// persists registrations via `EdgeResources` (Raft metadata store).
pub(crate) struct BrokerEdgeAuth {
    edge_resources: EdgeResources,
    auth_config: AuthConfig,
}

impl BrokerEdgeAuth {
    pub(crate) fn new(edge_resources: EdgeResources, auth_config: AuthConfig) -> Self {
        Self {
            edge_resources,
            auth_config,
        }
    }
}

#[async_trait]
impl EdgeAuth for BrokerEdgeAuth {
    async fn validate_edge_token(&self, edge_name: &str, token: &str) -> Result<String> {
        // If auth is disabled, trust the edge name directly
        if self.auth_config.is_auth_disabled() {
            debug!(
                edge_name = %edge_name,
                "auth disabled — accepting edge registration without token validation"
            );
            let namespace = format!("/{}", edge_name);
            return Ok(namespace);
        }

        // Validate the JWT token
        let jwt_config = self
            .auth_config
            .jwt_config()
            .ok_or_else(|| anyhow!("JWT is not configured on this broker"))?;

        let claims = danube_core::jwt::validate_token(token, &jwt_config.secret_key)
            .map_err(|e| anyhow!("JWT validation failed: {}", e))?;

        // Verify the token subject matches the edge name
        if claims.sub != edge_name {
            return Err(anyhow!(
                "token subject '{}' does not match edge name '{}'",
                claims.sub,
                edge_name
            ));
        }

        let namespace = format!("/{}", edge_name);
        Ok(namespace)
    }

    async fn is_edge_registered(&self, edge_name: &str) -> bool {
        self.edge_resources.is_edge_registered(edge_name).await
    }

    async fn get_edge_namespace(&self, edge_name: &str) -> Option<String> {
        self.edge_resources.get_edge_namespace(edge_name).await
    }

    async fn register_edge(&self, registration: &EdgeRegistration) -> Result<()> {
        self.edge_resources.register_edge(registration).await
    }
}

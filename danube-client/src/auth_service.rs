use crate::{
    connection_manager::ConnectionManager,
    errors::{DanubeError, Result},
};

use std::sync::Arc;
use tonic::{metadata::MetadataValue, transport::Uri, Request};

const INTERNAL_BROKER_HEADER: &str = "x-danube-internal-broker";

/// The `AuthService` struct provides methods for authenticating clients with the Danube messaging system.
///
/// With JWT-first authentication, the client uses a pre-generated JWT token (from
/// `danube-admin security tokens create`) that is sent as `Authorization: Bearer <token>`
/// on every gRPC request.
#[derive(Debug, Clone)]
pub struct AuthService {
    cnx_manager: Arc<ConnectionManager>,
}

impl AuthService {
    pub fn new(cnx_manager: Arc<ConnectionManager>) -> Self {
        AuthService { cnx_manager }
    }

    /// Insert an authentication token into a gRPC request if a token is configured.
    ///
    /// This is the single entry point for auth token insertion across the client.
    /// It attaches the JWT as a Bearer token in the request metadata.
    pub async fn insert_token_if_needed<T>(
        &self,
        token: Option<&str>,
        request: &mut Request<T>,
        _addr: &Uri,
    ) -> Result<()> {
        if let Some(token) = token {
            let bearer = MetadataValue::try_from(format!("Bearer {}", token))
                .map_err(|_| DanubeError::InvalidToken)?;
            request.metadata_mut().insert("authorization", bearer);
        }

        if let Some(internal_broker) = self.cnx_manager.connection_options.internal_broker.as_deref()
        {
            let internal_broker_metadata = MetadataValue::try_from(internal_broker)
                .map_err(|_| DanubeError::InvalidToken)?;
            request
                .metadata_mut()
                .insert(INTERNAL_BROKER_HEADER, internal_broker_metadata);
        }
        Ok(())
    }
}

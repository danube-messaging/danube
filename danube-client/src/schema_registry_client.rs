use std::sync::Arc;

use crate::{
    auth_service::AuthService,
    connection_manager::ConnectionManager,
    errors::{DanubeError, Result},
    schema_types::{CompatibilityMode, SchemaInfo, SchemaType},
};
use danube_core::proto::danube_schema::{
    schema_registry_client::SchemaRegistryClient as GrpcSchemaRegistryClient,
    CheckCompatibilityRequest, CheckCompatibilityResponse, GetLatestSchemaRequest,
    GetSchemaRequest, ListVersionsRequest, RegisterSchemaRequest, RegisterSchemaResponse,
    SetCompatibilityModeRequest, SetCompatibilityModeResponse,
};
use tonic::transport::Uri;

/// Client for interacting with the Danube Schema Registry.
///
/// Obtained via [`DanubeClient::schema()`]. Provides methods for registering,
/// retrieving, and managing schemas in the centralized schema registry.
///
/// Follows the same connection pattern as other Danube services — a fresh gRPC
/// connection is obtained from the shared `ConnectionManager` on each call.
#[derive(Debug, Clone)]
pub struct SchemaRegistryClient {
    cnx_manager: Arc<ConnectionManager>,
    auth_service: AuthService,
    uri: Uri,
}

impl SchemaRegistryClient {
    /// Create a new `SchemaRegistryClient` from shared connection infrastructure.
    pub(crate) fn new(
        cnx_manager: Arc<ConnectionManager>,
        auth_service: AuthService,
        uri: Uri,
    ) -> Self {
        SchemaRegistryClient {
            cnx_manager,
            auth_service,
            uri,
        }
    }

    /// Get a gRPC client and authenticated request for a schema registry call.
    async fn prepare_request<T>(
        &self,
        request: T,
    ) -> Result<(
        tonic::Request<T>,
        GrpcSchemaRegistryClient<tonic::transport::Channel>,
    )> {
        let grpc_cnx = self
            .cnx_manager
            .get_connection(&self.uri, &self.uri)
            .await?;
        let client = GrpcSchemaRegistryClient::new(grpc_cnx.grpc_cnx.clone());
        let mut req = tonic::Request::new(request);
        self.auth_service
            .insert_token_if_needed(
                self.cnx_manager.connection_options.api_key.as_deref(),
                &mut req,
                &self.uri,
            )
            .await?;
        Ok((req, client))
    }

    /// Register a new schema or get existing schema ID.
    ///
    /// Returns a builder for configuring schema registration.
    pub fn register_schema(&self, subject: impl Into<String>) -> SchemaRegistrationBuilder<'_> {
        SchemaRegistrationBuilder {
            client: self,
            subject: subject.into(),
            schema_type: None,
            schema_data: None,
        }
    }

    /// Get schema by ID.
    ///
    /// Returns schema information for the given schema ID.
    /// Schema ID identifies a subject (not a specific version).
    pub async fn get_schema_by_id(&self, schema_id: u64) -> Result<SchemaInfo> {
        let request = GetSchemaRequest {
            schema_id,
            version: None,
        };
        let (req, mut client) = self.prepare_request(request).await?;
        let response = client
            .get_schema(req)
            .await
            .map_err(|status| DanubeError::FromStatus(status))?
            .into_inner();
        Ok(SchemaInfo::from(response))
    }

    /// Get specific schema version.
    ///
    /// Returns schema information for a specific version of a schema subject.
    pub async fn get_schema_version(
        &self,
        schema_id: u64,
        version: Option<u32>,
    ) -> Result<SchemaInfo> {
        let request = GetSchemaRequest { schema_id, version };
        let (req, mut client) = self.prepare_request(request).await?;
        let response = client
            .get_schema(req)
            .await
            .map_err(|status| DanubeError::FromStatus(status))?
            .into_inner();
        Ok(SchemaInfo::from(response))
    }

    /// Get latest schema for a subject.
    ///
    /// Returns the latest schema version for the given subject.
    pub async fn get_latest_schema(&self, subject: impl Into<String>) -> Result<SchemaInfo> {
        let request = GetLatestSchemaRequest {
            subject: subject.into(),
        };
        let (req, mut client) = self.prepare_request(request).await?;
        let response = client
            .get_latest_schema(req)
            .await
            .map_err(|status| DanubeError::FromStatus(status))?
            .into_inner();
        Ok(SchemaInfo::from(response))
    }

    /// Check if a schema is compatible with existing versions.
    ///
    /// # Arguments
    /// * `subject` - Schema subject name
    /// * `schema_data` - Raw schema content
    /// * `schema_type` - Schema type (Avro, JsonSchema, Protobuf)
    /// * `mode` - Optional compatibility mode override (uses subject's default if None)
    pub async fn check_compatibility(
        &self,
        subject: impl Into<String>,
        schema_data: Vec<u8>,
        schema_type: SchemaType,
        mode: Option<CompatibilityMode>,
    ) -> Result<CheckCompatibilityResponse> {
        let request = CheckCompatibilityRequest {
            subject: subject.into(),
            new_schema_definition: schema_data,
            schema_type: schema_type.as_str().to_string(),
            compatibility_mode: mode.map(|m| m.as_str().to_string()),
        };
        let (req, mut client) = self.prepare_request(request).await?;
        let response = client
            .check_compatibility(req)
            .await
            .map_err(|status| DanubeError::FromStatus(status))?
            .into_inner();
        Ok(response)
    }

    /// Set compatibility mode for a subject.
    ///
    /// # Arguments
    /// * `subject` - Schema subject name
    /// * `mode` - Compatibility mode to set
    pub async fn set_compatibility_mode(
        &self,
        subject: impl Into<String>,
        mode: CompatibilityMode,
    ) -> Result<SetCompatibilityModeResponse> {
        let request = SetCompatibilityModeRequest {
            subject: subject.into(),
            compatibility_mode: mode.as_str().to_string(),
        };
        let (req, mut client) = self.prepare_request(request).await?;
        let response = client
            .set_compatibility_mode(req)
            .await
            .map_err(|status| DanubeError::FromStatus(status))?
            .into_inner();
        Ok(response)
    }

    /// List all versions for a subject.
    pub async fn list_versions(&self, subject: impl Into<String>) -> Result<Vec<u32>> {
        let request = ListVersionsRequest {
            subject: subject.into(),
        };
        let (req, mut client) = self.prepare_request(request).await?;
        let response = client
            .list_versions(req)
            .await
            .map_err(|status| DanubeError::FromStatus(status))?
            .into_inner();
        Ok(response.versions.into_iter().map(|v| v.version).collect())
    }

    /// Internal method to register schema via gRPC.
    async fn register_schema_internal(
        &self,
        subject: String,
        schema_type: String,
        schema_data: Vec<u8>,
    ) -> Result<RegisterSchemaResponse> {
        let request = RegisterSchemaRequest {
            subject,
            schema_type,
            schema_definition: schema_data,
            description: String::new(),
            created_by: String::from("danube-client"),
            tags: vec![],
        };
        let (req, mut client) = self.prepare_request(request).await?;
        let response = client
            .register_schema(req)
            .await
            .map_err(|status| DanubeError::FromStatus(status))?
            .into_inner();
        Ok(response)
    }
}

/// Builder for schema registration with fluent API.
///
/// # Example
///
/// ```no_run
/// use danube_client::{DanubeClient, SchemaType};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = DanubeClient::builder()
///     .service_url("http://localhost:6650")
///     .build()
///     .await?;
///
/// let schema_id = client.schema()
///     .register_schema("user-events-value")
///     .with_type(SchemaType::Avro)
///     .with_schema_data(avro_schema_bytes)
///     .execute()
///     .await?;
/// # Ok(())
/// # }
/// ```
///
/// # Schema Versioning
///
/// The registry automatically handles versioning:
/// - If the schema definition is new, a new version is created
/// - If the schema definition already exists, the existing schema_id and version are returned
/// - Compatibility checks are performed based on the subject's compatibility mode
pub struct SchemaRegistrationBuilder<'a> {
    client: &'a SchemaRegistryClient,
    subject: String,
    schema_type: Option<SchemaType>,
    schema_data: Option<Vec<u8>>,
}

impl<'a> SchemaRegistrationBuilder<'a> {
    /// Set the schema type
    pub fn with_type(mut self, schema_type: SchemaType) -> Self {
        self.schema_type = Some(schema_type);
        self
    }

    /// Set the schema data (raw schema content)
    pub fn with_schema_data(mut self, data: impl Into<Vec<u8>>) -> Self {
        self.schema_data = Some(data.into());
        self
    }

    /// Execute the schema registration
    pub async fn execute(self) -> Result<u64> {
        let schema_type = self
            .schema_type
            .ok_or_else(|| DanubeError::SchemaError("Schema type is required".into()))?;
        let schema_data = self
            .schema_data
            .ok_or_else(|| DanubeError::SchemaError("Schema data is required".into()))?;

        let response = self
            .client
            .register_schema_internal(self.subject, schema_type.as_str().to_string(), schema_data)
            .await?;

        Ok(response.schema_id)
    }
}

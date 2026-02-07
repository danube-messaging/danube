use crate::{
    errors::{DanubeError, Result},
    retry_manager::RetryManager,
    schema_types::{CompatibilityMode, SchemaInfo, SchemaType},
    DanubeClient,
};
use danube_core::proto::danube_schema::{
    schema_registry_client::SchemaRegistryClient as GrpcSchemaRegistryClient,
    CheckCompatibilityRequest, CheckCompatibilityResponse, GetLatestSchemaRequest,
    GetSchemaRequest, ListVersionsRequest, RegisterSchemaRequest, RegisterSchemaResponse,
    SetCompatibilityModeRequest, SetCompatibilityModeResponse,
};

/// Client for interacting with the Danube Schema Registry
///
/// Provides methods for registering, retrieving, and managing schemas
/// in the centralized schema registry.
#[derive(Debug, Clone)]
pub struct SchemaRegistryClient {
    client: DanubeClient,
    grpc_client: Option<GrpcSchemaRegistryClient<tonic::transport::Channel>>,
}

impl SchemaRegistryClient {
    /// Create a new SchemaRegistryClient from a DanubeClient
    pub async fn new(client: &DanubeClient) -> Result<Self> {
        Ok(Self::new_internal(client.clone()))
    }

    /// Internal constructor for building schema registry client synchronously
    pub(crate) fn new_internal(client: DanubeClient) -> Self {
        SchemaRegistryClient {
            client,
            grpc_client: None,
        }
    }

    /// Connect to the schema registry service
    async fn connect(&mut self) -> Result<()> {
        if self.grpc_client.is_some() {
            return Ok(());
        }

        let grpc_cnx = self
            .client
            .cnx_manager
            .get_connection(&self.client.uri, &self.client.uri)
            .await?;

        let client = GrpcSchemaRegistryClient::new(grpc_cnx.grpc_cnx.clone());
        self.grpc_client = Some(client);
        Ok(())
    }

    /// Prepare an authenticated gRPC request and return the ready client.
    ///
    /// Handles connect, auth token insertion, and client unwrap in one call.
    async fn prepare_request<T>(
        &mut self,
        request: T,
    ) -> Result<(
        tonic::Request<T>,
        &mut GrpcSchemaRegistryClient<tonic::transport::Channel>,
    )> {
        self.connect().await?;
        let mut req = tonic::Request::new(request);
        RetryManager::insert_auth_token(&self.client, &mut req, &self.client.uri).await?;
        let grpc_client = self.grpc_client.as_mut().ok_or_else(|| {
            DanubeError::SchemaError("Schema registry client not connected".into())
        })?;
        Ok((req, grpc_client))
    }

    /// Register a new schema or get existing schema ID
    ///
    /// Returns a builder for configuring schema registration
    pub fn register_schema(&mut self, subject: impl Into<String>) -> SchemaRegistrationBuilder<'_> {
        SchemaRegistrationBuilder {
            client: self,
            subject: subject.into(),
            schema_type: None,
            schema_data: None,
        }
    }

    /// Get schema by ID
    ///
    /// Returns schema information for the given schema ID.
    /// Schema ID identifies a subject (not a specific version).
    pub async fn get_schema_by_id(&mut self, schema_id: u64) -> Result<SchemaInfo> {
        let request = GetSchemaRequest {
            schema_id,
            version: None,
        };
        let (req, client) = self.prepare_request(request).await?;
        let response = client
            .get_schema(req)
            .await
            .map_err(|status| DanubeError::FromStatus(status, None))?
            .into_inner();
        Ok(SchemaInfo::from(response))
    }

    /// Get specific schema version
    ///
    /// Returns schema information for a specific version of a schema subject.
    pub async fn get_schema_version(
        &mut self,
        schema_id: u64,
        version: Option<u32>,
    ) -> Result<SchemaInfo> {
        let request = GetSchemaRequest { schema_id, version };
        let (req, client) = self.prepare_request(request).await?;
        let response = client
            .get_schema(req)
            .await
            .map_err(|status| DanubeError::FromStatus(status, None))?
            .into_inner();
        Ok(SchemaInfo::from(response))
    }

    /// Get latest schema for a subject
    ///
    /// Returns the latest schema version for the given subject.
    pub async fn get_latest_schema(&mut self, subject: impl Into<String>) -> Result<SchemaInfo> {
        let request = GetLatestSchemaRequest {
            subject: subject.into(),
        };
        let (req, client) = self.prepare_request(request).await?;
        let response = client
            .get_latest_schema(req)
            .await
            .map_err(|status| DanubeError::FromStatus(status, None))?
            .into_inner();
        Ok(SchemaInfo::from(response))
    }

    /// Check if a schema is compatible with existing versions
    ///
    /// # Arguments
    /// * `subject` - Schema subject name
    /// * `schema_data` - Raw schema content
    /// * `schema_type` - Schema type (Avro, JsonSchema, Protobuf)
    /// * `mode` - Optional compatibility mode override (uses subject's default if None)
    pub async fn check_compatibility(
        &mut self,
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
        let (req, client) = self.prepare_request(request).await?;
        let response = client
            .check_compatibility(req)
            .await
            .map_err(|status| DanubeError::FromStatus(status, None))?
            .into_inner();
        Ok(response)
    }

    /// Set compatibility mode for a subject
    ///
    /// # Arguments
    /// * `subject` - Schema subject name
    /// * `mode` - Compatibility mode to set
    ///
    /// # Example
    /// ```no_run
    /// use danube_client::{SchemaRegistryClient, CompatibilityMode};
    ///
    /// schema_client.set_compatibility_mode("critical-orders", CompatibilityMode::Full).await?;
    /// ```
    pub async fn set_compatibility_mode(
        &mut self,
        subject: impl Into<String>,
        mode: CompatibilityMode,
    ) -> Result<SetCompatibilityModeResponse> {
        let request = SetCompatibilityModeRequest {
            subject: subject.into(),
            compatibility_mode: mode.as_str().to_string(),
        };
        let (req, client) = self.prepare_request(request).await?;
        let response = client
            .set_compatibility_mode(req)
            .await
            .map_err(|status| DanubeError::FromStatus(status, None))?
            .into_inner();
        Ok(response)
    }

    /// List all versions for a subject
    pub async fn list_versions(&mut self, subject: impl Into<String>) -> Result<Vec<u32>> {
        let request = ListVersionsRequest {
            subject: subject.into(),
        };
        let (req, client) = self.prepare_request(request).await?;
        let response = client
            .list_versions(req)
            .await
            .map_err(|status| DanubeError::FromStatus(status, None))?
            .into_inner();
        Ok(response.versions.into_iter().map(|v| v.version).collect())
    }

    /// Internal method to register schema via gRPC
    async fn register_schema_internal(
        &mut self,
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
        let (req, client) = self.prepare_request(request).await?;
        let response = client
            .register_schema(req)
            .await
            .map_err(|status| DanubeError::FromStatus(status, None))?
            .into_inner();
        Ok(response)
    }
}

/// Builder for schema registration with fluent API
///
/// This builder provides a convenient way to register schemas in the Danube Schema Registry.
/// It supports all schema types (Avro, Protobuf, JSON Schema, etc.) and handles version management.
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
/// let mut schema_client = client.schema_registry_client();
///
/// // Register an Avro schema
/// let schema_id = schema_client
///     .register_schema("user-events-value")
///     .with_type(SchemaType::Avro)
///     .with_schema_data(avro_schema_bytes)
///     .execute()
///     .await?;
///
/// println!("Schema registered with ID: {}", schema_id);
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
    client: &'a mut SchemaRegistryClient,
    subject: String,
    schema_type: Option<SchemaType>,
    schema_data: Option<Vec<u8>>,
}

impl<'a> SchemaRegistrationBuilder<'a> {
    /// Set the schema type
    ///
    /// # Example
    /// ```no_run
    /// use danube_client::SchemaType;
    ///
    /// .with_type(SchemaType::Avro)
    /// ```
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

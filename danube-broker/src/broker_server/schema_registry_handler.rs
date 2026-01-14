use crate::resources::SchemaResources;
use crate::schema::{SchemaRegistry, ValidationPolicy};
use crate::topic_control::TopicManager;
use crate::{LocalCache, MetadataStorage};
use anyhow::Result;
use danube_core::proto::danube_schema::{
    schema_registry_server::SchemaRegistry as SchemaRegistryTrait, CheckCompatibilityRequest,
    CheckCompatibilityResponse, ConfigureTopicSchemaRequest, ConfigureTopicSchemaResponse,
    DeleteSchemaVersionRequest, DeleteSchemaVersionResponse, GetLatestSchemaRequest,
    GetSchemaRequest, GetSchemaResponse, GetTopicSchemaConfigRequest,
    GetTopicSchemaConfigResponse, ListVersionsRequest, ListVersionsResponse,
    RegisterSchemaRequest, RegisterSchemaResponse, SchemaVersionInfo,
    SetCompatibilityModeRequest, SetCompatibilityModeResponse,
    UpdateTopicValidationPolicyRequest, UpdateTopicValidationPolicyResponse,
};
use std::sync::Arc;
use tonic::{Request, Response, Status};
use tracing::{error, info, warn};

/// gRPC service implementation for Schema Registry
#[derive(Clone, Debug)]
pub struct SchemaRegistryService {
    registry: Arc<SchemaRegistry>,
    topic_manager: TopicManager,
}

impl SchemaRegistryService {
    pub fn new(
        local_cache: LocalCache,
        metadata_storage: MetadataStorage,
        topic_manager: TopicManager,
    ) -> Self {
        let schema_resources = SchemaResources::new(local_cache, metadata_storage);
        let registry = Arc::new(SchemaRegistry::new(Arc::new(schema_resources)));

        Self {
            registry,
            topic_manager,
        }
    }

    /// Get access to underlying SchemaRegistry (for testing/admin operations)
    #[allow(dead_code)]
    pub fn get_registry(&self) -> Arc<SchemaRegistry> {
        Arc::clone(&self.registry)
    }
}

#[tonic::async_trait]
impl SchemaRegistryTrait for SchemaRegistryService {
    async fn register_schema(
        &self,
        request: Request<RegisterSchemaRequest>,
    ) -> Result<Response<RegisterSchemaResponse>, Status> {
        let req = request.into_inner();
        info!(
            subject = %req.subject,
            schema_type = %req.schema_type,
            "registering schema for subject"
        );

        match self
            .registry
            .register_schema(
                &req.subject,
                &req.schema_type,
                &req.schema_definition,
                req.description,
                req.created_by,
                req.tags,
            )
            .await
        {
            Ok((schema_id, version, is_new_version, metadata)) => {
                // Get fingerprint from the returned metadata (no need for additional fetch)
                let schema_version = metadata
                    .get_version(version)
                    .ok_or_else(|| Status::internal("Version not found after registration"))?;

                info!(
                    schema_id = %schema_id,
                    version = %version,
                    is_new_version = %is_new_version,
                    subject = %req.subject,
                    "schema registered"
                );

                Ok(Response::new(RegisterSchemaResponse {
                    schema_id,
                    version,
                    is_new_version,
                    fingerprint: schema_version.fingerprint.clone(),
                }))
            }
            Err(e) => {
                error!(subject = %req.subject, error = %e, "failed to register schema");
                Err(Status::internal(format!(
                    "Failed to register schema: {}",
                    e
                )))
            }
        }
    }

    async fn get_schema(
        &self,
        request: Request<GetSchemaRequest>,
    ) -> Result<Response<GetSchemaResponse>, Status> {
        let req = request.into_inner();
        info!(schema_id = %req.schema_id, version = ?req.version, "getting schema by ID");

        // Use reverse index to look up subject and fetch schema
        match self.registry.get_schema(req.schema_id, req.version).await {
            Ok((subject, schema_version)) => {
                // Get metadata for compatibility mode
                let metadata = self
                    .registry
                    .get_metadata(&subject)
                    .await
                    .map_err(|e| Status::internal(format!("Failed to get metadata: {}", e)))?;

                // Serialize schema definition
                let schema_definition = serde_json::to_vec(&schema_version.schema_def)
                    .map_err(|e| Status::internal(format!("Failed to serialize schema: {}", e)))?;

                let schema_type = schema_version.schema_def.schema_type().to_string();

                info!(subject = %subject, version = %schema_version.version, schema_id = %req.schema_id, "found schema");

                Ok(Response::new(GetSchemaResponse {
                    schema_id: req.schema_id,
                    version: schema_version.version,
                    subject,
                    schema_type,
                    schema_definition,
                    description: schema_version.description,
                    created_at: schema_version.created_at,
                    created_by: schema_version.created_by,
                    tags: schema_version.tags,
                    fingerprint: schema_version.fingerprint,
                    compatibility_mode: metadata.compatibility_mode.to_string(),
                }))
            }
            Err(e) => {
                error!(schema_id = %req.schema_id, error = %e, "failed to get schema by ID");
                Err(Status::not_found(format!(
                    "Schema not found for ID: {}",
                    req.schema_id
                )))
            }
        }
    }

    async fn get_latest_schema(
        &self,
        request: Request<GetLatestSchemaRequest>,
    ) -> Result<Response<GetSchemaResponse>, Status> {
        let req = request.into_inner();
        info!(subject = %req.subject, "getting latest schema for subject");

        match self.registry.get_latest_schema(&req.subject).await {
            Ok(schema_version) => {
                let metadata = self
                    .registry
                    .get_metadata(&req.subject)
                    .await
                    .map_err(|e| Status::internal(format!("Failed to get metadata: {}", e)))?;

                // Serialize schema definition
                let schema_definition = serde_json::to_vec(&schema_version.schema_def)
                    .map_err(|e| Status::internal(format!("Failed to serialize schema: {}", e)))?;

                let schema_type = schema_version.schema_def.schema_type().to_string();

                Ok(Response::new(GetSchemaResponse {
                    schema_id: metadata.id,
                    version: schema_version.version,
                    subject: req.subject,
                    schema_type,
                    schema_definition,
                    description: schema_version.description,
                    created_at: schema_version.created_at,
                    created_by: schema_version.created_by,
                    tags: schema_version.tags,
                    fingerprint: schema_version.fingerprint,
                    compatibility_mode: metadata.compatibility_mode.to_string(),
                }))
            }
            Err(e) => {
                error!(subject = %req.subject, error = %e, "failed to get latest schema");
                Err(Status::not_found(format!(
                    "Schema not found for subject: {}",
                    req.subject
                )))
            }
        }
    }

    async fn list_versions(
        &self,
        request: Request<ListVersionsRequest>,
    ) -> Result<Response<ListVersionsResponse>, Status> {
        let req = request.into_inner();
        info!(subject = %req.subject, "listing versions for subject");

        // Get metadata for schema_id
        let metadata = self
            .registry
            .get_metadata(&req.subject)
            .await
            .map_err(|e| {
                error!(subject = %req.subject, error = %e, "failed to get metadata");
                Status::not_found(format!("Subject not found: {}", req.subject))
            })?;

        // Get list of version numbers
        let version_numbers = self
            .registry
            .list_versions(&req.subject)
            .await
            .map_err(|e| {
                error!(subject = %req.subject, error = %e, "failed to list versions");
                Status::internal("Failed to retrieve version list")
            })?;

        // Build version info by fetching each version's details
        let mut versions = Vec::new();
        for version_num in version_numbers {
            match self
                .registry
                .get_schema_version(&req.subject, version_num)
                .await
            {
                Ok(version) => {
                    versions.push(SchemaVersionInfo {
                        version: version_num,
                        created_at: version.created_at,
                        created_by: version.created_by,
                        description: version.description,
                        fingerprint: version.fingerprint,
                        schema_id: metadata.id,
                    });
                }
                Err(e) => {
                    warn!(subject = %req.subject, version = %version_num, error = %e, "failed to get version details");
                    // Continue with other versions
                }
            }
        }

        Ok(Response::new(ListVersionsResponse { versions }))
    }

    async fn check_compatibility(
        &self,
        request: Request<CheckCompatibilityRequest>,
    ) -> Result<Response<CheckCompatibilityResponse>, Status> {
        let req = request.into_inner();

        // Parse compatibility mode if provided
        let mode_override = req
            .compatibility_mode
            .as_ref()
            .and_then(|s| crate::schema::CompatibilityMode::from_str(s));

        info!(
            subject = %req.subject,
            mode_override = ?mode_override,
            "checking compatibility for subject (will use subject's default if None)"
        );

        match self
            .registry
            .check_compatibility(
                &req.subject,
                &req.schema_type,
                &req.new_schema_definition,
                mode_override,
            )
            .await
        {
            Ok(result) => Ok(Response::new(CheckCompatibilityResponse {
                is_compatible: result.is_compatible,
                errors: result.errors,
            })),
            Err(e) => {
                error!(subject = %req.subject, error = %e, "failed to check compatibility");
                Err(Status::internal(format!(
                    "Failed to check compatibility: {}",
                    e
                )))
            }
        }
    }

    async fn delete_schema_version(
        &self,
        request: Request<DeleteSchemaVersionRequest>,
    ) -> Result<Response<DeleteSchemaVersionResponse>, Status> {
        let req = request.into_inner();
        info!(
            subject = %req.subject,
            version = %req.version,
            "deleting schema version"
        );

        match self
            .registry
            .delete_schema_version(&req.subject, req.version)
            .await
        {
            Ok(_) => Ok(Response::new(DeleteSchemaVersionResponse {
                success: true,
                message: format!(
                    "Deleted version {} for subject {}",
                    req.version, req.subject
                ),
            })),
            Err(e) => {
                error!(subject = %req.subject, version = %req.version, error = %e, "failed to delete schema version");
                Err(Status::internal(format!(
                    "Failed to delete schema version: {}",
                    e
                )))
            }
        }
    }

    async fn set_compatibility_mode(
        &self,
        request: Request<SetCompatibilityModeRequest>,
    ) -> Result<Response<SetCompatibilityModeResponse>, Status> {
        let req = request.into_inner();
        info!(
            subject = %req.subject,
            mode = %req.compatibility_mode,
            "Setting compatibility mode for subject"
        );

        let mode = crate::schema::CompatibilityMode::from_str(&req.compatibility_mode).ok_or_else(
            || {
                Status::invalid_argument(format!(
                    "Invalid compatibility mode: {}",
                    req.compatibility_mode
                ))
            },
        )?;

        match self
            .registry
            .set_compatibility_mode(&req.subject, mode)
            .await
        {
            Ok(_) => Ok(Response::new(SetCompatibilityModeResponse {
                success: true,
                message: format!(
                    "Set compatibility mode to {} for subject {}",
                    req.compatibility_mode, req.subject
                ),
            })),
            Err(e) => {
                error!(subject = %req.subject, error = %e, "Failed to set compatibility mode");
                Err(Status::internal(format!(
                    "Failed to set compatibility mode: {}",
                    e
                )))
            }
        }
    }

    // ===== Admin-only operations for topic schema configuration =====

    async fn configure_topic_schema(
        &self,
        request: Request<ConfigureTopicSchemaRequest>,
    ) -> Result<Response<ConfigureTopicSchemaResponse>, Status> {
        let req = request.into_inner();
        
        // Parse validation policy
        let validation_policy = match req.validation_policy.to_lowercase().as_str() {
            "none" => ValidationPolicy::None,
            "warn" => ValidationPolicy::Warn,
            "enforce" => ValidationPolicy::Enforce,
            _ => {
                return Err(Status::invalid_argument(format!(
                    "Invalid validation policy: '{}'. Must be 'none', 'warn', or 'enforce'",
                    req.validation_policy
                )));
            }
        };

        // Verify schema subject exists in registry
        self.registry
            .get_metadata(&req.schema_subject)
            .await
            .map_err(|e| {
                Status::not_found(format!(
                    "Schema subject '{}' not found in registry: {}",
                    req.schema_subject, e
                ))
            })?;

        // Delegate to TopicManager
        let message = self
            .topic_manager
            .configure_topic_schema(
                &req.topic_name,
                req.schema_subject,
                validation_policy,
                req.enable_payload_validation,
            )
            .await?;

        Ok(Response::new(ConfigureTopicSchemaResponse {
            success: true,
            message,
        }))
    }

    async fn update_topic_validation_policy(
        &self,
        request: Request<UpdateTopicValidationPolicyRequest>,
    ) -> Result<Response<UpdateTopicValidationPolicyResponse>, Status> {
        let req = request.into_inner();

        // Parse validation policy
        let validation_policy = match req.validation_policy.to_lowercase().as_str() {
            "none" => ValidationPolicy::None,
            "warn" => ValidationPolicy::Warn,
            "enforce" => ValidationPolicy::Enforce,
            _ => {
                return Err(Status::invalid_argument(format!(
                    "Invalid validation policy: '{}'. Must be 'none', 'warn', or 'enforce'",
                    req.validation_policy
                )));
            }
        };

        // Delegate to TopicManager
        let message = self
            .topic_manager
            .update_topic_validation_policy(
                &req.topic_name,
                validation_policy,
                req.enable_payload_validation,
            )
            .await?;

        Ok(Response::new(UpdateTopicValidationPolicyResponse {
            success: true,
            message,
        }))
    }

    async fn get_topic_schema_config(
        &self,
        request: Request<GetTopicSchemaConfigRequest>,
    ) -> Result<Response<GetTopicSchemaConfigResponse>, Status> {
        let req = request.into_inner();

        // Delegate to TopicManager
        let (schema_subject, validation_policy, enable_payload_validation, schema_id) = self
            .topic_manager
            .get_topic_schema_config(&req.topic_name)
            .await?;

        let validation_policy_str = format!("{:?}", validation_policy).to_lowercase();

        Ok(Response::new(GetTopicSchemaConfigResponse {
            schema_subject,
            validation_policy: validation_policy_str,
            enable_payload_validation,
            schema_id,
        }))
    }
}

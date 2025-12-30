use crate::resources::SchemaResources;
use crate::schema::SchemaRegistry;
use crate::{LocalCache, MetadataStorage};
use anyhow::Result;
use danube_core::proto::danube_schema::{
    schema_registry_server::SchemaRegistry as SchemaRegistryTrait, CheckCompatibilityRequest,
    CheckCompatibilityResponse, DeleteSchemaVersionRequest, DeleteSchemaVersionResponse,
    GetLatestSchemaRequest, GetSchemaRequest, GetSchemaResponse, ListVersionsRequest,
    ListVersionsResponse, RegisterSchemaRequest, RegisterSchemaResponse, SchemaVersionInfo,
    SetCompatibilityModeRequest, SetCompatibilityModeResponse,
};
use std::sync::Arc;
use tonic::{Request, Response, Status};
use tracing::{error, info, warn};

/// gRPC service implementation for Schema Registry
#[derive(Clone, Debug)]
pub struct SchemaRegistryService {
    registry: Arc<SchemaRegistry>,
}

impl SchemaRegistryService {
    pub fn new(local_cache: LocalCache, metadata_storage: MetadataStorage) -> Self {
        // Phase 6: SchemaRegistry now uses SchemaResources directly (no wrapper needed)
        let schema_resources = SchemaResources::new(local_cache, metadata_storage);
        let registry = Arc::new(SchemaRegistry::new(Arc::new(schema_resources)));

        Self { registry }
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
            "Registering schema for subject: {}, type: {}",
            req.subject, req.schema_type
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
                    "Schema registered: id={}, version={}, new={}",
                    schema_id, version, is_new_version
                );

                Ok(Response::new(RegisterSchemaResponse {
                    schema_id,
                    version,
                    is_new_version,
                    fingerprint: schema_version.fingerprint.clone(),
                }))
            }
            Err(e) => {
                error!("Failed to register schema: {}", e);
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
        let _req = request.into_inner();

        // Note: Getting schema by ID requires a reverse index (schema_id -> subject)
        // This is a limitation of the current storage design
        // Clients should use get_latest_schema with subject name instead
        Err(Status::unimplemented(
            "get_schema by ID requires reverse index - use get_latest_schema with subject name instead",
        ))
    }

    async fn get_latest_schema(
        &self,
        request: Request<GetLatestSchemaRequest>,
    ) -> Result<Response<GetSchemaResponse>, Status> {
        let req = request.into_inner();
        info!("Getting latest schema for subject: {}", req.subject);

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
                error!("Failed to get latest schema: {}", e);
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
        info!("Listing versions for subject: {}", req.subject);

        // Get metadata for schema_id
        let metadata = self
            .registry
            .get_metadata(&req.subject)
            .await
            .map_err(|e| {
                error!("Failed to get metadata: {}", e);
                Status::not_found(format!("Subject not found: {}", req.subject))
            })?;

        // Get list of version numbers
        let version_numbers = self
            .registry
            .list_versions(&req.subject)
            .await
            .map_err(|e| {
                error!("Failed to list versions: {}", e);
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
                    warn!("Failed to get version {} details: {}", version_num, e);
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
            "Checking compatibility for subject: {}, mode_override: {:?} (will use subject's default if None)",
            req.subject, mode_override
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
                error!("Failed to check compatibility: {}", e);
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
            "Deleting schema version: subject={}, version={}",
            req.subject, req.version
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
                error!("Failed to delete schema version: {}", e);
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
            "Setting compatibility mode for subject: {}, mode: {}",
            req.subject, req.compatibility_mode
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
                error!("Failed to set compatibility mode: {}", e);
                Err(Status::internal(format!(
                    "Failed to set compatibility mode: {}",
                    e
                )))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    // Note: Integration tests would require setting up a test metadata store
    // For now, these are placeholder tests showing the structure

    #[test]
    fn test_service_creation() {
        // This would need a mock MetadataStorage
        // let metadata_storage = Arc::new(Mutex::new(MetadataStorage::new(/* ... */)));
        // let service = SchemaRegistryService::new(metadata_storage);
        // assert!(service.get_registry().is_ok());
    }
}

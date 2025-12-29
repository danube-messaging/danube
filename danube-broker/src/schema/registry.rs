use crate::resources::SchemaResources;
use crate::schema::compatibility::CompatibilityChecker;
use crate::schema::formats::{AvroSchemaHandler, JsonSchemaHandler};
use crate::schema::metadata::{
    CompatibilityResult, SchemaDefinition, SchemaMetadata, SchemaVersion,
};
use crate::schema::types::{CompatibilityMode, SchemaType};
use anyhow::{anyhow, Result};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Core schema registry managing all schemas in the cluster
#[derive(Debug)]
pub struct SchemaRegistry {
    /// Storage backend using Resources pattern (LocalCache + ETCD)
    storage: Arc<SchemaResources>,

    /// Schema ID generator (globally unique)
    id_generator: Arc<AtomicU64>,

    /// Compatibility checker
    compatibility_checker: Arc<CompatibilityChecker>,
}

impl SchemaRegistry {
    pub fn new(storage: Arc<SchemaResources>) -> Self {
        Self {
            storage,
            id_generator: Arc::new(AtomicU64::new(1)), // Start from 1
            compatibility_checker: Arc::new(CompatibilityChecker::new()),
        }
    }

    /// Register a new schema or return existing schema ID if identical schema exists
    /// Returns: (schema_id, version, is_new_version, metadata)
    pub async fn register_schema(
        &self,
        subject: &str,
        schema_type: &str,
        schema_def_bytes: &[u8],
        description: String,
        created_by: String,
        tags: Vec<String>,
    ) -> Result<(u64, u32, bool, SchemaMetadata)> {
        // Parse and validate the schema
        let schema_def = self.parse_schema(schema_type, schema_def_bytes)?;
        let fingerprint = Self::get_fingerprint(&schema_def);

        // Check if subject exists
        let subject_exists = self.storage.subject_exists(subject).await?;

        if subject_exists {
            // Subject exists, add new version
            let mut metadata = self.storage.get_metadata(subject).await?;

            // Check for duplicate fingerprint in existing versions
            for version in &metadata.versions {
                if version.fingerprint == fingerprint {
                    return Ok((metadata.id, version.version, false, metadata.clone()));
                }
            }

            // Check compatibility
            let compatibility_mode = metadata.compatibility_mode;
            if !matches!(compatibility_mode, CompatibilityMode::None) {
                self.check_compatibility_internal(&metadata, &schema_def, compatibility_mode)
                    .await?;
            }

            // Create new version
            let new_version_number = metadata.latest_version + 1;
            let new_version = SchemaVersion::new(
                new_version_number,
                schema_def.clone(),
                fingerprint.clone(),
                created_by,
                description,
            )
            .with_tags(tags);

            // Store new version
            self.storage
                .store_schema_version(subject, &new_version)
                .await?;

            // Update metadata
            metadata.add_version(new_version.clone());
            self.storage.update_metadata(&metadata).await?;

            Ok((metadata.id, new_version_number, true, metadata))
        } else {
            // New subject, create first version
            let schema_id = self.id_generator.fetch_add(1, Ordering::SeqCst);

            let first_version = SchemaVersion::new(
                1,
                schema_def,
                fingerprint.clone(),
                created_by.clone(),
                description,
            )
            .with_tags(tags);

            let metadata = SchemaMetadata::new(
                schema_id,
                subject.to_string(),
                first_version.clone(),
                created_by,
            );

            // Store version and metadata
            self.storage
                .store_schema_version(subject, &first_version)
                .await?;
            self.storage.store_schema_metadata(&metadata).await?;

            Ok((schema_id, 1, true, metadata))
        }
    }

    /// Get a specific schema by ID and optional version
    /// Note: This requires maintaining a reverse index from schema_id to subject
    /// Currently unimplemented - use get_latest_schema with subject instead
    #[allow(dead_code)]
    pub async fn get_schema(
        &self,
        _schema_id: u64,
        _version: Option<u32>,
    ) -> Result<(String, SchemaVersion)> {
        // TODO: We need a reverse index from schema_id to subject
        // For now, this is a limitation - we'll need to add schema_id index to storage
        Err(anyhow!(
            "get_schema by ID not fully implemented yet - need reverse index"
        ))
    }

    /// Get the latest schema for a subject
    pub async fn get_latest_schema(&self, subject: &str) -> Result<SchemaVersion> {
        let metadata = self.storage.get_metadata(subject).await?;
        metadata
            .get_latest_version()
            .cloned()
            .ok_or_else(|| anyhow!("No versions found for subject: {}", subject))
    }

    /// Get a specific version of a schema for a subject
    pub async fn get_schema_version(&self, subject: &str, version: u32) -> Result<SchemaVersion> {
        self.storage.get_version(subject, version).await
    }

    /// List all versions for a subject
    pub async fn list_versions(&self, subject: &str) -> Result<Vec<u32>> {
        self.storage.list_version_numbers(subject).await
    }

    /// Check compatibility of a new schema with existing versions
    pub async fn check_compatibility(
        &self,
        subject: &str,
        new_schema_type: &str,
        new_schema_bytes: &[u8],
        compatibility_mode: Option<CompatibilityMode>,
    ) -> Result<CompatibilityResult> {
        // Parse new schema
        let new_schema_def = self.parse_schema(new_schema_type, new_schema_bytes)?;

        // Get existing metadata
        let metadata = self.storage.get_metadata(subject).await?;

        // Use provided mode or subject's default mode
        let mode = compatibility_mode.unwrap_or(metadata.compatibility_mode);

        self.check_compatibility_internal(&metadata, &new_schema_def, mode)
            .await
    }

    /// Internal compatibility checking
    async fn check_compatibility_internal(
        &self,
        metadata: &SchemaMetadata,
        new_schema: &SchemaDefinition,
        mode: CompatibilityMode,
    ) -> Result<CompatibilityResult> {
        if matches!(mode, CompatibilityMode::None) {
            return Ok(CompatibilityResult::compatible());
        }

        let latest_version = metadata
            .get_latest_version()
            .ok_or_else(|| anyhow!("No existing versions found"))?;

        // Check against latest version
        self.compatibility_checker
            .check(&latest_version.schema_def, new_schema, mode)
    }

    /// Set compatibility mode for a subject
    pub async fn set_compatibility_mode(
        &self,
        subject: &str,
        mode: CompatibilityMode,
    ) -> Result<()> {
        // Update in storage
        self.storage
            .store_compatibility_mode(subject, &mode.to_string())
            .await?;

        // Update metadata
        let mut metadata = self.storage.get_metadata(subject).await?;
        metadata.set_compatibility_mode(mode);
        self.storage.update_metadata(&metadata).await?;

        Ok(())
    }

    /// Delete a specific schema version
    pub async fn delete_schema_version(&self, subject: &str, version: u32) -> Result<()> {
        self.storage.delete_version(subject, version).await
    }

    /// List all subjects (for CLI/admin tools)
    #[allow(dead_code)]
    pub async fn list_subjects(&self) -> Result<Vec<String>> {
        self.storage.list_subjects().await
    }

    /// Get schema metadata for a subject
    pub async fn get_metadata(&self, subject: &str) -> Result<SchemaMetadata> {
        self.storage.get_metadata(subject).await
    }

    /// Parse schema based on type
    fn parse_schema(&self, schema_type: &str, schema_bytes: &[u8]) -> Result<SchemaDefinition> {
        let schema_type_enum = SchemaType::from_str(schema_type)
            .ok_or_else(|| anyhow!("Unknown schema type: {}", schema_type))?;

        match schema_type_enum {
            SchemaType::Bytes => Ok(SchemaDefinition::Bytes),
            SchemaType::String => Ok(SchemaDefinition::String),
            SchemaType::Number => Ok(SchemaDefinition::Number),
            SchemaType::Avro => {
                let avro_schema = AvroSchemaHandler::parse(schema_bytes)?;
                Ok(SchemaDefinition::Avro(avro_schema))
            }
            SchemaType::JsonSchema => {
                let json_schema = JsonSchemaHandler::parse(schema_bytes)?;
                Ok(SchemaDefinition::JsonSchema(json_schema))
            }
            SchemaType::Protobuf => {
                // TODO: Implement protobuf parsing
                Err(anyhow!("Protobuf schemas not yet supported"))
            }
        }
    }

    /// Get fingerprint from schema definition
    fn get_fingerprint(schema_def: &SchemaDefinition) -> String {
        match schema_def {
            SchemaDefinition::Bytes => "bytes".to_string(),
            SchemaDefinition::String => "string".to_string(),
            SchemaDefinition::Number => "number".to_string(),
            SchemaDefinition::Avro(avro) => avro.fingerprint.clone(),
            SchemaDefinition::JsonSchema(json) => json.fingerprint.clone(),
            SchemaDefinition::Protobuf(proto) => proto.fingerprint.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: Full integration tests would require a real MetadataStorage with LocalCache
    // Tests removed after deleting SchemaStorage wrapper - SchemaRegistry now uses SchemaResources directly
}

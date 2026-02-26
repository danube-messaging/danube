use crate::metadata_storage::MetadataStorage;
use crate::{
    resources::BASE_SCHEMAS_PATH,
    schema::metadata::{SchemaMetadata, SchemaVersion},
    utils::join_path,
    LocalCache,
};
use anyhow::{anyhow, Result};
use danube_core::metadata::{MetaOptions, MetadataStore};
use serde_json::Value;

/// SchemaResources manages schema metadata in ETCD
/// Following the Danube pattern:
/// - Writes go to ETCD (which triggers LocalCache updates via watch)
/// - Reads use LocalCache for fast access
#[derive(Debug, Clone)]
pub(crate) struct SchemaResources {
    local_cache: LocalCache,
    store: MetadataStorage,
}

impl SchemaResources {
    pub(crate) fn new(local_cache: LocalCache, store: MetadataStorage) -> Self {
        SchemaResources { local_cache, store }
    }

    /// Check if a schema subject exists (reads from LocalCache)
    pub(crate) async fn subject_exists(&self, subject: &str) -> Result<bool> {
        let path = join_path(&[BASE_SCHEMAS_PATH, subject, "metadata"]);
        // Check if metadata exists AND can be deserialized
        match self.local_cache.get(&path) {
            Some(value) => {
                // Verify it's valid SchemaMetadata
                Ok(serde_json::from_value::<SchemaMetadata>(value).is_ok())
            }
            None => Ok(false),
        }
    }

    /// Get schema metadata from LocalCache (fast read)
    /// Returns (schema_id, metadata) tuple
    pub(crate) fn get_cached_metadata(&self, subject: &str) -> Option<(u64, Value)> {
        let path = format!("/schemas/{}/metadata", subject);
        self.local_cache.get(&path).map(|metadata| {
            // Extract schema_id from metadata
            let schema_id = metadata
                .get("schema_id")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            (schema_id, metadata)
        })
    }

    /// Get schema ID from LocalCache
    pub(crate) fn get_schema_id(&self, subject: &str) -> Option<u64> {
        self.get_cached_metadata(subject).map(|(id, _)| id)
    }

    /// Store compatibility mode in ETCD
    /// Path: /schemas/{subject}/compatibility
    pub(crate) async fn store_compatibility_mode(&self, subject: &str, mode: &str) -> Result<()> {
        let path = join_path(&[BASE_SCHEMAS_PATH, subject, "compatibility"]);
        let data = serde_json::json!({ "mode": mode });
        self.store.put(&path, data, MetaOptions::None).await?;
        Ok(())
    }

    /// Get compatibility mode from LocalCache
    #[allow(dead_code)]
    /// TODO: Intentional future operation
    pub(crate) fn get_compatibility_mode(&self, subject: &str) -> Option<String> {
        let path = format!("/schemas/{}/compatibility", subject);
        self.local_cache
            .get(&path)
            .and_then(|v| v.get("mode").and_then(|m| m.as_str().map(String::from)))
    }

    // ========== Schema ID Generation (Raft-atomic) ==========

    /// Atomically allocate the next schema ID through Raft consensus.
    ///
    /// This replaces the old racy get+put pattern with a single atomic
    /// `AllocateMonotonicId` Raft command, eliminating the race condition
    /// window that existed when multiple brokers registered schemas
    /// concurrently.
    pub(crate) async fn allocate_next_schema_id(&self) -> Result<u64> {
        const COUNTER_KEY: &str = "/schemas/_global/next_schema_id";
        let id = self.store.allocate_monotonic_id(COUNTER_KEY).await?;
        Ok(id)
    }

    // ========== Reverse Index (schema_id -> subject) ==========

    /// Store reverse index mapping schema_id to subject
    pub(crate) async fn store_schema_id_index(&self, schema_id: u64, subject: &str) -> Result<()> {
        let path = format!("/schemas/_index/by_id/{}", schema_id);
        let data = serde_json::json!({
            "subject": subject,
            "schema_id": schema_id
        });

        self.store.put(&path, data, MetaOptions::None).await?;
        Ok(())
    }

    /// Get subject name from ETCD if not in cache
    pub(crate) async fn fetch_subject_by_schema_id(&self, schema_id: u64) -> Result<String> {
        let path = format!("/schemas/_index/by_id/{}", schema_id);

        let value = self
            .store
            .get(&path, MetaOptions::None)
            .await?
            .ok_or_else(|| anyhow!("No subject found for schema_id {}", schema_id))?;

        value
            .get("subject")
            .and_then(|s| s.as_str())
            .map(String::from)
            .ok_or_else(|| anyhow!("Invalid index data for schema_id {}", schema_id))
    }

    /// Get subject name by schema_id (tries LocalCache first, then ETCD)
    /// This is the preferred method for fast lookups during message validation
    pub(crate) fn get_subject_by_schema_id(&self, schema_id: u64) -> Option<String> {
        let path = format!("/schemas/_index/by_id/{}", schema_id);

        // Try LocalCache first (fast path)
        if let Some(value) = self.local_cache.get(&path) {
            return value
                .get("subject")
                .and_then(|s| s.as_str())
                .map(String::from);
        }

        // Not in cache - caller should use fetch_subject_by_schema_id for async lookup
        None
    }

    // Additional methods for SchemaStorage compatibility

    /// Get schema metadata from LocalCache (fast read)
    pub(crate) async fn get_metadata(&self, subject: &str) -> Result<SchemaMetadata> {
        let path = join_path(&[BASE_SCHEMAS_PATH, subject, "metadata"]);
        let value = self
            .local_cache
            .get(&path)
            .ok_or_else(|| anyhow!("Schema metadata not found for subject: {}", subject))?;

        let metadata: SchemaMetadata = serde_json::from_value(value)
            .map_err(|e| anyhow!("Failed to deserialize schema metadata: {}", e))?;

        Ok(metadata)
    }

    /// Update schema metadata in ETCD
    pub(crate) async fn update_metadata(&self, metadata: &SchemaMetadata) -> Result<()> {
        let path = join_path(&[BASE_SCHEMAS_PATH, &metadata.subject, "metadata"]);
        let data = serde_json::to_value(metadata)
            .map_err(|e| anyhow!("Failed to serialize schema metadata: {}", e))?;
        self.store.put(&path, data, MetaOptions::None).await?;
        Ok(())
    }

    /// Store schema metadata (for new subjects)
    pub(crate) async fn store_schema_metadata(&self, metadata: &SchemaMetadata) -> Result<()> {
        let path = join_path(&[BASE_SCHEMAS_PATH, &metadata.subject, "metadata"]);
        let data = serde_json::to_value(metadata)
            .map_err(|e| anyhow!("Failed to serialize schema metadata: {}", e))?;
        self.store.put(&path, data, MetaOptions::None).await?;
        Ok(())
    }

    /// Store a specific schema version
    pub(crate) async fn store_schema_version(
        &self,
        subject: &str,
        version: &SchemaVersion,
    ) -> Result<()> {
        let path = join_path(&[
            BASE_SCHEMAS_PATH,
            subject,
            "versions",
            &version.version.to_string(),
        ]);
        let data = serde_json::to_value(version)
            .map_err(|e| anyhow!("Failed to serialize schema version: {}", e))?;
        self.store.put(&path, data, MetaOptions::None).await?;
        Ok(())
    }

    /// Get a specific schema version from LocalCache (fast read)
    pub(crate) async fn get_version(&self, subject: &str, version: u32) -> Result<SchemaVersion> {
        let path = join_path(&[BASE_SCHEMAS_PATH, subject, "versions", &version.to_string()]);
        let value = self.local_cache.get(&path).ok_or_else(|| {
            anyhow!(
                "Schema version {} not found for subject: {}",
                version,
                subject
            )
        })?;

        let schema_version: SchemaVersion = serde_json::from_value(value)
            .map_err(|e| anyhow!("Failed to deserialize schema version: {}", e))?;

        Ok(schema_version)
    }

    /// List all version numbers for a subject from ETCD
    pub(crate) async fn list_version_numbers(&self, subject: &str) -> Result<Vec<u32>> {
        let prefix = join_path(&[BASE_SCHEMAS_PATH, subject, "versions"]);
        let keys = self.local_cache.get_keys_with_prefix(&prefix).await;

        let mut versions: Vec<u32> = keys
            .iter()
            .filter_map(|key| key.split('/').last()?.parse::<u32>().ok())
            .collect();

        versions.sort_unstable();
        Ok(versions)
    }

    /// List all subjects from LocalCache
    pub(crate) async fn list_subjects(&self) -> Result<Vec<String>> {
        let keys = self
            .local_cache
            .get_keys_with_prefix(BASE_SCHEMAS_PATH)
            .await;

        let subjects: Vec<String> = keys
            .iter()
            .filter_map(|key| {
                // Extract subject from path like /schemas/{subject}/...
                let parts: Vec<&str> = key.split('/').collect();
                if parts.len() >= 3 && parts[1] == "schemas" {
                    Some(parts[2].to_string())
                } else {
                    None
                }
            })
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();

        Ok(subjects)
    }

    /// List all subjects with a prefix from LocalCache
    #[allow(dead_code)]
    /// TODO: Intentional future operation
    pub(crate) async fn get_subjects_with_prefix(&self, prefix: &str) -> Vec<String> {
        let full_prefix = if prefix.is_empty() {
            BASE_SCHEMAS_PATH.to_string()
        } else {
            join_path(&[BASE_SCHEMAS_PATH, prefix])
        };
        self.local_cache.get_keys_with_prefix(&full_prefix).await
    }

    /// Delete schema version from ETCD
    pub(crate) async fn delete_version(&self, subject: &str, version: u32) -> Result<()> {
        let path = join_path(&[BASE_SCHEMAS_PATH, subject, "versions", &version.to_string()]);
        self.store.delete(&path).await?;
        Ok(())
    }

    /// Delete all versions for a subject
    #[allow(dead_code)]
    /// TODO: Intentional future operation
    pub(crate) async fn delete_all_versions(&self, subject: &str) -> Result<()> {
        let prefix = join_path(&[BASE_SCHEMAS_PATH, subject, "versions"]);
        let keys = self.local_cache.get_keys_with_prefix(&prefix).await;
        for key in keys {
            self.store.delete(&key).await?;
        }
        // Delete the versions directory marker if present
        let _ = self.store.delete(&prefix).await;
        Ok(())
    }

    /// Delete subject metadata from ETCD
    #[allow(dead_code)]
    /// TODO: Intentional future operation
    pub(crate) async fn delete_metadata(&self, subject: &str) -> Result<()> {
        let path = join_path(&[BASE_SCHEMAS_PATH, subject, "metadata"]);
        self.store.delete(&path).await?;
        Ok(())
    }

    /// Delete entire subject (metadata + all versions)
    #[allow(dead_code)]
    pub(crate) async fn delete_subject(&self, subject: &str) -> Result<()> {
        // Delete all versions first
        self.delete_all_versions(subject).await?;
        // Delete metadata
        self.delete_metadata(subject).await?;
        // Delete subject root
        let root_path = join_path(&[BASE_SCHEMAS_PATH, subject]);
        let _ = self.store.delete(&root_path).await;
        Ok(())
    }

    /// Get schema details by subject for admin API
    /// Returns schema information including schema_id, version, type, and compatibility mode
    pub(crate) async fn get_schema_by_subject(&self, subject: &str) -> Option<SchemaDetails> {
        // Get metadata
        let metadata = match self.get_metadata(subject).await {
            Ok(meta) => meta,
            Err(_) => return None,
        };

        // Get latest version
        let latest_version = metadata.latest_version;
        let version_data = match self.get_version(subject, latest_version).await {
            Ok(v) => v,
            Err(_) => return None,
        };

        // Get schema type from the schema definition
        let schema_type = version_data.schema_def.schema_type().to_string();

        // Get compatibility mode (use Display trait)
        let compatibility_mode = metadata.compatibility_mode.to_string().to_uppercase();

        Some(SchemaDetails {
            schema_id: metadata.id,
            version: latest_version,
            schema_type,
            compatibility_mode,
        })
    }
}

/// Schema details for admin API responses
#[derive(Debug, Clone)]
pub(crate) struct SchemaDetails {
    pub(crate) schema_id: u64,
    pub(crate) version: u32,
    pub(crate) schema_type: String,
    pub(crate) compatibility_mode: String,
}

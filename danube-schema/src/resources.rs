use crate::metadata::{SchemaMetadata, SchemaVersion};
use anyhow::{anyhow, Result};
use danube_core::metadata::{MetaOptions, MetadataStore};
use serde_json::Value;
use std::sync::Arc;

/// Base path for all schema data in the metadata store.
pub static BASE_SCHEMAS_PATH: &str = "/schemas";

/// Joins path segments into a single path string.
pub fn join_path(parts: &[&str]) -> String {
    let mut result = String::new();
    for (i, part) in parts.iter().enumerate() {
        let part = part.trim_end_matches('/');
        if i == 0 {
            result.push_str(part.trim_end_matches('/'));
        } else {
            if !part.is_empty() && !part.starts_with('/') {
                result.push('/');
            }
            result.push_str(part);
        }
    }
    result
}

/// SchemaResources manages schema metadata in the metadata store.
///
/// All reads and writes go through the `MetadataStore` trait, which can be
/// backed by Raft (in the broker) or InMemory (in tests / edge).
#[derive(Clone)]
pub struct SchemaResources {
    store: Arc<dyn MetadataStore>,
}

impl std::fmt::Debug for SchemaResources {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SchemaResources").finish()
    }
}

impl SchemaResources {
    pub fn new(store: Arc<dyn MetadataStore>) -> Self {
        SchemaResources { store }
    }

    /// Check if a schema subject exists.
    pub async fn subject_exists(&self, subject: &str) -> Result<bool> {
        let path = join_path(&[BASE_SCHEMAS_PATH, subject, "metadata"]);
        match self.store.get(&path, MetaOptions::None).await? {
            Some(value) => Ok(serde_json::from_value::<SchemaMetadata>(value).is_ok()),
            None => Ok(false),
        }
    }

    /// Get schema metadata (fast read from metadata store)
    pub async fn get_cached_metadata(&self, subject: &str) -> Option<(u64, Value)> {
        let path = format!("/schemas/{}/metadata", subject);
        let metadata = self.store.get(&path, MetaOptions::None).await.ok()??;
        let schema_id = metadata
            .get("schema_id")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        Some((schema_id, metadata))
    }

    /// Get schema ID from store
    pub async fn get_schema_id(&self, subject: &str) -> Option<u64> {
        self.get_cached_metadata(subject).await.map(|(id, _)| id)
    }

    /// Store compatibility mode in the metadata store
    pub async fn store_compatibility_mode(&self, subject: &str, mode: &str) -> Result<()> {
        let path = join_path(&[BASE_SCHEMAS_PATH, subject, "compatibility"]);
        let data = serde_json::json!({ "mode": mode });
        self.store.put(&path, data, MetaOptions::None).await?;
        Ok(())
    }

    /// Get compatibility mode from store
    #[allow(dead_code)]
    pub async fn get_compatibility_mode(&self, subject: &str) -> Option<String> {
        let path = format!("/schemas/{}/compatibility", subject);
        let value = self.store.get(&path, MetaOptions::None).await.ok()??;
        value.get("mode").and_then(|m| m.as_str().map(String::from))
    }

    // ========== Schema ID Generation (Raft-atomic) ==========

    /// Atomically allocate the next schema ID through the metadata store.
    pub async fn allocate_next_schema_id(&self) -> Result<u64> {
        const COUNTER_KEY: &str = "/schemas/_global/next_schema_id";
        let id = self.store.allocate_monotonic_id(COUNTER_KEY).await?;
        Ok(id)
    }

    // ========== Reverse Index (schema_id -> subject) ==========

    /// Store reverse index mapping schema_id to subject
    pub async fn store_schema_id_index(&self, schema_id: u64, subject: &str) -> Result<()> {
        let path = format!("/schemas/_index/by_id/{}", schema_id);
        let data = serde_json::json!({
            "subject": subject,
            "schema_id": schema_id
        });
        self.store.put(&path, data, MetaOptions::None).await?;
        Ok(())
    }

    /// Get subject name from the metadata store
    pub async fn fetch_subject_by_schema_id(&self, schema_id: u64) -> Result<String> {
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

    /// Get subject name by schema_id (fast lookup)
    pub async fn get_subject_by_schema_id(&self, schema_id: u64) -> Option<String> {
        let path = format!("/schemas/_index/by_id/{}", schema_id);
        let value = self.store.get(&path, MetaOptions::None).await.ok()??;
        value
            .get("subject")
            .and_then(|s| s.as_str())
            .map(String::from)
    }

    /// Get schema metadata.
    pub async fn get_metadata(&self, subject: &str) -> Result<SchemaMetadata> {
        let path = join_path(&[BASE_SCHEMAS_PATH, subject, "metadata"]);
        let value = self
            .store
            .get(&path, MetaOptions::None)
            .await?
            .ok_or_else(|| anyhow!("Schema metadata not found for subject: {}", subject))?;
        let metadata: SchemaMetadata = serde_json::from_value(value)
            .map_err(|e| anyhow!("Failed to deserialize schema metadata: {}", e))?;
        Ok(metadata)
    }

    /// Update schema metadata in the metadata store
    pub async fn update_metadata(&self, metadata: &SchemaMetadata) -> Result<()> {
        let path = join_path(&[BASE_SCHEMAS_PATH, &metadata.subject, "metadata"]);
        let data = serde_json::to_value(metadata)
            .map_err(|e| anyhow!("Failed to serialize schema metadata: {}", e))?;
        self.store.put(&path, data, MetaOptions::None).await?;
        Ok(())
    }

    /// Store schema metadata (for new subjects)
    pub async fn store_schema_metadata(&self, metadata: &SchemaMetadata) -> Result<()> {
        let path = join_path(&[BASE_SCHEMAS_PATH, &metadata.subject, "metadata"]);
        let data = serde_json::to_value(metadata)
            .map_err(|e| anyhow!("Failed to serialize schema metadata: {}", e))?;
        self.store.put(&path, data, MetaOptions::None).await?;
        Ok(())
    }

    /// Store a specific schema version
    pub async fn store_schema_version(&self, subject: &str, version: &SchemaVersion) -> Result<()> {
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

    /// Get a specific schema version from store
    pub async fn get_version(&self, subject: &str, version: u32) -> Result<SchemaVersion> {
        let path = join_path(&[BASE_SCHEMAS_PATH, subject, "versions", &version.to_string()]);
        let value = self
            .store
            .get(&path, MetaOptions::None)
            .await?
            .ok_or_else(|| {
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

    /// List all version numbers for a subject from store
    pub async fn list_version_numbers(&self, subject: &str) -> Result<Vec<u32>> {
        let prefix = join_path(&[BASE_SCHEMAS_PATH, subject, "versions"]);
        let keys = self.store.get_childrens(&prefix).await.unwrap_or_default();
        let mut versions: Vec<u32> = keys
            .iter()
            .filter_map(|key| key.split('/').last()?.parse::<u32>().ok())
            .collect();
        versions.sort_unstable();
        Ok(versions)
    }

    /// List all subjects from store
    pub async fn list_subjects(&self) -> Result<Vec<String>> {
        let keys = self
            .store
            .get_childrens(BASE_SCHEMAS_PATH)
            .await
            .unwrap_or_default();
        let subjects: Vec<String> = keys
            .iter()
            .filter_map(|key| {
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

    /// List all subjects with a prefix from store
    #[allow(dead_code)]
    pub async fn get_subjects_with_prefix(&self, prefix: &str) -> Vec<String> {
        let full_prefix = if prefix.is_empty() {
            BASE_SCHEMAS_PATH.to_string()
        } else {
            join_path(&[BASE_SCHEMAS_PATH, prefix])
        };
        self.store
            .get_childrens(&full_prefix)
            .await
            .unwrap_or_default()
    }

    /// Delete schema version from the metadata store
    pub async fn delete_version(&self, subject: &str, version: u32) -> Result<()> {
        let path = join_path(&[BASE_SCHEMAS_PATH, subject, "versions", &version.to_string()]);
        self.store.delete(&path).await?;
        Ok(())
    }

    /// Delete all versions for a subject
    #[allow(dead_code)]
    pub async fn delete_all_versions(&self, subject: &str) -> Result<()> {
        let prefix = join_path(&[BASE_SCHEMAS_PATH, subject, "versions"]);
        let keys = self.store.get_childrens(&prefix).await.unwrap_or_default();
        for key in keys {
            self.store.delete(&key).await?;
        }
        let _ = self.store.delete(&prefix).await;
        Ok(())
    }

    /// Delete subject metadata from the metadata store
    #[allow(dead_code)]
    pub async fn delete_metadata(&self, subject: &str) -> Result<()> {
        let path = join_path(&[BASE_SCHEMAS_PATH, subject, "metadata"]);
        self.store.delete(&path).await?;
        Ok(())
    }

    /// Delete entire subject (metadata + all versions)
    #[allow(dead_code)]
    pub async fn delete_subject(&self, subject: &str) -> Result<()> {
        self.delete_all_versions(subject).await?;
        self.delete_metadata(subject).await?;
        let root_path = join_path(&[BASE_SCHEMAS_PATH, subject]);
        let _ = self.store.delete(&root_path).await;
        Ok(())
    }

    /// Get schema details by subject for admin API
    pub async fn get_schema_by_subject(&self, subject: &str) -> Option<SchemaDetails> {
        let metadata = match self.get_metadata(subject).await {
            Ok(meta) => meta,
            Err(_) => return None,
        };
        let latest_version = metadata.latest_version;
        let version_data = match self.get_version(subject, latest_version).await {
            Ok(v) => v,
            Err(_) => return None,
        };
        let schema_type = version_data.schema_def.schema_type().to_string();
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
pub struct SchemaDetails {
    pub schema_id: u64,
    pub version: u32,
    pub schema_type: String,
    pub compatibility_mode: String,
}

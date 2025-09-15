use crate::config::ObjectStoreConfig;
use crate::errors::{IcebergStorageError, Result};
use base64::Engine;
use std::collections::HashMap;

/// Create catalog properties for REST/Glue catalogs
///
/// REST and Glue catalogs create their own FileIO internally based on:
/// 1. Warehouse path scheme (s3://, gs://, file://)
/// 2. Properties passed via .props() method
/// 3. Environment variables
///
/// This function converts our ObjectStoreConfig into Iceberg property format.
pub fn create_catalog_properties(config: &ObjectStoreConfig) -> HashMap<String, String> {
    let mut props = HashMap::new();

    match config {
        ObjectStoreConfig::S3 {
            region,
            endpoint,
            path_style,
            profile,
            allow_anonymous,
            properties,
        } => {
            props.insert("s3.region".to_string(), region.clone());

            if let Some(endpoint) = endpoint {
                props.insert("s3.endpoint".to_string(), endpoint.clone());
            }

            if *path_style {
                props.insert("s3.path-style-access".to_string(), "true".to_string());
            }

            if let Some(profile) = profile {
                props.insert("s3.profile".to_string(), profile.clone());
            }

            if *allow_anonymous {
                props.insert("s3.allow-anonymous".to_string(), "true".to_string());
            }

            // Add custom properties
            for (key, value) in properties {
                props.insert(key.clone(), value.clone());
            }
        }

        ObjectStoreConfig::Gcs {
            project_id,
            endpoint,
            allow_anonymous,
            properties,
        } => {
            props.insert("gcs.project-id".to_string(), project_id.clone());

            if let Some(endpoint) = endpoint {
                props.insert("gcs.service.path".to_string(), endpoint.clone());
            }

            if *allow_anonymous {
                props.insert("gcs.no-auth".to_string(), "true".to_string());
            }

            // Add custom properties
            for (key, value) in properties {
                props.insert(key.clone(), value.clone());
            }
        }

        ObjectStoreConfig::Local { .. } => {
            // Local filesystem doesn't need additional properties
            // The warehouse path (file://) determines the FileIO type
        }

        ObjectStoreConfig::Memory { name } => {
            if let Some(name) = name {
                props.insert("memory.name".to_string(), name.clone());
            }
        }
    }

    // Apply environment variable overrides
    apply_env_overrides(&mut props, config);

    props
}

/// Create FileIO directly for Memory catalog
///
/// Memory catalog requires explicit FileIO creation since it doesn't use
/// warehouse path-based FileIO detection like REST/Glue catalogs.
///
/// This follows the same pattern as MemoryCatalog::new() in the Iceberg source:
/// FileIO::from_path(&warehouse)?.with_props(props).build()?
pub async fn create_file_io(
    config: &ObjectStoreConfig,
    warehouse: &str,
) -> Result<iceberg::io::FileIO> {
    tracing::debug!(
        "Creating FileIO for MemoryCatalog: config={:?}, warehouse={}",
        config,
        warehouse
    );

    // Get properties for this storage backend
    let props = create_catalog_properties(config);

    tracing::debug!("FileIO properties: {:?}", props);

    // Create FileIO using warehouse path + properties (same as MemoryCatalog)
    let file_io_builder = iceberg::io::FileIO::from_path(warehouse).map_err(|e| {
        tracing::error!("Failed to parse warehouse path '{}': {}", warehouse, e);
        IcebergStorageError::Catalog(format!(
            "Failed to parse warehouse path '{}': {}",
            warehouse, e
        ))
    })?;

    tracing::debug!("FileIO builder created successfully");

    let file_io = file_io_builder.with_props(props).build().map_err(|e| {
        tracing::error!("Failed to build FileIO: {}", e);
        IcebergStorageError::Catalog(format!("Failed to build FileIO: {}", e))
    })?;

    tracing::debug!("FileIO created successfully for MemoryCatalog");
    Ok(file_io)
}

/// Apply environment variable overrides to properties
///
/// This centralizes environment variable handling for all storage backends.
fn apply_env_overrides(props: &mut HashMap<String, String>, config: &ObjectStoreConfig) {
    match config {
        ObjectStoreConfig::S3 { .. } => {
            apply_s3_env_overrides(props);
        }
        ObjectStoreConfig::Gcs { .. } => {
            apply_gcs_env_overrides(props);
        }
        ObjectStoreConfig::Local { .. } | ObjectStoreConfig::Memory { .. } => {
            // No environment overrides needed
        }
    }
}

/// Apply S3 environment variable overrides to properties map
fn apply_s3_env_overrides(props: &mut HashMap<String, String>) {
    // AWS credentials from environment
    if let Ok(access_key) = std::env::var("AWS_ACCESS_KEY_ID") {
        props.insert("s3.access-key-id".to_string(), access_key);
    }

    if let Ok(secret_key) = std::env::var("AWS_SECRET_ACCESS_KEY") {
        props.insert("s3.secret-access-key".to_string(), secret_key);
    }

    if let Ok(session_token) = std::env::var("AWS_SESSION_TOKEN") {
        props.insert("s3.session-token".to_string(), session_token);
    }

    // AWS region and profile fallbacks
    if let Ok(region) = std::env::var("AWS_REGION") {
        props.insert("client.region".to_string(), region);
    }

    if let Ok(profile) = std::env::var("AWS_PROFILE") {
        props.insert("s3.profile".to_string(), profile);
    }

    // S3-specific environment variables
    if let Ok(endpoint) = std::env::var("S3_ENDPOINT") {
        props.insert("s3.endpoint".to_string(), endpoint);
    }

    if let Ok(sse_type) = std::env::var("S3_SSE_TYPE") {
        props.insert("s3.sse.type".to_string(), sse_type);
    }

    if let Ok(sse_key) = std::env::var("S3_SSE_KEY") {
        props.insert("s3.sse.key".to_string(), sse_key);
    }

    if let Ok(role_arn) = std::env::var("S3_ASSUME_ROLE_ARN") {
        props.insert("client.assume-role.arn".to_string(), role_arn);
    }

    if let Ok(disable_ec2) = std::env::var("S3_DISABLE_EC2_METADATA") {
        if disable_ec2.to_lowercase() == "true" {
            props.insert("s3.disable-ec2-metadata".to_string(), "true".to_string());
        }
    }
}

/// Apply GCS environment variable overrides to properties map
fn apply_gcs_env_overrides(props: &mut HashMap<String, String>) {
    // Google Cloud credentials
    if let Ok(cred_file) = std::env::var("GOOGLE_APPLICATION_CREDENTIALS") {
        // Read credentials file and base64 encode it
        if let Ok(cred_content) = std::fs::read_to_string(&cred_file) {
            let encoded = base64::prelude::BASE64_STANDARD.encode(cred_content.as_bytes());
            props.insert("gcs.credentials-json".to_string(), encoded);
        }
    }

    if let Ok(cred_json) = std::env::var("GCS_CREDENTIALS_JSON") {
        props.insert("gcs.credentials-json".to_string(), cred_json);
    }

    if let Ok(project) = std::env::var("GOOGLE_CLOUD_PROJECT") {
        props.insert("gcs.project-id".to_string(), project);
    }

    if let Ok(token) = std::env::var("GCS_TOKEN") {
        props.insert("gcs.oauth2.token".to_string(), token);
    }

    if let Ok(user_project) = std::env::var("GCS_USER_PROJECT") {
        props.insert("gcs.user-project".to_string(), user_project);
    }

    if let Ok(disable_vm) = std::env::var("GCS_DISABLE_VM_METADATA") {
        if disable_vm.to_lowercase() == "true" {
            props.insert("gcs.disable-vm-metadata".to_string(), "true".to_string());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_s3_catalog_properties() {
        let config = ObjectStoreConfig::S3 {
            region: "us-west-2".to_string(),
            endpoint: Some("https://s3.amazonaws.com".to_string()),
            path_style: false,
            profile: None,
            allow_anonymous: false,
            properties: HashMap::new(),
        };

        let props = create_catalog_properties(&config);
        assert_eq!(props.get("s3.region"), Some(&"us-west-2".to_string()));
        assert_eq!(
            props.get("s3.endpoint"),
            Some(&"https://s3.amazonaws.com".to_string())
        );
    }

    #[test]
    fn test_create_gcs_catalog_properties() {
        let config = ObjectStoreConfig::Gcs {
            project_id: "test-project".to_string(),
            endpoint: None,
            allow_anonymous: true,
            properties: HashMap::new(),
        };

        let props = create_catalog_properties(&config);
        assert_eq!(
            props.get("gcs.project-id"),
            Some(&"test-project".to_string())
        );
        assert_eq!(props.get("gcs.no-auth"), Some(&"true".to_string()));
    }

    #[tokio::test]
    async fn test_create_memory_file_io() {
        let config = ObjectStoreConfig::Memory {
            name: Some("test-memory".to_string()),
        };

        let file_io = create_file_io(&config, "memory://test").await.unwrap();
        // Basic smoke test - FileIO should be created successfully
        assert!(!file_io.into_builder().build().is_err());
    }
}

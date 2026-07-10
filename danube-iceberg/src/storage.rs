//! Object store construction for danube-iceberg.
//!
//! Builds an `Arc<dyn ObjectStore>` from the YAML config. This is independent
//! from `danube-persistent-storage`'s object_store (which uses v0.14) — here
//! we use the v0.13.2 that aligns with `parquet` 57.

use crate::config::StorageConfig;
use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use std::sync::Arc;
use tracing::warn;

/// Result of building the object store: the store itself plus the key prefix.
pub struct StorageHandle {
    pub store: Arc<dyn ObjectStore>,
    /// Key prefix extracted from the root URL (e.g., "danube" from "s3://bucket/danube").
    pub root_prefix: String,
}

impl StorageHandle {
    /// Join the root prefix with a relative path.
    pub fn path(&self, relative: &str) -> object_store::path::Path {
        let key = if self.root_prefix.is_empty() {
            relative.to_string()
        } else {
            format!("{}/{}", self.root_prefix, relative)
        };
        object_store::path::Path::from(key)
    }
}

/// Build an `Arc<dyn ObjectStore>` + key prefix from the iceberg config.
pub fn build_storage(config: &StorageConfig) -> anyhow::Result<StorageHandle> {
    match config.backend.as_str() {
        "s3" => build_s3(config),
        "gcs" => build_gcs(config),
        "azure" => build_azure(config),
        "fs" => build_fs(config),
        other => anyhow::bail!("unsupported storage backend: {}", other),
    }
}

fn build_s3(config: &StorageConfig) -> anyhow::Result<StorageHandle> {
    let (bucket, prefix) = split_bucket_prefix(&config.root)?;
    let opts = &config.options;

    let mut builder = AmazonS3Builder::new().with_bucket_name(&bucket);

    if let Some(endpoint) = opts.get("endpoint") {
        builder = builder.with_endpoint(endpoint);
    }
    if let Some(region) = opts.get("region") {
        builder = builder.with_region(region);
    }
    if let Some(ak) = opts.get("access_key") {
        builder = builder.with_access_key_id(ak);
    }
    if let Some(sk) = opts.get("secret_key") {
        builder = builder.with_secret_access_key(sk);
    }

    // Default to path-style for custom endpoints (e.g., MinIO)
    let has_custom_endpoint = opts.contains_key("endpoint");
    let vhost = opts
        .get("virtual_host_style")
        .map(|v| matches!(v.as_str(), "true" | "1" | "yes"))
        .unwrap_or(!has_custom_endpoint);
    if !vhost {
        builder = builder.with_virtual_hosted_style_request(false);
    }

    // Allow HTTP for non-TLS endpoints (e.g., MinIO)
    if opts
        .get("endpoint")
        .map_or(false, |e| e.starts_with("http://"))
    {
        builder = builder.with_allow_http(true);
    }

    let store = builder.build()?;
    Ok(StorageHandle {
        store: Arc::new(store),
        root_prefix: prefix,
    })
}

fn build_gcs(config: &StorageConfig) -> anyhow::Result<StorageHandle> {
    let (bucket, prefix) = split_bucket_prefix(&config.root)?;
    let opts = &config.options;

    let mut builder = GoogleCloudStorageBuilder::new().with_bucket_name(&bucket);

    if let Some(cred_file) = opts.get("credential_file") {
        builder = builder.with_service_account_path(cred_file);
    }
    if let Some(endpoint) = opts.get("endpoint") {
        builder = builder.with_url(endpoint);
    }

    let store = builder.build()?;
    Ok(StorageHandle {
        store: Arc::new(store),
        root_prefix: prefix,
    })
}

fn build_azure(config: &StorageConfig) -> anyhow::Result<StorageHandle> {
    let (container, prefix) = split_bucket_prefix(&config.root)?;
    let opts = &config.options;

    let mut builder = MicrosoftAzureBuilder::new().with_container_name(&container);

    if let Some(endpoint) = opts.get("endpoint") {
        builder = builder.with_endpoint(endpoint.clone());
    }
    if let Some(name) = opts.get("account_name") {
        builder = builder.with_account(name);
    }
    if let Some(key) = opts.get("account_key") {
        builder = builder.with_access_key(key);
    }

    let store = builder.build()?;
    Ok(StorageHandle {
        store: Arc::new(store),
        root_prefix: prefix,
    })
}

fn build_fs(config: &StorageConfig) -> anyhow::Result<StorageHandle> {
    let path = config
        .root
        .strip_prefix("file://")
        .unwrap_or(&config.root);
    std::fs::create_dir_all(path)?;
    let store = LocalFileSystem::new_with_prefix(path)?;
    Ok(StorageHandle {
        store: Arc::new(store),
        root_prefix: String::new(),
    })
}

fn split_bucket_prefix(uri: &str) -> anyhow::Result<(String, String)> {
    let parts: Vec<&str> = uri.splitn(2, "://").collect();
    if parts.len() == 2 {
        let rest = parts[1];
        let mut it = rest.splitn(2, '/');
        let bucket = it.next().unwrap_or("").to_string();
        if bucket.is_empty() {
            anyhow::bail!("invalid URI, missing bucket: {}", uri);
        }
        let prefix = it.next().unwrap_or("").trim_matches('/').to_string();
        Ok((bucket, prefix))
    } else {
        Ok((uri.to_string(), String::new()))
    }
}

fn _warn_unknown_options(service: &str, options: &std::collections::HashMap<String, String>, allowed: &[&str]) {
    for k in options.keys() {
        if !allowed.contains(&k.as_str()) {
            warn!("unknown {} option '{}'; accepted keys: {:?}", service, k, allowed);
        }
    }
}

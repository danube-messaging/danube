use danube_core::storage::PersistentStorageError;
use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::local::LocalFileSystem;
#[cfg(test)]
use object_store::memory::InMemory;
use object_store::ObjectStore;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::warn;

#[derive(Debug, Clone)]
pub enum ObjectStoreBackend {
    S3,
    Gcs,
    Azblob,
}

#[derive(Debug, Clone)]
pub(crate) enum BackendConfig {
    ObjectStore {
        backend: ObjectStoreBackend,
        /// A URI-like root, e.g. s3://bucket/prefix, gcs://bucket/prefix
        root: String,
        /// Optional backend-specific options (endpoint, region, credentials, etc.)
        options: HashMap<String, String>,
    },
    Filesystem {
        /// An absolute directory like file:///var/lib/danube or /var/lib/danube
        root: String,
    },
    #[cfg(test)]
    Memory {
        /// A logical namespace like memory:// (prefix is used as a virtual root)
        root: String,
    },
}

impl BackendConfig {
    /// Build an `Arc<dyn ObjectStore>` and return it along with a key prefix
    /// (for prepending to object keys) and a provider string.
    pub fn build_object_store(
        &self,
    ) -> Result<(Arc<dyn ObjectStore>, String, String), PersistentStorageError> {
        match self {
            BackendConfig::ObjectStore {
                backend,
                root,
                options,
            } => match backend {
                ObjectStoreBackend::S3 => {
                    let (bucket, prefix) =
                        split_bucket_prefix(root).map_err(PersistentStorageError::Other)?;

                    let mut builder = AmazonS3Builder::new().with_bucket_name(&bucket);

                    if let Some(endpoint) = options.get("endpoint") {
                        builder = builder.with_endpoint(endpoint);
                    }
                    if let Some(region) = options.get("region") {
                        builder = builder.with_region(region);
                    }
                    if let Some(ak) = options.get("access_key") {
                        builder = builder.with_access_key_id(ak);
                    }
                    if let Some(sk) = options.get("secret_key") {
                        builder = builder.with_secret_access_key(sk);
                    }

                    // Addressing mode: default to path-style for custom endpoints (e.g., MinIO)
                    let vhost_opt = options.get("virtual_host_style");
                    let has_custom_endpoint = options.contains_key("endpoint");
                    let vhost = match vhost_opt {
                        Some(v) => matches!(v.as_str(), "true" | "1" | "yes"),
                        None => !has_custom_endpoint,
                    };
                    if !vhost {
                        builder = builder.with_virtual_hosted_style_request(false);
                    }

                    // Allow HTTP for non-TLS endpoints (e.g., MinIO)
                    if options
                        .get("endpoint")
                        .map_or(false, |e| e.starts_with("http://"))
                    {
                        builder = builder.with_allow_http(true);
                    }

                    warn_unknown_options(
                        "s3",
                        options,
                        &[
                            "endpoint",
                            "region",
                            "access_key",
                            "secret_key",
                            "virtual_host_style",
                        ],
                    );

                    let store = builder.build().map_err(|e| {
                        PersistentStorageError::Other(format!("object_store s3 builder: {}", e))
                    })?;
                    Ok((Arc::new(store), normalize_prefix(&prefix), "s3".to_string()))
                }
                ObjectStoreBackend::Gcs => {
                    let (bucket, prefix) =
                        split_bucket_prefix(root).map_err(PersistentStorageError::Other)?;

                    warn_unknown_options("gcs", options, &["endpoint", "credential_file"]);

                    let mut builder = GoogleCloudStorageBuilder::new().with_bucket_name(&bucket);

                    if let Some(cred_file) = options.get("credential_file") {
                        builder = builder.with_service_account_path(cred_file);
                    }
                    if let Some(endpoint) = options.get("endpoint") {
                        builder = builder.with_base_url(endpoint);
                    }

                    let store = builder.build().map_err(|e| {
                        PersistentStorageError::Other(format!("object_store gcs builder: {}", e))
                    })?;
                    Ok((
                        Arc::new(store),
                        normalize_prefix(&prefix),
                        "gcs".to_string(),
                    ))
                }
                ObjectStoreBackend::Azblob => {
                    let (container, prefix) =
                        split_bucket_prefix(root).map_err(PersistentStorageError::Other)?;

                    warn_unknown_options(
                        "azblob",
                        options,
                        &["endpoint", "account_name", "account_key"],
                    );

                    let mut builder = MicrosoftAzureBuilder::new().with_container_name(&container);

                    if let Some(endpoint) = options.get("endpoint") {
                        builder = builder.with_endpoint(endpoint.clone());
                    }
                    if let Some(name) = options.get("account_name") {
                        builder = builder.with_account(name);
                    }
                    if let Some(key) = options.get("account_key") {
                        builder = builder.with_access_key(key);
                    }

                    let store = builder.build().map_err(|e| {
                        PersistentStorageError::Other(format!("object_store azblob builder: {}", e))
                    })?;
                    Ok((
                        Arc::new(store),
                        normalize_prefix(&prefix),
                        "azblob".to_string(),
                    ))
                }
            },
            BackendConfig::Filesystem { root } => {
                let (fs_root, prefix) =
                    split_fs_root(root).map_err(PersistentStorageError::Other)?;
                // Ensure the directory exists (OpenDAL would create it lazily;
                // object_store's LocalFileSystem requires it to exist for canonicalization).
                std::fs::create_dir_all(&fs_root).map_err(|e| {
                    PersistentStorageError::Other(format!("create fs root dir {}: {}", fs_root, e))
                })?;
                let store = LocalFileSystem::new_with_prefix(&fs_root).map_err(|e| {
                    PersistentStorageError::Other(format!("object_store fs builder: {}", e))
                })?;
                Ok((Arc::new(store), prefix, "fs".to_string()))
            }
            #[cfg(test)]
            BackendConfig::Memory { root } => {
                let store = InMemory::new();
                Ok((
                    Arc::new(store),
                    normalize_prefix(root),
                    "memory".to_string(),
                ))
            }
        }
    }
}

fn warn_unknown_options(
    service: &str,
    options: &std::collections::HashMap<String, String>,
    allowed: &[&str],
) {
    for k in options.keys() {
        if !allowed.contains(&k.as_str()) {
            warn!(
                target = "object_store_backend",
                "unknown {} option '{}'; accepted keys: {:?}", service, k, allowed
            );
        }
    }
}

pub(crate) fn split_bucket_prefix(uri: &str) -> Result<(String, String), String> {
    // Accept formats: s3://bucket, s3://bucket/prefix, gcs://bucket/prefix
    let parts: Vec<&str> = uri.splitn(2, "://").collect();
    if parts.len() == 2 {
        let rest = parts[1];
        let mut it = rest.splitn(2, '/');
        let bucket = it.next().unwrap_or("").to_string();
        if bucket.is_empty() {
            return Err(format!("invalid uri, missing bucket: {}", uri));
        }
        let prefix = it.next().unwrap_or("").to_string();
        Ok((bucket, normalize_prefix(&prefix)))
    } else {
        // If no scheme, treat entire string as bucket and no prefix
        Ok((uri.to_string(), String::new()))
    }
}

pub(crate) fn split_fs_root(uri_or_path: &str) -> Result<(String, String), String> {
    // Accept file:///abs/path/prefix or /abs/path/prefix
    let s = if let Some(rest) = uri_or_path.strip_prefix("file://") {
        rest
    } else {
        uri_or_path
    };
    // We keep entire path as fs root and no extra prefix.
    let fs_root = s.to_string();
    Ok((fs_root, String::new()))
}

fn normalize_prefix(p: &str) -> String {
    let trimmed = p.trim_matches('/');
    trimmed.to_string()
}

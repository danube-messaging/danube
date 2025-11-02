use danube_core::storage::PersistentStorageError;
use opendal::services::{Azblob, Fs, Gcs, Memory, S3};
use opendal::Operator;
use std::collections::HashMap;
use tracing::warn;

#[derive(Debug, Clone)]
pub enum CloudBackend {
    S3,
    Gcs,
    Azblob,
}

#[derive(Debug, Clone)]
pub enum LocalBackend {
    Fs,
    Memory,
}

#[derive(Debug, Clone)]
pub enum BackendConfig {
    /// Cloud backends hosted out of process (S3, GCS)
    Cloud {
        backend: CloudBackend,
        /// A URI-like root, e.g. s3://bucket/prefix, gcs://bucket/prefix
        root: String,
        /// Optional backend-specific options (endpoint, region, credentials, etc.)
        options: HashMap<String, String>,
    },
    /// Local backends colocated with the broker (fs, memory)
    Local {
        backend: LocalBackend,
        /// For fs: an absolute directory like file:///var/lib/danube or /var/lib/danube
        /// For memory: a logical namespace like memory:// (prefix is used as a virtual root)
        root: String,
    },
}

impl BackendConfig {
    /// Build an OpenDAL Operator and return it along with an optional root prefix
    /// to be prepended to object keys (used by some local backends), and a provider string.
    pub fn build_operator(&self) -> Result<(Operator, String, String), PersistentStorageError> {
        match self {
            BackendConfig::Cloud {
                backend,
                root,
                options,
            } => match backend {
                CloudBackend::S3 => {
                    // Expect root like s3://bucket or s3://bucket/prefix
                    let (bucket, prefix) =
                        split_bucket_prefix(root).map_err(|e| PersistentStorageError::Other(e))?;
                    let mut builder = S3::default();
                    builder = builder.bucket(&bucket);
                    if !prefix.is_empty() {
                        // S3 root must be an absolute path
                        builder = builder.root(&format!("/{}", prefix));
                    }
                    if let Some(endpoint) = options.get("endpoint") {
                        builder = builder.endpoint(endpoint);
                    }
                    if let Some(region) = options.get("region") {
                        builder = builder.region(region);
                    }
                    if let Some(ak) = options.get("access_key") {
                        builder = builder.access_key_id(ak);
                    }
                    if let Some(sk) = options.get("secret_key") {
                        builder = builder.secret_access_key(sk);
                    }
                    // Addressing mode: default to path-style for custom endpoints (e.g., MinIO)
                    let vhost_opt = options.get("virtual_host_style");
                    let has_custom_endpoint = options.get("endpoint").is_some();
                    let vhost = match vhost_opt {
                        Some(v) => matches!(v.as_str(), "true" | "1" | "yes"),
                        None => !has_custom_endpoint, // if custom endpoint, prefer path-style (false)
                    };
                    if vhost {
                        builder = builder.enable_virtual_host_style();
                    }
                    let op = Operator::new(builder)
                        .map_err(|e| {
                            PersistentStorageError::Other(format!("opendal s3 builder: {}", e))
                        })?
                        .finish();
                    Ok((op, String::new(), "s3".to_string()))
                }
                CloudBackend::Gcs => {
                    // Expect root like gcs://bucket or gcs://bucket/prefix
                    let (bucket, prefix) =
                        split_bucket_prefix(root).map_err(|e| PersistentStorageError::Other(e))?;
                    warn_unknown_options("gcs", options, &["endpoint", "credential_file"]);
                    let mut builder = Gcs::default();
                    builder = builder.bucket(&bucket);
                    if !prefix.is_empty() {
                        builder = builder.root(&format!("/{}", prefix));
                    }
                    if let Some(cred_file) = options.get("credential_file") {
                        builder = builder.credential_path(cred_file);
                    }
                    if let Some(endpoint) = options.get("endpoint") {
                        builder = builder.endpoint(endpoint);
                    }
                    let op = Operator::new(builder)
                        .map_err(|e| {
                            PersistentStorageError::Other(format!("opendal gcs builder: {}", e))
                        })?
                        .finish();
                    Ok((op, String::new(), "gcs".to_string()))
                }
                CloudBackend::Azblob => {
                    // Expect root like "container" or "container/prefix"
                    let (container, prefix) =
                        split_bucket_prefix(root).map_err(|e| PersistentStorageError::Other(e))?;
                    // Allowed options for azblob
                    warn_unknown_options(
                        "azblob",
                        options,
                        &["endpoint", "account_name", "account_key"],
                    );
                    let mut builder = Azblob::default();
                    builder = builder.container(&container);
                    if !prefix.is_empty() {
                        builder = builder.root(&format!("/{}", prefix));
                    }
                    if let Some(endpoint) = options.get("endpoint") {
                        builder = builder.endpoint(endpoint);
                    }
                    if let Some(name) = options.get("account_name") {
                        builder = builder.account_name(name);
                    }
                    if let Some(key) = options.get("account_key") {
                        builder = builder.account_key(key);
                    }
                    let op = Operator::new(builder)
                        .map_err(|e| {
                            PersistentStorageError::Other(format!("opendal azblob builder: {}", e))
                        })?
                        .finish();
                    Ok((op, String::new(), "azblob".to_string()))
                }
            },
            BackendConfig::Local { backend, root } => match backend {
                LocalBackend::Fs => {
                    // Accept either file:///abs/path or /abs/path
                    let (fs_root, prefix) =
                        split_fs_root(root).map_err(|e| PersistentStorageError::Other(e))?;
                    let builder = Fs::default().root(&fs_root);
                    let op = Operator::new(builder)
                        .map_err(|e| {
                            PersistentStorageError::Other(format!("opendal fs builder: {}", e))
                        })?
                        .finish();
                    Ok((op, prefix, "fs".to_string()))
                }
                LocalBackend::Memory => {
                    // Memory service ignores root but we keep a logical prefix
                    let builder = Memory::default();
                    let op = Operator::new(builder)
                        .map_err(|e| {
                            PersistentStorageError::Other(format!("opendal memory builder: {}", e))
                        })?
                        .finish();
                    Ok((op, normalize_prefix(root), "memory".to_string()))
                }
            },
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
                target = "cloud_store",
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

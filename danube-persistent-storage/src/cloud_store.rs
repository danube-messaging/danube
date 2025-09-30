use danube_core::storage::PersistentStorageError;
use opendal::services::{Fs, Gcs, Memory, S3};
use opendal::Operator;
use tracing::warn;

#[derive(Debug, Clone)]
pub enum CloudBackend {
    S3,
    Gcs,
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
        options: std::collections::HashMap<String, String>,
    },
    /// Local backends colocated with the broker (fs, memory)
    Local {
        backend: LocalBackend,
        /// For fs: an absolute directory like file:///var/lib/danube or /var/lib/danube
        /// For memory: a logical namespace like memory:// (prefix is used as a virtual root)
        root: String,
    },
}

#[derive(Debug, Clone)]
pub struct CloudStore {
    /// Optional extra prefix for key joining (used by Local backends)
    root_prefix: String,
    /// Opendal operator
    op: Operator,
}

fn warn_unknown_options(
    service: &str,
    options: &std::collections::HashMap<String, String>,
    allowed: &[&str],
) {
    for k in options.keys() {
        if !allowed.contains(&k.as_str()) {
            warn!(
                target: "cloud_store",
                "unknown {} option '{}'; accepted keys: {:?}",
                service,
                k,
                allowed
            );
        }
    }
}

impl CloudStore {
    pub fn new(cfg: BackendConfig) -> Result<Self, PersistentStorageError> {
        let (op, root_prefix) = match cfg {
            BackendConfig::Cloud {
                backend,
                root,
                options,
            } => match backend {
                CloudBackend::S3 => {
                    // Expect root like s3://bucket or s3://bucket/prefix
                    let (bucket, prefix) =
                        split_bucket_prefix(&root).map_err(|e| PersistentStorageError::Other(e))?;
                    // Builders in opendal 0.54 consume self: use chaining/reassignment
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
                    let op = Operator::new(builder)
                        .map_err(|e| {
                            PersistentStorageError::Other(format!("opendal s3 builder: {}", e))
                        })?
                        .finish();
                    (op, String::new())
                }
                CloudBackend::Gcs => {
                    // Expect root like gcs://bucket or gcs://bucket/prefix
                    let (bucket, prefix) =
                        split_bucket_prefix(&root).map_err(|e| PersistentStorageError::Other(e))?;
                    warn_unknown_options("gcs", &options, &["endpoint", "credential_file"]);
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
                    (op, String::new())
                }
            },
            BackendConfig::Local { backend, root } => match backend {
                LocalBackend::Fs => {
                    // Accept either file:///abs/path or /abs/path
                    let (fs_root, prefix) =
                        split_fs_root(&root).map_err(|e| PersistentStorageError::Other(e))?;
                    let builder = Fs::default().root(&fs_root);
                    let op = Operator::new(builder)
                        .map_err(|e| {
                            PersistentStorageError::Other(format!("opendal fs builder: {}", e))
                        })?
                        .finish();
                    (op, prefix)
                }
                LocalBackend::Memory => {
                    // Memory service ignores root but we keep a logical prefix
                    let builder = Memory::default();
                    let op = Operator::new(builder)
                        .map_err(|e| {
                            PersistentStorageError::Other(format!("opendal memory builder: {}", e))
                        })?
                        .finish();
                    (op, normalize_prefix(&root))
                }
            },
        };
        Ok(Self { root_prefix, op })
    }

    pub async fn put_object(&self, path: &str, bytes: &[u8]) -> Result<(), PersistentStorageError> {
        // Use Writer-based API to allow backend MPU and return Metadata; discard it here.
        let _ = self.put_object_meta(path, bytes).await?;
        Ok(())
    }

    pub async fn get_object(&self, path: &str) -> Result<Vec<u8>, PersistentStorageError> {
        let key = self.join(path);
        let data = self.op.read(&key).await.map_err(|e| {
            PersistentStorageError::Other(format!("cloud get_object {}: {}", key, e))
        })?;
        Ok(data.to_vec())
    }

    /// Write an object using the streaming writer API and return backend-provided metadata.
    /// This enables multipart uploads and exposes fields like ETag where supported.
    pub async fn put_object_meta(
        &self,
        path: &str,
        bytes: &[u8],
    ) -> Result<opendal::Metadata, PersistentStorageError> {
        let key = self.join(path);
        let mut writer =
            self.op.writer(&key).await.map_err(|e| {
                PersistentStorageError::Other(format!("cloud writer {}: {}", key, e))
            })?;
        // Write requires owned data for the async future; pass a Buffer
        let buf = opendal::Buffer::from(bytes.to_vec());
        writer
            .write(buf)
            .await
            .map_err(|e| PersistentStorageError::Other(format!("cloud write {}: {}", key, e)))?;
        let meta = writer
            .close()
            .await
            .map_err(|e| PersistentStorageError::Other(format!("cloud close {}: {}", key, e)))?;
        Ok(meta)
    }

    #[inline]
    fn join(&self, path: &str) -> String {
        let p = path.trim_matches('/');
        if self.root_prefix.is_empty() {
            p.to_string()
        } else {
            format!("{}/{}", self.root_prefix.trim_matches('/'), p)
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

pub(crate) fn normalize_prefix(p: &str) -> String {
    let trimmed = p.trim_matches('/');
    trimmed.to_string()
}

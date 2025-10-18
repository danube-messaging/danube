use std::collections::HashMap;

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

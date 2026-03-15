use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub(crate) enum StorageConfig {
    Local {
        root: String,
        #[serde(default)]
        metadata_root: Option<String>,
        #[serde(default)]
        wal: WalNode,
    },
    SharedFs {
        root: String,
        #[serde(default)]
        cache_root: Option<String>,
        #[serde(default)]
        metadata_root: Option<String>,
        #[serde(default)]
        wal: WalNode,
    },
    CloudNative {
        cloud: CloudConfig,
        #[serde(default)]
        cache_root: Option<String>,
        #[serde(default)]
        metadata_root: Option<String>,
        #[serde(default)]
        wal: WalNode,
    },
}

#[derive(Debug, Deserialize, Clone, Default)]
pub(crate) struct WalNode {
    pub(crate) dir: Option<String>,
    pub(crate) file_name: Option<String>,
    pub(crate) cache_capacity: Option<usize>,
    pub(crate) file_sync: Option<WalFlushNode>,
    pub(crate) rotation: Option<WalRotationNode>,
    pub(crate) retention: Option<WalRetentionNode>,
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct WalFlushNode {
    pub(crate) interval_ms: Option<u64>,
    pub(crate) max_batch_bytes: Option<usize>,
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct WalRotationNode {
    pub(crate) max_bytes: Option<u64>,
    pub(crate) max_hours: Option<u64>,
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct WalRetentionNode {
    pub(crate) time_minutes: Option<u64>,
    pub(crate) size_mb: Option<u64>,
    pub(crate) check_interval_minutes: Option<u64>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "backend")]
pub(crate) enum CloudConfig {
    #[serde(rename = "s3")]
    S3 {
        root: String,
        region: Option<String>,
        endpoint: Option<String>,
        access_key: Option<String>,
        secret_key: Option<String>,
        profile: Option<String>,
        role_arn: Option<String>,
        session_token: Option<String>,
        anonymous: Option<bool>,
        virtual_host_style: Option<bool>,
    },
    #[serde(rename = "gcs")]
    Gcs {
        root: String,
        project: Option<String>,
        credentials_json: Option<String>,
        credentials_path: Option<String>,
    },
    #[serde(rename = "azblob")]
    Azblob {
        root: String,
        endpoint: Option<String>,
        account_name: Option<String>,
        account_key: Option<String>,
    },
}

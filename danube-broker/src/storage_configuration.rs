use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub(crate) enum StorageConfig {
    Local {
        #[serde(alias = "root")]
        local_wal_root: String,
        #[serde(default, alias = "metadata_root")]
        metadata_prefix: Option<String>,
        #[serde(default)]
        local_retention: Option<LocalRetentionNode>,
        #[serde(default)]
        wal: WalNode,
    },
    SharedFs {
        #[serde(default, alias = "cache_root")]
        local_wal_root: Option<String>,
        #[serde(default, alias = "metadata_root")]
        metadata_prefix: Option<String>,
        #[serde(default)]
        durable: Option<SharedFsDurableNode>,
        #[serde(default, rename = "root")]
        legacy_root: Option<String>,
        #[serde(default)]
        local_retention: Option<LocalRetentionNode>,
        #[serde(default)]
        wal: WalNode,
    },
    ObjectStore {
        #[serde(default, alias = "cache_root")]
        local_wal_root: Option<String>,
        #[serde(default, alias = "metadata_root")]
        metadata_prefix: Option<String>,
        #[serde(default)]
        durable: Option<ObjectStoreNode>,
        #[serde(default, rename = "object_store")]
        legacy_object_store: Option<ObjectStoreNode>,
        #[serde(default)]
        local_retention: Option<LocalRetentionNode>,
        #[serde(default)]
        wal: WalNode,
    },
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct SharedFsDurableNode {
    pub(crate) root: String,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub(crate) struct WalNode {
    pub(crate) dir: Option<String>,
    pub(crate) file_name: Option<String>,
    pub(crate) rotation: Option<WalRotationNode>,
    pub(crate) advanced: Option<WalAdvancedNode>,

    #[serde(default)]
    pub(crate) cache_capacity: Option<usize>,
    #[serde(default)]
    pub(crate) file_sync: Option<WalFlushNode>,
    #[serde(default)]
    pub(crate) retention: Option<LocalRetentionNode>,
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
pub(crate) struct WalAdvancedNode {
    pub(crate) cache_capacity: Option<usize>,
    pub(crate) file_sync: Option<WalFlushNode>,
    pub(crate) rotation: Option<WalRotationNode>,
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct LocalRetentionNode {
    pub(crate) time_minutes: Option<u64>,
    pub(crate) size_mb: Option<u64>,
    pub(crate) check_interval_minutes: Option<u64>,
}

impl WalNode {
    pub(crate) fn cache_capacity(&self) -> Option<usize> {
        self.advanced
            .as_ref()
            .and_then(|advanced| advanced.cache_capacity)
            .or(self.cache_capacity)
    }

    pub(crate) fn file_sync(&self) -> Option<&WalFlushNode> {
        self.advanced
            .as_ref()
            .and_then(|advanced| advanced.file_sync.as_ref())
            .or(self.file_sync.as_ref())
    }

    pub(crate) fn rotate_max_bytes(&self) -> Option<u64> {
        self.rotation
            .as_ref()
            .and_then(|rotation| rotation.max_bytes)
            .or_else(|| {
                self.advanced
                    .as_ref()
                    .and_then(|advanced| advanced.rotation.as_ref())
                    .and_then(|rotation| rotation.max_bytes)
            })
    }

    pub(crate) fn rotate_max_hours(&self) -> Option<u64> {
        self.advanced
            .as_ref()
            .and_then(|advanced| advanced.rotation.as_ref())
            .and_then(|rotation| rotation.max_hours)
            .or_else(|| self.rotation.as_ref().and_then(|rotation| rotation.max_hours))
    }

    pub(crate) fn legacy_local_retention(&self) -> Option<&LocalRetentionNode> {
        self.retention.as_ref()
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "backend")]
pub(crate) enum ObjectStoreNode {
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

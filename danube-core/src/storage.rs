use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DiskConfig {
    pub path: String,
}

impl Display for DiskConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DiskConfig(path: {})", self.path)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct S3Config {
    pub bucket: String,
    pub region: String,
}

impl Display for S3Config {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "S3Config(bucket: {}, region: {})",
            self.bucket, self.region
        )
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type")]
pub enum StorageConfig {
    #[serde(rename = "inmemory")]
    InMemory,
    #[serde(rename = "disk")]
    Disk(DiskConfig),
    #[serde(rename = "s3")]
    S3(S3Config),
}

impl Display for StorageConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageConfig::InMemory => write!(f, "InMemory"),
            StorageConfig::Disk(config) => write!(f, "{}", config),
            StorageConfig::S3(config) => write!(f, "{}", config),
        }
    }
}

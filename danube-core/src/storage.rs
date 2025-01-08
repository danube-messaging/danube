use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct DiskConfig {
    pub path: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct S3Config {
    pub bucket: String,
    pub region: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum StorageConfig {
    #[serde(rename = "inmemory")]
    InMemory,
    #[serde(rename = "disk")]
    Disk(DiskConfig),
    #[serde(rename = "s3")]
    S3(S3Config),
}

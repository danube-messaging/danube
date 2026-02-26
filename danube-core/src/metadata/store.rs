use async_trait::async_trait;
use serde_json::Value;
use std::time::Duration;

use super::errors::Result;
use super::watch::WatchStream;

/// A key-value-version tuple returned by bulk queries.
#[derive(Debug)]
pub struct KeyValueVersion {
    pub key: String,
    pub value: Vec<u8>,
    pub version: i64,
}

/// Backend-agnostic options for metadata store operations.
#[derive(Debug)]
pub enum MetaOptions {
    None,
    /// Treat the key as a prefix and return all matching entries.
    WithPrefix,
    /// Return the previous key-value pair before the operation.
    WithPrevKey,
}

#[async_trait]
pub trait MetadataStore: Send + Sync + 'static {
    async fn get(&self, key: &str, get_options: MetaOptions) -> Result<Option<Value>>;
    async fn get_childrens(&self, path: &str) -> Result<Vec<String>>;
    async fn put(&self, key: &str, value: Value, put_options: MetaOptions) -> Result<()>;
    async fn delete(&self, key: &str) -> Result<()>;
    async fn watch(&self, prefix: &str) -> Result<WatchStream>;

    /// Put a key with a time-to-live. The key is automatically deleted after `ttl`.
    async fn put_with_ttl(&self, key: &str, value: Value, ttl: Duration) -> Result<()>;

    /// Retrieve all key-value pairs under a given prefix.
    async fn get_bulk(&self, prefix: &str) -> Result<Vec<KeyValueVersion>>;

    /// Atomically increment and return a monotonic counter stored at `counter_key`.
    ///
    /// Used for schema ID allocation — eliminates the read-modify-write race
    /// by going through Raft consensus as a single atomic command.
    async fn allocate_monotonic_id(&self, counter_key: &str) -> Result<u64>;
}

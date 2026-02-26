//! Broker-local metadata storage enum.
//!
//! Wraps any `MetadataStore` implementation behind `Arc<dyn MetadataStore>`.
//! Production uses the Raft backend; tests use the in-memory backend.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;

use danube_core::metadata::{
    KeyValueVersion, MemoryStore, MetaOptions, MetadataError, MetadataStore, WatchStream,
};

type Result<T> = std::result::Result<T, MetadataError>;

pub enum MetadataStorage {
    Raft(Arc<dyn MetadataStore>),
    InMemory(MemoryStore),
}

impl Clone for MetadataStorage {
    fn clone(&self) -> Self {
        match self {
            Self::Raft(s) => Self::Raft(Arc::clone(s)),
            Self::InMemory(s) => Self::InMemory(s.clone()),
        }
    }
}

impl fmt::Debug for MetadataStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Raft(_) => f.debug_tuple("Raft").field(&"...").finish(),
            Self::InMemory(s) => f.debug_tuple("InMemory").field(s).finish(),
        }
    }
}

#[async_trait]
impl MetadataStore for MetadataStorage {
    async fn get(&self, key: &str, opts: MetaOptions) -> Result<Option<Value>> {
        match self {
            Self::Raft(s) => s.get(key, opts).await,
            Self::InMemory(s) => s.get(key, opts).await,
        }
    }

    async fn get_childrens(&self, path: &str) -> Result<Vec<String>> {
        match self {
            Self::Raft(s) => s.get_childrens(path).await,
            Self::InMemory(s) => s.get_childrens(path).await,
        }
    }

    async fn put(&self, key: &str, value: Value, opts: MetaOptions) -> Result<()> {
        match self {
            Self::Raft(s) => s.put(key, value, opts).await,
            Self::InMemory(s) => s.put(key, value, opts).await,
        }
    }

    async fn delete(&self, key: &str) -> Result<()> {
        match self {
            Self::Raft(s) => s.delete(key).await,
            Self::InMemory(s) => s.delete(key).await,
        }
    }

    async fn watch(&self, prefix: &str) -> Result<WatchStream> {
        match self {
            Self::Raft(s) => s.watch(prefix).await,
            Self::InMemory(s) => s.watch(prefix).await,
        }
    }

    async fn put_with_ttl(&self, key: &str, value: Value, ttl: Duration) -> Result<()> {
        match self {
            Self::Raft(s) => s.put_with_ttl(key, value, ttl).await,
            Self::InMemory(s) => s.put_with_ttl(key, value, ttl).await,
        }
    }

    async fn get_bulk(&self, prefix: &str) -> Result<Vec<KeyValueVersion>> {
        match self {
            Self::Raft(s) => s.get_bulk(prefix).await,
            Self::InMemory(s) => s.get_bulk(prefix).await,
        }
    }
}

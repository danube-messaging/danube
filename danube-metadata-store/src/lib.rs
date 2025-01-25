mod errors;
pub use errors::MetadataError;
pub(crate) use errors::Result;

mod store;
pub use store::MetaOptions;
pub use store::MetadataStore;

mod watch;
pub use watch::{WatchEvent, WatchStream};

mod providers;
pub use providers::{
    etcd::{EtcdStore, KeyValueVersion},
    in_memory::MemoryStore,
};

use async_trait::async_trait;
pub use etcd_client::GetOptions as EtcdGetOptions;
use etcd_client::LeaseGrantResponse;
use serde_json::Value;

#[derive(Debug, Clone)]
pub enum MetadataStorage {
    Etcd(EtcdStore),
    InMemory(MemoryStore), // InMemory is used for testing purposes
                           // Is there a need to add other backends, like Redis,  Consul, Zookeeper ?
}

#[async_trait]
impl MetadataStore for MetadataStorage {
    async fn get(&self, key: &str, get_options: MetaOptions) -> Result<Option<Value>> {
        match self {
            MetadataStorage::Etcd(store) => store.get(key, get_options).await,
            MetadataStorage::InMemory(store) => store.get(key, get_options).await,
        }
    }

    async fn get_childrens(&self, path: &str) -> Result<Vec<String>> {
        match self {
            MetadataStorage::Etcd(store) => store.get_childrens(path).await,
            MetadataStorage::InMemory(store) => store.get_childrens(path).await,
        }
    }

    async fn put(&self, key: &str, value: Value, put_options: MetaOptions) -> Result<()> {
        match self {
            MetadataStorage::Etcd(store) => store.put(key, value, put_options).await,
            MetadataStorage::InMemory(store) => store.put(key, value, put_options).await,
        }
    }

    async fn delete(&self, key: &str) -> Result<()> {
        match self {
            MetadataStorage::Etcd(store) => store.delete(key).await,
            MetadataStorage::InMemory(store) => store.delete(key).await,
        }
    }

    async fn watch(&self, prefix: &str) -> Result<WatchStream> {
        match self {
            MetadataStorage::Etcd(store) => store.watch(prefix).await,
            MetadataStorage::InMemory(store) => store.watch(prefix).await,
        }
    }
}

impl MetadataStorage {
    pub async fn create_lease(&self, ttl: i64) -> Result<LeaseGrantResponse> {
        match self {
            MetadataStorage::Etcd(store) => store.create_lease(ttl).await,
            _ => Err(MetadataError::UnsupportedOperation.into()),
        }
    }

    pub async fn keep_lease_alive(&self, lease_id: i64, role: &str) -> Result<()> {
        match self {
            MetadataStorage::Etcd(store) => store.keep_lease_alive(lease_id, role).await,
            _ => Err(MetadataError::UnsupportedOperation.into()),
        }
    }

    pub async fn put_with_lease(&self, key: &str, value: Value, lease_id: i64) -> Result<()> {
        match self {
            MetadataStorage::Etcd(store) => store.put_with_lease(key, value, lease_id).await,
            _ => Err(MetadataError::UnsupportedOperation.into()),
        }
    }
    pub async fn get_bulk(&self, prefix: &str) -> Result<Vec<KeyValueVersion>> {
        match self {
            MetadataStorage::Etcd(store) => store.get_bulk(prefix).await,
            _ => Err(MetadataError::UnsupportedOperation.into()),
        }
    }
}

use std::time::Duration;

use super::{
    errors::Result,
    store::{KeyValueVersion, MetaOptions, MetadataStore},
    watch::{WatchEvent, WatchStream},
    MetadataError,
};

use async_trait::async_trait;
use dashmap::{mapref::one::RefMut, DashMap};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::broadcast;

/// MemoryStore is a simple in-memory key-value store that implements the MetadataStore trait.
/// SHOULD BE USED ONLY FOR TESTING PURPOSES
#[derive(Debug, Clone)]
pub struct MemoryStore {
    inner: Arc<DashMap<String, BTreeMap<String, Value>>>,
    watchers: Arc<DashMap<String, broadcast::Sender<WatchEvent>>>,
}

impl MemoryStore {
    #[allow(dead_code)]
    pub async fn new() -> Result<Self> {
        Ok(MemoryStore {
            inner: Arc::new(DashMap::new()),
            watchers: Arc::new(DashMap::new()),
        })
    }

    fn notify_watchers(&self, event: WatchEvent) {
        let key_str = match &event {
            WatchEvent::Put { key, .. } => String::from_utf8_lossy(key).to_string(),
            WatchEvent::Delete { key, .. } => String::from_utf8_lossy(key).to_string(),
        };
        for entry in self.watchers.iter() {
            if key_str.starts_with(entry.key()) {
                let _ = entry.value().send(event.clone());
            }
        }
    }

    fn get_map(&self, path: &str) -> Result<RefMut<'_, String, BTreeMap<String, Value>>> {
        let parts: Vec<&str> = path.split('/').take(3).collect();

        // Validate that path has at least 3 parts (empty, namespace, category)
        if parts.len() < 3 {
            return Err(MetadataError::InvalidArguments(format!(
                "Path must have at least 3 segments: {}",
                path
            )));
        }

        let key = parts.join("/");
        let bmap = self.inner.entry(key.to_owned()).or_insert(BTreeMap::new());

        Ok(bmap)
    }
}

#[async_trait]
impl MetadataStore for MemoryStore {
    // Read the value of one key, identified by the path
    async fn get(&self, path: &str, _get_options: MetaOptions) -> Result<Option<Value>> {
        let bmap = self.get_map(path)?;

        let parts: Vec<&str> = path.split('/').skip(3).collect();
        let key = parts.join("/");

        match bmap.get(&key) {
            Some(value) => Ok(Some(value.clone())),
            None => Ok(None),
        }
    }

    // Return all the paths that are children to the specific path.
    // Returns full paths to match ETCD behavior for production compatibility
    async fn get_childrens(&self, path: &str) -> Result<Vec<String>> {
        let parts: Vec<&str> = path.split('/').skip(3).collect();
        let minimum_path = parts.join("/");

        // Get the map prefix (first 3 parts of the path)
        let path_parts: Vec<&str> = path.split('/').take(3).collect();
        let map_prefix = path_parts.join("/");
        let map_key = map_prefix.clone();

        let mut child_paths = Vec::new();

        // Access the map directly without creating a new one if it doesn't exist
        if let Some(bmap_ref) = self.inner.get(&map_key) {
            for key in bmap_ref.keys() {
                if key.starts_with(&minimum_path)
                    && key.len() > minimum_path.len()
                    && key.chars().nth(minimum_path.len()).unwrap() == '/'
                {
                    // Return full path to match ETCD behavior
                    let full_path = format!("{}/{}", map_prefix, key);
                    child_paths.push(full_path);
                }
            }
        }
        Ok(child_paths)
    }

    // Put a new value for a given key
    async fn put(&self, path: &str, value: Value, _put_options: MetaOptions) -> Result<()> {
        let mut bmap = self.get_map(path)?;

        let parts: Vec<&str> = path.split('/').skip(3).collect();
        let key = parts.join("/");

        // Validate that there's actually a key to store (path must have more than 3 parts)
        if key.is_empty() {
            return Err(MetadataError::InvalidArguments(format!(
                "Path must have a key component: {}",
                path
            )));
        }

        let value_bytes = serde_json::to_vec(&value)?;
        bmap.insert(key, value);

        self.notify_watchers(WatchEvent::Put {
            key: path.as_bytes().to_vec(),
            value: value_bytes,
            mod_revision: None,
            version: None,
        });

        Ok(())
    }

    // Delete the key / value from the store
    async fn delete(&self, path: &str) -> Result<()> {
        let mut bmap = self.get_map(path)?;

        let parts: Vec<&str> = path.split('/').skip(3).collect();
        let key = parts.join("/");

        if key.is_empty() {
            return Err(MetadataError::Unknown("wrong path".to_string()).into());
        }

        let _value = bmap.remove(&key);

        self.notify_watchers(WatchEvent::Delete {
            key: path.as_bytes().to_vec(),
            mod_revision: None,
            version: None,
        });

        Ok(())
    }

    async fn watch(&self, prefix: &str) -> Result<WatchStream> {
        let (tx, rx) = broadcast::channel(256);
        self.watchers.insert(prefix.to_string(), tx);
        Ok(WatchStream::from_broadcast(rx))
    }

    async fn put_with_ttl(&self, key: &str, value: Value, _ttl: Duration) -> Result<()> {
        // MemoryStore ignores TTL — testing only. Real TTL comes with the Raft backend.
        self.put(key, value, MetaOptions::None).await
    }

    async fn allocate_monotonic_id(&self, counter_key: &str) -> Result<u64> {
        // Simple atomic-ish increment for testing. Real atomicity comes from Raft.
        let current = self.get(counter_key, MetaOptions::None).await?;
        let next = current.and_then(|v| v.as_u64()).unwrap_or(0) + 1;
        self.put(counter_key, serde_json::json!(next), MetaOptions::None)
            .await?;
        Ok(next)
    }

    async fn get_bulk(&self, prefix: &str) -> Result<Vec<KeyValueVersion>> {
        let map_parts: Vec<&str> = prefix.split('/').take(3).collect();
        if map_parts.len() < 3 {
            return Err(MetadataError::InvalidArguments(format!(
                "Prefix must have at least 3 segments: {}",
                prefix
            )));
        }
        let map_prefix = map_parts.join("/");
        let suffix_parts: Vec<&str> = prefix.split('/').skip(3).collect();
        let suffix = suffix_parts.join("/");

        let mut out: Vec<KeyValueVersion> = Vec::new();
        if let Some(bmap_ref) = self.inner.get(&map_prefix) {
            for (k, v) in bmap_ref.iter() {
                if k.starts_with(&suffix) {
                    let full_key = format!("{}/{}", map_prefix, k);
                    let value_bytes = serde_json::to_vec(v)?;
                    out.push(KeyValueVersion {
                        key: full_key,
                        value: value_bytes,
                        version: 0,
                    });
                }
            }
        }
        Ok(out)
    }
}

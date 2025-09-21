use crate::{
    errors::Result,
    store::{MetaOptions, MetadataStore},
    watch::WatchStream,
    KeyValueVersion, MetadataError,
};

use async_trait::async_trait;
use dashmap::{mapref::one::RefMut, DashMap};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;

/// MemoryStore is a simple in-memory key-value store that implements the MetadataStore trait.
/// SHOULD BE USED ONLY FOR TESTING PURPOSES
#[derive(Debug, Clone)]
pub struct MemoryStore {
    inner: Arc<DashMap<String, BTreeMap<String, Value>>>,
}

impl MemoryStore {
    #[allow(dead_code)]
    pub async fn new() -> Result<Self> {
        Ok(MemoryStore {
            inner: Arc::new(DashMap::new()),
        })
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

        bmap.insert(key, value);

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
        Ok(())
    }

    async fn watch(&self, _prefix: &str) -> Result<WatchStream> {
        unimplemented!()
    }
}

impl MemoryStore {
    // Delete a key-value pair and all the children nodes
    #[allow(dead_code)]
    async fn delete_recursive(&mut self, path: &str) -> Result<()> {
        let mut bmap = self.get_map(path)?;
        let mut keys_to_remove = Vec::new();

        let parts: Vec<&str> = path.split('/').skip(3).collect();
        let path_to_remove = parts.join("/");

        for key in bmap.keys() {
            if key.starts_with(&path_to_remove)
                && key.len() > path_to_remove.len()
                && key.chars().nth(path_to_remove.len()).unwrap() == '/'
            {
                keys_to_remove.push(key.clone());
            }
        }

        for key in keys_to_remove {
            bmap.remove(&key);
        }

        Ok(())
    }

    /// Return all key/value/version entries under a given prefix, similar to Etcd `get_bulk`.
    /// Keys are returned as full paths and values are JSON-serialized bytes of the stored Value.
    pub async fn get_bulk(&self, prefix: &str) -> Result<Vec<KeyValueVersion>> {
        // Determine map key (first 3 path segments) and the suffix to match inside the map.
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
                        version: 0, // in-memory backend doesn't track versions
                    });
                }
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tests basic CRUD operations: put, get, and delete
    /// Purpose: Validates core store functionality with valid paths
    /// Expected: Successful storage, retrieval, and removal of key-value pairs
    #[tokio::test]
    async fn test_put_get_delete() -> Result<()> {
        let store = MemoryStore::new().await?;

        let topic_id = "another-topic";
        let value: Value = serde_json::from_str("{\"sampling_rate\": 0.5}").unwrap();

        let path = format!("/danube/topics/{}/conf", topic_id);

        // Put a new value
        store.put(&path, value.clone(), MetaOptions::None).await?;

        // Get the value
        let retrieved_value = store.get(&path, MetaOptions::None).await?;
        assert_eq!(retrieved_value, Some(value));

        // Delete the key
        store.delete(&path).await?;

        // Try to get the value again and assert it's None (key not found)
        let result = store.get(&path, MetaOptions::None).await;
        assert!(matches!(result, Ok(None)));

        Ok(())
    }

    /// Tests retrieval of non-existent keys
    /// Purpose: Ensures proper None return for missing keys
    /// Expected: Returns Ok(None) without errors for unknown keys
    #[tokio::test]
    async fn test_get_nonexistent_key() -> Result<()> {
        let store = MemoryStore::new().await?;
        let topic_id = "unknown-topic";

        // Try to get a non-existent key
        let result = store
            .get(
                format!("/danube/topics/{}/metrics", topic_id).as_str(),
                MetaOptions::None,
            )
            .await;
        // Assert that the result is None (key not found)
        match result {
            Ok(Some(_)) => {
                // If some value was found, fail the test
                panic!("Expected None (key not found), but got Some(value)");
            }
            Ok(None) => {
                // This is the expected case: key not found
                assert_eq!(result.unwrap(), None);
            }
            Err(err) => {
                // If there's an unexpected error, fail the test
                panic!("Unexpected error: {:?}", err);
            }
        }
        Ok(())
    }

    /// Tests error handling for invalid path formats
    /// Purpose: Validates path validation and error reporting
    /// Expected: Returns error for paths missing required segments
    #[tokio::test]
    async fn test_put_invalid_path() -> Result<()> {
        let store = MemoryStore::new().await?;
        let value: Value = serde_json::from_str("{\"sampling_rate\": 0.5}").unwrap();

        // Try to put with invalid path (missing segment)
        let result = store
            .put("/danube/topics", value.clone(), MetaOptions::None)
            .await;
        assert!(result.is_err());

        Ok(())
    }

    /// Tests recursive deletion of path hierarchies
    /// Purpose: Validates bulk deletion while preserving unrelated data
    /// Expected: Removes all children under target path, leaves others intact
    #[tokio::test]
    async fn test_delete_recursive() -> Result<()> {
        let mut store = MemoryStore::new().await?;

        // Create a sample data structure
        store
            .put(
                "/danube/topics/topic_1/key1",
                Value::String("value1".to_string()),
                MetaOptions::None,
            )
            .await?;
        store
            .put(
                "/danube/topics/topic_1/key2",
                Value::String("value2".to_string()),
                MetaOptions::None,
            )
            .await?;
        store
            .put(
                "/danube/topics/topic_2/key3",
                Value::String("value3".to_string()),
                MetaOptions::None,
            )
            .await?;

        // Test deleting a path with its contents
        store.delete_recursive("/danube/topics/topic_1").await?;

        // Assert that keys under the deleted path are gone
        assert!(store
            .get("/danube/topics/topic_1/key1", MetaOptions::None)
            .await
            .unwrap()
            .is_none());
        assert!(store
            .get("/danube/topics/topic_1/key2", MetaOptions::None)
            .await
            .unwrap()
            .is_none());

        // Assert that other directory and its key remain
        assert!(store
            .get("/danube/topics/topic_2/key3", MetaOptions::None)
            .await
            .unwrap()
            .is_some());

        Ok(())
    }

    /// Tests child path discovery functionality
    /// Purpose: Validates hierarchical path traversal and filtering
    /// Expected: Returns all child paths under given prefix, excludes unrelated paths
    #[tokio::test]
    async fn test_get_childrens() -> Result<()> {
        let store = MemoryStore::new().await?;

        // Create a sample data structure
        store
            .put(
                "/danube/topics/topic_1/key1",
                Value::String("value1".to_string()),
                MetaOptions::None,
            )
            .await?;
        store
            .put(
                "/danube/topics/topic_2/key2",
                Value::String("value2".to_string()),
                MetaOptions::None,
            )
            .await?;
        store
            .put(
                "/danube/topics/topic_1/subtopic/key3",
                Value::String("value3".to_string()),
                MetaOptions::None,
            )
            .await?;
        store
            .put(
                "/data/other/path",
                Value::String("value4".to_string()),
                MetaOptions::None,
            )
            .await?;

        // Test finding paths containing "/danube/topics/topic_1"
        let paths = store.get_childrens("/danube/topics/topic_1").await?;

        // Assert that all matching paths are found
        assert_eq!(paths.len(), 2);
        assert!(paths.contains(&"/danube/topics/topic_1/key1".to_string()));
        assert!(paths.contains(&"/danube/topics/topic_1/subtopic/key3".to_string()));

        // Test finding a non-existent path
        let paths = store.get_childrens("/non/existent/path").await?;

        // Assert that no paths are found
        assert!(paths.is_empty());

        Ok(())
    }

    /// Tests get_bulk API for in-memory backend
    /// Purpose: Ensures MemoryStore::get_bulk returns full paths and serialized values like Etcd
    /// Expected: Returns all keys under the given prefix with JSON-serialized values
    #[tokio::test]
    async fn test_get_bulk() -> Result<()> {
        let store = MemoryStore::new().await?;

        // Seed data
        store
            .put(
                "/danube/storage/topics/tenant/ns/topic/objects/00000000000000000010",
                serde_json::json!({"object_id":"data-10-20.dnb1","start_offset":10}),
                MetaOptions::None,
            )
            .await?;
        store
            .put(
                "/danube/storage/topics/tenant/ns/topic/objects/00000000000000000030",
                serde_json::json!({"object_id":"data-30-40.dnb1","start_offset":30}),
                MetaOptions::None,
            )
            .await?;

        // Call get_bulk on the directory prefix
        let prefix = "/danube/storage/topics/tenant/ns/topic/objects/";
        let kvs = store.get_bulk(prefix).await?;

        // Should return two entries with full paths and JSON bytes
        assert_eq!(kvs.len(), 2);
        let keys: std::collections::HashSet<String> = kvs.iter().map(|kv| kv.key.clone()).collect();
        assert!(
            keys.contains("/danube/storage/topics/tenant/ns/topic/objects/00000000000000000010")
        );
        assert!(
            keys.contains("/danube/storage/topics/tenant/ns/topic/objects/00000000000000000030")
        );

        // Validate values are valid JSON
        for kv in kvs {
            let _v: serde_json::Value =
                serde_json::from_slice(&kv.value).expect("valid json bytes");
        }

        Ok(())
    }
}

use crate::{
    errors::Result,
    store::{MetaOptions, MetadataStore},
    watch::WatchStream,
    MetadataError,
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

    fn get_map(&self, path: &str) -> Result<RefMut<String, BTreeMap<String, Value>>> {
        let parts: Vec<&str> = path.split('/').take(3).collect();
        
        // Validate that path has at least 3 parts (empty, namespace, category)
        if parts.len() < 3 {
            return Err(MetadataError::InvalidArguments(format!("Path must have at least 3 segments: {}", path)));
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
            return Err(MetadataError::InvalidArguments(format!("Path must have a key component: {}", path)));
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
}

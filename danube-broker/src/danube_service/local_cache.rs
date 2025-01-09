mod trie;
pub(crate) use trie::Trie;

use anyhow::Result;
use danube_metadata_store::{MetadataStorage, MetadataStore, WatchEvent, WatchStream};
use dashmap::DashMap;
use futures::StreamExt;
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{error, info};

use crate::resources::{
    BASE_CLUSTER_PATH, BASE_NAMESPACES_PATH, BASE_SUBSCRIPTIONS_PATH, BASE_TOPICS_PATH,
};

// It caches various types of metadata required by Danube brokers, such as topic and namespace data,
// which are frequently accessed during message production and consumption.
// This reduces the need for frequent queries to the central metadata store: ETCD.
//
// The docs/internal_resources.md document describe how the resources are organized in Metadata Store
//
// By updating the local cache based on the events,
// brokers ensure they have the latest metadata without repeatedly querying the central store.
//
// The updates/events are received via the metadata event synchronizer and/or the Watch events.
//
// Note: The instance can be safety Cloned as all it's fields ar wrapped in Arc<>,
// allowing the LocalCache struct to be cloned without deep copying the underlying data.
#[derive(Debug, Clone)]
pub(crate) struct LocalCache {
    keys: Arc<Mutex<Trie>>,
    // holds information about the cluster and the cluster's brokers
    cluster: Arc<DashMap<String, (i64, Value)>>,
    // holds information about the namespace policy and the namespace's topics
    namespaces: Arc<DashMap<String, (i64, Value)>>,
    // holds information about the topic policy and the associated producers and subscriptions,
    // including partitioned topics.
    topics: Arc<DashMap<String, (i64, Value)>>,
    // holds information about the topic subscriptions, including their consumers
    subscriptions: Arc<DashMap<String, (i64, Value)>>,
    // metadata store
    metadata_store: MetadataStorage,
}

impl LocalCache {
    pub(crate) fn new(metadata_store: MetadataStorage) -> Self {
        LocalCache {
            keys: Arc::new(Mutex::new(Trie::new())),
            cluster: Arc::new(DashMap::new()),
            namespaces: Arc::new(DashMap::new()),
            topics: Arc::new(DashMap::new()),
            subscriptions: Arc::new(DashMap::new()),
            metadata_store,
        }
    }

    // updates the LocalCache
    // if the value is present, then it is a put operation and the new value is added
    // if the value it is not present, then it is a delete operation and the path & value are deleted
    pub(crate) async fn update_cache(&self, key: &str, version: i64, value: Option<&[u8]>) {
        let parts: Vec<&str> = key.split('/').collect();
        if parts.len() < 2 {
            return;
        }

        let category = parts[1];
        let cache = match category {
            "cluster" => &self.cluster,
            "namespaces" => &self.namespaces,
            "topics" => &self.topics,
            "consumers" => &self.subscriptions,
            _ => return,
        };

        if let Some(value) = value {
            // if it's a Put Operation make sure that the new version is bigger that current version from local cache
            // otherwise you may update with the same value or even an older value
            // the updates may be received from multiple sources, like Watch event and/or Syncronizer
            //
            // The version of the key.
            // A deletion event resets the version to zero and any modification of the key increases its version.
            if cache.contains_key(key) {
                if cache.get(key).unwrap().0 >= version {
                    return;
                }
            }

            if let Ok(json_value) = serde_json::from_slice(value) {
                cache.insert(key.to_string(), (version, json_value));
                let mut keys = self.keys.lock().await;
                keys.insert(key);
            }
        } else {
            cache.remove(key);
            let mut keys = self.keys.lock().await;
            keys.remove(key);
        }
    }

    // Function to populate cache with initial data from etcd
    // It fetches the data from the etcd store and updates the local cache
    pub(crate) async fn populate_start_local_cache(&self) -> Result<WatchStream> {
        let prefixes = vec![
            BASE_CLUSTER_PATH,
            BASE_NAMESPACES_PATH,
            BASE_TOPICS_PATH,
            BASE_SUBSCRIPTIONS_PATH,
        ];

        for prefix in &prefixes {
            let kvs = self.metadata_store.get_bulk(prefix).await?;
            for kv in kvs {
                self.update_cache(&kv.key, kv.version, Some(&kv.value))
                    .await;
            }
        }
        info!("Initial cache populated");

        // Create combined watch stream for all prefixes
        let mut streams = Vec::new();
        for prefix in prefixes {
            let watch_stream = self.metadata_store.watch(prefix).await?;
            streams.push(watch_stream);
        }

        let combined_stream = futures::stream::select_all(streams);
        Ok(WatchStream::new(combined_stream))
    }

    pub(crate) async fn process_event(&self, mut watch_stream: WatchStream) {
        while let Some(result) = watch_stream.next().await {
            match result {
                Ok(event) => match event {
                    WatchEvent::Put {
                        key,
                        value,
                        version,
                        ..
                    } => {
                        let key_str = match std::str::from_utf8(&key) {
                            Ok(s) => s,
                            Err(e) => {
                                error!("Invalid UTF-8 in key: {}", e);
                                continue;
                            }
                        };
                        self.update_cache(key_str, version.unwrap_or(0), Some(&value))
                            .await;
                    }
                    WatchEvent::Delete { key, version, .. } => {
                        let key_str = match std::str::from_utf8(&key) {
                            Ok(s) => s,
                            Err(e) => {
                                error!("Invalid UTF-8 in key: {}", e);
                                continue;
                            }
                        };
                        self.update_cache(key_str, version.unwrap_or(0), None).await;
                    }
                },
                Err(e) => {
                    error!("Error receiving watch event: {}", e);
                }
            }
        }
    }

    // get the Value from Cache for the requested path
    pub fn get(&self, path: &str) -> Option<Value> {
        // Split the path by '/' and collect the segments into a vector
        let segments: Vec<&str> = path.split('/').collect();

        // Ensure the path has at least two segments (e.g., "/cluster/{key}")
        if segments.len() < 2 {
            return None;
        }

        // Determine which DashMap to access based on the first segment
        match segments[1] {
            "cluster" => self.cluster.get(path).map(|entry| entry.value().1.clone()),
            "namespaces" => self
                .namespaces
                .get(path)
                .map(|entry| entry.value().1.clone()),
            "topics" => self.topics.get(path).map(|entry| entry.value().1.clone()),
            "subscriptions" => self
                .subscriptions
                .get(path)
                .map(|entry| entry.value().1.clone()),
            _ => None,
        }
    }

    // remove the list of keys from both the DashMap and the Trie.
    #[allow(dead_code)]
    pub(crate) async fn remove_keys(&self, keys_to_remove: Vec<&str>) {
        for key in keys_to_remove {
            let parts: Vec<&str> = key.split('/').collect();
            if parts.len() < 2 {
                continue;
            }

            let category = parts[1];
            let cache = match category {
                "cluster" => &self.cluster,
                "namespaces" => &self.namespaces,
                "topics" => &self.topics,
                "consumers" => &self.subscriptions,
                _ => continue,
            };

            cache.remove(key);
            let mut keys = self.keys.lock().await;
            keys.remove(key);
        }
    }

    // retrieve keys with a specific prefix from the Trie.
    pub(crate) async fn get_keys_with_prefix(&self, prefix: &str) -> Vec<String> {
        let keys = self.keys.lock().await;
        keys.search(prefix)
    }
}

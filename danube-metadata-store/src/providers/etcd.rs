use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, error};

use crate::{
    errors::{MetadataError, Result},
    store::{MetaOptions, MetadataStore},
    watch::WatchStream,
};
use async_trait::async_trait;
use etcd_client::{Client, GetOptions, LeaseGrantResponse, PutOptions, WatchOptions};
use serde_json::Value;

#[derive(Clone)]
pub struct EtcdStore {
    client: Arc<Mutex<Client>>,
}

impl EtcdStore {
    pub async fn new(endpoint: String) -> Result<Self> {
        let client = Client::connect([endpoint], None)
            .await
            .map_err(MetadataError::from)?;
        Ok(Self {
            client: Arc::new(Mutex::new(client)),
        })
    }
}

#[async_trait]
impl MetadataStore for EtcdStore {
    async fn get(&self, key: &str, get_options: MetaOptions) -> Result<Option<Value>> {
        // Extract ETCD options if provided
        let options = if let MetaOptions::EtcdGet(etcd_options) = get_options {
            Some(etcd_options)
        } else {
            None
        };

        let mut client = self.client.lock().await;
        let response = client
            .get(key, options)
            .await
            .map_err(MetadataError::from)?;
        if let Some(kv) = response.kvs().first() {
            let value = serde_json::from_slice(kv.value())?;
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    async fn get_childrens(&self, path: &str) -> Result<Vec<String>> {
        let client = self.client.lock().await;
        let mut kv_client = client.kv_client();

        // Retrieve all keys with the specified prefix
        let range_resp = kv_client
            .get(path, Some(GetOptions::new().with_keys_only().with_prefix()))
            .await?;

        let mut child_paths: Vec<String> = Vec::new();

        for kv in range_resp.kvs() {
            let key = kv.key_str()?.to_owned();
            child_paths.push(key);
        }

        Ok(child_paths)
    }

    async fn put(&self, key: &str, value: Value, put_options: MetaOptions) -> Result<()> {
        let options = if let MetaOptions::EtcdPut(etcd_options) = put_options {
            Some(etcd_options)
        } else {
            None
        };

        // Serialize serde_json::Value to a byte array
        let value_bytes = serde_json::to_vec(&value)?;

        let mut client = self.client.lock().await;
        client
            .put(key, value_bytes, options)
            .await
            .map_err(MetadataError::from)?;
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let mut client = self.client.lock().await;
        client
            .delete(key, None)
            .await
            .map_err(MetadataError::from)?;
        Ok(())
    }

    async fn watch(&self, prefix: &str) -> Result<WatchStream> {
        let mut client = self.client.lock().await;
        let (_watcher, watch_stream) = client
            .watch(prefix, Some(WatchOptions::new().with_prefix()))
            .await
            .map_err(MetadataError::from)?;
        Ok(WatchStream::from_etcd(watch_stream))
    }
}

#[derive(Debug)]
pub struct KeyValueVersion {
    pub key: String,
    pub value: Vec<u8>,
    pub version: i64,
}

impl EtcdStore {
    pub async fn get_bulk(&self, prefix: &str) -> Result<Vec<KeyValueVersion>> {
        let mut client = self.client.lock().await;
        let response = client
            .get(prefix, Some(etcd_client::GetOptions::new().with_prefix()))
            .await
            .map_err(MetadataError::from)?;

        let mut results = Vec::new();
        for kv in response.kvs() {
            results.push(KeyValueVersion {
                key: String::from_utf8(kv.key().to_vec())
                    .map_err(|e| MetadataError::Unknown(format!("Invalid UTF-8 in key: {}", e)))?,
                value: kv.value().to_vec(),
                version: kv.version(),
            });
        }

        Ok(results)
    }
    pub async fn create_lease(&self, ttl: i64) -> Result<LeaseGrantResponse> {
        let mut client = self.client.lock().await;
        let lease = client.lease_grant(ttl, None).await?;
        Ok(lease)
    }

    pub async fn keep_lease_alive(&self, lease_id: i64, role: &str) -> Result<()> {
        let mut client = self.client.lock().await;
        let (mut keeper, mut stream) = client.lease_keep_alive(lease_id).await?;
        // Attempt to send a keep-alive request
        match keeper.keep_alive().await {
            Ok(_) => debug!(role = %role, lease_id = %lease_id, "keep-alive request sent for lease"),
            Err(e) => {
                error!(
                    role = %role,
                    lease_id = %lease_id,
                    error = %e,
                    "failed to send keep-alive request for lease"
                );
            }
        }

        // Check for responses from etcd to confirm the lease is still alive
        match stream.message().await {
            Ok(Some(_response)) => {
                debug!(
                    role = %role,
                    lease_id = %lease_id,
                    "received keep-alive response for lease"
                );
            }
            Ok(None) => {
                error!(
                    role = %role,
                    lease_id = %lease_id,
                    "keep-alive response stream ended unexpectedly for lease"
                );
            }
            Err(e) => {
                error!(
                    role = %role,
                    lease_id = %lease_id,
                    error = %e,
                    "failed to receive keep-alive response for lease"
                );
            }
        }
        Ok(())
    }

    pub async fn put_with_lease(&self, key: &str, value: Value, lease_id: i64) -> Result<()> {
        let etcd_opts = PutOptions::new().with_lease(lease_id);
        let opts = MetaOptions::EtcdPut(etcd_opts);
        self.put(key, value, opts).await?;
        Ok(())
    }
}

impl Debug for EtcdStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EtcdStore").finish()
    }
}

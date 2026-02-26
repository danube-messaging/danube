//! `MetadataStore` implementation backed by Raft consensus.
//!
//! Reads are served from the local state machine replica.
//! Writes are proposed through the Raft leader and applied after consensus.

use std::time::Duration;

use async_trait::async_trait;
use openraft::Raft;
use serde_json::Value;
use tokio::sync::broadcast;

use danube_core::metadata::{
    KeyValueVersion, MetaOptions, MetadataError, MetadataStore, WatchStream,
};

type Result<T> = std::result::Result<T, MetadataError>;

use crate::commands::{RaftCommand, RaftResponse};
use crate::state_machine::SharedStateMachineData;
use crate::typ::TypeConfig;

/// Raft-backed metadata store.
///
/// - **Reads** go directly to the local state machine (eventually consistent).
/// - **Writes** are proposed through Raft consensus (strongly consistent).
/// - **Watches** subscribe to the local broadcast channels.
pub struct RaftMetadataStore {
    raft: Raft<TypeConfig>,
    data: SharedStateMachineData,
}

impl RaftMetadataStore {
    pub fn new(raft: Raft<TypeConfig>, data: SharedStateMachineData) -> Self {
        Self { raft, data }
    }

    /// Propose a command through Raft and return the response.
    async fn propose(&self, cmd: RaftCommand) -> Result<RaftResponse> {
        let resp = self
            .raft
            .client_write(cmd)
            .await
            .map_err(|e| MetadataError::Unknown(format!("raft propose failed: {}", e)))?;
        Ok(resp.data)
    }
}

#[async_trait]
impl MetadataStore for RaftMetadataStore {
    async fn get(&self, key: &str, get_options: MetaOptions) -> Result<Option<Value>> {
        let d = self.data.read().await;
        match get_options {
            MetaOptions::None | MetaOptions::WithPrevKey => {
                Ok(d.kv.get(key).map(|v| v.value.clone()))
            }
            MetaOptions::WithPrefix => {
                let first =
                    d.kv.range(key.to_string()..)
                        .take_while(|(k, _)| k.starts_with(key))
                        .next();
                match first {
                    Some((_k, v)) => Ok(Some(v.value.clone())),
                    None => Ok(None),
                }
            }
        }
    }

    async fn get_childrens(&self, path: &str) -> Result<Vec<String>> {
        let d = self.data.read().await;
        Ok(d.kv
            .range(path.to_string()..)
            .take_while(|(k, _)| k.starts_with(path))
            .map(|(k, _)| k.clone())
            .collect())
    }

    async fn put(&self, key: &str, value: Value, _put_options: MetaOptions) -> Result<()> {
        let resp = self
            .propose(RaftCommand::Put {
                key: key.to_string(),
                value,
            })
            .await?;
        match resp {
            RaftResponse::Ok => Ok(()),
            other => Err(MetadataError::Unknown(format!(
                "unexpected response: {:?}",
                other
            ))),
        }
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let resp = self
            .propose(RaftCommand::Delete {
                key: key.to_string(),
            })
            .await?;
        match resp {
            RaftResponse::Ok => Ok(()),
            other => Err(MetadataError::Unknown(format!(
                "unexpected response: {:?}",
                other
            ))),
        }
    }

    async fn watch(&self, prefix: &str) -> Result<WatchStream> {
        let d = self.data.read().await;
        let entry = d
            .watchers
            .entry(prefix.to_string())
            .or_insert_with(|| broadcast::channel(256).0);
        let rx = entry.value().subscribe();
        Ok(WatchStream::from_broadcast(rx))
    }

    async fn put_with_ttl(&self, key: &str, value: Value, ttl: Duration) -> Result<()> {
        let resp = self
            .propose(RaftCommand::PutWithTTL {
                key: key.to_string(),
                value,
                ttl_ms: ttl.as_millis() as u64,
            })
            .await?;
        match resp {
            RaftResponse::Ok => Ok(()),
            other => Err(MetadataError::Unknown(format!(
                "unexpected response: {:?}",
                other
            ))),
        }
    }

    async fn get_bulk(&self, prefix: &str) -> Result<Vec<KeyValueVersion>> {
        let d = self.data.read().await;
        Ok(d.kv
            .range(prefix.to_string()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .filter_map(|(k, v)| {
                serde_json::to_vec(&v.value)
                    .ok()
                    .map(|bytes| KeyValueVersion {
                        key: k.clone(),
                        value: bytes,
                        version: v.version,
                    })
            })
            .collect())
    }
}

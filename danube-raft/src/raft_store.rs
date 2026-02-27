//! `MetadataStore` implementation backed by Raft consensus.
//!
//! Reads are served from the local state machine replica.
//! Writes are proposed through the Raft leader and applied after consensus.

use std::time::Duration;

use async_trait::async_trait;
use openraft::error::{ClientWriteError, RaftError};
use openraft::Raft;
use serde_json::Value;
use tokio::sync::broadcast;
use tonic::transport::Endpoint;
use tracing::debug;

use danube_core::metadata::{
    KeyValueVersion, MetaOptions, MetadataError, MetadataStore, WatchStream,
};
use danube_core::raft_proto::raft_transport_client::RaftTransportClient;
use danube_core::raft_proto::ClientWriteRequest;

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
    ///
    /// If this node is not the leader, the write is transparently forwarded
    /// to the current leader via the `ClientWrite` gRPC RPC.
    async fn propose(&self, cmd: RaftCommand) -> Result<RaftResponse> {
        match self.raft.client_write(cmd.clone()).await {
            Ok(resp) => Ok(resp.data),
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(fwd))) => {
                if let Some(leader_node) = &fwd.leader_node {
                    debug!(
                        leader_addr = %leader_node.addr,
                        "forwarding write to leader"
                    );
                    self.forward_to_leader(&leader_node.addr, cmd).await
                } else {
                    // Leader unknown
                    Err(MetadataError::Unknown(
                        "raft propose failed: leader unknown (election in progress?)".into(),
                    ))
                }
            }
            Err(e) => Err(MetadataError::Unknown(format!(
                "raft propose failed: {}",
                e
            ))),
        }
    }

    /// Forward a write command to the leader via the ClientWrite gRPC RPC.
    async fn forward_to_leader(&self, leader_addr: &str, cmd: RaftCommand) -> Result<RaftResponse> {
        let endpoint_url = format!("http://{}", leader_addr);
        let channel = Endpoint::from_shared(endpoint_url)
            .map_err(|e| MetadataError::Unknown(format!("invalid leader endpoint: {}", e)))?
            .connect_timeout(Duration::from_secs(5))
            .connect()
            .await
            .map_err(|e| MetadataError::Unknown(format!("connect to leader failed: {}", e)))?;

        let mut client = RaftTransportClient::new(channel);
        let data = serde_json::to_vec(&cmd)
            .map_err(|e| MetadataError::Unknown(format!("serialize command failed: {}", e)))?;

        let resp = client
            .client_write(ClientWriteRequest { data })
            .await
            .map_err(|e| {
                MetadataError::Unknown(format!("leader client_write RPC failed: {}", e))
            })?;

        let reply = resp.into_inner();
        if !reply.error.is_empty() {
            return Err(MetadataError::Unknown(format!(
                "leader returned error: {}",
                reply.error
            )));
        }

        let raft_resp: RaftResponse = serde_json::from_slice(&reply.data)
            .map_err(|e| MetadataError::Unknown(format!("deserialize response failed: {}", e)))?;
        Ok(raft_resp)
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

    async fn allocate_monotonic_id(&self, counter_key: &str) -> Result<u64> {
        let resp = self
            .propose(RaftCommand::AllocateMonotonicId {
                counter_key: counter_key.to_string(),
            })
            .await?;
        match resp {
            RaftResponse::AllocatedId(id) => Ok(id),
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

//! gRPC Raft network transport.
//!
//! Implements `RaftNetworkFactory` and `RaftNetwork` using the proto-generated
//! `RaftTransportClient`. Each openraft request/response is JSON-serialized
//! and sent as opaque bytes over gRPC.

use std::future::Future;

use openraft::error::{
    InstallSnapshotError, RPCError, RaftError, ReplicationClosed, StreamingError,
};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::storage::Snapshot;
use openraft::{BasicNode, OptionalSend, Vote};
use tonic::transport::Channel;
use tracing::{info, warn};

use danube_core::raft_proto::raft_transport_client::RaftTransportClient;
use danube_core::raft_proto::RaftRequest;

use crate::typ::TypeConfig;

type NodeId = u64;
type Node = BasicNode;

// ---------------------------------------------------------------------------
// RaftNetworkFactory — creates a RaftNetwork for each target node
// ---------------------------------------------------------------------------

/// Factory that creates gRPC connections to peer Raft nodes.
#[derive(Clone)]
pub struct DanubeNetworkFactory;

impl RaftNetworkFactory<TypeConfig> for DanubeNetworkFactory {
    type Network = DanubeNetwork;

    async fn new_client(&mut self, target: NodeId, node: &Node) -> Self::Network {
        DanubeNetwork {
            target,
            addr: node.addr.clone(),
            client: None,
            connected: false,
        }
    }
}

// ---------------------------------------------------------------------------
// RaftNetwork — sends RPCs to a single target node
// ---------------------------------------------------------------------------

/// gRPC network connection to a single Raft peer.
pub struct DanubeNetwork {
    target: NodeId,
    addr: String,
    client: Option<RaftTransportClient<Channel>>,
    /// Tracks whether the peer is currently reachable.
    /// Used to log state transitions once (unreachable / reconnected)
    /// instead of flooding logs on every failed RPC.
    connected: bool,
}

impl DanubeNetwork {
    /// Lazily connect to the peer.
    async fn ensure_client(&mut self) -> Result<&mut RaftTransportClient<Channel>, tonic::Status> {
        if self.client.is_none() {
            let endpoint = format!("http://{}", self.addr);
            let client = RaftTransportClient::connect(endpoint)
                .await
                .map_err(|e| tonic::Status::unavailable(format!("connect failed: {}", e)))?;
            self.client = Some(client);
        }
        Ok(self.client.as_mut().unwrap())
    }

    /// Log once when a peer becomes reachable again after being unreachable.
    fn mark_success(&mut self) {
        if !self.connected {
            info!(
                target_node = self.target,
                addr = %self.addr,
                "peer is reachable"
            );
            self.connected = true;
        }
    }

    /// Log once when a peer becomes unreachable after being reachable.
    fn mark_failure(&mut self) {
        if self.connected {
            warn!(
                target_node = self.target,
                addr = %self.addr,
                "peer became unreachable"
            );
            self.connected = false;
        }
    }
}

impl RaftNetwork<TypeConfig> for DanubeNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, Node, RaftError<NodeId>>> {
        let data = serde_json::to_vec(&rpc).map_err(|e| net_err(&e.to_string()))?;
        if let Err(e) = self.ensure_client().await {
            self.mark_failure();
            return Err(unreachable_err(&e));
        }

        let resp = match self
            .client
            .as_mut()
            .unwrap()
            .append_entries(RaftRequest { data })
            .await
        {
            Ok(r) => r,
            Err(e) => {
                self.client = None;
                self.mark_failure();
                return Err(unreachable_err(&e));
            }
        };

        self.mark_success();
        let reply = resp.into_inner();
        if !reply.error.is_empty() {
            return Err(net_err(&reply.error));
        }
        serde_json::from_slice(&reply.data).map_err(|e| net_err(&e.to_string()))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, Node, RaftError<NodeId>>> {
        let data = serde_json::to_vec(&rpc).map_err(|e| net_err(&e.to_string()))?;
        if let Err(e) = self.ensure_client().await {
            self.mark_failure();
            return Err(unreachable_err(&e));
        }

        let resp = match self
            .client
            .as_mut()
            .unwrap()
            .vote(RaftRequest { data })
            .await
        {
            Ok(r) => r,
            Err(e) => {
                self.client = None;
                self.mark_failure();
                return Err(unreachable_err(&e));
            }
        };
        self.mark_success();

        let reply = resp.into_inner();
        if !reply.error.is_empty() {
            return Err(net_err(&reply.error));
        }
        serde_json::from_slice(&reply.data).map_err(|e| net_err(&e.to_string()))
    }

    async fn full_snapshot(
        &mut self,
        vote: Vote<NodeId>,
        snapshot: Snapshot<TypeConfig>,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<NodeId>, StreamingError<TypeConfig, openraft::error::Fatal<NodeId>>>
    {
        let snapshot_data = snapshot.snapshot.into_inner();
        let resp = self
            .install_snapshot(
                InstallSnapshotRequest {
                    vote,
                    meta: snapshot.meta.clone(),
                    offset: 0,
                    data: snapshot_data,
                    done: true,
                },
                option,
            )
            .await
            .map_err(|e| {
                warn!(?e, "install_snapshot failed");
                let io = std::io::Error::new(std::io::ErrorKind::Other, e.to_string());
                StreamingError::Unreachable(openraft::error::Unreachable::new(&io))
            })?;

        Ok(SnapshotResponse { vote: resp.vote })
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, Node, RaftError<NodeId, InstallSnapshotError>>,
    > {
        let data = serde_json::to_vec(&rpc).map_err(|e| net_err(&e.to_string()))?;
        if let Err(e) = self.ensure_client().await {
            self.mark_failure();
            return Err(unreachable_err(&e));
        }

        let resp = match self
            .client
            .as_mut()
            .unwrap()
            .install_snapshot(RaftRequest { data })
            .await
        {
            Ok(r) => r,
            Err(e) => {
                self.client = None;
                self.mark_failure();
                return Err(unreachable_err(&e));
            }
        };
        self.mark_success();

        let reply = resp.into_inner();
        if !reply.error.is_empty() {
            return Err(net_err(&reply.error));
        }
        serde_json::from_slice(&reply.data).map_err(|e| net_err(&e.to_string()))
    }
}

// ---------------------------------------------------------------------------
// Error helpers
// ---------------------------------------------------------------------------

fn unreachable_err<RE: std::error::Error>(
    e: &(impl std::error::Error + 'static),
) -> RPCError<NodeId, Node, RE> {
    RPCError::Unreachable(openraft::error::Unreachable::new(e))
}

fn net_err<RE: std::error::Error>(msg: &str) -> RPCError<NodeId, Node, RE> {
    let e = std::io::Error::new(std::io::ErrorKind::Other, msg.to_string());
    RPCError::Network(openraft::error::NetworkError::new(&e))
}

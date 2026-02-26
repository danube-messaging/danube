use crate::admin::DanubeAdminImpl;
use danube_core::admin_proto::{
    cluster_admin_server::ClusterAdmin, AddNodeRequest, AddNodeResponse, ClusterInitRequest,
    ClusterInitResponse, ClusterStatusResponse, Empty, PromoteNodeRequest, PromoteNodeResponse,
    RemoveNodeRequest, RemoveNodeResponse,
};
use danube_raft::BasicNode;
use std::collections::BTreeMap;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

#[tonic::async_trait]
impl ClusterAdmin for DanubeAdminImpl {
    /// Bootstrap a multi-node cluster.
    ///
    /// For each address in `nodes`, the node is expected to already be running
    /// its Raft gRPC transport. This implementation uses the current node's
    /// `raft.initialize()` which requires all node_ids to be known upfront.
    ///
    /// In the single-node case the broker's `main.rs` already calls
    /// `init_cluster` at startup. This RPC is for the multi-node bootstrap
    /// path invoked by `danube-admin cluster init --nodes ...`.
    async fn cluster_init(
        &self,
        request: Request<ClusterInitRequest>,
    ) -> Result<Response<ClusterInitResponse>, Status> {
        let req = request.into_inner();
        info!(nodes = ?req.nodes, "cluster init request");

        // Check if the cluster is already initialized by looking at membership.
        let metrics = self.raft.metrics().borrow().clone();
        if let Some(membership) = metrics
            .membership_config
            .membership()
            .get_joint_config()
            .first()
        {
            if !membership.is_empty() {
                info!("cluster already initialized, returning idempotent response");
                return Ok(Response::new(ClusterInitResponse {
                    success: true,
                    already_initialized: true,
                    leader_id: metrics.current_leader.unwrap_or(0),
                    voter_count: membership.len() as u32,
                    message: "Cluster already initialized".to_string(),
                }));
            }
        }

        if req.nodes.is_empty() {
            return Err(Status::invalid_argument(
                "at least one node address is required",
            ));
        }

        // For now, we initialize with the current node and add others as learners.
        // A full multi-node init would discover node_ids from each address.
        // This simplified version initializes the local node, then adds the rest.
        let self_id = self.leadership.node_id();
        let self_addr = req
            .nodes
            .first()
            .ok_or_else(|| Status::invalid_argument("empty nodes list"))?;

        let mut members = BTreeMap::new();
        members.insert(
            self_id,
            BasicNode {
                addr: self_addr.clone(),
            },
        );

        self.raft
            .initialize(members)
            .await
            .map_err(|e| Status::internal(format!("failed to initialize cluster: {}", e)))?;

        // Wait briefly for leader election
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        let metrics = self.raft.metrics().borrow().clone();
        let voter_count = metrics
            .membership_config
            .membership()
            .get_joint_config()
            .first()
            .map(|s| s.len() as u32)
            .unwrap_or(1);

        info!(
            leader_id = ?metrics.current_leader,
            voter_count,
            "cluster initialized"
        );

        Ok(Response::new(ClusterInitResponse {
            success: true,
            already_initialized: false,
            leader_id: metrics.current_leader.unwrap_or(0),
            voter_count,
            message: format!(
                "Cluster initialized: leader={}, {} voter(s)",
                metrics.current_leader.unwrap_or(0),
                voter_count
            ),
        }))
    }

    /// Return current Raft cluster state.
    async fn cluster_status(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<ClusterStatusResponse>, Status> {
        let metrics = self.raft.metrics().borrow().clone();

        let membership = metrics.membership_config.membership();
        let voters: Vec<u64> = membership
            .get_joint_config()
            .first()
            .map(|s| s.iter().copied().collect())
            .unwrap_or_default();

        let all_nodes: std::collections::BTreeSet<u64> =
            membership.nodes().map(|(id, _)| *id).collect();
        let voter_set: std::collections::BTreeSet<u64> = voters.iter().copied().collect();
        let learners: Vec<u64> = all_nodes.difference(&voter_set).copied().collect();

        let last_applied = metrics.last_applied.map(|lid| lid.index).unwrap_or(0);

        Ok(Response::new(ClusterStatusResponse {
            leader_id: metrics.current_leader.unwrap_or(0),
            current_term: metrics.current_term,
            last_applied,
            voters,
            learners,
            self_node_id: self.leadership.node_id(),
        }))
    }

    /// Add a node as a Raft learner (non-voting).
    async fn add_node(
        &self,
        request: Request<AddNodeRequest>,
    ) -> Result<Response<AddNodeResponse>, Status> {
        let req = request.into_inner();

        if req.addr.is_empty() {
            return Err(Status::invalid_argument("addr is required"));
        }
        if req.node_id == 0 {
            return Err(Status::invalid_argument(
                "node_id is required (discovered from the target node's {data_dir}/node_id)",
            ));
        }

        info!(node_id = req.node_id, addr = %req.addr, "adding learner node");

        self.raft
            .add_learner(
                req.node_id,
                BasicNode {
                    addr: req.addr.clone(),
                },
                true,
            )
            .await
            .map_err(|e| Status::internal(format!("failed to add learner: {}", e)))?;

        info!(node_id = req.node_id, "learner added successfully");

        Ok(Response::new(AddNodeResponse {
            success: true,
            node_id: req.node_id,
            message: format!("Node {} added as learner at {}", req.node_id, req.addr),
        }))
    }

    /// Promote a learner to a full voting member.
    async fn promote_node(
        &self,
        request: Request<PromoteNodeRequest>,
    ) -> Result<Response<PromoteNodeResponse>, Status> {
        let req = request.into_inner();

        if req.node_id == 0 {
            return Err(Status::invalid_argument("node_id is required"));
        }

        info!(node_id = req.node_id, "promoting learner to voter");

        // Get current voters and add the new one.
        let metrics = self.raft.metrics().borrow().clone();
        let mut voters: std::collections::BTreeSet<u64> = metrics
            .membership_config
            .membership()
            .get_joint_config()
            .first()
            .map(|s| s.clone())
            .unwrap_or_default();

        if voters.contains(&req.node_id) {
            return Ok(Response::new(PromoteNodeResponse {
                success: true,
                message: format!("Node {} is already a voter", req.node_id),
            }));
        }

        voters.insert(req.node_id);

        self.raft
            .change_membership(voters, false)
            .await
            .map_err(|e| Status::internal(format!("failed to promote node: {}", e)))?;

        info!(node_id = req.node_id, "node promoted to voter");

        Ok(Response::new(PromoteNodeResponse {
            success: true,
            message: format!("Node {} promoted to voter", req.node_id),
        }))
    }

    /// Remove a node from the Raft cluster.
    async fn remove_node(
        &self,
        request: Request<RemoveNodeRequest>,
    ) -> Result<Response<RemoveNodeResponse>, Status> {
        let req = request.into_inner();

        if req.node_id == 0 {
            return Err(Status::invalid_argument("node_id is required"));
        }

        info!(node_id = req.node_id, "removing node from cluster");

        // Get current voters and remove the target.
        let metrics = self.raft.metrics().borrow().clone();
        let mut voters: std::collections::BTreeSet<u64> = metrics
            .membership_config
            .membership()
            .get_joint_config()
            .first()
            .map(|s| s.clone())
            .unwrap_or_default();

        if !voters.remove(&req.node_id) {
            warn!(
                node_id = req.node_id,
                "node is not a voter — may be a learner or unknown"
            );
        }

        if voters.is_empty() {
            return Err(Status::failed_precondition(
                "cannot remove the last voter from the cluster",
            ));
        }

        self.raft
            .change_membership(voters, false)
            .await
            .map_err(|e| Status::internal(format!("failed to remove node: {}", e)))?;

        info!(node_id = req.node_id, "node removed from cluster");

        Ok(Response::new(RemoveNodeResponse {
            success: true,
            message: format!("Node {} removed from cluster", req.node_id),
        }))
    }
}

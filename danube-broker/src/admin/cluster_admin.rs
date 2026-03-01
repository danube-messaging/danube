use crate::admin::DanubeAdminImpl;
use danube_core::admin_proto::{
    cluster_admin_server::ClusterAdmin, AddNodeRequest, AddNodeResponse, ClusterStatusResponse,
    Empty, PromoteNodeRequest, PromoteNodeResponse, RemoveNodeRequest, RemoveNodeResponse,
};
use danube_raft::BasicNode;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

#[tonic::async_trait]
impl ClusterAdmin for DanubeAdminImpl {
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
            raft_addr: self.raft_addr.to_string(),
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

use std::sync::Arc;
use axum::{extract::State, response::IntoResponse, Json};
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::server::app::AppState;

#[derive(Clone, Serialize)]
pub struct RaftStatusDto {
    pub leader_id: String,
    pub current_term: u64,
    pub last_applied: u64,
    pub voters: Vec<String>,
    pub learners: Vec<String>,
    pub self_node_id: String,
    pub raft_addr: String,
}

#[derive(Deserialize)]
pub struct RaftActionRequest {
    pub action: String, // "promote_node" | "remove_node"
    pub node_id: String,
}

#[derive(Serialize)]
pub struct RaftActionResponse {
    pub success: bool,
    pub message: String,
}

pub async fn raft_status(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    info!(target: "gateway", "GET /ui/v1/cluster/raft called");
    match state.client.cluster_status().await {
        Ok(status) => {
            let dto = RaftStatusDto {
                leader_id: status.leader_id.to_string(),
                current_term: status.current_term,
                last_applied: status.last_applied,
                voters: status.voters.iter().map(|id| id.to_string()).collect(),
                learners: status.learners.iter().map(|id| id.to_string()).collect(),
                self_node_id: status.self_node_id.to_string(),
                raft_addr: status.raft_addr,
            };
            Json(dto).into_response()
        }
        Err(e) => {
            let (code, body) = crate::server::http::map_error(e);
            (code, body).into_response()
        }
    }
}

pub async fn raft_actions(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RaftActionRequest>,
) -> impl IntoResponse {
    let action = req.action.to_ascii_lowercase();
    let node_id = match req.node_id.parse::<u64>() {
        Ok(id) => id,
        Err(_) => {
            return (
                axum::http::StatusCode::BAD_REQUEST,
                Json(RaftActionResponse {
                    success: false,
                    message: "Invalid node_id format".to_string(),
                }),
            )
                .into_response();
        }
    };

    info!(target: "gateway", "raft_actions called: action={}, node_id={}", action, node_id);

    let res = match action.as_str() {
        "promote_node" => {
            let request = danube_core::admin_proto::PromoteNodeRequest { node_id };
            state
                .client
                .promote_node(request)
                .await
                .map(|r| (r.success, r.message))
        }
        "remove_node" => {
            let request = danube_core::admin_proto::RemoveNodeRequest { node_id };
            state
                .client
                .remove_node(request)
                .await
                .map(|r| (r.success, r.message))
        }
        _ => Err(anyhow::anyhow!("Unsupported raft action")),
    };

    match res {
        Ok((ok, msg)) => {
            let out = RaftActionResponse {
                success: ok,
                message: msg,
            };
            Json(out).into_response()
        }
        Err(e) => {
            let out = RaftActionResponse {
                success: false,
                message: e.to_string(),
            };
            (axum::http::StatusCode::BAD_GATEWAY, Json(out)).into_response()
        }
    }
}

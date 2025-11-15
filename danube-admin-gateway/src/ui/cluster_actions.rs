use std::sync::Arc;

use axum::{extract::State, response::IntoResponse, Json};
use serde::{Deserialize, Serialize};

use crate::app::AppState;

#[derive(Debug, Deserialize)]
pub struct ClusterActionRequest {
    pub action: String,     // unload | activate
    pub broker_id: String,  // required for both
    // unload-only params (optional)
    pub max_parallel: Option<u32>,
    pub namespaces_include: Option<Vec<String>>,
    pub namespaces_exclude: Option<Vec<String>>,
    pub dry_run: Option<bool>,
    pub timeout_seconds: Option<u32>,
    // activate-only params (optional)
    pub reason: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ClusterActionResponse {
    pub success: bool,
    pub message: String,
}

pub async fn cluster_actions(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ClusterActionRequest>,
) -> impl IntoResponse {
    let action = req.action.to_ascii_lowercase();
    if req.broker_id.trim().is_empty() {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            Json(ClusterActionResponse { success: false, message: "missing broker_id".to_string() }),
        )
            .into_response();
    }

    let res = match action.as_str() {
        "unload" => {
            let max_parallel = req.max_parallel.unwrap_or(1);
            let namespaces_include = req.namespaces_include.unwrap_or_default();
            let namespaces_exclude = req.namespaces_exclude.unwrap_or_default();
            let dry_run = req.dry_run.unwrap_or(false);
            let timeout_seconds = req.timeout_seconds.unwrap_or(60);
            state
                .client
                .unload_broker(
                    &req.broker_id,
                    max_parallel,
                    namespaces_include,
                    namespaces_exclude,
                    dry_run,
                    timeout_seconds,
                )
                .await
                .map(|r| (r.started, format!("started={} total={} succeeded={} failed={} pending={}", r.started, r.total, r.succeeded, r.failed, r.pending)))
        }
        "activate" => {
            let reason = req.reason.unwrap_or_else(|| "admin_activate".to_string());
            state
                .client
                .activate_broker(&req.broker_id, &reason)
                .await
                .map(|r| (r.success, format!("success={}", r.success)))
        }
        _ => Err(anyhow::anyhow!("unsupported action")),
    };

    match res {
        Ok((ok, msg)) => Json(ClusterActionResponse { success: ok, message: msg }).into_response(),
        Err(e) => (
            axum::http::StatusCode::BAD_GATEWAY,
            Json(ClusterActionResponse { success: false, message: e.to_string() }),
        )
            .into_response(),
    }
}

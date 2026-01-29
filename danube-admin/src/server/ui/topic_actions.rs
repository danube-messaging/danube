use std::sync::Arc;

use axum::{extract::State, response::IntoResponse, Json};
use serde::{Deserialize, Serialize};

use crate::server::app::AppState;
use tracing::info;

#[derive(Debug, Deserialize)]
pub struct TopicActionRequest {
    pub action: String,            // create | delete | unload
    pub topic: String,             // "/ns/topic" or "topic" with namespace
    pub namespace: Option<String>, // optional when topic includes namespace
    pub partitions: Option<u32>,   // for create
    pub schema_type: Option<String>,
    pub schema_data: Option<String>,
    pub dispatch_strategy: Option<String>, // non_reliable | reliable
}

#[derive(Debug, Serialize)]
pub struct TopicActionResponse {
    pub success: bool,
    pub message: String,
}

fn validate_topic_format(input: &str) -> bool {
    let parts: Vec<&str> = input.split('/').collect();
    if parts.len() != 3 {
        return false;
    }
    for p in parts.iter() {
        if !p
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
        {
            return false;
        }
    }
    true
}

fn normalize_topic(input: &str, namespace: Option<&str>) -> Result<String, String> {
    let s = input.trim();
    if s.starts_with('/') {
        if validate_topic_format(s) {
            return Ok(s.to_string());
        }
        return Err("wrong topic format, should be /namespace/topic".to_string());
    }
    let parts: Vec<&str> = s.split('/').collect();
    match parts.len() {
        2 => Ok(format!("/{}", s)),
        1 => {
            let ns = namespace
                .ok_or_else(|| "missing namespace for topic without namespace".to_string())?;
            Ok(format!("/{}/{}", ns, s))
        }
        _ => Err("wrong topic format, should be /namespace/topic".to_string()),
    }
}

fn parse_dispatch_strategy(input: Option<&str>) -> i32 {
    match input
        .unwrap_or("non_reliable")
        .to_ascii_lowercase()
        .as_str()
    {
        "reliable" | "reliable_dispatch" | "reliable-dispatch" => {
            danube_core::admin_proto::DispatchStrategy::Reliable as i32
        }
        _ => danube_core::admin_proto::DispatchStrategy::NonReliable as i32,
    }
}

pub async fn topic_actions(
    State(state): State<Arc<AppState>>,
    Json(req): Json<TopicActionRequest>,
) -> impl IntoResponse {
    let action = req.action.to_ascii_lowercase();

    // Normalize topic path
    let name = match normalize_topic(&req.topic, req.namespace.as_deref()) {
        Ok(v) => v,
        Err(e) => {
            return (
                axum::http::StatusCode::BAD_REQUEST,
                Json(TopicActionResponse {
                    success: false,
                    message: e,
                }),
            )
                .into_response()
        }
    };

    let res = match action.as_str() {
        "create" => {
            let schema_type = req.schema_type.unwrap_or_else(|| "String".to_string());
            let schema_data = req.schema_data.unwrap_or_else(|| "{}".to_string());
            let ds = parse_dispatch_strategy(req.dispatch_strategy.as_deref());
            let ds_label = if ds == danube_core::admin_proto::DispatchStrategy::Reliable as i32 {
                "Reliable"
            } else {
                "NonReliable"
            };
            if let Some(parts) = req.partitions {
                info!(target = "gateway", "topic_actions create_partitioned request: name={}, partitions={}, schema_type={}, schema_data_len={}, dispatch_strategy={}", name, parts, schema_type, schema_data.len(), ds_label);
                let request = danube_core::admin_proto::PartitionedTopicRequest {
                    base_name: name.clone(),
                    partitions: parts,
                    schema_subject: if schema_data.is_empty() { None } else { Some(schema_type.clone()) },
                    dispatch_strategy: ds,
                };
                state.client.create_partitioned_topic(request).await.map(|r| r.success)
            } else {
                info!(target = "gateway", "topic_actions create request: name={}, schema_type={}, schema_data_len={}, dispatch_strategy={}", name, schema_type, schema_data.len(), ds_label);
                let request = danube_core::admin_proto::NewTopicRequest {
                    name: name.clone(),
                    schema_subject: if schema_data.is_empty() { None } else { Some(schema_type.clone()) },
                    dispatch_strategy: ds,
                };
                state.client.create_topic(request).await.map(|r| r.success)
            }
        }
        "delete" => {
            info!(
                target = "gateway",
                "topic_actions delete request: name={}", name
            );
            let request = danube_core::admin_proto::TopicRequest { name: name.clone() };
            state.client.delete_topic(request).await.map(|r| r.success)
        }
        "unload" => {
            info!(
                target = "gateway",
                "topic_actions unload request: name={}", name
            );
            let request = danube_core::admin_proto::TopicRequest { name: name.clone() };
            state.client.unload_topic(request).await.map(|r| r.success)
        }
        _ => Err(anyhow::anyhow!("unsupported action")),
    };

    match res {
        Ok(ok) => {
            // Optionally: invalidate caches touching topics; keep simple per request
            let msg = if ok { "ok" } else { "not_ok" }.to_string();
            let out = TopicActionResponse {
                success: ok,
                message: msg,
            };
            info!(target = "gateway", "topic_actions response: {:?}", out);
            Json(out).into_response()
        }
        Err(e) => {
            let out = TopicActionResponse {
                success: false,
                message: e.to_string(),
            };
            info!(
                target = "gateway",
                "topic_actions error response: {:?}", out
            );
            (axum::http::StatusCode::BAD_GATEWAY, Json(out)).into_response()
        }
    }
}

use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde::Deserialize;
use tonic::Status as GrpcStatus;

use crate::{AppState, CacheEntry};
use std::time::Instant;

#[derive(Deserialize)]
pub struct TopicsQuery {
    pub namespace: String,
}

fn map_error(err: anyhow::Error) -> (StatusCode, Json<serde_json::Value>) {
    if let Some(st) = err.downcast_ref::<GrpcStatus>() {
        let code = st.code();
        let status = match code {
            tonic::Code::InvalidArgument => StatusCode::BAD_REQUEST,
            tonic::Code::Unauthenticated => StatusCode::UNAUTHORIZED,
            tonic::Code::PermissionDenied => StatusCode::FORBIDDEN,
            tonic::Code::NotFound => StatusCode::NOT_FOUND,
            tonic::Code::DeadlineExceeded => StatusCode::GATEWAY_TIMEOUT,
            tonic::Code::Unavailable | tonic::Code::Unknown => StatusCode::BAD_GATEWAY,
            _ => StatusCode::BAD_GATEWAY,
        };
        return (
            status,
            Json(serde_json::json!({ "error": st.message(), "code": format!("{:?}", code) })),
        );
    }
    let msg = err.to_string();
    if msg == "upstream timeout" {
        return (
            StatusCode::GATEWAY_TIMEOUT,
            Json(serde_json::json!({ "error": msg })),
        );
    }
    (
        StatusCode::BAD_GATEWAY,
        Json(serde_json::json!({ "error": msg })),
    )
}

pub async fn brokers_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    // Try cache
    {
        let cache = state.brokers_cache.lock().await;
        let now = Instant::now();
        if let Some(entry) = cache.as_ref() {
            if entry.expires_at > now {
                return Json(serde_json::json!(entry
                    .value
                    .brokers
                    .iter()
                    .map(|br| {
                        serde_json::json!({
                            "broker_id": br.broker_id,
                            "broker_addr": br.broker_addr,
                            "broker_role": br.broker_role,
                        })
                    })
                    .collect::<Vec<_>>()))
                .into_response();
            }
        }
        drop(cache);
    }

    let resp = state.client.list_brokers().await;
    match resp {
        Ok(b) => {
            // populate cache
            let mut cache = state.brokers_cache.lock().await;
            *cache = Some(CacheEntry {
                expires_at: Instant::now() + state.ttl,
                value: b.clone(),
            });
            let items: Vec<serde_json::Value> = b
                .brokers
                .iter()
                .map(|br| {
                    serde_json::json!({
                        "broker_id": br.broker_id,
                        "broker_addr": br.broker_addr,
                        "broker_role": br.broker_role,
                    })
                })
                .collect();
            Json(serde_json::json!(items)).into_response()
        }
        Err(e) => map_error(e).into_response(),
    }
}

pub async fn leader_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let resp = state.client.get_leader().await;
    match resp {
        Ok(l) => Json(serde_json::json!({"leader": l.leader})).into_response(),
        Err(e) => map_error(e).into_response(),
    }
}

pub async fn namespaces_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    // cache check
    {
        let cache = state.namespaces_cache.lock().await;
        let now = Instant::now();
        if let Some(entry) = cache.as_ref() {
            if entry.expires_at > now {
                return Json(serde_json::json!({"namespaces": entry.value.namespaces.clone()}))
                    .into_response();
            }
        }
        drop(cache);
    }

    let resp = state.client.list_namespaces().await;
    match resp {
        Ok(n) => {
            let mut cache = state.namespaces_cache.lock().await;
            *cache = Some(CacheEntry {
                expires_at: Instant::now() + state.ttl,
                value: n.clone(),
            });
            Json(serde_json::json!({"namespaces": n.namespaces})).into_response()
        }
        Err(e) => map_error(e).into_response(),
    }
}

pub async fn topics_handler(
    State(state): State<Arc<AppState>>,
    Query(q): Query<TopicsQuery>,
) -> impl IntoResponse {
    // cache check per-namespace
    {
        let map = state.topics_cache.lock().await;
        if let Some(entry) = map.get(&q.namespace) {
            if entry.expires_at > Instant::now() {
                return Json(serde_json::json!({"topics": entry.value.topics.clone()}))
                    .into_response();
            }
        }
        drop(map);
    }

    let resp = state.client.list_topics(&q.namespace).await;
    match resp {
        Ok(t) => {
            let mut map = state.topics_cache.lock().await;
            map.insert(
                q.namespace.clone(),
                CacheEntry {
                    expires_at: Instant::now() + state.ttl,
                    value: t.clone(),
                },
            );
            Json(serde_json::json!({"topics": t.topics})).into_response()
        }
        Err(e) => map_error(e).into_response(),
    }
}

pub async fn topic_desc_handler(
    State(state): State<Arc<AppState>>,
    Path(topic): Path<String>,
) -> impl IntoResponse {
    let resp = state.client.describe_topic(&topic).await;
    match resp {
        Ok(d) => Json(serde_json::json!({
            "name": d.name,
            "type_schema": d.type_schema,
            "schema_data": STANDARD.encode(d.schema_data),
            "subscriptions": d.subscriptions,
        }))
        .into_response(),
        Err(e) => map_error(e).into_response(),
    }
}

pub async fn topic_subs_handler(
    State(state): State<Arc<AppState>>,
    Path(topic): Path<String>,
) -> impl IntoResponse {
    let resp = state.client.list_subscriptions(&topic).await;
    match resp {
        Ok(s) => Json(serde_json::json!({"subscriptions": s.subscriptions})).into_response(),
        Err(e) => map_error(e).into_response(),
    }
}

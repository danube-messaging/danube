use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Instant;

use axum::{extract::State, response::IntoResponse, Json};
use serde::Serialize;

use crate::app::{AppState, CacheEntry};

#[derive(Clone, Serialize)]
pub struct NamespaceDto {
    pub name: String,
    pub topics: Vec<String>,
    pub policies: String,
}

#[derive(Clone, Serialize)]
pub struct NamespacesResponse {
    pub timestamp: String,
    pub namespaces: Vec<NamespaceDto>,
    pub errors: Vec<String>,
}

pub async fn list_namespaces_with_policies(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    {
        let cache = state.namespaces_cache.lock().await;
        if let Some(entry) = cache.as_ref() {
            if entry.expires_at > Instant::now() {
                return Json(entry.value.clone()).into_response();
            }
        }
        drop(cache);
    }
    let mut errors: Vec<String> = Vec::new();

    // list namespaces via gRPC
    let ns_resp = match state.client.list_namespaces().await {
        Ok(v) => v,
        Err(e) => {
            let (code, body) = crate::http::map_error(e);
            return (code, body).into_response();
        }
    };

    // Extract names directly from the response and de-duplicate
    let unique_names: Vec<String> = {
        let set: BTreeSet<String> = ns_resp.namespaces.into_iter().collect();
        set.into_iter().collect()
    };

    let mut out: Vec<NamespaceDto> = Vec::new();

    for ns in unique_names.iter() {
        // list topics for this namespace
        let topics_list = match state.client.list_topics(ns).await {
            Ok(v) => v,
            Err(e) => {
                errors.push(format!("list_topics failed for {}: {}", ns, e));
                // continue with empty topics but still gather policies
                // Default empty response
                danube_core::admin_proto::TopicListResponse { topics: Vec::new() }
            }
        };

        let topics: Vec<String> = topics_list.topics;

        // get policies for this namespace
        let policies_resp = match state.client.get_namespace_policies(ns).await {
            Ok(v) => v,
            Err(e) => {
                errors.push(format!("get_namespace_policies failed for {}: {}", ns, e));
                // default empty string
                danube_core::admin_proto::PolicyResponse {
                    policies: "".to_string(),
                }
            }
        };

        let policies = policies_resp.policies;

        out.push(NamespaceDto {
            name: ns.clone(),
            topics,
            policies,
        });
    }

    let dto = NamespacesResponse {
        timestamp: chrono::Utc::now().to_rfc3339(),
        namespaces: out,
        errors,
    };

    let mut cache = state.namespaces_cache.lock().await;
    *cache = Some(CacheEntry {
        expires_at: Instant::now() + state.ttl,
        value: dto.clone(),
    });

    Json(dto).into_response()
}

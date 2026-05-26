use std::sync::Arc;
use axum::{extract::State, response::IntoResponse, Json};
use serde::{Deserialize, Serialize};

use crate::server::app::AppState;
use crate::server::ui::shared::fetch_brokers;

#[derive(Serialize)]
pub struct RoleDto {
    pub name: String,
    pub permissions: Vec<String>,
    pub system: bool,
}

#[derive(Serialize)]
pub struct RolesResponse {
    pub roles: Vec<RoleDto>,
}

#[derive(Serialize)]
pub struct BindingDto {
    pub id: String,
    pub principal_type: String,
    pub principal_name: String,
    pub role_names: Vec<String>,
    pub scope: String,
    pub resource_name: String,
}

#[derive(Serialize)]
pub struct BindingsResponse {
    pub bindings: Vec<BindingDto>,
}

#[derive(Deserialize)]
pub struct RoleActionDto {
    pub action: String, // "create" or "delete"
    pub name: String,
    pub permissions: Option<Vec<String>>,
}

#[derive(Deserialize)]
pub struct BindingActionDto {
    pub action: String, // "create" or "delete"
    pub id: String,
    pub principal_type: Option<String>,
    pub principal_name: Option<String>,
    pub roles: Option<Vec<String>>,
    pub scope: String,
    pub resource: Option<String>,
}

pub async fn list_roles(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match state.client.list_roles().await {
        Ok(res) => {
            let roles = res.roles.into_iter().map(|r| RoleDto {
                name: r.name,
                permissions: r.permissions,
                system: r.system,
            }).collect();
            Json(RolesResponse { roles }).into_response()
        }
        Err(e) => {
            let (code, body) = crate::server::http::map_error(e);
            (code, body).into_response()
        }
    }
}

pub async fn list_bindings(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let client = &state.client;

    // 1. Cluster bindings
    let cluster_req = danube_core::admin_proto::ListBindingsRequest {
        scope: "cluster".to_string(),
        resource_name: "".to_string(),
    };
    let cluster_bindings_fut = client.list_bindings(cluster_req);

    // 2. Namespace bindings: list all namespaces first
    let ns_bindings_fut = async {
        let mut list = Vec::new();
        if let Ok(ns_res) = client.list_namespaces().await {
            for ns in ns_res.namespaces {
                let req = danube_core::admin_proto::ListBindingsRequest {
                    scope: "namespace".to_string(),
                    resource_name: ns.clone(),
                };
                if let Ok(res) = client.list_bindings(req).await {
                    list.extend(res.bindings);
                }
            }
        }
        list
    };

    // 3. Topic bindings: list topics from all brokers
    let topic_bindings_fut = async {
        let mut list = Vec::new();
        if let Ok(brokers_res) = fetch_brokers(&state).await {
            let mut unique_topics = std::collections::HashSet::new();
            for b in brokers_res.brokers {
                let req = danube_core::admin_proto::BrokerRequest {
                    broker_id: b.broker_id,
                };
                if let Ok(topics_res) = client.list_broker_topics(req).await {
                    for topic in topics_res.topics {
                        unique_topics.insert(topic.name);
                    }
                }
            }

            for topic_name in unique_topics {
                let req = danube_core::admin_proto::ListBindingsRequest {
                    scope: "topic".to_string(),
                    resource_name: topic_name,
                };
                if let Ok(res) = client.list_bindings(req).await {
                    list.extend(res.bindings);
                }
            }
        }
        list
    };

    let (cluster_res, ns_res, topic_res) = tokio::join!(
        cluster_bindings_fut,
        ns_bindings_fut,
        topic_bindings_fut
    );

    let mut all_bindings = Vec::new();

    if let Ok(res) = cluster_res {
        all_bindings.extend(res.bindings);
    }
    all_bindings.extend(ns_res);
    all_bindings.extend(topic_res);

    let bindings = all_bindings.into_iter().map(|b| BindingDto {
        id: b.id,
        principal_type: b.principal_type,
        principal_name: b.principal_name,
        role_names: b.role_names,
        scope: b.scope,
        resource_name: b.resource_name,
    }).collect();

    Json(BindingsResponse { bindings }).into_response()
}

pub async fn role_actions(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<RoleActionDto>,
) -> impl IntoResponse {
    match payload.action.as_str() {
        "create" => {
            let req = danube_core::admin_proto::CreateRoleRequest {
                role: Some(danube_core::admin_proto::RoleDefinition {
                    name: payload.name,
                    permissions: payload.permissions.unwrap_or_default(),
                    system: false,
                }),
            };
            match state.client.create_role(req).await {
                Ok(_) => Json(serde_json::json!({ "success": true })).into_response(),
                Err(e) => {
                    let (code, body) = crate::server::http::map_error(e);
                    (code, body).into_response()
                }
            }
        }
        "delete" => {
            let req = danube_core::admin_proto::DeleteRoleRequest {
                name: payload.name,
            };
            match state.client.delete_role(req).await {
                Ok(_) => Json(serde_json::json!({ "success": true })).into_response(),
                Err(e) => {
                    let (code, body) = crate::server::http::map_error(e);
                    (code, body).into_response()
                }
            }
        }
        _ => (
            axum::http::StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "invalid action" })),
        ).into_response(),
    }
}

pub async fn binding_actions(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<BindingActionDto>,
) -> impl IntoResponse {
    match payload.action.as_str() {
        "create" => {
            let req = danube_core::admin_proto::CreateBindingRequest {
                binding: Some(danube_core::admin_proto::BindingDefinition {
                    id: payload.id,
                    principal_type: payload.principal_type.unwrap_or_default(),
                    principal_name: payload.principal_name.unwrap_or_default(),
                    role_names: payload.roles.unwrap_or_default(),
                    scope: payload.scope,
                    resource_name: payload.resource.unwrap_or_default(),
                }),
            };
            match state.client.create_binding(req).await {
                Ok(_) => Json(serde_json::json!({ "success": true })).into_response(),
                Err(e) => {
                    let (code, body) = crate::server::http::map_error(e);
                    (code, body).into_response()
                }
            }
        }
        "delete" => {
            let req = danube_core::admin_proto::DeleteBindingRequest {
                binding_id: payload.id,
                scope: payload.scope,
                resource_name: payload.resource.unwrap_or_default(),
            };
            match state.client.delete_binding(req).await {
                Ok(_) => Json(serde_json::json!({ "success": true })).into_response(),
                Err(e) => {
                    let (code, body) = crate::server::http::map_error(e);
                    (code, body).into_response()
                }
            }
        }
        _ => (
            axum::http::StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "invalid action" })),
        ).into_response(),
    }
}

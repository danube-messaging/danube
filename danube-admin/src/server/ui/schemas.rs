use std::sync::Arc;
use std::time::Instant;
use axum::{
    extract::{Path, State},
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use danube_core::proto::danube_schema::{
    GetLatestSchemaRequest, ListVersionsRequest, ListSubjectsRequest,
    SetCompatibilityModeRequest, DeleteSchemaVersionRequest,
};

use crate::server::app::{AppState, CacheEntry};

#[derive(Clone, Serialize)]
pub struct SchemaSummaryDto {
    pub subject: String,
    pub schema_type: String,
    pub latest_version: u32,
    pub compatibility_mode: String,
    pub schema_id: u64,
    pub tags: Vec<String>,
}

#[derive(Clone, Serialize)]
pub struct SchemasListResponse {
    pub timestamp: String,
    pub subjects: Vec<SchemaSummaryDto>,
}

#[derive(Clone, Serialize)]
pub struct SchemaVersionDto {
    pub version: u32,
    pub created_at: u64,
    pub created_by: String,
    pub description: String,
    pub fingerprint: String,
    pub schema_id: u64,
}

#[derive(Clone, Serialize)]
pub struct SchemaDetailDto {
    pub schema_id: u64,
    pub version: u32,
    pub subject: String,
    pub schema_type: String,
    pub schema_definition: String, // Stringified JSON/Avro/Proto
    pub description: String,
    pub created_at: u64,
    pub created_by: String,
    pub tags: Vec<String>,
    pub fingerprint: String,
    pub compatibility_mode: String,
}

#[derive(Clone, Serialize)]
pub struct SchemaDetailPageResponse {
    pub timestamp: String,
    pub subject: String,
    pub schema_type: String,
    pub compatibility_mode: String,
    pub latest: Option<SchemaDetailDto>,
    pub versions: Vec<SchemaVersionDto>,
}

#[derive(Debug, Deserialize)]
pub struct SchemaActionRequest {
    pub action: String, // set_compatibility | delete_version
    pub subject: String,
    pub compatibility_mode: Option<String>, // for set_compatibility
    pub version: Option<u32>,               // for delete_version
}

#[derive(Debug, Serialize)]
pub struct SchemaActionResponse {
    pub success: bool,
    pub message: String,
}

pub async fn list_schemas(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    // Check Cache
    {
        let cache = state.schemas_cache.lock().await;
        if let Some(entry) = &*cache {
            if entry.expires_at > Instant::now() {
                return Json(entry.value.clone()).into_response();
            }
        }
    }

    let req = ListSubjectsRequest {};
    match state.client.list_subjects(req).await {
        Ok(resp) => {
            let mut subjects: Vec<SchemaSummaryDto> = resp
                .subjects
                .into_iter()
                .map(|s| SchemaSummaryDto {
                    subject: s.subject,
                    schema_type: s.schema_type,
                    latest_version: s.latest_version,
                    compatibility_mode: s.compatibility_mode,
                    schema_id: s.schema_id,
                    tags: s.tags,
                })
                .collect();

            // Sort subjects by schema_id to prevent changing order on refetch
            subjects.sort_by_key(|s| s.schema_id);

            let dto = SchemasListResponse {
                timestamp: chrono::Utc::now().to_rfc3339(),
                subjects,
            };

            // Update Cache
            {
                let mut cache = state.schemas_cache.lock().await;
                *cache = Some(CacheEntry {
                    expires_at: Instant::now() + state.ttl,
                    value: dto.clone(),
                });
            }

            Json(dto).into_response()
        }
        Err(e) => {
            let (code, body) = crate::server::http::map_error(e);
            (code, body).into_response()
        }
    }
}

pub async fn schema_detail(
    Path(subject): Path<String>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    // Check Cache
    {
        let cache = state.schema_detail_cache.lock().await;
        if let Some(entry) = cache.get(&subject) {
            if entry.expires_at > Instant::now() {
                return Json(entry.value.clone()).into_response();
            }
        }
    }

    // 1. Fetch latest schema concurrently with version list
    let state1 = Arc::clone(&state);
    let state2 = Arc::clone(&state);
    let subject1 = subject.clone();
    let subject2 = subject.clone();

    let latest_fut = tokio::spawn(async move {
        let req = GetLatestSchemaRequest { subject: subject1 };
        state1.client.get_latest_schema(req).await
    });

    let versions_fut = tokio::spawn(async move {
        let req = ListVersionsRequest { subject: subject2 };
        state2.client.list_versions(req).await
    });

    let latest_res = match latest_fut.await {
        Ok(r) => r,
        Err(e) => return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };

    let versions_res = match versions_fut.await {
        Ok(r) => r,
        Err(e) => return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };

    let latest_schema = match latest_res {
        Ok(l) => l,
        Err(e) => {
            let (code, body) = crate::server::http::map_error(e);
            return (code, body).into_response();
        }
    };

    let versions_list = match versions_res {
        Ok(v) => v,
        Err(e) => {
            let (code, body) = crate::server::http::map_error(e);
            return (code, body).into_response();
        }
    };

    let schema_definition = String::from_utf8(latest_schema.schema_definition)
        .unwrap_or_else(|_| "binary schema definition".to_string());

    let latest_dto = SchemaDetailDto {
        schema_id: latest_schema.schema_id,
        version: latest_schema.version,
        subject: latest_schema.subject,
        schema_type: latest_schema.schema_type.clone(),
        schema_definition,
        description: latest_schema.description,
        created_at: latest_schema.created_at,
        created_by: latest_schema.created_by,
        tags: latest_schema.tags,
        fingerprint: latest_schema.fingerprint,
        compatibility_mode: latest_schema.compatibility_mode.clone(),
    };

    let versions_dto = versions_list
        .versions
        .into_iter()
        .map(|v| SchemaVersionDto {
            version: v.version,
            created_at: v.created_at,
            created_by: v.created_by,
            description: v.description,
            fingerprint: v.fingerprint,
            schema_id: v.schema_id,
        })
        .collect();

    let dto = SchemaDetailPageResponse {
        timestamp: chrono::Utc::now().to_rfc3339(),
        subject: subject.clone(),
        schema_type: latest_schema.schema_type,
        compatibility_mode: latest_schema.compatibility_mode,
        latest: Some(latest_dto),
        versions: versions_dto,
    };

    // Update Cache
    {
        let mut cache = state.schema_detail_cache.lock().await;
        cache.insert(
            subject,
            CacheEntry {
                expires_at: Instant::now() + state.ttl,
                value: dto.clone(),
            },
        );
    }

    Json(dto).into_response()
}

pub async fn schema_actions(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SchemaActionRequest>,
) -> impl IntoResponse {
    let action = req.action.to_ascii_lowercase();

    let res = match action.as_str() {
        "set_compatibility" => {
            let mode = match req.compatibility_mode {
                Some(m) => m,
                None => {
                    return (
                        axum::http::StatusCode::BAD_REQUEST,
                        Json(SchemaActionResponse {
                            success: false,
                            message: "Missing compatibility_mode for set_compatibility action".to_string(),
                        }),
                    )
                        .into_response()
                }
            };
            let request = SetCompatibilityModeRequest {
                subject: req.subject.clone(),
                compatibility_mode: mode,
            };
            state.client.set_compatibility_mode(request).await.map(|r| r.success)
        }
        "delete_version" => {
            let version = match req.version {
                Some(v) => v,
                None => {
                    return (
                        axum::http::StatusCode::BAD_REQUEST,
                        Json(SchemaActionResponse {
                            success: false,
                            message: "Missing version for delete_version action".to_string(),
                        }),
                    )
                        .into_response()
                }
            };
            let request = DeleteSchemaVersionRequest {
                subject: req.subject.clone(),
                version,
            };
            state.client.delete_schema_version(request).await.map(|r| r.success)
        }
        _ => Err(anyhow::anyhow!("Unsupported action: {}", action)),
    };

    match res {
        Ok(ok) => {
            if ok {
                // Invalidate Cache
                {
                    let mut list_cache = state.schemas_cache.lock().await;
                    *list_cache = None;
                }
                {
                    let mut detail_cache = state.schema_detail_cache.lock().await;
                    detail_cache.remove(&req.subject);
                }
            }
            let msg = if ok { "ok" } else { "not_ok" }.to_string();
            Json(SchemaActionResponse {
                success: ok,
                message: msg,
            })
            .into_response()
        }
        Err(e) => {
            Json(SchemaActionResponse {
                success: false,
                message: e.to_string(),
            })
            .into_response()
        }
    }
}

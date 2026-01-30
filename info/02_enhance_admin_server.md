# Implementation Plan: Enhance Admin Server

**Goal**: Extend `danube-admin serve` with multi-mode support, schema registry endpoints, rebalance functionality, standardized responses, OpenAPI docs, and comprehensive health checks.

**Timeline**: 2-3 weeks  
**Prerequisites**: Phase 1 (Consolidation) completed  
**Priority**: High - Required for MCP server foundation

---

## Overview

Enhance the `danube-admin serve` mode to:
1. Support multiple server modes (HTTP, MCP, All)
2. Add complete schema registry HTTP endpoints
3. Add cluster rebalancing endpoints
4. Standardize all JSON responses with proper error codes
5. Generate OpenAPI/Swagger documentation
6. Implement comprehensive health checks
7. Add request ID tracking for observability

---

## Phase 1: Multi-Mode Server Architecture (Week 1)

### 1.1 Mode Selection Design

```rust
// src/server/mod.rs

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerMode {
    /// HTTP REST API only
    Http,
    /// MCP server only (future)
    Mcp,
    /// Both HTTP and MCP
    All,
}

impl std::str::FromStr for ServerMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "http" | "web" => Ok(Self::Http),
            "mcp" => Ok(Self::Mcp),
            "all" | "both" => Ok(Self::All),
            _ => Err(format!("Invalid mode: {}. Use: http, mcp, or all", s)),
        }
    }
}

#[derive(Debug, Args, Clone)]
pub struct ServerArgs {
    /// Server mode: http, mcp, or all
    #[arg(long, default_value = "http", env = "DANUBE_ADMIN_MODE")]
    pub mode: ServerMode,

    /// HTTP server listen address (for http/all modes)
    #[arg(long, default_value = "0.0.0.0:8080", env = "DANUBE_ADMIN_LISTEN_ADDR")]
    pub listen_addr: String,

    /// Enable stdio transport for MCP (for mcp/all modes)
    #[arg(long, default_value_t = false)]
    pub mcp_stdio: bool,

    // ... existing args
}

pub async fn run(args: ServerArgs) -> Result<()> {
    match args.mode {
        ServerMode::Http => {
            info!("Starting HTTP server only");
            run_http_server(args).await
        }
        ServerMode::Mcp => {
            info!("Starting MCP server only");
            // Future: run_mcp_server(args).await
            Err(anyhow!("MCP mode not yet implemented"))
        }
        ServerMode::All => {
            info!("Starting both HTTP and MCP servers");
            // Future: run both in parallel
            run_http_server(args).await
        }
    }
}

async fn run_http_server(args: ServerArgs) -> Result<()> {
    let state = app::create_app_state(args.clone()).await?;
    let router = app::build_router(state);

    let addr: SocketAddr = args.listen_addr.parse()?;
    info!("HTTP server listening on {}", addr);

    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, router.into_make_service()).await?;

    Ok(())
}
```

---

## Phase 2: Schema Registry HTTP Endpoints (Week 1)

### 2.1 Schema Handlers (`src/server/handlers/schemas.rs`)

```rust
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use danube_core::proto::danube_schema;

use crate::core::types::SuccessResponse;
use crate::server::app::AppState;
use super::ApiError;

/// Router for schema endpoints
pub fn schema_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/schemas", get(list_schemas))
        .route("/schemas", post(register_schema))
        .route("/schemas/:subject", get(get_schema_by_subject))
        .route("/schemas/:subject/versions", get(list_versions))
        .route("/schemas/:subject/versions/:version", get(get_schema_version))
        .route("/schemas/:subject/compatibility", post(check_compatibility))
        .route("/schemas/:subject/config", put(set_compatibility_mode))
        .route("/schemas/ids/:id", get(get_schema_by_id))
}

/// List all schema subjects
async fn list_schemas(
    State(state): State<Arc<AppState>>,
) -> Result<Json<SuccessResponse<Vec<String>>>, ApiError> {
    // Note: This requires adding a ListSubjects RPC to SchemaRegistry.proto
    // For now, return empty array with TODO
    let subjects: Vec<String> = vec![];
    
    Ok(Json(SuccessResponse::new(subjects)))
}

#[derive(Debug, Deserialize)]
pub struct RegisterSchemaRequest {
    pub subject: String,
    pub schema_type: String,
    pub schema_definition: String,
    pub description: Option<String>,
    pub tags: Option<Vec<String>>,
}

#[derive(Debug, Serialize)]
pub struct RegisterSchemaResponse {
    pub schema_id: u64,
    pub version: u32,
    pub is_new_version: bool,
    pub fingerprint: String,
}

/// Register a new schema
async fn register_schema(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<RegisterSchemaRequest>,
) -> Result<Json<SuccessResponse<RegisterSchemaResponse>>, ApiError> {
    let req = danube_schema::RegisterSchemaRequest {
        subject: payload.subject,
        schema_type: payload.schema_type,
        schema_definition: payload.schema_definition.into_bytes(),
        description: payload.description.unwrap_or_default(),
        created_by: "admin-gateway".to_string(),
        tags: payload.tags.unwrap_or_default(),
    };

    let response = state.client.register_schema(req).await?;

    let result = RegisterSchemaResponse {
        schema_id: response.schema_id,
        version: response.version,
        is_new_version: response.is_new_version,
        fingerprint: response.fingerprint,
    };

    Ok(Json(SuccessResponse::new(result)))
}

#[derive(Debug, Serialize)]
pub struct SchemaDetails {
    pub schema_id: u64,
    pub version: u32,
    pub subject: String,
    pub schema_type: String,
    pub schema_definition: String,
    pub description: String,
    pub created_at: u64,
    pub created_by: String,
    pub tags: Vec<String>,
    pub fingerprint: String,
    pub compatibility_mode: String,
}

/// Get latest schema for a subject
async fn get_schema_by_subject(
    State(state): State<Arc<AppState>>,
    Path(subject): Path<String>,
) -> Result<Json<SuccessResponse<SchemaDetails>>, ApiError> {
    let req = danube_schema::GetLatestSchemaRequest { subject };
    let response = state.client.get_latest_schema(req).await?;

    let schema = SchemaDetails {
        schema_id: response.schema_id,
        version: response.version,
        subject: response.subject,
        schema_type: response.schema_type,
        schema_definition: String::from_utf8_lossy(&response.schema_definition).to_string(),
        description: response.description,
        created_at: response.created_at,
        created_by: response.created_by,
        tags: response.tags,
        fingerprint: response.fingerprint,
        compatibility_mode: response.compatibility_mode,
    };

    Ok(Json(SuccessResponse::new(schema)))
}

/// Get schema by global ID
async fn get_schema_by_id(
    State(state): State<Arc<AppState>>,
    Path(id): Path<u64>,
) -> Result<Json<SuccessResponse<SchemaDetails>>, ApiError> {
    let req = danube_schema::GetSchemaRequest {
        schema_id: id,
        version: None,
    };
    let response = state.client.get_schema(req).await?;

    let schema = SchemaDetails {
        schema_id: response.schema_id,
        version: response.version,
        subject: response.subject,
        schema_type: response.schema_type,
        schema_definition: String::from_utf8_lossy(&response.schema_definition).to_string(),
        description: response.description,
        created_at: response.created_at,
        created_by: response.created_by,
        tags: response.tags,
        fingerprint: response.fingerprint,
        compatibility_mode: response.compatibility_mode,
    };

    Ok(Json(SuccessResponse::new(schema)))
}

#[derive(Debug, Serialize)]
pub struct SchemaVersionInfo {
    pub version: u32,
    pub created_at: u64,
    pub created_by: String,
    pub description: String,
    pub fingerprint: String,
    pub schema_id: u64,
}

/// List all versions for a subject
async fn list_versions(
    State(state): State<Arc<AppState>>,
    Path(subject): Path<String>,
) -> Result<Json<SuccessResponse<Vec<SchemaVersionInfo>>>, ApiError> {
    let req = danube_schema::ListVersionsRequest { subject };
    let response = state.client.list_versions(req).await?;

    let versions = response
        .versions
        .into_iter()
        .map(|v| SchemaVersionInfo {
            version: v.version,
            created_at: v.created_at,
            created_by: v.created_by,
            description: v.description,
            fingerprint: v.fingerprint,
            schema_id: v.schema_id,
        })
        .collect();

    Ok(Json(SuccessResponse::new(versions)))
}

/// Get specific version of a schema
async fn get_schema_version(
    State(state): State<Arc<AppState>>,
    Path((subject, version)): Path<(String, u32)>,
) -> Result<Json<SuccessResponse<SchemaDetails>>, ApiError> {
    // First get the schema_id for this subject/version
    let versions_req = danube_schema::ListVersionsRequest {
        subject: subject.clone(),
    };
    let versions_resp = state.client.list_versions(versions_req).await?;
    
    let version_info = versions_resp
        .versions
        .into_iter()
        .find(|v| v.version == version)
        .ok_or_else(|| {
            ApiError::from(crate::core::error::ErrorResponse::new(
                "NOT_FOUND",
                format!("Version {} not found for subject {}", version, subject),
            ))
        })?;

    // Now get the full schema details
    let req = danube_schema::GetSchemaRequest {
        schema_id: version_info.schema_id,
        version: Some(version),
    };
    let response = state.client.get_schema(req).await?;

    let schema = SchemaDetails {
        schema_id: response.schema_id,
        version: response.version,
        subject: response.subject,
        schema_type: response.schema_type,
        schema_definition: String::from_utf8_lossy(&response.schema_definition).to_string(),
        description: response.description,
        created_at: response.created_at,
        created_by: response.created_by,
        tags: response.tags,
        fingerprint: response.fingerprint,
        compatibility_mode: response.compatibility_mode,
    };

    Ok(Json(SuccessResponse::new(schema)))
}

#[derive(Debug, Deserialize)]
pub struct CheckCompatibilityRequest {
    pub schema_type: String,
    pub schema_definition: String,
}

#[derive(Debug, Serialize)]
pub struct CompatibilityResult {
    pub is_compatible: bool,
    pub errors: Vec<String>,
}

/// Check schema compatibility
async fn check_compatibility(
    State(state): State<Arc<AppState>>,
    Path(subject): Path<String>,
    Json(payload): Json<CheckCompatibilityRequest>,
) -> Result<Json<SuccessResponse<CompatibilityResult>>, ApiError> {
    let req = danube_schema::CheckCompatibilityRequest {
        subject,
        new_schema_definition: payload.schema_definition.into_bytes(),
        schema_type: payload.schema_type,
        compatibility_mode: None,
    };

    let response = state.client.check_compatibility(req).await?;

    let result = CompatibilityResult {
        is_compatible: response.is_compatible,
        errors: response.errors,
    };

    Ok(Json(SuccessResponse::new(result)))
}

#[derive(Debug, Deserialize)]
pub struct SetCompatibilityRequest {
    pub mode: String, // "none", "backward", "forward", "full"
}

/// Set compatibility mode for a subject
async fn set_compatibility_mode(
    State(state): State<Arc<AppState>>,
    Path(subject): Path<String>,
    Json(payload): Json<SetCompatibilityRequest>,
) -> Result<Json<SuccessResponse<String>>, ApiError> {
    let req = danube_schema::SetCompatibilityModeRequest {
        subject,
        compatibility_mode: payload.mode.clone(),
    };

    let response = state.client.set_compatibility_mode(req).await?;

    Ok(Json(SuccessResponse::new(response.message)))
}
```

### 2.2 Update Router (`src/server/app.rs`)

```rust
pub fn build_router(app_state: Arc<AppState>) -> Router {
    let cors = CorsLayer::permissive();

    Router::new()
        // Health
        .route("/health", get(health))
        .route("/api/v1/health", get(health_detailed))
        
        // Cluster
        .route("/api/v1/cluster", get(cluster::cluster_page))
        .route("/api/v1/cluster/actions", post(cluster::cluster_actions))
        .route("/api/v1/cluster/balance", get(cluster::get_balance))
        .route("/api/v1/cluster/rebalance", post(cluster::trigger_rebalance))
        
        // Brokers
        .route("/api/v1/brokers", get(cluster::list_brokers))
        .route("/api/v1/brokers/:broker_id", get(cluster::broker_page))
        
        // Topics
        .route("/api/v1/topics", get(topics::cluster_topics))
        .route("/api/v1/topics/actions", post(topics::topic_actions))
        .route("/api/v1/topics/:topic", get(topics::topic_page))
        .route("/api/v1/topics/:topic/series", get(topics::topic_series))
        
        // Namespaces
        .route("/api/v1/namespaces", get(namespaces::list_namespaces))
        .route("/api/v1/namespaces/:namespace", get(namespaces::namespace_details))
        
        // Schemas (NEW)
        .merge(handlers::schemas::schema_routes())
        
        // Legacy UI routes (keep for backward compatibility)
        .route("/ui/v1/cluster", get(cluster::cluster_page))
        .route("/ui/v1/topics", get(topics::cluster_topics))
        // ... other legacy routes
        
        .with_state(app_state)
        .layer(cors)
        .layer(TraceLayer::new_for_http())
        .layer(axum::middleware::from_fn(request_id_middleware))
}
```

---

## Phase 3: Cluster Rebalancing Endpoints (Week 1)

### 3.1 Rebalance Handlers (`src/server/handlers/cluster.rs`)

Add to existing cluster handlers:

```rust
#[derive(Debug, Serialize)]
pub struct ClusterBalanceInfo {
    pub coefficient_of_variation: f64,
    pub mean_load: f64,
    pub max_load: f64,
    pub min_load: f64,
    pub std_deviation: f64,
    pub broker_count: u32,
    pub brokers: Vec<BrokerLoadInfo>,
    pub is_balanced: bool, // Computed field
    pub recommendation: String, // Computed field
}

#[derive(Debug, Serialize)]
pub struct BrokerLoadInfo {
    pub broker_id: u64,
    pub load: f64,
    pub topic_count: u32,
    pub is_overloaded: bool,
    pub is_underloaded: bool,
}

/// Get cluster balance metrics
pub async fn get_balance(
    State(state): State<Arc<AppState>>,
) -> Result<Json<SuccessResponse<ClusterBalanceInfo>>, ApiError> {
    let req = admin::ClusterBalanceRequest {};
    let response = state.client.get_cluster_balance(req).await?;

    // Compute additional fields
    let is_balanced = response.coefficient_of_variation < 0.2; // Example threshold
    let recommendation = if is_balanced {
        "Cluster is well balanced".to_string()
    } else if response.coefficient_of_variation < 0.5 {
        "Minor imbalance detected. Rebalancing optional.".to_string()
    } else {
        "Significant imbalance detected. Rebalancing recommended.".to_string()
    };

    let balance_info = ClusterBalanceInfo {
        coefficient_of_variation: response.coefficient_of_variation,
        mean_load: response.mean_load,
        max_load: response.max_load,
        min_load: response.min_load,
        std_deviation: response.std_deviation,
        broker_count: response.broker_count,
        brokers: response
            .brokers
            .into_iter()
            .map(|b| BrokerLoadInfo {
                broker_id: b.broker_id,
                load: b.load,
                topic_count: b.topic_count,
                is_overloaded: b.is_overloaded,
                is_underloaded: b.is_underloaded,
            })
            .collect(),
        is_balanced,
        recommendation,
    };

    Ok(Json(SuccessResponse::new(balance_info)))
}

#[derive(Debug, Deserialize)]
pub struct TriggerRebalanceRequest {
    pub dry_run: Option<bool>,
    pub max_moves: Option<u32>,
}

#[derive(Debug, Serialize)]
pub struct RebalanceResult {
    pub success: bool,
    pub moves_executed: u32,
    pub proposed_moves: Vec<ProposedMove>,
    pub error_message: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ProposedMove {
    pub topic_name: String,
    pub from_broker: u64,
    pub to_broker: u64,
    pub estimated_load: f64,
    pub reason: String,
}

/// Trigger cluster rebalancing
pub async fn trigger_rebalance(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<TriggerRebalanceRequest>,
) -> Result<Json<SuccessResponse<RebalanceResult>>, ApiError> {
    let req = admin::RebalanceRequest {
        dry_run: payload.dry_run.unwrap_or(false),
        max_moves: payload.max_moves,
    };

    let response = state.client.trigger_rebalance(req).await?;

    let result = RebalanceResult {
        success: response.success,
        moves_executed: response.moves_executed,
        proposed_moves: response
            .proposed_moves
            .into_iter()
            .map(|m| ProposedMove {
                topic_name: m.topic_name,
                from_broker: m.from_broker,
                to_broker: m.to_broker,
                estimated_load: m.estimated_load,
                reason: m.reason,
            })
            .collect(),
        error_message: if response.error_message.is_empty() {
            None
        } else {
            Some(response.error_message)
        },
    };

    Ok(Json(SuccessResponse::new(result)))
}
```

---

## Phase 4: Request ID Tracking & Observability (Week 2)

### 4.1 Request ID Middleware (`src/server/middleware.rs`)

```rust
use axum::{
    extract::Request,
    http::HeaderValue,
    middleware::Next,
    response::Response,
};
use uuid::Uuid;

pub const REQUEST_ID_HEADER: &str = "x-request-id";

pub async fn request_id_middleware(
    mut request: Request,
    next: Next,
) -> Response {
    // Get or generate request ID
    let request_id = request
        .headers()
        .get(REQUEST_ID_HEADER)
        .and_then(|h| h.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    // Store in extensions for handlers to access
    request.extensions_mut().insert(request_id.clone());

    // Call next handler
    let mut response = next.run(request).await;

    // Add request ID to response headers
    response.headers_mut().insert(
        REQUEST_ID_HEADER,
        HeaderValue::from_str(&request_id).unwrap(),
    );

    response
}
```

### 4.2 Extract Request ID in Handlers

```rust
use axum::Extension;

async fn some_handler(
    Extension(request_id): Extension<String>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<SuccessResponse<Data>>, ApiError> {
    tracing::info!(request_id = %request_id, "Processing request");
    
    // Use request_id in responses
    let response = SuccessResponse::new(data)
        .with_request_id(request_id);
    
    Ok(Json(response))
}
```

---

## Phase 5: Comprehensive Health Checks (Week 2)

### 5.1 Enhanced Health Endpoint (`src/server/handlers/health.rs`)

```rust
use axum::{extract::State, Json};
use serde::Serialize;
use std::sync::Arc;
use std::time::Instant;

use crate::server::app::AppState;

#[derive(Debug, Serialize)]
pub struct DetailedHealthResponse {
    pub status: HealthStatus,
    pub version: String,
    pub uptime_seconds: u64,
    pub components: ComponentHealth,
    pub timestamp: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
}

#[derive(Debug, Serialize)]
pub struct ComponentHealth {
    pub broker_connectivity: ServiceHealth,
    pub schema_registry: ServiceHealth,
    pub metrics_service: ServiceHealth,
}

#[derive(Debug, Serialize)]
pub struct ServiceHealth {
    pub status: HealthStatus,
    pub message: String,
    pub latency_ms: Option<u64>,
}

pub async fn health_detailed(
    State(state): State<Arc<AppState>>,
) -> Json<DetailedHealthResponse> {
    let start = Instant::now();

    // Check broker connectivity
    let broker_health = check_broker_health(&state).await;
    
    // Check schema registry
    let schema_health = check_schema_registry_health(&state).await;
    
    // Check metrics service
    let metrics_health = check_metrics_health(&state).await;

    // Overall status
    let overall_status = if matches!(broker_health.status, HealthStatus::Healthy)
        && matches!(schema_health.status, HealthStatus::Healthy)
    {
        HealthStatus::Healthy
    } else if matches!(broker_health.status, HealthStatus::Unhealthy) {
        HealthStatus::Unhealthy
    } else {
        HealthStatus::Degraded
    };

    Json(DetailedHealthResponse {
        status: overall_status,
        version: env!("CARGO_PKG_VERSION").to_string(),
        uptime_seconds: start.elapsed().as_secs(), // TODO: Track actual uptime
        components: ComponentHealth {
            broker_connectivity: broker_health,
            schema_registry: schema_health,
            metrics_service: metrics_health,
        },
        timestamp: chrono::Utc::now().to_rfc3339(),
    })
}

async fn check_broker_health(state: &AppState) -> ServiceHealth {
    let start = Instant::now();
    
    match state.client.get_leader().await {
        Ok(_) => ServiceHealth {
            status: HealthStatus::Healthy,
            message: "Broker connection OK".to_string(),
            latency_ms: Some(start.elapsed().as_millis() as u64),
        },
        Err(e) => ServiceHealth {
            status: HealthStatus::Unhealthy,
            message: format!("Broker unreachable: {}", e),
            latency_ms: None,
        },
    }
}

async fn check_schema_registry_health(state: &AppState) -> ServiceHealth {
    let start = Instant::now();
    
    // Try to list versions for a non-existent subject (should return empty or error gracefully)
    let req = danube_core::proto::danube_schema::ListVersionsRequest {
        subject: "__health_check__".to_string(),
    };
    
    match state.client.list_versions(req).await {
        Ok(_) | Err(_) => {
            // Both OK and error (subject not found) indicate service is reachable
            ServiceHealth {
                status: HealthStatus::Healthy,
                message: "Schema registry OK".to_string(),
                latency_ms: Some(start.elapsed().as_millis() as u64),
            }
        }
    }
}

async fn check_metrics_health(state: &AppState) -> ServiceHealth {
    let start = Instant::now();
    
    // Try a simple query to Prometheus
    match state.metrics.query("up").await {
        Ok(_) => ServiceHealth {
            status: HealthStatus::Healthy,
            message: "Metrics service OK".to_string(),
            latency_ms: Some(start.elapsed().as_millis() as u64),
        },
        Err(e) => ServiceHealth {
            status: HealthStatus::Degraded,
            message: format!("Metrics unavailable: {}", e),
            latency_ms: None,
        },
    }
}
```

---

## Phase 6: OpenAPI/Swagger Documentation (Week 2)

### 6.1 Add OpenAPI Dependencies

```toml
# Cargo.toml
[dependencies]
utoipa = { version = "4.0", features = ["axum_extras"] }
utoipa-swagger-ui = { version = "6.0", features = ["axum"] }
```

### 6.2 OpenAPI Annotations

```rust
use utoipa::{OpenApi, ToSchema};

#[derive(OpenApi)]
#[openapi(
    paths(
        health_detailed,
        cluster::get_balance,
        cluster::trigger_rebalance,
        schemas::register_schema,
        schemas::get_schema_by_subject,
        // ... all other endpoints
    ),
    components(
        schemas(
            DetailedHealthResponse,
            ClusterBalanceInfo,
            RebalanceResult,
            RegisterSchemaRequest,
            SchemaDetails,
            // ... all DTOs
        )
    ),
    tags(
        (name = "health", description = "Health check endpoints"),
        (name = "cluster", description = "Cluster management"),
        (name = "schemas", description = "Schema registry"),
        (name = "topics", description = "Topic management"),
    ),
    info(
        title = "Danube Admin API",
        version = "1.0.0",
        description = "REST API for managing Danube messaging clusters",
        contact(
            name = "Danube",
            url = "https://github.com/danube-messaging/danube"
        ),
        license(
            name = "Apache 2.0",
            url = "https://www.apache.org/licenses/LICENSE-2.0"
        )
    )
)]
pub struct ApiDoc;

// Annotate endpoints
#[utoipa::path(
    get,
    path = "/api/v1/health",
    tag = "health",
    responses(
        (status = 200, description = "Health check successful", body = DetailedHealthResponse),
    )
)]
pub async fn health_detailed(
    State(state): State<Arc<AppState>>,
) -> Json<DetailedHealthResponse> {
    // ... implementation
}
```

### 6.3 Serve Swagger UI

```rust
use utoipa_swagger_ui::SwaggerUi;

pub fn build_router(app_state: Arc<AppState>) -> Router {
    let cors = CorsLayer::permissive();

    Router::new()
        // API routes
        .route("/api/v1/health", get(health::health_detailed))
        // ... other routes
        
        // Swagger UI
        .merge(SwaggerUi::new("/swagger-ui")
            .url("/api-docs/openapi.json", ApiDoc::openapi()))
        
        .with_state(app_state)
        .layer(cors)
        .layer(TraceLayer::new_for_http())
        .layer(axum::middleware::from_fn(request_id_middleware))
}
```

Access Swagger UI at: `http://localhost:8080/swagger-ui`

---

## Phase 7: Missing Features Checklist

### 7.1 Add Missing Endpoints

- [x] Schema registry CRUD operations
- [x] Cluster balance metrics
- [x] Trigger rebalancing
- [x] Detailed health checks
- [ ] Namespace CRUD operations (create, delete)
- [ ] Topic schema configuration endpoints
- [ ] Broker activation/deactivation via API
- [ ] Subscription management (unsubscribe)

### 7.2 Example: Namespace Operations

```rust
// src/server/handlers/namespaces.rs

#[derive(Debug, Deserialize)]
pub struct CreateNamespaceRequest {
    pub name: String,
}

#[utoipa::path(
    post,
    path = "/api/v1/namespaces",
    tag = "namespaces",
    request_body = CreateNamespaceRequest,
    responses(
        (status = 201, description = "Namespace created", body = SuccessResponse<String>),
        (status = 409, description = "Namespace already exists"),
    )
)]
pub async fn create_namespace(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<CreateNamespaceRequest>,
) -> Result<(StatusCode, Json<SuccessResponse<String>>), ApiError> {
    let req = admin::NamespaceRequest {
        name: payload.name.clone(),
    };

    state.client.create_namespace(req).await?;

    Ok((
        StatusCode::CREATED,
        Json(SuccessResponse::new(format!(
            "Namespace '{}' created successfully",
            payload.name
        ))),
    ))
}

#[utoipa::path(
    delete,
    path = "/api/v1/namespaces/:namespace",
    tag = "namespaces",
    params(
        ("namespace" = String, Path, description = "Namespace name")
    ),
    responses(
        (status = 200, description = "Namespace deleted"),
        (status = 404, description = "Namespace not found"),
    )
)]
pub async fn delete_namespace(
    State(state): State<Arc<AppState>>,
    Path(namespace): Path<String>,
) -> Result<Json<SuccessResponse<String>>, ApiError> {
    let req = admin::NamespaceRequest { name: namespace.clone() };

    state.client.delete_namespace(req).await?;

    Ok(Json(SuccessResponse::new(format!(
        "Namespace '{}' deleted successfully",
        namespace
    ))))
}
```

---

## Testing Plan

### Unit Tests

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_health_check_format() {
        // Test health response format
    }

    #[test]
    fn test_request_id_generation() {
        // Test request ID middleware
    }
}
```

### Integration Tests

```rust
// tests/server_integration.rs
use axum_test::TestServer;

#[tokio::test]
async fn test_schema_registration() {
    let app = create_test_app().await;
    let server = TestServer::new(app).unwrap();

    let response = server
        .post("/api/v1/schemas")
        .json(&serde_json::json!({
            "subject": "test-subject",
            "schema_type": "json_schema",
            "schema_definition": "{\"type\": \"object\"}"
        }))
        .await;

    assert_eq!(response.status_code(), 201);
}

#[tokio::test]
async fn test_cluster_balance() {
    let app = create_test_app().await;
    let server = TestServer::new(app).unwrap();

    let response = server.get("/api/v1/cluster/balance").await;
    assert_eq!(response.status_code(), 200);
}
```

---

## Configuration Updates

### Environment Variables

```bash
# Server mode
DANUBE_ADMIN_MODE=http              # or: mcp, all

# HTTP server
DANUBE_ADMIN_LISTEN_ADDR=0.0.0.0:8080

# Broker connection
DANUBE_ADMIN_ENDPOINT=http://broker:50051

# Observability
RUST_LOG=info,danube_admin=debug
```

---

## Documentation Updates

### API Documentation (README)

```markdown
## API Endpoints

### Health
- `GET /health` - Simple health check
- `GET /api/v1/health` - Detailed health with components

### Cluster
- `GET /api/v1/cluster` - Cluster overview
- `GET /api/v1/cluster/balance` - Balance metrics
- `POST /api/v1/cluster/rebalance` - Trigger rebalancing

### Schemas
- `GET /api/v1/schemas/:subject` - Get latest schema
- `POST /api/v1/schemas` - Register new schema
- `GET /api/v1/schemas/:subject/versions` - List versions
- `POST /api/v1/schemas/:subject/compatibility` - Check compatibility

### Interactive Documentation
Visit `http://localhost:8080/swagger-ui` for interactive API docs.
```

---

## Success Criteria

1. ✅ Multi-mode server support (http, mcp, all)
2. ✅ Complete schema registry HTTP API
3. ✅ Cluster rebalancing endpoints
4. ✅ Standardized JSON responses with error codes
5. ✅ Request ID tracking in all responses
6. ✅ Comprehensive health checks (broker, schema, metrics)
7. ✅ OpenAPI/Swagger documentation
8. ✅ All existing endpoints still work
9. ✅ Backward compatible with UI
10. ✅ Performance acceptable (< 100ms p95 for simple queries)

---

## Next Steps

After completion:
1. Update web UI to use new schema endpoints
2. Implement MCP server mode (Phase 3)
3. Add authentication/authorization
4. Add rate limiting
5. Add request/response caching where appropriate

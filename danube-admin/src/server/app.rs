use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Result;
use axum::{
    extract::State,
    routing::{get, post},
    Json, Router,
};
use tokio::sync::Mutex;
use tower_http::{cors::CorsLayer, trace::TraceLayer};

use super::ui::{
    broker::{broker_page, BrokerPage},
    cluster::{cluster_page, ClusterPage},
    cluster_actions::cluster_actions,
    namespaces::{list_namespaces_with_policies, NamespacesResponse},
    topic::{topic_page, TopicPage},
    topic_actions::topic_actions,
    topic_series::topic_series,
    topics::{cluster_topics, TopicsResponse},
};
use super::ServerArgs;
use crate::core::{AdminGrpcClient, GrpcClientConfig};
use crate::metrics::{MetricsClient, MetricsConfig};

#[derive(Clone)]
pub struct CacheEntry<T> {
    pub expires_at: Instant,
    pub value: T,
}

pub struct AppState {
    pub client: AdminGrpcClient,
    pub ttl: Duration,
    pub metrics: MetricsClient,
    pub cluster_page_cache: Mutex<Option<CacheEntry<ClusterPage>>>,
    pub broker_page_cache: Mutex<HashMap<String, CacheEntry<BrokerPage>>>,
    pub topic_page_cache: Mutex<HashMap<String, CacheEntry<TopicPage>>>,
    pub topics_cache: Mutex<Option<CacheEntry<TopicsResponse>>>,
    pub namespaces_cache: Mutex<Option<CacheEntry<NamespacesResponse>>>,
}

#[derive(serde::Serialize)]
struct HealthDto {
    status: &'static str,
    leader_reachable: bool,
}

pub async fn create_app_state(args: ServerArgs) -> Result<Arc<AppState>> {
    tracing::info!("Connecting to broker at {}", args.broker_endpoint);

    let grpc_config = GrpcClientConfig {
        endpoint: args.broker_endpoint.clone(),
        request_timeout_ms: args.request_timeout_ms,
        enable_tls: args.grpc_enable_tls,
        domain: args.grpc_domain.clone(),
        ca_path: args.grpc_ca.clone(),
        cert_path: args.grpc_cert.clone(),
        key_path: args.grpc_key.clone(),
    };

    let client = AdminGrpcClient::connect(grpc_config).await?;
    tracing::info!("Connected to broker successfully");

    let metrics_config = MetricsConfig {
        base_url: args.prometheus_url.clone(),
        timeout_ms: args.metrics_timeout_ms,
    };
    let metrics = MetricsClient::new(metrics_config)?;

    let ttl = Duration::from_millis(args.cache_ttl_ms);

    Ok(Arc::new(AppState {
        client,
        ttl,
        metrics,
        cluster_page_cache: Mutex::new(None),
        broker_page_cache: Mutex::new(HashMap::new()),
        topic_page_cache: Mutex::new(HashMap::new()),
        topics_cache: Mutex::new(None),
        namespaces_cache: Mutex::new(None),
    }))
}

// API endpoints
// - GET  /ui/v1/health
//   Health check for the gateway. Returns overall status and whether the broker cluster leader is reachable.
//
// - GET  /ui/v1/cluster
//   Cluster overview. Aggregates broker identities (id, role, status, addr) and selected metrics
//   such as rpc_total and topics_owned. Uses short-lived caching to reduce load.
//
// - POST /ui/v1/cluster/actions
//   Execute broker actions via Admin gRPC (e.g., unload, activate). Accepts a JSON payload with
//   action parameters and returns a status message.
//
// - GET  /ui/v1/topics
//   Cluster-wide topics summary. Uses authoritative topic lists via gRPC and enriches with Prometheus
//   metrics (producers/consumers/subscriptions). Returns brokers with their topics.
//
// - GET  /ui/v1/namespaces
//   List namespaces, their topics, and current policies as reported by the Admin gRPC service.
//
// - GET  /ui/v1/brokers/{broker_id}
//   Detailed broker page. Returns broker identity, aggregated metrics, and the list of topics assigned
//   to that broker (including producers/consumers/subscriptions and delivery strategy).
//
// - GET  /ui/v1/topics/{topic}
//   Topic details page. Returns schema, subscriptions, and aggregated metrics. The topic path must be
//   URL-encoded (e.g., /default/my-topic => %2Fdefault%2Fmy-topic).
//
// - GET  /ui/v1/topics/{topic}/series
//   Time series for the topic using Prometheus range queries. Query params:
//   from (unix seconds), to (unix seconds), step (e.g., 15s, 30s, 1m).
//
// - POST /ui/v1/topics/actions
//   Execute topic actions via Admin gRPC (create, delete, unload). Accepts a JSON payload; the service
//   normalizes topic names and applies sensible defaults where applicable.
//
pub fn build_router(app_state: Arc<AppState>) -> Router {
    let cors = CorsLayer::permissive();

    Router::new()
        .route("/ui/v1/health", get(health))
        .route("/ui/v1/cluster", get(cluster_page))
        .route("/ui/v1/cluster/actions", post(cluster_actions))
        .route("/ui/v1/topics", get(cluster_topics))
        .route("/ui/v1/namespaces", get(list_namespaces_with_policies))
        .route("/ui/v1/brokers/{broker_id}", get(broker_page))
        .route("/ui/v1/topics/{topic}", get(topic_page))
        .route("/ui/v1/topics/{topic}/series", get(topic_series))
        .route("/ui/v1/topics/actions", post(topic_actions))
        .with_state(app_state)
        .layer(cors)
        .layer(TraceLayer::new_for_http())
}

async fn health(State(state): State<Arc<AppState>>) -> Json<HealthDto> {
    let reachable = state.client.get_leader().await.is_ok();
    Json(HealthDto {
        status: "ok",
        leader_reachable: reachable,
    })
}

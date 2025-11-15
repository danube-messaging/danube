use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{extract::State, routing::{get, post}, Json, Router};
use tower_http::{cors::CorsLayer, trace::TraceLayer};

use tokio::sync::Mutex;

use crate::grpc_client::AdminGrpcClient;
use crate::metrics::MetricsClient;
use crate::ui::{
    broker::{broker_page, BrokerPage},
    cluster::{cluster_page, ClusterPage},
    cluster_actions::cluster_actions,
    namespaces::{list_namespaces_with_policies, NamespacesResponse},
    topic::{topic_page, TopicPage},
    topic_series::topic_series,
    topics::{cluster_topics, TopicsResponse},
    topic_actions::topic_actions,
};

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

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{extract::State, routing::get, Json, Router};
use tower_http::{cors::CorsLayer, trace::TraceLayer};

use tokio::sync::Mutex;

use crate::grpc_client::AdminGrpcClient;
use crate::metrics::MetricsClient;
use crate::ui::{
    broker::{broker_page, BrokerPage},
    cluster::{cluster_page, ClusterPage},
    topic::TopicPage,
    topic::{topic_page},
    topic_series::topic_series,
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
        .route("/ui/v1/brokers/{broker_id}", get(broker_page))
        .route("/ui/v1/topics/{topic}", get(topic_page))
        .route("/ui/v1/topics/{topic}/series", get(topic_series))
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

use std::{collections::HashMap, net::SocketAddr, path::PathBuf, sync::Arc, time::{Duration, Instant}};

use anyhow::Result;
use axum::{extract::State, routing::get, Json, Router};
use axum_server::tls_rustls::RustlsConfig;
use clap::Parser;
use serde::Serialize;
use tokio::net::TcpListener;
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing::info;

mod grpc_client;
mod http;
mod metrics;
mod dto;
mod ui {
    pub mod cluster;
    pub mod broker;
    pub mod topic;
}

use crate::grpc_client::{AdminGrpcClient, GrpcClientOptions};
use danube_core::admin_proto as admin;
use tokio::sync::Mutex;
use crate::metrics::{MetricsClient, MetricsConfig};
use crate::ui::{cluster::cluster_page, broker::broker_page, topic::topic_page};
use crate::dto::{ClusterPageDto, BrokerPageDto, TopicPageDto};

#[derive(Parser, Debug, Clone)]
struct Config {
    #[arg(long, default_value = "0.0.0.0:8080")]
    listen_addr: String,

    #[arg(long)]
    tls_cert: Option<PathBuf>,

    #[arg(long)]
    tls_key: Option<PathBuf>,

    #[arg(long)]
    broker_endpoint: String,

    #[arg(long)]
    cors_allow_origin: Option<String>,

    #[arg(long, default_value_t = 800)]
    request_timeout_ms: u64,

    #[arg(long, default_value_t = 3000)]
    per_endpoint_cache_ms: u64,

    // gRPC TLS/mTLS to brokers (CLI overrides env if provided)
    #[arg(long)]
    grpc_enable_tls: Option<bool>,
    #[arg(long)]
    grpc_domain: Option<String>,
    #[arg(long)]
    grpc_ca: Option<String>,
    #[arg(long)]
    grpc_cert: Option<String>,
    #[arg(long)]
    grpc_key: Option<String>,

    // Metrics scraping configuration
    #[arg(long, default_value = "http")]
    metrics_scheme: String,
    #[arg(long, default_value_t = 9040)]
    metrics_port: u16,
    #[arg(long, default_value = "/metrics")]
    metrics_path: String,
    #[arg(long, default_value_t = 800)]
    metrics_timeout_ms: u64,
}

#[derive(Serialize)]
struct HealthDto {
    status: &'static str,
    leader_reachable: bool,
}

pub struct CacheEntry<T> {
    expires_at: Instant,
    value: T,
}

pub struct AppState {
    pub client: AdminGrpcClient,
    pub ttl: Duration,
    pub brokers_cache: Mutex<Option<CacheEntry<admin::BrokerListResponse>>>,
    pub namespaces_cache: Mutex<Option<CacheEntry<admin::NamespaceListResponse>>>,
    pub topics_cache: Mutex<HashMap<String, CacheEntry<admin::TopicListResponse>>>,
    pub metrics: MetricsClient,
    pub cluster_page_cache: Mutex<Option<CacheEntry<ClusterPageDto>>>,
    pub broker_page_cache: Mutex<HashMap<String, CacheEntry<BrokerPageDto>>>,
    pub topic_page_cache: Mutex<HashMap<String, CacheEntry<TopicPageDto>>>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let cfg = Config::parse();
    let client_opts = GrpcClientOptions {
        request_timeout_ms: cfg.request_timeout_ms,
        enable_tls: cfg.grpc_enable_tls,
        domain: cfg.grpc_domain.clone(),
        ca_path: cfg.grpc_ca.clone(),
        cert_path: cfg.grpc_cert.clone(),
        key_path: cfg.grpc_key.clone(),
    };
    let client = AdminGrpcClient::connect(cfg.broker_endpoint.clone(), client_opts).await?;
    let metrics_client = MetricsClient::new(MetricsConfig {
        scheme: cfg.metrics_scheme.clone(),
        port: cfg.metrics_port,
        path: cfg.metrics_path.clone(),
        timeout_ms: cfg.metrics_timeout_ms,
    })?;
    let app_state = Arc::new(AppState {
        client,
        ttl: Duration::from_millis(cfg.per_endpoint_cache_ms),
        brokers_cache: Mutex::new(None),
        namespaces_cache: Mutex::new(None),
        topics_cache: Mutex::new(HashMap::new()),
        metrics: metrics_client,
        cluster_page_cache: Mutex::new(None),
        broker_page_cache: Mutex::new(HashMap::new()),
        topic_page_cache: Mutex::new(HashMap::new()),
    });

    let cors = CorsLayer::permissive();

    let app = Router::new()
        .route("/ui/v1/health", get(health))
        .route("/ui/v1/cluster", get(cluster_page))
        .route("/ui/v1/brokers/{broker_id}", get(broker_page))
        .route("/ui/v1/topics/{topic}", get(topic_page))
        .with_state(app_state)
        .layer(cors)
        .layer(TraceLayer::new_for_http());

    let addr: SocketAddr = cfg.listen_addr.parse().expect("invalid listen addr");

    match (cfg.tls_cert, cfg.tls_key) {
        (Some(cert), Some(key)) => {
            let rustls = RustlsConfig::from_pem_file(cert, key).await?;
            info!("listening on https://{}", addr);
            axum_server::bind_rustls(addr, rustls)
                .serve(app.into_make_service())
                .await?;
        }
        _ => {
            info!("listening on http://{}", addr);
            let listener = TcpListener::bind(addr).await?;
            axum::serve(listener, app.into_make_service()).await?;
        }
    }

    Ok(())
}

async fn health(State(state): State<Arc<AppState>>) -> Json<HealthDto> {
    let reachable = state.client.get_leader().await.is_ok();
    Json(HealthDto {
        status: "ok",
        leader_reachable: reachable,
    })
}

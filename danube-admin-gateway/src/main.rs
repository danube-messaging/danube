use std::{collections::HashMap, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use anyhow::Result;
use axum_server::tls_rustls::RustlsConfig;
use clap::Parser;
use tokio::net::TcpListener;
use tracing::info;

mod app;
mod grpc_client;
mod http;
mod metrics;
mod ui {
    pub mod broker;
    pub mod cluster;
    pub mod topic;
    pub mod shared;
}

use crate::app::{build_router, AppState};
use crate::grpc_client::{AdminGrpcClient, GrpcClientOptions};
use crate::metrics::{MetricsClient, MetricsConfig};
use tokio::sync::Mutex;

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
        metrics: metrics_client,
        cluster_page_cache: Mutex::new(None),
        broker_page_cache: Mutex::new(HashMap::new()),
        topic_page_cache: Mutex::new(HashMap::new()),
    });

    let app = build_router(app_state);

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

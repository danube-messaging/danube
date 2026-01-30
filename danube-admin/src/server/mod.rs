mod app;
pub mod http;
pub mod metrics;
pub mod ui;

use anyhow::Result;
use clap::Args;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tracing::info;

#[derive(Debug, Args, Clone)]
pub struct ServerArgs {
    /// HTTP server listen address
    #[arg(long, default_value = "0.0.0.0:8080", env = "DANUBE_ADMIN_LISTEN_ADDR")]
    pub listen_addr: String,

    /// Broker gRPC endpoint
    #[arg(long, env = "DANUBE_ADMIN_ENDPOINT", default_value = "http://127.0.0.1:50051")]
    pub broker_endpoint: String,

    /// Request timeout in milliseconds
    #[arg(long, default_value_t = 5000)]
    pub request_timeout_ms: u64,

    /// Cache TTL in milliseconds
    #[arg(long, default_value_t = 3000)]
    pub cache_ttl_ms: u64,

    /// CORS allow origin
    #[arg(long)]
    pub cors_allow_origin: Option<String>,

    // gRPC TLS/mTLS options
    #[arg(long)]
    pub grpc_enable_tls: Option<bool>,
    #[arg(long)]
    pub grpc_domain: Option<String>,
    #[arg(long)]
    pub grpc_ca: Option<String>,
    #[arg(long)]
    pub grpc_cert: Option<String>,
    #[arg(long)]
    pub grpc_key: Option<String>,

    /// Prometheus base URL
    #[arg(long, default_value = "http://localhost:9090")]
    pub prometheus_url: String,

    /// Metrics timeout in milliseconds
    #[arg(long, default_value_t = 5000)]
    pub metrics_timeout_ms: u64,
}

pub async fn run(args: ServerArgs) -> Result<()> {
    info!("Initializing danube-admin server");

    let state = app::create_app_state(args.clone()).await?;
    let router = app::build_router(state);

    let addr: SocketAddr = args.listen_addr.parse()?;
    info!("Starting HTTP server on {}", addr);

    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, router.into_make_service()).await?;

    Ok(())
}

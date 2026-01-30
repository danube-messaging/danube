mod app;
pub mod http;
pub mod metrics;
pub mod ui;

use anyhow::Result;
use clap::Args;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::info;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerMode {
    /// Admin UI server (HTTP REST API)
    Ui,
    /// MCP server only
    Mcp,
    /// Both UI and MCP
    All,
}

impl std::str::FromStr for ServerMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "ui" => Ok(Self::Ui),
            "mcp" => Ok(Self::Mcp),
            "all" => Ok(Self::All),
            _ => Err(format!("Invalid mode: {}. Use: ui, mcp, or all", s)),
        }
    }
}

#[derive(Debug, Args, Clone)]
pub struct ServerArgs {
    /// Server mode: ui, mcp, or all (required)
    #[arg(long, env = "DANUBE_ADMIN_MODE")]
    pub mode: ServerMode,

    /// MCP configuration file (optional, enables log access and resources)
    #[arg(long, env = "DANUBE_MCP_CONFIG")]
    pub config: Option<std::path::PathBuf>,

    /// HTTP server listen address
    #[arg(long, default_value = "0.0.0.0:8080", env = "DANUBE_ADMIN_LISTEN_ADDR")]
    pub listen_addr: String,

    /// Broker gRPC endpoint (can be overridden by config file)
    #[arg(
        long,
        env = "DANUBE_ADMIN_ENDPOINT",
        default_value = "http://127.0.0.1:50051"
    )]
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
    match args.mode {
        ServerMode::Ui => {
            info!("Starting Admin UI server");
            run_http_server(args).await
        }
        ServerMode::Mcp => {
            info!("Starting MCP server");
            run_mcp_server(args).await
        }
        ServerMode::All => {
            info!("Starting both UI and MCP servers");

            // Clone args for UI server (MCP takes ownership)
            let ui_args = args.clone();

            // Spawn both servers concurrently
            let ui_handle = tokio::spawn(async move { run_http_server(ui_args).await });
            let mcp_handle = tokio::spawn(async move { run_mcp_server(args).await });

            // Wait for both - if either fails, propagate error
            let (ui_result, mcp_result) = tokio::join!(ui_handle, mcp_handle);
            ui_result??;
            mcp_result??;

            Ok(())
        }
    }
}

async fn run_http_server(args: ServerArgs) -> Result<()> {
    info!("Initializing HTTP server");

    let state = app::create_app_state(args.clone()).await?;
    let router = app::build_router(state);

    let addr: SocketAddr = args.listen_addr.parse()?;
    info!("HTTP server listening on {}", addr);

    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, router.into_make_service()).await?;

    Ok(())
}

async fn run_mcp_server(args: ServerArgs) -> Result<()> {
    use crate::core::{AdminGrpcClient, GrpcClientConfig};
    use crate::mcp::config::McpConfig;

    info!("Initializing MCP server");

    // Load MCP config: from file if provided, otherwise minimal config
    let mcp_config = if let Some(config_path) = &args.config {
        info!("Loading MCP config from {:?}", config_path);
        McpConfig::from_file(config_path)?
    } else {
        info!("No MCP config file provided - using minimal config (no log access)");
        McpConfig::minimal(args.broker_endpoint.clone())
    };

    // Use broker endpoint from config file if available, otherwise from CLI args
    let broker_endpoint = mcp_config.broker_endpoint.clone();

    let grpc_config = GrpcClientConfig {
        endpoint: broker_endpoint,
        request_timeout_ms: args.request_timeout_ms,
        enable_tls: args.grpc_enable_tls,
        domain: args.grpc_domain.clone(),
        ca_path: args.grpc_ca.clone(),
        cert_path: args.grpc_cert.clone(),
        key_path: args.grpc_key.clone(),
    };

    let client = Arc::new(AdminGrpcClient::connect(grpc_config).await?);
    crate::mcp::run_mcp_server(client, mcp_config).await
}

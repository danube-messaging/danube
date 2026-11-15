mod app;
pub mod http;
pub mod ui;

use anyhow::Result;
use clap::Args;

use crate::cli::{ClientArgs, Connection};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::info;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerMode {
    /// HTTP server: console REST API (`/ui/v1/...`) + streamable HTTP MCP (`/mcp`)
    Http,
    /// MCP over stdio (for local AI agents / IDEs)
    McpStdio,
}

/// `danube console` — web admin console backend.
#[derive(Debug, Args, Clone)]
#[command(
    about = "Run the web admin console backend (HTTP API + MCP over HTTP)",
    long_about = "Run the web admin console backend.\n\n\
Serves the console REST API under /ui/v1/... and the streamable HTTP MCP endpoint \
under /mcp, both backed by the broker admin API.",
    after_help = "Examples:
  danube console
  danube console --listen 0.0.0.0:8080 --endpoint http://broker:6650
  danube console --require-auth --http-token \"$TOKEN\" --prometheus-url http://prometheus:9090"
)]
pub struct ConsoleArgs {
    /// HTTP listen address
    #[arg(long, default_value = "0.0.0.0:8080", env = "DANUBE_CONSOLE_LISTEN", value_name = "ADDR", help_heading = "Server")]
    pub listen: String,

    /// Require a bearer token on HTTP requests
    #[arg(long, env = "DANUBE_HTTP_REQUIRE_AUTH", help_heading = "Server")]
    pub require_auth: bool,

    /// Bearer token accepted on HTTP requests
    #[arg(long, env = "DANUBE_HTTP_TOKEN", hide_env_values = true, value_name = "TOKEN", help_heading = "Server")]
    pub http_token: Option<String>,

    /// CORS allowed origin
    #[arg(long, value_name = "ORIGIN", help_heading = "Server")]
    pub cors_allow_origin: Option<String>,

    /// Response cache TTL in milliseconds
    #[arg(long, default_value_t = 3000, value_name = "MS", help_heading = "Server")]
    pub cache_ttl_ms: u64,

    #[command(flatten)]
    pub backend: BackendArgs,

    #[command(flatten)]
    pub client: ClientArgs,
}

/// `danube mcp` — Model Context Protocol server for AI agents.
#[derive(Debug, Args, Clone)]
#[command(
    about = "Run the MCP server for AI agents (stdio by default)",
    long_about = "Run the Model Context Protocol server that exposes Danube admin tools, \
prompts and resources to AI agents.\n\n\
By default it speaks MCP over stdio, which is what IDEs and local agents expect. \
Use --http to serve streamable HTTP MCP at http://<ADDR>/mcp instead.",
    after_help = "Examples:
  danube mcp                                   # stdio, broker at http://127.0.0.1:6650
  danube mcp --endpoint http://broker:6650 --config ./mcp.yaml
  danube mcp --http 0.0.0.0:8080               # streamable HTTP at /mcp"
)]
pub struct McpArgs {
    /// Serve MCP over streamable HTTP on this address instead of stdio
    #[arg(long, value_name = "ADDR", help_heading = "Transport")]
    pub http: Option<String>,

    #[command(flatten)]
    pub backend: BackendArgs,

    #[command(flatten)]
    pub client: ClientArgs,
}

/// Backend settings shared by `danube console` and `danube mcp`.
#[derive(Debug, Args, Clone)]
pub struct BackendArgs {
    /// MCP configuration file (enables log access tools and resources)
    #[arg(long, env = "DANUBE_MCP_CONFIG", value_name = "FILE", value_hint = clap::ValueHint::FilePath, help_heading = "Backend")]
    pub config: Option<std::path::PathBuf>,

    /// Admin API request timeout in milliseconds
    #[arg(long, default_value_t = 5000, value_name = "MS", help_heading = "Backend")]
    pub request_timeout_ms: u64,

    /// Prometheus base URL used for metrics tools
    #[arg(long, default_value = "http://localhost:9090", env = "DANUBE_PROMETHEUS_URL", value_name = "URL", help_heading = "Backend")]
    pub prometheus_url: String,

    /// Prometheus query timeout in milliseconds
    #[arg(long, default_value_t = 5000, value_name = "MS", help_heading = "Backend")]
    pub metrics_timeout_ms: u64,
}

/// Resolved runtime settings for the HTTP / MCP servers.
#[derive(Debug, Clone)]
pub struct ServerArgs {
    pub mode: ServerMode,
    pub config: Option<std::path::PathBuf>,
    pub listen_addr: String,
    pub broker_endpoint: String,
    pub connection: Connection,
    pub request_timeout_ms: u64,
    pub cache_ttl_ms: u64,
    pub cors_allow_origin: Option<String>,
    pub prometheus_url: String,
    pub metrics_timeout_ms: u64,
    pub http_token: Option<String>,
    pub require_auth: bool,
}

impl ServerArgs {
    fn grpc_config(&self, endpoint: String) -> crate::core::GrpcClientConfig {
        crate::core::GrpcClientConfig {
            endpoint,
            request_timeout_ms: self.request_timeout_ms,
            ..self.connection.grpc_config()
        }
    }
}

impl ConsoleArgs {
    pub fn into_server_args(self) -> Result<ServerArgs> {
        let connection = self.client.resolve()?;
        Ok(ServerArgs {
            mode: ServerMode::Http,
            config: self.backend.config,
            listen_addr: self.listen,
            broker_endpoint: connection.endpoint.clone(),
            connection,
            request_timeout_ms: self.backend.request_timeout_ms,
            cache_ttl_ms: self.cache_ttl_ms,
            cors_allow_origin: self.cors_allow_origin,
            prometheus_url: self.backend.prometheus_url,
            metrics_timeout_ms: self.backend.metrics_timeout_ms,
            http_token: self.http_token,
            require_auth: self.require_auth,
        })
    }
}

impl McpArgs {
    pub fn into_server_args(self) -> Result<ServerArgs> {
        let connection = self.client.resolve()?;
        let (mode, listen_addr) = match self.http {
            Some(addr) => (ServerMode::Http, addr),
            None => (ServerMode::McpStdio, String::new()),
        };
        Ok(ServerArgs {
            mode,
            config: self.backend.config,
            listen_addr,
            broker_endpoint: connection.endpoint.clone(),
            connection,
            request_timeout_ms: self.backend.request_timeout_ms,
            cache_ttl_ms: 3000,
            cors_allow_origin: None,
            prometheus_url: self.backend.prometheus_url,
            metrics_timeout_ms: self.backend.metrics_timeout_ms,
            http_token: None,
            require_auth: false,
        })
    }
}

pub async fn run(args: ServerArgs) -> Result<()> {
    match args.mode {
        ServerMode::Http => {
            info!("Starting HTTP server (console API + MCP at /mcp)");
            run_http_server(args).await
        }
        ServerMode::McpStdio => {
            info!("Starting MCP server (stdio)");
            run_mcp_server(args).await
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
    use crate::core::AdminGrpcClient;
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

    let grpc_config = args.grpc_config(broker_endpoint);

    let client = Arc::new(AdminGrpcClient::connect(grpc_config).await?);
    crate::mcp::run_mcp_server(client, mcp_config).await
}

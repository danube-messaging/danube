//! MCP (Model Context Protocol) server implementation
//!
//! Enables AI assistants (Claude, Cursor, Windsurf) to manage Danube clusters
//! through natural language using the Model Context Protocol.

pub mod config;
pub mod prompts;
pub mod resources;
pub mod server;
pub mod tools;
pub mod types;

use crate::core::AdminGrpcClient;
use crate::metrics::{MetricsClient, MetricsConfig};
use std::sync::Arc;

pub use config::McpConfig;
pub use server::DanubeMcpServer;

pub async fn run_mcp_server(client: Arc<AdminGrpcClient>, config: McpConfig) -> anyhow::Result<()> {
    use rmcp::ServiceExt;
    use tokio::io::{stdin, stdout};

    tracing::info!("Starting Danube MCP server with stdio transport");

    if config.has_log_access() {
        tracing::info!(
            "Log access enabled via {:?}",
            config.deployment.as_ref().map(|d| &d.deployment_type)
        );
    } else {
        tracing::info!("Log access not configured - log fetching tools will be unavailable");
    }

    // Create metrics client for Prometheus queries
    let metrics_config = MetricsConfig {
        base_url: config.prometheus_url.clone(),
        timeout_ms: 5000,
    };
    let metrics = MetricsClient::new(metrics_config)?;
    tracing::info!("Metrics client configured for {}", config.prometheus_url);

    let server = DanubeMcpServer::new(client, config, metrics);
    let transport = (stdin(), stdout());
    let service = server.serve(transport).await?;

    tracing::info!("MCP server running, waiting for requests...");
    service.waiting().await?;

    Ok(())
}

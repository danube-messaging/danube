//! MCP (Model Context Protocol) server implementation
//!
//! Enables AI assistants (Claude, Cursor, Windsurf) to manage Danube clusters
//! through natural language using the Model Context Protocol.

pub mod tools;
pub mod types;

use crate::core::AdminGrpcClient;
use rmcp::{
    handler::server::{tool::ToolRouter, wrapper::Parameters},
    model::*,
    tool, tool_handler, tool_router, ErrorData as McpError, ServerHandler, ServiceExt,
};
use std::sync::Arc;
#[derive(Clone)]
pub struct DanubeMcpServer {
    client: Arc<AdminGrpcClient>,
    tool_router: ToolRouter<Self>,
}

#[tool_router]
impl DanubeMcpServer {
    pub fn new(client: Arc<AdminGrpcClient>) -> Self {
        Self {
            client,
            tool_router: Self::tool_router(),
        }
    }

    // ===== CLUSTER MANAGEMENT TOOLS =====

    #[tool(
        description = "List all brokers in the Danube cluster with their status, role, and addresses"
    )]
    async fn list_brokers(&self) -> Result<CallToolResult, McpError> {
        let output = tools::cluster::list_brokers(&self.client).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    #[tool(description = "Get the current leader broker information")]
    async fn get_leader(&self) -> Result<CallToolResult, McpError> {
        let output = tools::cluster::get_leader(&self.client).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    #[tool(
        description = "Get cluster load balance metrics including coefficient of variation and per-broker loads"
    )]
    async fn get_cluster_balance(&self) -> Result<CallToolResult, McpError> {
        let output = tools::cluster::get_cluster_balance(&self.client).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    #[tool(
        description = "Trigger cluster rebalancing to distribute topics evenly across brokers. Use dry_run=true to preview changes."
    )]
    async fn trigger_rebalance(
        &self,
        Parameters(params): Parameters<tools::cluster::RebalanceParams>,
    ) -> Result<CallToolResult, McpError> {
        let output = tools::cluster::trigger_rebalance(&self.client, params).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    #[tool(description = "Unload all topics from a specific broker (for maintenance or removal)")]
    async fn unload_broker(
        &self,
        Parameters(params): Parameters<tools::cluster::UnloadBrokerParams>,
    ) -> Result<CallToolResult, McpError> {
        let output = tools::cluster::unload_broker(&self.client, params).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    #[tool(description = "List all namespaces in the cluster")]
    async fn list_namespaces(&self) -> Result<CallToolResult, McpError> {
        let output = tools::cluster::list_namespaces(&self.client).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    // ===== TOPIC MANAGEMENT TOOLS =====

    #[tool(
        description = "List topics in a specific namespace with details including broker assignment and delivery strategy"
    )]
    async fn list_topics(
        &self,
        Parameters(params): Parameters<tools::topics::ListTopicsParams>,
    ) -> Result<CallToolResult, McpError> {
        let output = tools::topics::list_topics(&self.client, params).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    #[tool(
        description = "Get detailed information about a specific topic including subscriptions, schema, and metrics"
    )]
    async fn describe_topic(
        &self,
        Parameters(params): Parameters<tools::topics::DescribeTopicParams>,
    ) -> Result<CallToolResult, McpError> {
        let output = tools::topics::describe_topic(&self.client, params).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    #[tool(
        description = "Create a new topic with specified configuration (partitions, schema, dispatch strategy)"
    )]
    async fn create_topic(
        &self,
        Parameters(params): Parameters<tools::topics::CreateTopicParams>,
    ) -> Result<CallToolResult, McpError> {
        let output = tools::topics::create_topic(&self.client, params).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    #[tool(description = "Delete a topic and all its data (WARNING: irreversible operation)")]
    async fn delete_topic(
        &self,
        Parameters(params): Parameters<tools::topics::DeleteTopicParams>,
    ) -> Result<CallToolResult, McpError> {
        let output = tools::topics::delete_topic(&self.client, params).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    #[tool(description = "List all subscriptions on a topic")]
    async fn list_subscriptions(
        &self,
        Parameters(params): Parameters<tools::topics::ListSubscriptionsParams>,
    ) -> Result<CallToolResult, McpError> {
        let output = tools::topics::list_subscriptions(&self.client, params).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    // ===== SCHEMA REGISTRY TOOLS =====

    #[tool(
        description = "Register a new schema in the schema registry with compatibility checking"
    )]
    async fn register_schema(
        &self,
        Parameters(params): Parameters<tools::schemas::RegisterSchemaParams>,
    ) -> Result<CallToolResult, McpError> {
        let output = tools::schemas::register_schema(&self.client, params).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    #[tool(description = "Get the latest version of a schema by subject name")]
    async fn get_schema(
        &self,
        Parameters(params): Parameters<tools::schemas::GetSchemaParams>,
    ) -> Result<CallToolResult, McpError> {
        let output = tools::schemas::get_schema(&self.client, params).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    #[tool(description = "List all versions of a schema subject")]
    async fn list_schema_versions(
        &self,
        Parameters(params): Parameters<tools::schemas::ListVersionsParams>,
    ) -> Result<CallToolResult, McpError> {
        let output = tools::schemas::list_schema_versions(&self.client, params).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    #[tool(description = "Check if a new schema is compatible with existing versions")]
    async fn check_schema_compatibility(
        &self,
        Parameters(params): Parameters<tools::schemas::CheckCompatibilityParams>,
    ) -> Result<CallToolResult, McpError> {
        let output = tools::schemas::check_compatibility(&self.client, params).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    // ===== DIAGNOSTIC TOOLS =====

    #[tool(description = "Diagnose why a consumer might be lagging on a topic")]
    async fn diagnose_consumer_lag(
        &self,
        Parameters(params): Parameters<tools::diagnostics::ConsumerLagParams>,
    ) -> Result<CallToolResult, McpError> {
        let output = tools::diagnostics::diagnose_consumer_lag(&self.client, params).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    #[tool(description = "Analyze cluster health and identify potential issues")]
    async fn health_check(&self) -> Result<CallToolResult, McpError> {
        let output = tools::diagnostics::health_check(&self.client).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    #[tool(description = "Get recommended actions to improve cluster performance")]
    async fn get_recommendations(&self) -> Result<CallToolResult, McpError> {
        let output = tools::diagnostics::get_recommendations(&self.client).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for DanubeMcpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            ..Default::default()
        }
    }
}

pub async fn run_mcp_server(client: Arc<AdminGrpcClient>) -> anyhow::Result<()> {
    use tokio::io::{stdin, stdout};

    tracing::info!("Starting Danube MCP server with stdio transport");

    let server = DanubeMcpServer::new(client);
    let transport = (stdin(), stdout());
    let service = server.serve(transport).await?;

    tracing::info!("MCP server running, waiting for requests...");
    service.waiting().await?;

    Ok(())
}

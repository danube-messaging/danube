//! MCP Server implementation
//!
//! Contains the DanubeMcpServer struct, tool definitions,
//! and ServerHandler implementation.

use super::config::McpConfig;
use super::{prompts, resources, tools};
use crate::core::AdminGrpcClient;
use crate::metrics::MetricsClient;
use rmcp::{
    handler::server::{tool::ToolRouter, wrapper::Parameters},
    model::*,
    tool, tool_handler, tool_router, ErrorData as McpError, ServerHandler,
};
use std::sync::Arc;

#[derive(Clone)]
pub struct DanubeMcpServer {
    client: Arc<AdminGrpcClient>,
    config: Arc<McpConfig>,
    metrics: Arc<MetricsClient>,
    tool_router: ToolRouter<Self>,
}

#[tool_router]
impl DanubeMcpServer {
    pub fn new(client: Arc<AdminGrpcClient>, config: McpConfig, metrics: MetricsClient) -> Self {
        Self {
            client,
            config: Arc::new(config),
            metrics: Arc::new(metrics),
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

    // ===== LOG ACCESS TOOLS =====

    #[tool(
        description = "Get logs from a specific broker by its ID (e.g., 'broker1'). Use list_configured_brokers first to see available broker IDs. Returns recent log output for debugging issues."
    )]
    async fn get_broker_logs(
        &self,
        Parameters(params): Parameters<tools::logs::BrokerLogsParams>,
    ) -> Result<CallToolResult, McpError> {
        let output = tools::logs::get_broker_logs(&self.config, params).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    #[tool(
        description = "List all brokers and containers configured in the MCP config file that can be queried for logs. Call this first before get_broker_logs to see available broker IDs."
    )]
    async fn list_configured_brokers(&self) -> Result<CallToolResult, McpError> {
        let output = tools::logs::list_configured_brokers(&self.config);
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    #[tool(
        description = "List all running Docker containers with their names, status, and ports. Useful to discover container names for fetch_container_logs."
    )]
    async fn list_docker_containers(&self) -> Result<CallToolResult, McpError> {
        let output = tools::logs::list_docker_containers();
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    #[tool(
        description = "List all Kubernetes pods in a specific namespace with their status. Useful to discover pod names for fetch_pod_logs."
    )]
    async fn list_k8s_pods(
        &self,
        Parameters(params): Parameters<tools::logs::ListPodsParams>,
    ) -> Result<CallToolResult, McpError> {
        let output = tools::logs::list_k8s_pods(&params.namespace);
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    #[tool(
        description = "Fetch logs directly from any Docker container by its exact container name. Use list_docker_containers to find container names. Returns the last N lines (default 500)."
    )]
    async fn fetch_container_logs(
        &self,
        Parameters(params): Parameters<tools::logs::ContainerLogsParams>,
    ) -> Result<CallToolResult, McpError> {
        let output = tools::logs::fetch_docker_logs(&params.container_name, params.lines);
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    #[tool(
        description = "Fetch logs directly from a Kubernetes pod by namespace and pod name. Use list_k8s_pods to find pod names. Returns the last N lines (default 500)."
    )]
    async fn fetch_pod_logs(
        &self,
        Parameters(params): Parameters<tools::logs::PodLogsParams>,
    ) -> Result<CallToolResult, McpError> {
        let output = tools::logs::fetch_k8s_logs(&params.namespace, &params.pod_name, params.lines);
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    // ===== METRICS TOOLS =====

    #[tool(
        description = "Get cluster-wide metrics from Prometheus: broker count, total topics, producers, consumers, message rates, and cluster balance. Requires Prometheus to be running."
    )]
    async fn get_cluster_metrics(&self) -> Result<CallToolResult, McpError> {
        let output = tools::metrics::get_cluster_metrics(&self.metrics).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    #[tool(
        description = "Get metrics for a specific broker: topics owned, RPC count, producers, consumers, bytes in/out. Use list_brokers first to get broker IDs."
    )]
    async fn get_broker_metrics(
        &self,
        Parameters(params): Parameters<tools::metrics::BrokerMetricsParams>,
    ) -> Result<CallToolResult, McpError> {
        let output = tools::metrics::get_broker_metrics(&self.metrics, params).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    #[tool(
        description = "Get metrics for a specific topic: message counts, byte counts, active producers/consumers, publish/dispatch rates, subscription lag, and latency percentiles."
    )]
    async fn get_topic_metrics(
        &self,
        Parameters(params): Parameters<tools::metrics::TopicMetricsParams>,
    ) -> Result<CallToolResult, McpError> {
        let output = tools::metrics::get_topic_metrics(&self.metrics, params).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    #[tool(
        description = "Execute a raw PromQL query against Prometheus. Useful for custom metric investigations. Example: sum(rate(danube_topic_messages_in_total[5m]))"
    )]
    async fn query_prometheus(
        &self,
        Parameters(params): Parameters<tools::metrics::RawQueryParams>,
    ) -> Result<CallToolResult, McpError> {
        let output = tools::metrics::query_prometheus(&self.metrics, params).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for DanubeMcpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            capabilities: ServerCapabilities::builder()
                .enable_tools()
                .enable_resources()
                .enable_prompts()
                .build(),
            ..Default::default()
        }
    }

    async fn list_resources(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: rmcp::service::RequestContext<rmcp::RoleServer>,
    ) -> Result<ListResourcesResult, McpError> {
        Ok(resources::list_resources())
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        _context: rmcp::service::RequestContext<rmcp::RoleServer>,
    ) -> Result<ReadResourceResult, McpError> {
        resources::read_resource(&request).ok_or_else(|| {
            McpError::resource_not_found(format!("Unknown resource: {}", request.uri), None)
        })
    }

    async fn list_prompts(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: rmcp::service::RequestContext<rmcp::RoleServer>,
    ) -> Result<ListPromptsResult, McpError> {
        Ok(prompts::list_prompts())
    }

    async fn get_prompt(
        &self,
        request: GetPromptRequestParams,
        _context: rmcp::service::RequestContext<rmcp::RoleServer>,
    ) -> Result<GetPromptResult, McpError> {
        prompts::get_prompt(&request).ok_or_else(|| {
            McpError::invalid_params(format!("Unknown prompt: {}", request.name), None)
        })
    }
}

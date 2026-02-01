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

    /// List all brokers in the cluster.
    ///
    /// Returns broker IDs, status (active/inactive), role (leader/follower), and network
    /// addresses (broker, admin, metrics endpoints).
    ///
    /// Use this first to discover broker IDs required by other broker-specific tools like
    /// get_broker_metrics, unload_broker, or activate_broker.
    #[tool]
    async fn list_brokers(&self) -> Result<CallToolResult, McpError> {
        let output = tools::cluster::list_brokers(&self.client).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    /// Get the current leader broker in the cluster.
    ///
    /// Returns the broker ID of the leader responsible for cluster coordination,
    /// metadata management, and load balancing decisions.
    ///
    /// The leader is automatically elected via ETCD. Use this to verify cluster health
    /// or troubleshoot coordination issues.
    #[tool]
    async fn get_leader(&self) -> Result<CallToolResult, McpError> {
        let output = tools::cluster::get_leader(&self.client).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    /// Get load balance metrics for the cluster.
    ///
    /// Returns Coefficient of Variation (CV), mean/max/min load, and per-broker topic counts.
    /// CV interpretation: <20% = well balanced, 20-30% = balanced, 30-40% = imbalanced,
    /// >40% = severely imbalanced.
    ///
    /// Use this to assess if trigger_rebalance is needed. Run before and after rebalancing
    /// to verify improvements.
    #[tool]
    async fn get_cluster_balance(&self) -> Result<CallToolResult, McpError> {
        let output = tools::cluster::get_cluster_balance(&self.client).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    /// Trigger cluster rebalancing to distribute topics evenly across brokers.
    ///
    /// Side effects: Moves topics between brokers gracefully (no message loss or downtime).
    /// Topics are reassigned to balance load automatically based on current broker utilization.
    ///
    /// Always use dry_run=true first to preview which topics will move without applying changes.
    /// Only execute when get_cluster_balance shows CV > 30%. Use after adding/removing brokers
    /// or before maintenance windows.
    #[tool]
    async fn trigger_rebalance(
        &self,
        Parameters(params): Parameters<tools::cluster::RebalanceParams>,
    ) -> Result<CallToolResult, McpError> {
        let output = tools::cluster::trigger_rebalance(&self.client, params).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    /// Unload all topics from a broker to prepare for maintenance or removal.
    ///
    /// Side effects: Topics are unloaded and reassigned to other brokers automatically.
    /// The broker goes into draining/drained mode and owns no topics after completion.
    ///
    /// Use this before graceful broker shutdown, rolling upgrades, or decommissioning.
    /// Always use dry_run=true first to preview which topics will be unloaded.
    /// Use list_brokers first to get available broker IDs.
    #[tool]
    async fn unload_broker(
        &self,
        Parameters(params): Parameters<tools::cluster::UnloadBrokerParams>,
    ) -> Result<CallToolResult, McpError> {
        let output = tools::cluster::unload_broker(&self.client, params).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    /// List all namespaces in the cluster.
    ///
    /// Returns the names of all namespaces. Namespaces provide logical isolation for
    /// topics and policies (rate limits, consumer limits, message size limits).
    ///
    /// Use this before creating topics if you're unsure about available namespaces.
    /// The 'default' namespace is always present.
    #[tool]
    async fn list_namespaces(&self) -> Result<CallToolResult, McpError> {
        let output = tools::cluster::list_namespaces(&self.client).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    /// Activate a broker to bring it back into service after maintenance.
    ///
    /// Side effects: Changes broker state to active, making it eligible for topic assignment.
    /// The broker will start receiving new topics during rebalancing operations.
    ///
    /// Use this after completing maintenance on a broker that was previously unloaded.
    /// Provide a reason parameter for audit trail and operational documentation.
    #[tool]
    async fn activate_broker(
        &self,
        Parameters(params): Parameters<tools::cluster::ActivateBrokerParams>,
    ) -> Result<CallToolResult, McpError> {
        let output = tools::cluster::activate_broker(&self.client, params).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    /// Get configuration policies for a specific namespace.
    ///
    /// Returns policies including max producers/consumers per topic, publish/dispatch rate limits,
    /// and max message size. These policies control resource usage and prevent overload.
    ///
    /// Use list_namespaces first to discover available namespaces. Policies are optional and
    /// may be unset (no limits enforced).
    #[tool]
    async fn get_namespace_policies(
        &self,
        Parameters(params): Parameters<tools::cluster::NamespaceParams>,
    ) -> Result<CallToolResult, McpError> {
        let output = tools::cluster::get_namespace_policies(&self.client, params).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    /// Create a new namespace for logical isolation of topics.
    ///
    /// Side effects: Creates a new namespace in the cluster metadata. Namespaces provide
    /// isolation boundaries for topics and allow different policies per namespace.
    ///
    /// Use this to organize topics by environment (dev/staging/prod), team, or application.
    /// After creation, use get_namespace_policies to configure resource limits.
    #[tool]
    async fn create_namespace(
        &self,
        Parameters(params): Parameters<tools::cluster::NamespaceParams>,
    ) -> Result<CallToolResult, McpError> {
        let output = tools::cluster::create_namespace(&self.client, params).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    /// Delete an empty namespace.
    ///
    /// WARNING: Namespace must be empty (no topics) before deletion.
    /// Side effects: Permanently removes the namespace from cluster metadata.
    ///
    /// This operation will fail if any topics exist in the namespace. Use list_topics
    /// first to verify the namespace is empty, then delete any remaining topics before
    /// attempting namespace deletion.
    #[tool]
    async fn delete_namespace(
        &self,
        Parameters(params): Parameters<tools::cluster::NamespaceParams>,
    ) -> Result<CallToolResult, McpError> {
        let output = tools::cluster::delete_namespace(&self.client, params).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    // ===== TOPIC MANAGEMENT TOOLS =====

    /// List all topics in a specific namespace.
    ///
    /// Returns topic names, assigned broker IDs, and delivery strategy (reliable/non_reliable).
    /// Topics may show as "unassigned" if not yet assigned to a broker.
    ///
    /// Use list_namespaces first to discover available namespaces. Use this before describe_topic
    /// to find specific topic names.
    #[tool]
    async fn list_topics(
        &self,
        Parameters(params): Parameters<tools::topics::ListTopicsParams>,
    ) -> Result<CallToolResult, McpError> {
        let output = tools::topics::list_topics(&self.client, params).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    /// Get detailed information about a specific topic.
    ///
    /// Returns broker assignment, delivery strategy, schema configuration (subject, ID, version,
    /// type, compatibility mode), and list of active subscriptions.
    ///
    /// Use this to inspect topic configuration before making changes or troubleshooting issues.
    /// Schema information is only present if the topic has schema validation configured.
    #[tool]
    async fn describe_topic(
        &self,
        Parameters(params): Parameters<tools::topics::DescribeTopicParams>,
    ) -> Result<CallToolResult, McpError> {
        let output = tools::topics::describe_topic(&self.client, params).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    /// Create a new topic with optional partitioning and schema validation.
    ///
    /// Side effects: Creates a new topic in cluster metadata and assigns it to a broker.
    /// Supports both non-partitioned (single partition) and partitioned topics.
    ///
    /// Topic name must follow format: "/namespace/topic-name". Use list_namespaces to verify
    /// namespace exists. For schema-validated topics, register the schema first with register_schema.
    /// Delivery strategy: "reliable" (WAL + cloud storage) or "non_reliable" (in-memory only).
    #[tool]
    async fn create_topic(
        &self,
        Parameters(params): Parameters<tools::topics::CreateTopicParams>,
    ) -> Result<CallToolResult, McpError> {
        let output = tools::topics::create_topic(&self.client, params).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    /// Delete a topic and all its data permanently.
    ///
    /// WARNING: This operation is IRREVERSIBLE. All messages, subscriptions, and metadata
    /// will be permanently deleted. There is no recovery mechanism.
    ///
    /// Use describe_topic first to verify you're deleting the correct topic. Consider unsubscribing
    /// consumers first to avoid application errors. Use list_subscriptions to check for active consumers.
    #[tool]
    async fn delete_topic(
        &self,
        Parameters(params): Parameters<tools::topics::DeleteTopicParams>,
    ) -> Result<CallToolResult, McpError> {
        let output = tools::topics::delete_topic(&self.client, params).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    /// List all active subscriptions on a topic.
    ///
    /// Returns subscription names for all consumers currently subscribed to the topic.
    /// Subscriptions represent consumer groups with independent message cursors.
    ///
    /// Use this before deleting a topic to identify active consumers, or when troubleshooting
    /// consumer lag issues.
    #[tool]
    async fn list_subscriptions(
        &self,
        Parameters(params): Parameters<tools::topics::ListSubscriptionsParams>,
    ) -> Result<CallToolResult, McpError> {
        let output = tools::topics::list_subscriptions(&self.client, params).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    /// Delete a subscription from a topic.
    ///
    /// Side effects: Removes the subscription and its cursor position. Consumers using this
    /// subscription will be disconnected and unable to reconnect with the same subscription name.
    ///
    /// Use this to clean up abandoned subscriptions or reset consumer position (delete and recreate).
    /// Use list_subscriptions first to verify the subscription exists.
    #[tool]
    async fn unsubscribe(
        &self,
        Parameters(params): Parameters<tools::topics::UnsubscribeParams>,
    ) -> Result<CallToolResult, McpError> {
        let output = tools::topics::unsubscribe(&self.client, params).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    /// Unload a topic from its current broker for reassignment.
    ///
    /// Side effects: Topic is unloaded and will be reassigned to a different broker automatically
    /// by the load manager. Brief disruption to producers/consumers during reassignment.
    ///
    /// Use this for load balancing, broker maintenance, or troubleshooting broker-specific issues.
    /// The topic remains available but may experience a few seconds of unavailability during transfer.
    #[tool]
    async fn unload_topic(
        &self,
        Parameters(params): Parameters<tools::topics::UnloadTopicParams>,
    ) -> Result<CallToolResult, McpError> {
        let output = tools::topics::unload_topic(&self.client, params).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    /// Configure schema validation for a topic.
    ///
    /// Side effects: Associates a schema subject with the topic and sets validation policy.
    /// Producers must publish messages matching the schema when validation is enforced.
    ///
    /// The schema subject must already exist in the schema registry (use register_schema first).
    /// Validation policies: "none" (no validation), "warn" (log violations), "enforce" (reject invalid messages).
    /// Payload validation performs deep field-level checks beyond schema structure validation.
    #[tool]
    async fn configure_topic_schema(
        &self,
        Parameters(params): Parameters<tools::topics::ConfigureTopicSchemaParams>,
    ) -> Result<CallToolResult, McpError> {
        let output = tools::topics::configure_topic_schema(&self.client, params).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    /// Update the schema validation policy for a topic.
    ///
    /// Side effects: Changes how strictly the broker validates messages against the configured schema.
    /// Existing messages are not re-validated, only new incoming messages.
    ///
    /// Use this to gradually enforce schema validation (none → warn → enforce) without disrupting producers.
    /// The topic must already have a schema configured (use configure_topic_schema first).
    #[tool]
    async fn set_topic_validation_policy(
        &self,
        Parameters(params): Parameters<tools::topics::SetValidationPolicyParams>,
    ) -> Result<CallToolResult, McpError> {
        let output = tools::topics::set_validation_policy(&self.client, params).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    /// Get the current schema validation configuration for a topic.
    ///
    /// Returns schema subject, validation policy (none/warn/enforce), payload validation status,
    /// and cached schema ID if present.
    ///
    /// Use this to verify schema configuration before publishing messages or troubleshooting
    /// validation errors. Returns empty if no schema is configured.
    #[tool]
    async fn get_topic_schema_config(
        &self,
        Parameters(params): Parameters<tools::topics::GetSchemaConfigParams>,
    ) -> Result<CallToolResult, McpError> {
        let output = tools::topics::get_topic_schema_config(&self.client, params).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    // ===== SCHEMA REGISTRY TOOLS =====

    /// Register a new schema or update an existing schema subject.
    ///
    /// Side effects: Creates a new schema version if the definition differs from existing versions.
    /// Automatically performs compatibility checking against existing versions based on the subject's
    /// compatibility mode.
    ///
    /// Schema types: "json_schema" (JSON Schema), "avro" (Apache Avro), "protobuf" (Protocol Buffers).
    /// Returns schema ID, version number, and fingerprint. Use this before configure_topic_schema.
    #[tool]
    async fn register_schema(
        &self,
        Parameters(params): Parameters<tools::schemas::RegisterSchemaParams>,
    ) -> Result<CallToolResult, McpError> {
        let output = tools::schemas::register_schema(&self.client, params).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    /// Get the latest version of a schema by subject name.
    ///
    /// Returns schema ID, version, type, definition, compatibility mode, creation metadata,
    /// and fingerprint for the most recent version.
    ///
    /// Use this to inspect schema definitions before registering updates or configuring topics.
    /// The schema definition is returned as-is (JSON, Avro, or Protobuf format).
    #[tool]
    async fn get_schema(
        &self,
        Parameters(params): Parameters<tools::schemas::GetSchemaParams>,
    ) -> Result<CallToolResult, McpError> {
        let output = tools::schemas::get_schema(&self.client, params).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    /// List all versions of a schema subject.
    ///
    /// Returns version numbers, schema IDs, creation timestamps, creators, descriptions,
    /// and fingerprints for all versions of the subject.
    ///
    /// Use this to track schema evolution history, identify who made changes, or select
    /// a specific version for rollback or comparison.
    #[tool]
    async fn list_schema_versions(
        &self,
        Parameters(params): Parameters<tools::schemas::ListVersionsParams>,
    ) -> Result<CallToolResult, McpError> {
        let output = tools::schemas::list_schema_versions(&self.client, params).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    /// Check if a new schema is compatible with existing versions.
    ///
    /// Returns compatibility status and detailed error messages if incompatible.
    /// Checks against the subject's configured compatibility mode (backward/forward/full/none).
    ///
    /// Use this BEFORE register_schema to validate changes won't break existing consumers/producers.
    /// Prevents registration failures and identifies breaking changes early in development.
    #[tool]
    async fn check_schema_compatibility(
        &self,
        Parameters(params): Parameters<tools::schemas::CheckCompatibilityParams>,
    ) -> Result<CallToolResult, McpError> {
        let output = tools::schemas::check_compatibility(&self.client, params).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    /// Get the current compatibility mode for a schema subject.
    ///
    /// Returns the compatibility mode: NONE, BACKWARD, FORWARD, or FULL.
    /// This mode determines how strictly schema evolution is enforced.
    ///
    /// Use this to understand what schema changes are allowed before attempting updates.
    #[tool]
    async fn get_schema_compatibility_mode(
        &self,
        Parameters(params): Parameters<tools::schemas::CompatibilityModeParams>,
    ) -> Result<CallToolResult, McpError> {
        let output = tools::schemas::get_compatibility_mode(&self.client, params).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    /// Set the compatibility mode for a schema subject.
    ///
    /// Side effects: Changes schema evolution rules for all future versions of this subject.
    /// Does not re-validate existing versions against the new mode.
    ///
    /// Modes: "none" (no checks), "backward" (new reads old data), "forward" (old reads new data),
    /// "full" (both backward and forward). Use this to enforce stricter or looser evolution policies.
    #[tool]
    async fn set_schema_compatibility_mode(
        &self,
        Parameters(params): Parameters<tools::schemas::SetCompatibilityModeParams>,
    ) -> Result<CallToolResult, McpError> {
        let output = tools::schemas::set_compatibility_mode(&self.client, params).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    /// Delete a specific version of a schema.
    ///
    /// WARNING: This operation may break topics or consumers using this schema version.
    /// Side effects: Permanently removes the schema version from the registry.
    ///
    /// Use with extreme caution. Verify no topics are using this version with get_topic_schema_config.
    /// Typically used to clean up test/development schemas or unused versions.
    #[tool]
    async fn delete_schema_version(
        &self,
        Parameters(params): Parameters<tools::schemas::DeleteSchemaVersionParams>,
    ) -> Result<CallToolResult, McpError> {
        let output = tools::schemas::delete_schema_version(&self.client, params).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    // ===== DIAGNOSTIC TOOLS =====

    /// Analyze consumer lag data for a topic and subscription.
    ///
    /// Returns factual data about the topic, subscription status, and cluster balance metrics.
    /// Use this to gather diagnostic information that helps identify the root cause of lag.
    ///
    /// Checks: topic existence, subscription presence, broker assignment, cluster balance status.
    /// Does not provide recommendations - use troubleshooting prompts for guided workflows.
    #[tool]
    async fn analyze_consumer_lag(
        &self,
        Parameters(params): Parameters<tools::diagnostics::ConsumerLagParams>,
    ) -> Result<CallToolResult, McpError> {
        let output = tools::diagnostics::analyze_consumer_lag(&self.client, params).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    /// Perform a comprehensive cluster health check.
    ///
    /// Returns health status for brokers (active/inactive count), leader election status,
    /// cluster balance metrics (CoV), and namespace count. Lists detected issues if any.
    ///
    /// Use this as a first step for troubleshooting or regular health monitoring. Provides
    /// factual status without prescriptive recommendations.
    #[tool]
    async fn health_check(&self) -> Result<CallToolResult, McpError> {
        let output = tools::diagnostics::health_check(&self.client).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    /// Get optimization recommendations based on current cluster state.
    ///
    /// Analyzes cluster metrics and provides prioritized recommendations for:
    /// load imbalance, overloaded brokers, and high availability concerns.
    ///
    /// Recommendations include specific tool calls to address issues. Use this after health_check
    /// to get actionable next steps. Note: This tool provides suggestions based on heuristics;
    /// use troubleshooting prompts for comprehensive guided workflows.
    #[tool]
    async fn get_recommendations(&self) -> Result<CallToolResult, McpError> {
        let output = tools::diagnostics::get_recommendations(&self.client).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    // ===== LOG ACCESS TOOLS =====
    // NOTE: These tools require mcp-config.yml with deployment configuration.
    // Without config, tools will return error messages indicating unavailability.

    /// Get logs from a specific broker by its ID.
    ///
    /// **Config Required**: This tool requires mcp-config.yml with deployment configuration
    /// (Docker, Kubernetes, or local file paths). Returns an error message if config is missing.
    ///
    /// Returns the last N lines (default 500) of log output for the specified broker.
    /// Use list_configured_brokers first to discover available broker IDs.
    ///
    /// Supports: Docker containers (docker logs), Kubernetes pods (kubectl logs),
    /// and local log files (file reads).
    #[tool]
    async fn get_broker_logs(
        &self,
        Parameters(params): Parameters<tools::logs::BrokerLogsParams>,
    ) -> Result<CallToolResult, McpError> {
        let output = tools::logs::get_broker_logs(&self.config, params).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    /// List all brokers configured in the MCP config file.
    ///
    /// **Config Required**: Returns available broker IDs only if mcp-config.yml is provided.
    /// Returns an error message if no deployment configuration exists.
    ///
    /// Call this first before get_broker_logs to discover which broker IDs are available.
    /// Output format depends on deployment type (Docker container names, K8s service info,
    /// or local file mappings).
    #[tool]
    async fn list_configured_brokers(&self) -> Result<CallToolResult, McpError> {
        let output = tools::logs::list_configured_brokers(&self.config);
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    /// List all running Docker containers on the host.
    ///
    /// **No Config Required**: Directly queries Docker daemon via `docker ps` command.
    /// Returns container names, status, and exposed ports.
    ///
    /// Use this to discover container names for fetch_container_logs when you don't have
    /// a config file, or to see all containers beyond configured brokers.
    /// Requires Docker to be installed and running.
    #[tool]
    async fn list_docker_containers(&self) -> Result<CallToolResult, McpError> {
        let output = tools::logs::list_docker_containers();
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    /// List all Kubernetes pods in a specific namespace.
    ///
    /// **No Config Required**: Directly queries Kubernetes via `kubectl get pods` command.
    /// Returns pod names, status, and node placement.
    ///
    /// Use this to discover pod names for fetch_pod_logs. Requires kubectl to be installed
    /// and configured with access to the cluster.
    #[tool]
    async fn list_k8s_pods(
        &self,
        Parameters(params): Parameters<tools::logs::ListPodsParams>,
    ) -> Result<CallToolResult, McpError> {
        let output = tools::logs::list_k8s_pods(&params.namespace);
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    /// Fetch logs directly from any Docker container.
    ///
    /// **No Config Required**: Directly executes `docker logs` command.
    /// Works with any container name, not limited to configured brokers.
    ///
    /// Use list_docker_containers to discover container names. Returns the last N lines
    /// (default 500). Useful when config is unavailable or for ad-hoc log inspection.
    #[tool]
    async fn fetch_container_logs(
        &self,
        Parameters(params): Parameters<tools::logs::ContainerLogsParams>,
    ) -> Result<CallToolResult, McpError> {
        let output = tools::logs::fetch_docker_logs(&params.container_name, params.lines);
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    /// Fetch logs directly from a Kubernetes pod.
    ///
    /// **No Config Required**: Directly executes `kubectl logs` command.
    /// Works with any pod in any namespace.
    ///
    /// Use list_k8s_pods to discover pod names. Returns the last N lines (default 500).
    /// Requires kubectl to be installed and configured with cluster access.
    #[tool]
    async fn fetch_pod_logs(
        &self,
        Parameters(params): Parameters<tools::logs::PodLogsParams>,
    ) -> Result<CallToolResult, McpError> {
        let output = tools::logs::fetch_k8s_logs(&params.namespace, &params.pod_name, params.lines);
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    // ===== METRICS TOOLS =====
    // NOTE: These tools require Prometheus to be running and accessible.
    // Prometheus URL is configured in mcp-config.yml (default: http://localhost:9090).

    /// Get cluster-wide metrics summary from Prometheus.
    ///
    /// **Prometheus Required**: Queries Prometheus at the configured URL (default: http://localhost:9090).
    /// Set prometheus_url in mcp-config.yml or ensure Prometheus is accessible at the default location.
    ///
    /// Returns: broker count, total topics/producers/consumers/subscriptions, message rates
    /// (messages in/out, bytes in/out per second), and cluster balance coefficient of variation.
    ///
    /// Use this for high-level cluster health overview and capacity planning.
    #[tool]
    async fn get_cluster_metrics(&self) -> Result<CallToolResult, McpError> {
        let output = tools::metrics::get_cluster_metrics(&self.metrics).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    /// Get metrics for a specific broker from Prometheus.
    ///
    /// **Prometheus Required**: Queries Prometheus for broker-specific metrics.
    ///
    /// Returns: topics owned, total RPC count, active producers/consumers, and cumulative
    /// bytes in/out. Use list_brokers first to discover valid broker IDs.
    ///
    /// Use this to identify overloaded brokers or investigate broker-specific issues.
    #[tool]
    async fn get_broker_metrics(
        &self,
        Parameters(params): Parameters<tools::metrics::BrokerMetricsParams>,
    ) -> Result<CallToolResult, McpError> {
        let output = tools::metrics::get_broker_metrics(&self.metrics, params).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    /// Get comprehensive metrics for a specific topic from Prometheus.
    ///
    /// **Prometheus Required**: Queries Prometheus for topic-specific metrics.
    ///
    /// Returns: message/byte counters (in/out totals), active producers/consumers/subscriptions,
    /// publish and dispatch rates (1m average), total subscription lag, and latency percentiles
    /// (p50/p95/p99 in milliseconds).
    ///
    /// Use this for performance analysis and troubleshooting consumer lag or latency issues.
    #[tool]
    async fn get_topic_metrics(
        &self,
        Parameters(params): Parameters<tools::metrics::TopicMetricsParams>,
    ) -> Result<CallToolResult, McpError> {
        let output = tools::metrics::get_topic_metrics(&self.metrics, params).await;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    /// Execute a raw PromQL query against Prometheus.
    ///
    /// **Prometheus Required**: Sends a custom PromQL query to Prometheus.
    ///
    /// Use this for advanced metric investigations not covered by other tools.
    /// Supports full PromQL syntax including aggregations, functions, and time ranges.
    ///
    /// Example queries:
    /// - `sum(rate(danube_topic_messages_in_total[5m]))` - Total cluster message rate
    /// - `histogram_quantile(0.99, rate(danube_producer_send_latency_bucket[5m]))` - p99 latency
    /// - `danube_subscription_lag{topic="/default/events"}` - Lag for specific topic
    #[tool]
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

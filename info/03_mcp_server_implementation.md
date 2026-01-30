# Implementation Plan: MCP Server for Danube

**Goal**: Implement Model Context Protocol (MCP) server that allows AI assistants (Claude, Cursor, Windsurf) to interact with and manage Danube clusters through natural language.

**Timeline**: 2-3 weeks  
**Prerequisites**: Phase 1 (Consolidation) and Phase 2 (Server Enhancement) completed  
**Priority**: High - Strategic differentiator

---

## Overview

Implement an MCP server that provides:
1. **Tools** - Actions AI can perform (list brokers, create topics, diagnose issues)
2. **Resources** - Context for AI (cluster config, broker logs, schemas)
3. **Prompts** - Guided workflows (troubleshooting, optimization)

The MCP server will be a mode of `danube-admin serve` and communicate with the cluster via the existing HTTP gateway endpoints.

---

## Architecture Overview

```
┌─────────────────────┐
│   AI Clients        │
│ (Claude, Cursor,    │
│  Windsurf)          │
└──────────┬──────────┘
           │ stdio/SSE
┌──────────▼──────────┐
│   MCP Server        │  danube-admin serve --mode mcp
│   (rmcp crate)      │
└──────────┬──────────┘
           │ Internal API calls
┌──────────▼──────────┐
│   Core gRPC Client  │  Shared with CLI/HTTP modes
└──────────┬──────────┘
           │ gRPC
┌──────────▼──────────┐
│  Danube Brokers     │
└─────────────────────┘
```

**Decision**: MCP server uses core gRPC client directly (not HTTP gateway) for lower latency and direct access to all proto methods.

---

## Phase 1: Project Setup & Dependencies (Days 1-2)

### 1.1 Add MCP Dependencies

```toml
# danube-admin/Cargo.toml

[dependencies]
# Existing dependencies...

# MCP server (only enabled when mcp feature is active)
rmcp = { version = "0.3", features = ["server", "macros", "transport-io"], optional = true }
uuid = { version = "1.0", optional = true }

[features]
default = ["cli", "server"]
cli = []
server = []
mcp = ["rmcp", "uuid"]  # MCP support is optional feature
```

### 1.2 MCP Module Structure

```
danube-admin/src/mcp/
├── mod.rs              # MCP server setup & entry point
├── tools/
│   ├── mod.rs
│   ├── cluster.rs      # Cluster management tools
│   ├── topics.rs       # Topic operation tools
│   ├── diagnostics.rs  # Diagnostic tools
│   └── schemas.rs      # Schema registry tools
├── resources/
│   ├── mod.rs
│   ├── cluster.rs      # Cluster config resources
│   ├── logs.rs         # Log streaming resources
│   └── metrics.rs      # Metrics resources
├── prompts/
│   ├── mod.rs
│   └── troubleshooting.rs  # Guided workflows
└── types.rs            # MCP-specific types
```

---

## Phase 2: Core MCP Server Implementation (Days 3-5)

### 2.1 MCP Server Entry Point (`src/mcp/mod.rs`)

```rust
#[cfg(feature = "mcp")]
pub mod tools;
#[cfg(feature = "mcp")]
pub mod resources;
#[cfg(feature = "mcp")]
pub mod prompts;
#[cfg(feature = "mcp")]
pub mod types;

#[cfg(feature = "mcp")]
use rmcp::prelude::*;
#[cfg(feature = "mcp")]
use std::sync::Arc;
#[cfg(feature = "mcp")]
use crate::core::grpc_client::AdminGrpcClient;

#[cfg(feature = "mcp")]
pub struct DanubeMcpServer {
    client: Arc<AdminGrpcClient>,
    tool_router: ToolRouter<Self>,
}

#[cfg(feature = "mcp")]
#[tool_router]
impl DanubeMcpServer {
    pub fn new(client: Arc<AdminGrpcClient>) -> Self {
        Self {
            client,
            tool_router: Self::tool_router(),
        }
    }

    // ===== CLUSTER MANAGEMENT TOOLS =====
    
    #[tool(description = "List all brokers in the Danube cluster with their status, role, and addresses")]
    async fn list_brokers(&self) -> String {
        tools::cluster::list_brokers(&self.client).await
    }

    #[tool(description = "Get the current leader broker information")]
    async fn get_leader(&self) -> String {
        tools::cluster::get_leader(&self.client).await
    }

    #[tool(description = "Get cluster load balance metrics including coefficient of variation and per-broker loads")]
    async fn get_cluster_balance(&self) -> String {
        tools::cluster::get_cluster_balance(&self.client).await
    }

    #[tool(description = "Trigger cluster rebalancing to distribute topics evenly across brokers. Use dry_run=true to preview changes.")]
    async fn trigger_rebalance(
        &self,
        Parameters(params): Parameters<tools::cluster::RebalanceParams>,
    ) -> String {
        tools::cluster::trigger_rebalance(&self.client, params).await
    }

    #[tool(description = "Unload all topics from a specific broker (for maintenance or removal)")]
    async fn unload_broker(
        &self,
        Parameters(params): Parameters<tools::cluster::UnloadBrokerParams>,
    ) -> String {
        tools::cluster::unload_broker(&self.client, params).await
    }

    #[tool(description = "List all namespaces in the cluster")]
    async fn list_namespaces(&self) -> String {
        tools::cluster::list_namespaces(&self.client).await
    }

    // ===== TOPIC MANAGEMENT TOOLS =====

    #[tool(description = "List topics in a specific namespace with details including broker assignment and delivery strategy")]
    async fn list_topics(
        &self,
        Parameters(params): Parameters<tools::topics::ListTopicsParams>,
    ) -> String {
        tools::topics::list_topics(&self.client, params).await
    }

    #[tool(description = "Get detailed information about a specific topic including subscriptions, schema, and metrics")]
    async fn describe_topic(
        &self,
        Parameters(params): Parameters<tools::topics::DescribeTopicParams>,
    ) -> String {
        tools::topics::describe_topic(&self.client, params).await
    }

    #[tool(description = "Create a new topic with specified configuration (partitions, schema, dispatch strategy)")]
    async fn create_topic(
        &self,
        Parameters(params): Parameters<tools::topics::CreateTopicParams>,
    ) -> String {
        tools::topics::create_topic(&self.client, params).await
    }

    #[tool(description = "Delete a topic and all its data (WARNING: irreversible operation)")]
    async fn delete_topic(
        &self,
        Parameters(params): Parameters<tools::topics::DeleteTopicParams>,
    ) -> String {
        tools::topics::delete_topic(&self.client, params).await
    }

    #[tool(description = "List all subscriptions on a topic")]
    async fn list_subscriptions(
        &self,
        Parameters(params): Parameters<tools::topics::ListSubscriptionsParams>,
    ) -> String {
        tools::topics::list_subscriptions(&self.client, params).await
    }

    // ===== SCHEMA REGISTRY TOOLS =====

    #[tool(description = "Register a new schema in the schema registry with compatibility checking")]
    async fn register_schema(
        &self,
        Parameters(params): Parameters<tools::schemas::RegisterSchemaParams>,
    ) -> String {
        tools::schemas::register_schema(&self.client, params).await
    }

    #[tool(description = "Get the latest version of a schema by subject name")]
    async fn get_schema(
        &self,
        Parameters(params): Parameters<tools::schemas::GetSchemaParams>,
    ) -> String {
        tools::schemas::get_schema(&self.client, params).await
    }

    #[tool(description = "List all versions of a schema subject")]
    async fn list_schema_versions(
        &self,
        Parameters(params): Parameters<tools::schemas::ListVersionsParams>,
    ) -> String {
        tools::schemas::list_schema_versions(&self.client, params).await
    }

    #[tool(description = "Check if a new schema is compatible with existing versions")]
    async fn check_schema_compatibility(
        &self,
        Parameters(params): Parameters<tools::schemas::CheckCompatibilityParams>,
    ) -> String {
        tools::schemas::check_compatibility(&self.client, params).await
    }

    // ===== DIAGNOSTIC TOOLS =====

    #[tool(description = "Diagnose why a consumer might be lagging on a topic")]
    async fn diagnose_consumer_lag(
        &self,
        Parameters(params): Parameters<tools::diagnostics::ConsumerLagParams>,
    ) -> String {
        tools::diagnostics::diagnose_consumer_lag(&self.client, params).await
    }

    #[tool(description = "Analyze cluster health and identify potential issues")]
    async fn health_check(&self) -> String {
        tools::diagnostics::health_check(&self.client).await
    }

    #[tool(description = "Get recommended actions to improve cluster performance")]
    async fn get_recommendations(&self) -> String {
        tools::diagnostics::get_recommendations(&self.client).await
    }
}

#[cfg(feature = "mcp")]
#[tool_handler]
impl ServerHandler for DanubeMcpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            name: "danube-mcp-server".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            capabilities: ServerCapabilities::builder()
                .enable_tools()
                .enable_resources()
                .enable_prompts()
                .build(),
            ..Default::default()
        }
    }
}

#[cfg(feature = "mcp")]
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
```

---

## Phase 3: Tool Implementations (Days 6-10)

### 3.1 Cluster Tools (`src/mcp/tools/cluster.rs`)

```rust
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use crate::core::grpc_client::AdminGrpcClient;
use danube_core::admin_proto;

pub async fn list_brokers(client: &Arc<AdminGrpcClient>) -> String {
    match client.list_brokers().await {
        Ok(response) => {
            if response.brokers.is_empty() {
                return "No brokers found in the cluster.".to_string();
            }

            let mut output = format!("Found {} broker(s):\n\n", response.brokers.len());
            
            for broker in &response.brokers {
                output.push_str(&format!(
                    "Broker ID: {}\n\
                     Status: {}\n\
                     Role: {}\n\
                     Address: {}\n\
                     Admin: {}\n\
                     Metrics: {}\n\n",
                    broker.broker_id,
                    broker.broker_status,
                    broker.broker_role,
                    broker.broker_addr,
                    broker.admin_addr,
                    broker.metrics_addr,
                ));
            }
            
            output
        }
        Err(e) => format!("Error listing brokers: {}", e),
    }
}

pub async fn get_leader(client: &Arc<AdminGrpcClient>) -> String {
    match client.get_leader().await {
        Ok(response) => {
            format!("Current leader broker: {}", response.leader)
        }
        Err(e) => format!("Error getting leader: {}", e),
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RebalanceParams {
    /// Perform a dry run without actually moving topics
    #[serde(default)]
    pub dry_run: bool,
    /// Maximum number of topic moves to execute
    pub max_moves: Option<u32>,
}

pub async fn trigger_rebalance(
    client: &Arc<AdminGrpcClient>,
    params: RebalanceParams,
) -> String {
    let req = admin_proto::RebalanceRequest {
        dry_run: params.dry_run,
        max_moves: params.max_moves,
    };

    match client.trigger_rebalance(req).await {
        Ok(response) => {
            if !response.success {
                return format!("Rebalancing failed: {}", response.error_message);
            }

            let mut output = String::new();
            
            if params.dry_run {
                output.push_str("DRY RUN - No changes were made\n\n");
            } else {
                output.push_str("Rebalancing completed successfully!\n\n");
            }

            output.push_str(&format!(
                "Moves executed: {}\n\
                 Proposed moves: {}\n\n",
                response.moves_executed,
                response.proposed_moves.len()
            ));

            if !response.proposed_moves.is_empty() {
                output.push_str("Topic movements:\n");
                for (i, mv) in response.proposed_moves.iter().enumerate() {
                    output.push_str(&format!(
                        "  {}. {} (broker {} -> broker {})\n     Reason: {}\n     Est. load: {:.2}\n",
                        i + 1,
                        mv.topic_name,
                        mv.from_broker,
                        mv.to_broker,
                        mv.reason,
                        mv.estimated_load
                    ));
                }
            }

            output
        }
        Err(e) => format!("Error triggering rebalance: {}", e),
    }
}

pub async fn get_cluster_balance(client: &Arc<AdminGrpcClient>) -> String {
    let req = admin_proto::ClusterBalanceRequest {};

    match client.get_cluster_balance(req).await {
        Ok(response) => {
            let is_balanced = response.coefficient_of_variation < 0.2;
            
            let mut output = String::new();
            output.push_str("Cluster Balance Metrics:\n\n");
            output.push_str(&format!(
                "Status: {}\n\
                 Coefficient of Variation: {:.4}\n\
                 Mean Load: {:.2}\n\
                 Max Load: {:.2}\n\
                 Min Load: {:.2}\n\
                 Std Deviation: {:.2}\n\
                 Active Brokers: {}\n\n",
                if is_balanced { "✓ Balanced" } else { "⚠ Imbalanced" },
                response.coefficient_of_variation,
                response.mean_load,
                response.max_load,
                response.min_load,
                response.std_deviation,
                response.broker_count
            ));

            if !response.brokers.is_empty() {
                output.push_str("Broker Load Distribution:\n");
                for broker in &response.brokers {
                    let status = if broker.is_overloaded {
                        "⚠ OVERLOADED"
                    } else if broker.is_underloaded {
                        "↓ UNDERLOADED"
                    } else {
                        "✓ OK"
                    };

                    output.push_str(&format!(
                        "  Broker {}: {:.2} load ({} topics) - {}\n",
                        broker.broker_id,
                        broker.load,
                        broker.topic_count,
                        status
                    ));
                }
            }

            if !is_balanced {
                output.push_str("\nRecommendation: Consider running 'trigger_rebalance' to improve balance.\n");
            }

            output
        }
        Err(e) => format!("Error getting cluster balance: {}", e),
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct UnloadBrokerParams {
    /// Broker ID to unload
    pub broker_id: String,
    /// Maximum parallel unloads
    #[serde(default = "default_max_parallel")]
    pub max_parallel: u32,
    /// Perform dry run
    #[serde(default)]
    pub dry_run: bool,
}

fn default_max_parallel() -> u32 { 1 }

pub async fn unload_broker(
    client: &Arc<AdminGrpcClient>,
    params: UnloadBrokerParams,
) -> String {
    let req = admin_proto::UnloadBrokerRequest {
        broker_id: params.broker_id.clone(),
        max_parallel: params.max_parallel,
        namespaces_include: vec![],
        namespaces_exclude: vec![],
        dry_run: params.dry_run,
        timeout_seconds: 60,
    };

    match client.unload_broker(req).await {
        Ok(response) => {
            let mut output = String::new();
            
            if params.dry_run {
                output.push_str("DRY RUN - No topics were actually unloaded\n\n");
            }

            output.push_str(&format!(
                "Unload Results for Broker {}:\n\
                 Total topics: {}\n\
                 Succeeded: {}\n\
                 Failed: {}\n\
                 Pending: {}\n",
                params.broker_id,
                response.total,
                response.succeeded,
                response.failed,
                response.pending
            ));

            if !response.failed_topics.is_empty() {
                output.push_str("\nFailed topics:\n");
                for topic in &response.failed_topics {
                    output.push_str(&format!("  - {}\n", topic));
                }
            }

            output
        }
        Err(e) => format!("Error unloading broker: {}", e),
    }
}

pub async fn list_namespaces(client: &Arc<AdminGrpcClient>) -> String {
    match client.list_namespaces().await {
        Ok(response) => {
            if response.namespaces.is_empty() {
                return "No namespaces found.".to_string();
            }

            let mut output = format!("Found {} namespace(s):\n\n", response.namespaces.len());
            for (i, ns) in response.namespaces.iter().enumerate() {
                output.push_str(&format!("  {}. {}\n", i + 1, ns));
            }
            
            output
        }
        Err(e) => format!("Error listing namespaces: {}", e),
    }
}
```

### 3.2 Topic Tools (`src/mcp/tools/topics.rs`)

```rust
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use crate::core::grpc_client::AdminGrpcClient;
use danube_core::admin_proto;

#[derive(Debug, Deserialize, Serialize)]
pub struct ListTopicsParams {
    /// Namespace to list topics from (e.g., "default")
    pub namespace: String,
}

pub async fn list_topics(
    client: &Arc<AdminGrpcClient>,
    params: ListTopicsParams,
) -> String {
    let req = admin_proto::NamespaceRequest {
        name: params.namespace.clone(),
    };

    match client.list_namespace_topics(req).await {
        Ok(response) => {
            if response.topics.is_empty() {
                return format!("No topics found in namespace '{}'.", params.namespace);
            }

            let mut output = format!(
                "Found {} topic(s) in namespace '{}':\n\n",
                response.topics.len(),
                params.namespace
            );

            for topic_info in &response.topics {
                output.push_str(&format!(
                    "Topic: {}\n\
                     Broker: {}\n\
                     Delivery: {}\n\n",
                    topic_info.name,
                    if topic_info.broker_id.is_empty() {
                        "unassigned"
                    } else {
                        &topic_info.broker_id
                    },
                    topic_info.delivery
                ));
            }

            output
        }
        Err(e) => format!("Error listing topics: {}", e),
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct DescribeTopicParams {
    /// Topic name (e.g., "/default/my-topic")
    pub topic: String,
}

pub async fn describe_topic(
    client: &Arc<AdminGrpcClient>,
    params: DescribeTopicParams,
) -> String {
    let req = admin_proto::DescribeTopicRequest {
        name: params.topic.clone(),
    };

    match client.describe_topic(req).await {
        Ok(response) => {
            let mut output = format!("Topic: {}\n\n", response.name);
            output.push_str(&format!("Broker ID: {}\n", response.broker_id));
            output.push_str(&format!("Delivery: {}\n", response.delivery));

            if let Some(schema_subject) = response.schema_subject {
                output.push_str(&format!("\nSchema Information:\n"));
                output.push_str(&format!("  Subject: {}\n", schema_subject));
                if let Some(schema_id) = response.schema_id {
                    output.push_str(&format!("  Schema ID: {}\n", schema_id));
                }
                if let Some(version) = response.schema_version {
                    output.push_str(&format!("  Version: {}\n", version));
                }
                if let Some(schema_type) = response.schema_type {
                    output.push_str(&format!("  Type: {}\n", schema_type));
                }
                if let Some(compat) = response.compatibility_mode {
                    output.push_str(&format!("  Compatibility: {}\n", compat));
                }
            }

            if !response.subscriptions.is_empty() {
                output.push_str(&format!("\nSubscriptions ({}):\n", response.subscriptions.len()));
                for (i, sub) in response.subscriptions.iter().enumerate() {
                    output.push_str(&format!("  {}. {}\n", i + 1, sub));
                }
            } else {
                output.push_str("\nNo subscriptions found.\n");
            }

            output
        }
        Err(e) => format!("Error describing topic: {}", e),
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CreateTopicParams {
    /// Topic name (e.g., "/default/my-topic")
    pub name: String,
    /// Number of partitions (0 for non-partitioned)
    #[serde(default)]
    pub partitions: u32,
    /// Dispatch strategy: "reliable" or "non_reliable"
    #[serde(default = "default_dispatch_strategy")]
    pub dispatch_strategy: String,
    /// Optional schema subject
    pub schema_subject: Option<String>,
}

fn default_dispatch_strategy() -> String {
    "reliable".to_string()
}

pub async fn create_topic(
    client: &Arc<AdminGrpcClient>,
    params: CreateTopicParams,
) -> String {
    let dispatch = if params.dispatch_strategy.to_lowercase() == "reliable" {
        1 // Reliable
    } else {
        0 // NonReliable
    };

    if params.partitions > 0 {
        // Create partitioned topic
        let req = admin_proto::PartitionedTopicRequest {
            base_name: params.name.clone(),
            partitions: params.partitions,
            schema_subject: params.schema_subject,
            dispatch_strategy: dispatch,
        };

        match client.create_partitioned_topic(req).await {
            Ok(_) => {
                format!(
                    "Successfully created partitioned topic '{}' with {} partitions ({})",
                    params.name,
                    params.partitions,
                    params.dispatch_strategy
                )
            }
            Err(e) => format!("Error creating partitioned topic: {}", e),
        }
    } else {
        // Create non-partitioned topic
        let req = admin_proto::NewTopicRequest {
            name: params.name.clone(),
            schema_subject: params.schema_subject,
            dispatch_strategy: dispatch,
        };

        match client.create_topic(req).await {
            Ok(_) => {
                format!(
                    "Successfully created topic '{}' ({})",
                    params.name,
                    params.dispatch_strategy
                )
            }
            Err(e) => format!("Error creating topic: {}", e),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct DeleteTopicParams {
    /// Topic name to delete (e.g., "/default/my-topic")
    pub topic: String,
}

pub async fn delete_topic(
    client: &Arc<AdminGrpcClient>,
    params: DeleteTopicParams,
) -> String {
    let req = admin_proto::TopicRequest {
        name: params.topic.clone(),
    };

    match client.delete_topic(req).await {
        Ok(_) => {
            format!("Successfully deleted topic '{}'", params.topic)
        }
        Err(e) => format!("Error deleting topic: {}", e),
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ListSubscriptionsParams {
    /// Topic name (e.g., "/default/my-topic")
    pub topic: String,
}

pub async fn list_subscriptions(
    client: &Arc<AdminGrpcClient>,
    params: ListSubscriptionsParams,
) -> String {
    let req = admin_proto::TopicRequest {
        name: params.topic.clone(),
    };

    match client.list_subscriptions(req).await {
        Ok(response) => {
            if response.subscriptions.is_empty() {
                return format!("No subscriptions found on topic '{}'", params.topic);
            }

            let mut output = format!(
                "Found {} subscription(s) on topic '{}':\n\n",
                response.subscriptions.len(),
                params.topic
            );

            for (i, sub) in response.subscriptions.iter().enumerate() {
                output.push_str(&format!("  {}. {}\n", i + 1, sub));
            }

            output
        }
        Err(e) => format!("Error listing subscriptions: {}", e),
    }
}
```

### 3.3 Schema Tools (`src/mcp/tools/schemas.rs`)

```rust
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use crate::core::grpc_client::AdminGrpcClient;
use danube_core::proto::danube_schema;

#[derive(Debug, Deserialize, Serialize)]
pub struct RegisterSchemaParams {
    /// Schema subject name (e.g., "user-events")
    pub subject: String,
    /// Schema type: "json_schema", "avro", "protobuf", "string", "bytes"
    pub schema_type: String,
    /// Schema definition (JSON string for json_schema)
    pub schema_definition: String,
    /// Optional description
    pub description: Option<String>,
}

pub async fn register_schema(
    client: &Arc<AdminGrpcClient>,
    params: RegisterSchemaParams,
) -> String {
    let req = danube_schema::RegisterSchemaRequest {
        subject: params.subject.clone(),
        schema_type: params.schema_type.clone(),
        schema_definition: params.schema_definition.into_bytes(),
        description: params.description.unwrap_or_default(),
        created_by: "mcp-server".to_string(),
        tags: vec![],
    };

    match client.register_schema(req).await {
        Ok(response) => {
            let status = if response.is_new_version {
                "New version registered"
            } else {
                "Schema already exists"
            };

            format!(
                "Schema '{}' - {}\n\
                 Schema ID: {}\n\
                 Version: {}\n\
                 Fingerprint: {}",
                params.subject,
                status,
                response.schema_id,
                response.version,
                response.fingerprint
            )
        }
        Err(e) => format!("Error registering schema: {}", e),
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct GetSchemaParams {
    /// Schema subject name or ID
    pub subject: String,
}

pub async fn get_schema(
    client: &Arc<AdminGrpcClient>,
    params: GetSchemaParams,
) -> String {
    let req = danube_schema::GetLatestSchemaRequest {
        subject: params.subject.clone(),
    };

    match client.get_latest_schema(req).await {
        Ok(response) => {
            let schema_def = String::from_utf8_lossy(&response.schema_definition);
            
            format!(
                "Schema Subject: {}\n\
                 Schema ID: {}\n\
                 Version: {}\n\
                 Type: {}\n\
                 Compatibility: {}\n\
                 Created: {} (by {})\n\
                 Fingerprint: {}\n\n\
                 Definition:\n{}",
                response.subject,
                response.schema_id,
                response.version,
                response.schema_type,
                response.compatibility_mode,
                chrono::DateTime::from_timestamp(response.created_at as i64, 0)
                    .map(|dt| dt.to_rfc3339())
                    .unwrap_or_else(|| "unknown".to_string()),
                response.created_by,
                response.fingerprint,
                schema_def
            )
        }
        Err(e) => format!("Error getting schema: {}", e),
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ListVersionsParams {
    /// Schema subject name
    pub subject: String,
}

pub async fn list_schema_versions(
    client: &Arc<AdminGrpcClient>,
    params: ListVersionsParams,
) -> String {
    let req = danube_schema::ListVersionsRequest {
        subject: params.subject.clone(),
    };

    match client.list_versions(req).await {
        Ok(response) => {
            if response.versions.is_empty() {
                return format!("No versions found for subject '{}'", params.subject);
            }

            let mut output = format!(
                "Found {} version(s) for schema '{}':\n\n",
                response.versions.len(),
                params.subject
            );

            for version_info in &response.versions {
                output.push_str(&format!(
                    "Version {}: Schema ID {}\n\
                     Created: {} (by {})\n\
                     Description: {}\n\
                     Fingerprint: {}\n\n",
                    version_info.version,
                    version_info.schema_id,
                    chrono::DateTime::from_timestamp(version_info.created_at as i64, 0)
                        .map(|dt| dt.to_rfc3339())
                        .unwrap_or_else(|| "unknown".to_string()),
                    version_info.created_by,
                    version_info.description,
                    version_info.fingerprint
                ));
            }

            output
        }
        Err(e) => format!("Error listing versions: {}", e),
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CheckCompatibilityParams {
    /// Schema subject name
    pub subject: String,
    /// Schema type
    pub schema_type: String,
    /// New schema definition to check
    pub schema_definition: String,
}

pub async fn check_compatibility(
    client: &Arc<AdminGrpcClient>,
    params: CheckCompatibilityParams,
) -> String {
    let req = danube_schema::CheckCompatibilityRequest {
        subject: params.subject.clone(),
        new_schema_definition: params.schema_definition.into_bytes(),
        schema_type: params.schema_type,
        compatibility_mode: None,
    };

    match client.check_compatibility(req).await {
        Ok(response) => {
            if response.is_compatible {
                format!("✓ Schema is COMPATIBLE with subject '{}'", params.subject)
            } else {
                let mut output = format!("✗ Schema is INCOMPATIBLE with subject '{}':\n\n", params.subject);
                for (i, error) in response.errors.iter().enumerate() {
                    output.push_str(&format!("  {}. {}\n", i + 1, error));
                }
                output
            }
        }
        Err(e) => format!("Error checking compatibility: {}", e),
    }
}
```

### 3.4 Diagnostic Tools (`src/mcp/tools/diagnostics.rs`)

```rust
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use crate::core::grpc_client::AdminGrpcClient;
use danube_core::admin_proto;

#[derive(Debug, Deserialize, Serialize)]
pub struct ConsumerLagParams {
    /// Topic to diagnose
    pub topic: String,
    /// Subscription name
    pub subscription: String,
}

pub async fn diagnose_consumer_lag(
    client: &Arc<AdminGrpcClient>,
    params: ConsumerLagParams,
) -> String {
    // This would require additional metrics/stats endpoints
    // For now, provide a structured diagnostic approach
    
    let mut output = format!(
        "Diagnosing consumer lag for:\n\
         Topic: {}\n\
         Subscription: {}\n\n",
        params.topic,
        params.subscription
    );

    // 1. Check topic details
    let topic_req = admin_proto::DescribeTopicRequest {
        name: params.topic.clone(),
    };

    match client.describe_topic(topic_req).await {
        Ok(topic_info) => {
            output.push_str(&format!(
                "Topic Info:\n\
                 - Broker: {}\n\
                 - Delivery: {}\n\
                 - Subscriptions: {}\n\n",
                topic_info.broker_id,
                topic_info.delivery,
                topic_info.subscriptions.len()
            ));

            // Check if subscription exists
            if !topic_info.subscriptions.contains(&params.subscription) {
                output.push_str("⚠ WARNING: Subscription not found on this topic!\n");
                output.push_str("This could explain why no messages are being consumed.\n\n");
            }
        }
        Err(e) => {
            output.push_str(&format!("✗ Error getting topic info: {}\n\n", e));
        }
    }

    // 2. Check cluster balance
    let balance_req = admin_proto::ClusterBalanceRequest {};
    match client.get_cluster_balance(balance_req).await {
        Ok(balance) => {
            // Find the broker hosting this topic
            output.push_str("Cluster Balance:\n");
            output.push_str(&format!(
                "- Coefficient of Variation: {:.4}\n",
                balance.coefficient_of_variation
            ));

            if balance.coefficient_of_variation > 0.5 {
                output.push_str("⚠ Cluster is significantly imbalanced - this may affect performance\n");
            }
            output.push_str("\n");
        }
        Err(_) => {}
    }

    output.push_str("Possible Causes of Consumer Lag:\n");
    output.push_str("1. Consumer processing is too slow (check consumer application logs)\n");
    output.push_str("2. Not enough consumers (check if subscription is 'shared' type)\n");
    output.push_str("3. Broker overload (check cluster balance metrics)\n");
    output.push_str("4. Network issues between consumer and broker\n");
    output.push_str("5. Consumer group rebalancing (temporary lag spikes)\n\n");

    output.push_str("Recommended Actions:\n");
    output.push_str("1. Run 'get_cluster_balance' to check broker load\n");
    output.push_str("2. Check if topic has multiple partitions for parallelism\n");
    output.push_str("3. Scale consumer group if using 'shared' subscription\n");
    output.push_str("4. Monitor broker metrics for throughput limits\n");

    output
}

pub async fn health_check(client: &Arc<AdminGrpcClient>) -> String {
    let mut output = String::from("Danube Cluster Health Check:\n\n");
    let mut issues = Vec::new();

    // 1. Check brokers
    match client.list_brokers().await {
        Ok(brokers_resp) => {
            output.push_str(&format!("✓ Brokers: {} active\n", brokers_resp.brokers.len()));
            
            if brokers_resp.brokers.is_empty() {
                issues.push("No brokers found - cluster is DOWN");
            }

            for broker in &brokers_resp.brokers {
                if broker.broker_status != "active" {
                    issues.push(&format!("Broker {} is not active: {}", 
                        broker.broker_id, broker.broker_status));
                }
            }
        }
        Err(e) => {
            output.push_str(&format!("✗ Brokers: UNREACHABLE\n"));
            issues.push(&format!("Cannot connect to cluster: {}", e));
        }
    }

    // 2. Check leader
    match client.get_leader().await {
        Ok(leader) => {
            output.push_str(&format!("✓ Leader: {}\n", leader.leader));
        }
        Err(_) => {
            output.push_str("✗ Leader: UNKNOWN\n");
            issues.push("No leader elected - cluster may be in election");
        }
    }

    // 3. Check cluster balance
    match client.get_cluster_balance(admin_proto::ClusterBalanceRequest {}).await {
        Ok(balance) => {
            let status = if balance.coefficient_of_variation < 0.2 {
                "✓ Balanced"
            } else if balance.coefficient_of_variation < 0.5 {
                "⚠ Slightly imbalanced"
            } else {
                "✗ Significantly imbalanced"
            };

            output.push_str(&format!(
                "{} (CoV: {:.4})\n",
                status,
                balance.coefficient_of_variation
            ));

            if balance.coefficient_of_variation > 0.5 {
                issues.push("Cluster is significantly imbalanced - consider rebalancing");
            }
        }
        Err(_) => {
            output.push_str("⚠ Balance: Unable to check\n");
        }
    }

    // 4. Check namespaces
    match client.list_namespaces().await {
        Ok(ns_resp) => {
            output.push_str(&format!("✓ Namespaces: {}\n", ns_resp.namespaces.len()));
        }
        Err(_) => {
            output.push_str("⚠ Namespaces: Unable to list\n");
        }
    }

    output.push_str("\n");

    if issues.is_empty() {
        output.push_str("Overall Status: ✓ HEALTHY\n");
    } else {
        output.push_str("Overall Status: ⚠ ISSUES DETECTED\n\n");
        output.push_str("Issues:\n");
        for (i, issue) in issues.iter().enumerate() {
            output.push_str(&format!("  {}. {}\n", i + 1, issue));
        }
    }

    output
}

pub async fn get_recommendations(client: &Arc<AdminGrpcClient>) -> String {
    let mut recommendations = Vec::new();

    // Check cluster balance
    if let Ok(balance) = client.get_cluster_balance(admin_proto::ClusterBalanceRequest {}).await {
        if balance.coefficient_of_variation > 0.3 {
            recommendations.push((
                "High Priority",
                "Cluster Load Imbalance",
                format!(
                    "Coefficient of variation is {:.2}. Run 'trigger_rebalance' with dry_run=true to preview rebalancing.",
                    balance.coefficient_of_variation
                )
            ));
        }

        // Check for overloaded brokers
        for broker in &balance.brokers {
            if broker.is_overloaded {
                recommendations.push((
                    "High Priority",
                    &format!("Broker {} Overloaded", broker.broker_id),
                    format!(
                        "Broker has load of {:.2} with {} topics. Consider unloading some topics or adding more brokers.",
                        broker.load,
                        broker.topic_count
                    )
                ));
            }
        }
    }

    // Check broker count
    if let Ok(brokers) = client.list_brokers().await {
        if brokers.brokers.len() < 3 {
            recommendations.push((
                "Medium Priority",
                "Low Broker Count",
                format!(
                    "Only {} broker(s) running. For high availability, consider running at least 3 brokers.",
                    brokers.brokers.len()
                )
            ));
        }
    }

    let mut output = String::from("Cluster Optimization Recommendations:\n\n");

    if recommendations.is_empty() {
        output.push_str("✓ No optimization recommendations at this time.\n");
        output.push_str("Your cluster appears to be running optimally.\n");
    } else {
        for (i, (priority, category, recommendation)) in recommendations.iter().enumerate() {
            output.push_str(&format!(
                "{}. [{}] {}\n   {}\n\n",
                i + 1,
                priority,
                category,
                recommendation
            ));
        }
    }

    output
}
```

---

## Phase 4: Resources Implementation (Days 11-12)

### 4.1 Resource Types (`src/mcp/resources/mod.rs`)

```rust
pub mod cluster;
pub mod logs;
pub mod metrics;

use rmcp::prelude::*;
use std::sync::Arc;
use crate::core::grpc_client::AdminGrpcClient;

/// Resource URIs for Danube MCP server
pub mod uris {
    pub const CLUSTER_CONFIG: &str = "danube://cluster/config";
    pub const CLUSTER_STATUS: &str = "danube://cluster/status";
    
    pub fn broker_logs(broker_id: &str) -> String {
        format!("danube://brokers/{}/logs", broker_id)
    }
    
    pub fn topic_metrics(topic: &str) -> String {
        format!("danube://topics/{}/metrics", topic)
    }
    
    pub fn schema_definition(subject: &str) -> String {
        format!("danube://schemas/{}", subject)
    }
}

// Future: Implement resource handlers
// These would be registered in the MCP server impl
```

---

## Phase 5: Prompts Implementation (Days 13-14)

### 5.1 Troubleshooting Prompts (`src/mcp/prompts/troubleshooting.rs`)

```rust
use rmcp::prelude::*;

pub fn register_prompts() -> Vec<Prompt> {
    vec![
        Prompt {
            name: "debug-message-lag".to_string(),
            description: Some("Diagnose and resolve message consumption lag".to_string()),
            arguments: Some(vec![
                PromptArgument {
                    name: "topic".to_string(),
                    description: Some("Topic name experiencing lag".to_string()),
                    required: true,
                },
                PromptArgument {
                    name: "subscription".to_string(),
                    description: Some("Subscription name".to_string()),
                    required: true,
                },
            ]),
        },
        Prompt {
            name: "optimize-cluster".to_string(),
            description: Some("Analyze and optimize cluster performance".to_string()),
            arguments: None,
        },
        Prompt {
            name: "troubleshoot-schema".to_string(),
            description: Some("Debug schema validation issues".to_string()),
            arguments: Some(vec![
                PromptArgument {
                    name: "subject".to_string(),
                    description: Some("Schema subject name".to_string()),
                    required: true,
                },
            ]),
        },
    ]
}

pub fn get_prompt_template(name: &str, args: &serde_json::Value) -> Option<String> {
    match name {
        "debug-message-lag" => Some(format!(
            "I'll help you debug message consumption lag.\n\n\
             Topic: {}\n\
             Subscription: {}\n\n\
             Let me:\n\
             1. Check topic configuration and status\n\
             2. Verify subscription exists and is active\n\
             3. Analyze broker load distribution\n\
             4. Check cluster balance\n\
             5. Provide specific recommendations\n\n\
             Running diagnostics...",
            args.get("topic").and_then(|v| v.as_str()).unwrap_or("unknown"),
            args.get("subscription").and_then(|v| v.as_str()).unwrap_or("unknown")
        )),
        
        "optimize-cluster" => Some(
            "I'll analyze your cluster and provide optimization recommendations.\n\n\
             Steps:\n\
             1. Check cluster health\n\
             2. Analyze load distribution across brokers\n\
             3. Identify bottlenecks\n\
             4. Review topic/partition distribution\n\
             5. Provide actionable recommendations\n\n\
             Starting analysis...".to_string()
        ),
        
        "troubleshoot-schema" => Some(format!(
            "I'll help troubleshoot schema validation issues.\n\n\
             Subject: {}\n\n\
             Investigation plan:\n\
             1. Retrieve current schema definition\n\
             2. Check schema version history\n\
             3. Verify compatibility settings\n\
             4. Test compatibility if you have a new schema\n\
             5. Identify common validation errors\n\n\
             Retrieving schema information...",
            args.get("subject").and_then(|v| v.as_str()).unwrap_or("unknown")
        )),
        
        _ => None,
    }
}
```

---

## Phase 6: Integration & Testing (Days 15-16)

### 6.1 Update Server Mode Router

```rust
// src/server/mod.rs

pub async fn run(args: ServerArgs) -> Result<()> {
    match args.mode {
        ServerMode::Http => {
            info!("Starting HTTP server only");
            run_http_server(args).await
        }
        ServerMode::Mcp => {
            info!("Starting MCP server only");
            #[cfg(feature = "mcp")]
            {
                let config = crate::core::grpc_client::GrpcClientConfig {
                    endpoint: args.broker_endpoint.clone(),
                    request_timeout_ms: args.request_timeout_ms,
                    enable_tls: args.grpc_enable_tls,
                    domain: args.grpc_domain.clone(),
                    ca_path: args.grpc_ca.clone(),
                    cert_path: args.grpc_cert.clone(),
                    key_path: args.grpc_key.clone(),
                };
                let client = Arc::new(
                    crate::core::grpc_client::AdminGrpcClient::connect(config).await?
                );
                crate::mcp::run_mcp_server(client).await
            }
            #[cfg(not(feature = "mcp"))]
            {
                Err(anyhow!("MCP support not compiled. Build with --features mcp"))
            }
        }
        ServerMode::All => {
            info!("Starting both HTTP and MCP servers");
            // Future: Run both in parallel using tokio::select!
            run_http_server(args).await
        }
    }
}
```

### 6.2 Claude Desktop Configuration

Create configuration file for testing:

```json
// ~/Library/Application Support/Claude/claude_desktop_config.json (macOS)
// %APPDATA%\Claude\claude_desktop_config.json (Windows)
// ~/.config/Claude/claude_desktop_config.json (Linux)

{
  "mcpServers": {
    "danube": {
      "command": "/path/to/danube-admin",
      "args": [
        "serve",
        "--mode", "mcp",
        "--broker-endpoint", "http://localhost:50051"
      ],
      "env": {
        "RUST_LOG": "info,danube_admin=debug"
      }
    }
  }
}
```

### 6.3 Testing Scenarios

```bash
# Build with MCP support
cargo build --release --features mcp

# Test MCP server standalone
./target/release/danube-admin serve --mode mcp

# Test in Claude Desktop
# 1. Configure claude_desktop_config.json
# 2. Restart Claude
# 3. Verify MCP server appears in tools
# 4. Test commands:
#    - "List all brokers in the cluster"
#    - "Create a topic called /default/test-topic"
#    - "Show me the cluster balance"
#    - "Diagnose consumer lag on /default/events for subscription 'analytics'"
```

---

## Phase 7: Documentation (Day 17)

### 7.1 MCP Server README

Create `danube-admin/MCP_SERVER.md`:

```markdown
# Danube MCP Server

AI-native management interface for Danube clusters using Model Context Protocol (MCP).

## What is MCP?

Model Context Protocol allows AI assistants (Claude, Cursor, Windsurf) to interact with your Danube cluster through natural language.

## Setup

### 1. Build with MCP Support

```bash
cargo build --release --features mcp
```

### 2. Configure Claude Desktop

Edit `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "danube": {
      "command": "/path/to/danube-admin",
      "args": ["serve", "--mode", "mcp", "--broker-endpoint", "http://localhost:50051"]
    }
  }
}
```

### 3. Restart Claude Desktop

The Danube MCP server will now be available in Claude.

## Available Tools

### Cluster Management
- `list_brokers` - List all brokers with status
- `get_leader` - Get current leader broker
- `get_cluster_balance` - View load distribution
- `trigger_rebalance` - Rebalance topic distribution
- `list_namespaces` - List available namespaces

### Topic Operations
- `list_topics` - List topics in a namespace
- `describe_topic` - Get detailed topic information
- `create_topic` - Create a new topic
- `delete_topic` - Delete a topic
- `list_subscriptions` - List topic subscriptions

### Schema Registry
- `register_schema` - Register a new schema
- `get_schema` - Get schema by subject
- `list_schema_versions` - List all schema versions
- `check_schema_compatibility` - Validate compatibility

### Diagnostics
- `diagnose_consumer_lag` - Investigate lag issues
- `health_check` - Check cluster health
- `get_recommendations` - Get optimization suggestions

## Example Interactions

Ask Claude:

- "What brokers are in my Danube cluster?"
- "Create a topic called /default/user-events with 3 partitions"
- "Is my cluster balanced?"
- "Why is my consumer lagging on /default/orders?"
- "Register a schema for user-events from this JSON: {...}"
- "What's the health status of my cluster?"

## Troubleshooting

Enable debug logging:
```json
"env": {
  "RUST_LOG": "debug,danube_admin=trace"
}
```

Check MCP server logs in Claude Desktop console.
```

---

## Success Criteria

1. ✅ MCP server compiles with `--features mcp`
2. ✅ All tool implementations return structured, helpful responses
3. ✅ MCP server connects to Danube cluster via gRPC
4. ✅ Claude Desktop successfully loads and uses MCP server
5. ✅ All 15+ tools work correctly
6. ✅ Error handling is robust and informative
7. ✅ Resource and prompt implementations (future phases)
8. ✅ Documentation is comprehensive
9. ✅ Performance is acceptable (< 2s for most operations)
10. ✅ Can manage real Danube cluster through AI assistant

---

## Future Enhancements

### Phase 2 (Post-MVP)
- [ ] Implement resource handlers (logs, metrics, schemas)
- [ ] Add prompt templates for common workflows
- [ ] Add interactive wizards for complex operations
- [ ] Support SSE transport (in addition to stdio)
- [ ] Add authentication/authorization
- [ ] Metrics collection for MCP usage

### Advanced Features
- [ ] Multi-cluster support
- [ ] Batch operations
- [ ] Rollback capabilities
- [ ] Audit logging of AI actions
- [ ] Integration with CI/CD pipelines

---

## Security Considerations

1. **Authentication**: MCP server inherits credentials from environment/config
2. **Authorization**: All operations use existing Danube RBAC (future)
3. **Audit**: Log all MCP operations for compliance
4. **Validation**: Validate all AI-provided parameters
5. **Rate Limiting**: Prevent runaway AI operations

Example audit log entry:
```json
{
  "timestamp": "2026-01-29T12:34:56Z",
  "tool": "delete_topic",
  "params": {"topic": "/default/test"},
  "user": "mcp-server",
  "result": "success",
  "request_id": "abc-123"
}
```

---

## Architecture Decisions

### Why stdio transport?
- Standard for MCP servers
- Works with all MCP clients (Claude Desktop, Cursor, etc.)
- Simple, reliable, no network overhead

### Why direct gRPC (not HTTP gateway)?
- Lower latency
- Access to all proto methods
- Type-safe API
- Can always add HTTP gateway fallback later

### Why separate feature flag?
- Optional dependency on rmcp
- Smaller binary for users who don't need MCP
- Easier to maintain and test

---

This completes the MCP server implementation plan. The result is an AI-native management interface that makes Danube accessible to developers through natural language, providing a significant competitive advantage in the messaging platform space.

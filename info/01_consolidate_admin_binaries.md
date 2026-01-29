# Implementation Plan: Consolidate Admin Binaries

**Goal**: Merge `danube-admin-cli` and `danube-admin-gateway` into a single `danube-admin` binary that supports both CLI and server modes.

**Timeline**: 3-4 weeks  
**Priority**: High - Foundation for MCP implementation

---

## Overview

Create a new `danube-admin` crate that:
- Runs as CLI tool (current `danube-admin-cli` functionality)
- Runs as HTTP server (current `danube-admin-gateway` functionality)
- Eliminates ~400-500 lines of duplicate code
- Provides unified configuration and error handling
- Serves as foundation for future MCP server mode

---

## Phase 1: Project Setup & Core Infrastructure (Week 1)

### 1.1 Create New Crate Structure

```bash
# Create new binary crate
cargo new danube-admin --bin

# Update workspace Cargo.toml
[workspace]
members = [
    "danube-broker",
    "danube-client",
    "danube-cli",
    "danube-admin-cli",      # Keep for now (deprecated)
    "danube-admin-gateway",   # Keep for now (deprecated)
    "danube-admin",           # NEW
    # ... other crates
]
```

### 1.2 Project Structure

```
danube-admin/
├── Cargo.toml
├── README.md
├── src/
│   ├── main.rs              # Entry point & mode router
│   ├── lib.rs               # Library interface for testing
│   │
│   ├── core/                # Shared core (DRY)
│   │   ├── mod.rs
│   │   ├── grpc_client.rs   # Unified gRPC client (merge both implementations)
│   │   ├── config.rs        # Unified configuration
│   │   ├── error.rs         # Standard error types
│   │   └── types.rs         # Common types & responses
│   │
│   ├── cli/                 # CLI mode (from danube-admin-cli)
│   │   ├── mod.rs
│   │   ├── brokers.rs       # Broker commands
│   │   ├── namespaces.rs    # Namespace commands
│   │   ├── topics.rs        # Topic commands
│   │   ├── schemas.rs       # Schema registry commands
│   │   └── output.rs        # Output formatting (table/json)
│   │
│   └── server/              # Server mode (from danube-admin-gateway)
│       ├── mod.rs
│       ├── app.rs           # Router & app setup
│       ├── handlers/
│       │   ├── mod.rs
│       │   ├── cluster.rs   # Cluster endpoints
│       │   ├── topics.rs    # Topic endpoints
│       │   ├── namespaces.rs # Namespace endpoints
│       │   └── schemas.rs   # Schema endpoints (NEW)
│       ├── metrics.rs       # Prometheus integration
│       └── ui/              # UI pages
│           ├── mod.rs
│           ├── broker.rs
│           ├── cluster.rs
│           ├── topic.rs
│           └── shared.rs
│
└── tests/
    ├── cli_tests.rs
    └── server_tests.rs
```

### 1.3 Dependencies (Cargo.toml)

```toml
[package]
name = "danube-admin"
version = "0.7.0"
edition = "2021"

[[bin]]
name = "danube-admin"
path = "src/main.rs"

[dependencies]
danube-core = { path = "../danube-core" }

# CLI dependencies
clap = { workspace = true, features = ["derive", "env"] }
prettytable-rs = "0.10.0"

# Server dependencies
axum = "0.8"
tower = "0.5"
tower-http = { version = "0.6", features = ["cors", "trace"] }

# Shared dependencies
tokio = { workspace = true }
tonic = { workspace = true }
tonic-types = { workspace = true }
prost = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
anyhow = { workspace = true }
tracing = { workspace = true }
tracing-subscriber = { workspace = true }
rustls = { workspace = true }
base64 = { workspace = true }
reqwest = { version = "0.12", default-features = false, features = ["gzip", "json", "rustls-tls"] }
chrono = { version = "0.4", features = ["clock", "serde"] }

[lints]
workspace = true

[dev-dependencies]
assert_cmd = "2.0"
predicates = "3.1"
tempfile = { workspace = true }
rand = { workspace = true }
```

---

## Phase 2: Core Infrastructure (Week 1)

### 2.1 Unified gRPC Client (`src/core/grpc_client.rs`)

Merge the gRPC client implementations from both `danube-admin-cli/src/client.rs` and `danube-admin-gateway/src/grpc_client.rs`.

```rust
use anyhow::{anyhow, Result};
use danube_core::admin_proto as admin;
use danube_core::proto::danube_schema;
use std::time::Duration;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};

/// Configuration for the admin gRPC client
#[derive(Clone, Debug)]
pub struct GrpcClientConfig {
    pub endpoint: String,
    pub request_timeout_ms: u64,
    pub enable_tls: Option<bool>,
    pub domain: Option<String>,
    pub ca_path: Option<String>,
    pub cert_path: Option<String>,
    pub key_path: Option<String>,
}

impl Default for GrpcClientConfig {
    fn default() -> Self {
        Self {
            endpoint: std::env::var("DANUBE_ADMIN_ENDPOINT")
                .unwrap_or_else(|_| "http://127.0.0.1:50051".to_string()),
            request_timeout_ms: 5000,
            enable_tls: None,
            domain: None,
            ca_path: None,
            cert_path: None,
            key_path: None,
        }
    }
}

pub struct AdminGrpcClient {
    channel: Channel,
    timeout: Duration,
}

impl AdminGrpcClient {
    pub async fn connect(config: GrpcClientConfig) -> Result<Self> {
        let endpoint_url = if config.endpoint.starts_with("http://") 
            || config.endpoint.starts_with("https://") {
            config.endpoint
        } else {
            format!("http://{}", config.endpoint)
        };

        let mut endpoint = Endpoint::from_shared(endpoint_url.clone())?
            .tcp_nodelay(true);

        // TLS configuration
        let enable_tls = config.enable_tls.unwrap_or_else(|| {
            endpoint_url.starts_with("https://")
                || std::env::var("DANUBE_ADMIN_TLS")
                    .map(|v| v == "true")
                    .unwrap_or(false)
        });

        if enable_tls {
            let domain = config.domain
                .or_else(|| std::env::var("DANUBE_ADMIN_DOMAIN").ok())
                .unwrap_or_else(|| "localhost".to_string());

            let mut tls = ClientTlsConfig::new().domain_name(domain);

            // Optional Root CA
            if let Some(ca_path) = config.ca_path
                .or_else(|| std::env::var("DANUBE_ADMIN_CA").ok()) {
                let ca_pem = tokio::fs::read(ca_path).await?;
                let ca = Certificate::from_pem(ca_pem);
                tls = tls.ca_certificate(ca);
            }

            // Optional client identity (mTLS)
            let cert_path_env = std::env::var("DANUBE_ADMIN_CERT").ok();
            let key_path_env = std::env::var("DANUBE_ADMIN_KEY").ok();
            if let (Some(cert_path), Some(key_path)) = (
                config.cert_path.or(cert_path_env),
                config.key_path.or(key_path_env),
            ) {
                let cert = tokio::fs::read(cert_path).await?;
                let key = tokio::fs::read(key_path).await?;
                let identity = Identity::from_pem(cert, key);
                tls = tls.identity(identity);
            }

            endpoint = endpoint.tls_config(tls)?;
        }

        let channel = endpoint.connect().await?;
        Ok(Self {
            channel,
            timeout: Duration::from_millis(config.request_timeout_ms),
        })
    }

    // Broker Admin Methods
    pub async fn list_brokers(&self) -> Result<admin::BrokerListResponse> {
        let mut client = admin::broker_admin_client::BrokerAdminClient::new(
            self.channel.clone()
        );
        let fut = async move { client.list_brokers(admin::Empty {}).await };
        self.execute_with_timeout(fut).await
    }

    pub async fn get_leader(&self) -> Result<admin::BrokerResponse> {
        let mut client = admin::broker_admin_client::BrokerAdminClient::new(
            self.channel.clone()
        );
        let fut = async move { client.get_leader_broker(admin::Empty {}).await };
        self.execute_with_timeout(fut).await
    }

    pub async fn unload_broker(
        &self,
        req: admin::UnloadBrokerRequest,
    ) -> Result<admin::UnloadBrokerResponse> {
        let mut client = admin::broker_admin_client::BrokerAdminClient::new(
            self.channel.clone()
        );
        let fut = async move { client.unload_broker(req).await };
        self.execute_with_timeout(fut).await
    }

    pub async fn activate_broker(
        &self,
        req: admin::ActivateBrokerRequest,
    ) -> Result<admin::ActivateBrokerResponse> {
        let mut client = admin::broker_admin_client::BrokerAdminClient::new(
            self.channel.clone()
        );
        let fut = async move { client.activate_broker(req).await };
        self.execute_with_timeout(fut).await
    }

    pub async fn get_cluster_balance(
        &self,
        req: admin::ClusterBalanceRequest,
    ) -> Result<admin::ClusterBalanceResponse> {
        let mut client = admin::broker_admin_client::BrokerAdminClient::new(
            self.channel.clone()
        );
        let fut = async move { client.get_cluster_balance(req).await };
        self.execute_with_timeout(fut).await
    }

    pub async fn trigger_rebalance(
        &self,
        req: admin::RebalanceRequest,
    ) -> Result<admin::RebalanceResponse> {
        let mut client = admin::broker_admin_client::BrokerAdminClient::new(
            self.channel.clone()
        );
        let fut = async move { client.trigger_rebalance(req).await };
        self.execute_with_timeout(fut).await
    }

    // Namespace Admin Methods
    pub async fn list_namespaces(&self) -> Result<admin::NamespaceListResponse> {
        let mut client = admin::broker_admin_client::BrokerAdminClient::new(
            self.channel.clone()
        );
        let fut = async move { client.list_namespaces(admin::Empty {}).await };
        self.execute_with_timeout(fut).await
    }

    pub async fn create_namespace(
        &self,
        req: admin::NamespaceRequest,
    ) -> Result<admin::NamespaceResponse> {
        let mut client = admin::namespace_admin_client::NamespaceAdminClient::new(
            self.channel.clone()
        );
        let fut = async move { client.create_namespace(req).await };
        self.execute_with_timeout(fut).await
    }

    pub async fn delete_namespace(
        &self,
        req: admin::NamespaceRequest,
    ) -> Result<admin::NamespaceResponse> {
        let mut client = admin::namespace_admin_client::NamespaceAdminClient::new(
            self.channel.clone()
        );
        let fut = async move { client.delete_namespace(req).await };
        self.execute_with_timeout(fut).await
    }

    pub async fn get_namespace_policies(
        &self,
        req: admin::NamespaceRequest,
    ) -> Result<admin::PolicyResponse> {
        let mut client = admin::namespace_admin_client::NamespaceAdminClient::new(
            self.channel.clone()
        );
        let fut = async move { client.get_namespace_policies(req).await };
        self.execute_with_timeout(fut).await
    }

    // Topic Admin Methods
    pub async fn list_namespace_topics(
        &self,
        req: admin::NamespaceRequest,
    ) -> Result<admin::TopicInfoListResponse> {
        let mut client = admin::topic_admin_client::TopicAdminClient::new(
            self.channel.clone()
        );
        let fut = async move { client.list_namespace_topics(req).await };
        self.execute_with_timeout(fut).await
    }

    pub async fn list_broker_topics(
        &self,
        req: admin::BrokerRequest,
    ) -> Result<admin::TopicInfoListResponse> {
        let mut client = admin::topic_admin_client::TopicAdminClient::new(
            self.channel.clone()
        );
        let fut = async move { client.list_broker_topics(req).await };
        self.execute_with_timeout(fut).await
    }

    pub async fn create_topic(
        &self,
        req: admin::NewTopicRequest,
    ) -> Result<admin::TopicResponse> {
        let mut client = admin::topic_admin_client::TopicAdminClient::new(
            self.channel.clone()
        );
        let fut = async move { client.create_topic(req).await };
        self.execute_with_timeout(fut).await
    }

    pub async fn create_partitioned_topic(
        &self,
        req: admin::PartitionedTopicRequest,
    ) -> Result<admin::TopicResponse> {
        let mut client = admin::topic_admin_client::TopicAdminClient::new(
            self.channel.clone()
        );
        let fut = async move { client.create_partitioned_topic(req).await };
        self.execute_with_timeout(fut).await
    }

    pub async fn delete_topic(
        &self,
        req: admin::TopicRequest,
    ) -> Result<admin::TopicResponse> {
        let mut client = admin::topic_admin_client::TopicAdminClient::new(
            self.channel.clone()
        );
        let fut = async move { client.delete_topic(req).await };
        self.execute_with_timeout(fut).await
    }

    pub async fn unload_topic(
        &self,
        req: admin::TopicRequest,
    ) -> Result<admin::TopicResponse> {
        let mut client = admin::topic_admin_client::TopicAdminClient::new(
            self.channel.clone()
        );
        let fut = async move { client.unload_topic(req).await };
        self.execute_with_timeout(fut).await
    }

    pub async fn describe_topic(
        &self,
        req: admin::DescribeTopicRequest,
    ) -> Result<admin::DescribeTopicResponse> {
        let mut client = admin::topic_admin_client::TopicAdminClient::new(
            self.channel.clone()
        );
        let fut = async move { client.describe_topic(req).await };
        self.execute_with_timeout(fut).await
    }

    pub async fn list_subscriptions(
        &self,
        req: admin::TopicRequest,
    ) -> Result<admin::SubscriptionListResponse> {
        let mut client = admin::topic_admin_client::TopicAdminClient::new(
            self.channel.clone()
        );
        let fut = async move { client.list_subscriptions(req).await };
        self.execute_with_timeout(fut).await
    }

    pub async fn unsubscribe(
        &self,
        req: admin::SubscriptionRequest,
    ) -> Result<admin::SubscriptionResponse> {
        let mut client = admin::topic_admin_client::TopicAdminClient::new(
            self.channel.clone()
        );
        let fut = async move { client.unsubscribe(req).await };
        self.execute_with_timeout(fut).await
    }

    // Schema Registry Methods
    pub async fn register_schema(
        &self,
        req: danube_schema::RegisterSchemaRequest,
    ) -> Result<danube_schema::RegisterSchemaResponse> {
        let mut client = danube_schema::schema_registry_client::SchemaRegistryClient::new(
            self.channel.clone()
        );
        let fut = async move { client.register_schema(req).await };
        self.execute_with_timeout(fut).await
    }

    pub async fn get_schema(
        &self,
        req: danube_schema::GetSchemaRequest,
    ) -> Result<danube_schema::GetSchemaResponse> {
        let mut client = danube_schema::schema_registry_client::SchemaRegistryClient::new(
            self.channel.clone()
        );
        let fut = async move { client.get_schema(req).await };
        self.execute_with_timeout(fut).await
    }

    pub async fn get_latest_schema(
        &self,
        req: danube_schema::GetLatestSchemaRequest,
    ) -> Result<danube_schema::GetSchemaResponse> {
        let mut client = danube_schema::schema_registry_client::SchemaRegistryClient::new(
            self.channel.clone()
        );
        let fut = async move { client.get_latest_schema(req).await };
        self.execute_with_timeout(fut).await
    }

    pub async fn list_versions(
        &self,
        req: danube_schema::ListVersionsRequest,
    ) -> Result<danube_schema::ListVersionsResponse> {
        let mut client = danube_schema::schema_registry_client::SchemaRegistryClient::new(
            self.channel.clone()
        );
        let fut = async move { client.list_versions(req).await };
        self.execute_with_timeout(fut).await
    }

    pub async fn check_compatibility(
        &self,
        req: danube_schema::CheckCompatibilityRequest,
    ) -> Result<danube_schema::CheckCompatibilityResponse> {
        let mut client = danube_schema::schema_registry_client::SchemaRegistryClient::new(
            self.channel.clone()
        );
        let fut = async move { client.check_compatibility(req).await };
        self.execute_with_timeout(fut).await
    }

    pub async fn set_compatibility_mode(
        &self,
        req: danube_schema::SetCompatibilityModeRequest,
    ) -> Result<danube_schema::SetCompatibilityModeResponse> {
        let mut client = danube_schema::schema_registry_client::SchemaRegistryClient::new(
            self.channel.clone()
        );
        let fut = async move { client.set_compatibility_mode(req).await };
        self.execute_with_timeout(fut).await
    }

    pub async fn delete_schema_version(
        &self,
        req: danube_schema::DeleteSchemaVersionRequest,
    ) -> Result<danube_schema::DeleteSchemaVersionResponse> {
        let mut client = danube_schema::schema_registry_client::SchemaRegistryClient::new(
            self.channel.clone()
        );
        let fut = async move { client.delete_schema_version(req).await };
        self.execute_with_timeout(fut).await
    }

    // Helper: Execute request with timeout
    async fn execute_with_timeout<F, T>(&self, fut: F) -> Result<T>
    where
        F: std::future::Future<Output = Result<tonic::Response<T>, tonic::Status>>,
    {
        match tokio::time::timeout(self.timeout, fut).await {
            Err(_) => Err(anyhow!("request timeout")),
            Ok(Err(status)) => Err(anyhow!("gRPC error: {}", status)),
            Ok(Ok(resp)) => Ok(resp.into_inner()),
        }
    }
}
```

### 2.2 Standard Error Types (`src/core/error.rs`)

```rust
use serde::{Deserialize, Serialize};
use std::fmt;

/// Standard error response for both CLI and HTTP
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub error_code: String,
    pub message: String,
    pub details: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}

impl ErrorResponse {
    pub fn new(error_code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            error_code: error_code.into(),
            message: message.into(),
            details: None,
            request_id: None,
        }
    }

    pub fn with_details(mut self, details: impl Into<String>) -> Self {
        self.details = Some(details.into());
        self
    }

    pub fn with_request_id(mut self, request_id: impl Into<String>) -> Self {
        self.request_id = Some(request_id.into());
        self
    }
}

impl fmt::Display for ErrorResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}", self.error_code, self.message)?;
        if let Some(details) = &self.details {
            write!(f, "\nDetails: {}", details)?;
        }
        Ok(())
    }
}

impl std::error::Error for ErrorResponse {}

/// Error codes
pub mod error_codes {
    pub const INVALID_REQUEST: &str = "INVALID_REQUEST";
    pub const NOT_FOUND: &str = "NOT_FOUND";
    pub const ALREADY_EXISTS: &str = "ALREADY_EXISTS";
    pub const GRPC_ERROR: &str = "GRPC_ERROR";
    pub const TIMEOUT: &str = "TIMEOUT";
    pub const INTERNAL_ERROR: &str = "INTERNAL_ERROR";
    pub const UNAUTHORIZED: &str = "UNAUTHORIZED";
    pub const FORBIDDEN: &str = "FORBIDDEN";
}

/// Convert anyhow::Error to ErrorResponse
impl From<anyhow::Error> for ErrorResponse {
    fn from(err: anyhow::Error) -> Self {
        let message = err.to_string();
        
        // Try to detect error type from message
        let error_code = if message.contains("timeout") {
            error_codes::TIMEOUT
        } else if message.contains("not found") {
            error_codes::NOT_FOUND
        } else if message.contains("already exists") {
            error_codes::ALREADY_EXISTS
        } else if message.contains("gRPC") {
            error_codes::GRPC_ERROR
        } else {
            error_codes::INTERNAL_ERROR
        };

        ErrorResponse::new(error_code, message)
    }
}
```

### 2.3 Standard Response Types (`src/core/types.rs`)

```rust
use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};

/// Standard successful response envelope
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SuccessResponse<T> {
    pub success: bool,
    pub data: T,
    pub metadata: ResponseMetadata,
}

impl<T> SuccessResponse<T> {
    pub fn new(data: T) -> Self {
        Self {
            success: true,
            data,
            metadata: ResponseMetadata::default(),
        }
    }

    pub fn with_request_id(mut self, request_id: impl Into<String>) -> Self {
        self.metadata.request_id = Some(request_id.into());
        self
    }
}

/// Response metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseMetadata {
    pub timestamp: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}

impl Default for ResponseMetadata {
    fn default() -> Self {
        Self {
            timestamp: Utc::now(),
            request_id: None,
        }
    }
}

/// Standardized error response for HTTP
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorResponseEnvelope {
    pub success: bool,
    pub error: super::error::ErrorResponse,
    pub metadata: ResponseMetadata,
}

impl ErrorResponseEnvelope {
    pub fn new(error: super::error::ErrorResponse) -> Self {
        Self {
            success: false,
            error,
            metadata: ResponseMetadata::default(),
        }
    }
}
```

### 2.4 Main Entry Point (`src/main.rs`)

```rust
mod cli;
mod core;
mod server;

use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(name = "danube-admin")]
#[command(about = "Danube Admin - Unified CLI and server for managing Danube clusters", long_about = None)]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Start the HTTP admin server
    #[command(alias = "server")]
    Serve(server::ServerArgs),

    /// Manage brokers in the cluster
    Brokers(cli::brokers::Brokers),

    /// Manage namespaces
    Namespaces(cli::namespaces::Namespaces),

    /// Manage topics
    Topics(cli::topics::Topics),

    /// Manage schemas in the schema registry
    Schemas(cli::schemas::Schemas),
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing/logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into())
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Serve(args) => {
            tracing::info!("Starting danube-admin server");
            server::run(args).await
        }
        Commands::Brokers(cmd) => {
            cli::brokers::handle(cmd).await
        }
        Commands::Namespaces(cmd) => {
            cli::namespaces::handle(cmd).await
        }
        Commands::Topics(cmd) => {
            cli::topics::handle(cmd).await
        }
        Commands::Schemas(cmd) => {
            cli::schemas::handle(cmd).await
        }
    }
}
```

---

## Phase 3: Migrate CLI Commands (Week 2)

### 3.1 Copy and Adapt CLI Modules

Copy from `danube-admin-cli/src/`:
- `brokers.rs` → `danube-admin/src/cli/brokers.rs`
- `topics.rs` → `danube-admin/src/cli/topics.rs`
- `namespaces.rs` → `danube-admin/src/cli/namespaces.rs`
- `schema_registry.rs` → `danube-admin/src/cli/schemas.rs`

**Key Changes**:
1. Replace `crate::client::*` with `crate::core::grpc_client::*`
2. Use unified `AdminGrpcClient` instead of individual client functions
3. Standardize error handling with `crate::core::error::ErrorResponse`
4. Keep all existing clap arguments and functionality

### 3.2 Example: Adapted Brokers Module (`src/cli/brokers.rs`)

```rust
use clap::{Args, Subcommand};
use crate::core::grpc_client::{AdminGrpcClient, GrpcClientConfig};
use crate::core::error::ErrorResponse;
use danube_core::admin_proto::*;
use prettytable::{format, Cell, Row, Table};

#[derive(Debug, Args)]
pub struct Brokers {
    #[command(subcommand)]
    command: BrokersCommands,
}

#[derive(Debug, Subcommand)]
pub enum BrokersCommands {
    #[command(about = "List all active brokers in the cluster")]
    List {
        #[arg(long, value_parser = ["json"], help = "Output format: json (default: table)")]
        output: Option<String>,
    },
    // ... other commands
}

pub async fn handle(cmd: Brokers) -> anyhow::Result<()> {
    let config = GrpcClientConfig::default();
    let client = AdminGrpcClient::connect(config).await?;

    match cmd.command {
        BrokersCommands::List { output } => {
            let response = client.list_brokers().await?;
            
            if output.as_deref() == Some("json") {
                println!("{}", serde_json::to_string_pretty(&response)?);
            } else {
                print_brokers_table(&response);
            }
        }
        // ... handle other commands
    }
    
    Ok(())
}

fn print_brokers_table(response: &BrokerListResponse) {
    // ... existing table printing logic
}
```

### 3.3 Output Formatting (`src/cli/output.rs`)

```rust
use serde::Serialize;

pub enum OutputFormat {
    Table,
    Json,
}

impl OutputFormat {
    pub fn from_option(opt: Option<String>) -> Self {
        match opt.as_deref() {
            Some("json") => Self::Json,
            _ => Self::Table,
        }
    }

    pub fn print<T: Serialize>(&self, data: &T) -> anyhow::Result<()> {
        match self {
            OutputFormat::Json => {
                println!("{}", serde_json::to_string_pretty(data)?);
            }
            OutputFormat::Table => {
                // Table formatting handled in specific modules
            }
        }
        Ok(())
    }
}
```

---

## Phase 4: Migrate Server Mode (Week 3)

### 4.1 Copy Server Infrastructure

Copy from `danube-admin-gateway/src/`:
- `app.rs` → `danube-admin/src/server/app.rs`
- `metrics.rs` → `danube-admin/src/server/metrics.rs`
- `ui/*` → `danube-admin/src/server/ui/`

**Key Changes**:
1. Replace `crate::grpc_client::AdminGrpcClient` with `crate::core::grpc_client::AdminGrpcClient`
2. Use standardized error responses from `crate::core::error`
3. Use standardized success responses from `crate::core::types`

### 4.2 Server Entry Point (`src/server/mod.rs`)

```rust
mod app;
mod handlers;
mod metrics;
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
```

### 4.3 Standardize HTTP Responses (`src/server/handlers/mod.rs`)

```rust
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use crate::core::{error::ErrorResponse, types::ErrorResponseEnvelope};

pub mod cluster;
pub mod namespaces;
pub mod schemas;
pub mod topics;

/// Standard error response for HTTP endpoints
pub struct ApiError(ErrorResponse);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match self.0.error_code.as_str() {
            "NOT_FOUND" => StatusCode::NOT_FOUND,
            "ALREADY_EXISTS" => StatusCode::CONFLICT,
            "INVALID_REQUEST" => StatusCode::BAD_REQUEST,
            "UNAUTHORIZED" => StatusCode::UNAUTHORIZED,
            "FORBIDDEN" => StatusCode::FORBIDDEN,
            "TIMEOUT" => StatusCode::GATEWAY_TIMEOUT,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };

        let envelope = ErrorResponseEnvelope::new(self.0);
        (status, Json(envelope)).into_response()
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(err: anyhow::Error) -> Self {
        Self(err.into())
    }
}

impl From<ErrorResponse> for ApiError {
    fn from(err: ErrorResponse) -> Self {
        Self(err)
    }
}
```

---

## Phase 5: Testing & Documentation (Week 4)

### 5.1 Integration Tests

```rust
// tests/cli_tests.rs
use assert_cmd::Command;

#[test]
fn test_cli_brokers_list() {
    let mut cmd = Command::cargo_bin("danube-admin").unwrap();
    cmd.arg("brokers").arg("list");
    // Test requires running broker
}

#[test]
fn test_cli_help() {
    let mut cmd = Command::cargo_bin("danube-admin").unwrap();
    cmd.arg("--help");
    cmd.assert().success();
}

// tests/server_tests.rs
#[tokio::test]
async fn test_server_health() {
    // Start server in background
    // Test health endpoint
}
```

### 5.2 Update Documentation

Create `danube-admin/README.md`:

```markdown
# danube-admin

Unified CLI and HTTP server for managing Danube clusters.

## Usage

### CLI Mode

```bash
# List brokers
danube-admin brokers list

# Create topic
danube-admin topics create /default/mytopic --dispatch-strategy reliable

# Manage schemas
danube-admin schemas register user-events --file schema.json
```

### Server Mode

```bash
# Start HTTP server
danube-admin serve --listen-addr 0.0.0.0:8080

# With custom broker endpoint
danube-admin serve --broker-endpoint https://broker.example.com:50051
```

## Configuration

Environment variables:
- `DANUBE_ADMIN_ENDPOINT`: Broker gRPC endpoint (default: http://127.0.0.1:50051)
- `DANUBE_ADMIN_LISTEN_ADDR`: Server listen address (default: 0.0.0.0:8080)
- TLS: `DANUBE_ADMIN_TLS`, `DANUBE_ADMIN_DOMAIN`, `DANUBE_ADMIN_CA`, etc.

## Migration from Old Binaries

`danube-admin` replaces both `danube-admin-cli` and `danube-admin-gateway`:

```bash
# Old
danube-admin-cli brokers list
danube-admin-gateway --listen-addr 0.0.0.0:8080

# New
danube-admin brokers list
danube-admin serve --listen-addr 0.0.0.0:8080
```
```

---

## Phase 6: Deprecation of Old Binaries (Post-Release)

### 6.1 Add Deprecation Warnings

In `danube-admin-cli/src/main.rs`:
```rust
eprintln!("⚠️  DEPRECATION WARNING:");
eprintln!("   danube-admin-cli is deprecated and will be removed in v0.9.0");
eprintln!("   Please use 'danube-admin' instead:");
eprintln!("   danube-admin brokers list");
eprintln!();
```

### 6.2 Update All Documentation

- Update README.md
- Update website docs
- Update Docker examples
- Add migration guide

### 6.3 Release Plan

- **v0.7.0**: Introduce `danube-admin`, keep old binaries
- **v0.8.0**: Add deprecation warnings to old binaries
- **v0.9.0**: Remove `danube-admin-cli` and `danube-admin-gateway` crates

---

## Testing Checklist

### CLI Mode
- [ ] All broker commands work
- [ ] All topic commands work
- [ ] All namespace commands work
- [ ] All schema commands work
- [ ] JSON output works for all commands
- [ ] Table output works for all commands
- [ ] TLS/mTLS connection works
- [ ] Error messages are clear

### Server Mode
- [ ] All HTTP endpoints respond
- [ ] Cluster page works
- [ ] Topic pages work
- [ ] Broker pages work
- [ ] CORS works
- [ ] Metrics integration works
- [ ] Caching works
- [ ] Error responses are standardized

### Integration
- [ ] Can switch between CLI and server without conflicts
- [ ] Configuration is consistent
- [ ] Docker image works
- [ ] Binary size is reasonable

---

## Success Criteria

1. ✅ Single binary works in both CLI and server modes
2. ✅ All existing functionality preserved
3. ✅ ~400-500 lines of duplicate code eliminated
4. ✅ Standardized error responses across all modes
5. ✅ Documentation updated
6. ✅ Migration path clear for users
7. ✅ Binary size < 25 MB
8. ✅ All tests passing

---

## Rollback Plan

If consolidation causes issues:
1. Keep old binaries in workspace (already done)
2. Deprecation can be delayed
3. Users can continue using old binaries
4. Fix issues in danube-admin before forcing migration

---

## Next Steps After Completion

Once consolidation is complete:
1. Proceed with Phase 2: Enhanced server modes
2. Add MCP server mode (Phase 3)
3. Implement additional features without code duplication

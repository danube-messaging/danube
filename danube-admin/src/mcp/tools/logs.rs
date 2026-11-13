//! Log fetching tools for Docker, Kubernetes, and local file deployments

use crate::core::grpc_client::AdminGrpcClient;
use crate::mcp::config::{DeploymentConfig, DeploymentType, McpConfig};
use danube_core::admin_proto::GetBrokerLogsRequest;
use schemars::JsonSchema;
use serde::Deserialize;
use std::process::Command;

/// Parameters for fetching broker logs
#[derive(Debug, Deserialize, JsonSchema)]
pub struct BrokerLogsParams {
    /// Broker identifier as configured in mcp-config.yml (optional when using gRPC ring-buffer fallback).
    /// When deployment config is present, must match an ID from mcp-config.yml.
    /// Example: "broker1", "broker-0", "danube-broker-2"
    #[serde(default)]
    pub broker_id: String,

    /// Number of log lines to fetch from the end of the log.
    /// Higher values return more history but take longer to fetch.
    /// Default: 500
    /// Range: 1-10000 (implementation may cap at reasonable limits)
    #[serde(default = "default_lines")]
    pub lines: u32,

    /// Optional minimum log level filter for gRPC ring-buffer logs (e.g. "INFO", "WARN", "ERROR").
    #[serde(default)]
    pub level: Option<String>,

    /// Optional module target substring filter for gRPC ring-buffer logs.
    #[serde(default)]
    pub target: Option<String>,

    /// Optional text substring filter for log messages.
    #[serde(default)]
    pub contains: Option<String>,
}

fn default_lines() -> u32 {
    500
}

/// Parameters for fetching container logs (Docker)
#[derive(Debug, Deserialize, JsonSchema)]
pub struct ContainerLogsParams {
    /// Exact Docker container name or ID.
    /// Use list_docker_containers to discover running containers.
    /// Example: "danube-broker1", "my-service", "abc123def456"
    pub container_name: String,

    /// Number of log lines to fetch from the end of the log.
    /// Default: 500
    #[serde(default = "default_lines")]
    pub lines: u32,
}

/// Parameters for fetching pod logs (Kubernetes)
#[derive(Debug, Deserialize, JsonSchema)]
pub struct PodLogsParams {
    /// Kubernetes namespace where the pod is running.
    /// Example: "default", "danube-system", "production"
    pub namespace: String,

    /// Exact pod name.
    /// Use list_k8s_pods to discover pods in a namespace.
    /// Example: "danube-broker-0", "my-service-abc123-xyz"
    pub pod_name: String,

    /// Number of log lines to fetch from the end of the log.
    /// Default: 500
    #[serde(default = "default_lines")]
    pub lines: u32,
}

/// Parameters for listing K8s pods
#[derive(Debug, Deserialize, JsonSchema)]
pub struct ListPodsParams {
    /// Kubernetes namespace to list pods from.
    /// Returns all pods in this namespace regardless of labels or status.
    /// Example: "default", "danube-system", "kube-system"
    pub namespace: String,
}

/// Get logs for a specific broker using deployment config when available,
/// falling back to the broker admin gRPC `GetBrokerLogs` ring buffer.
pub async fn get_broker_logs(
    config: &McpConfig,
    client: &AdminGrpcClient,
    params: BrokerLogsParams,
) -> String {
    if let Some(deployment) = &config.deployment {
        return match deployment.deployment_type {
            DeploymentType::Docker => get_docker_broker_logs(deployment, &params),
            DeploymentType::Kubernetes => get_k8s_broker_logs(deployment, &params).await,
            DeploymentType::Local => get_local_broker_logs(deployment, &params),
        };
    }

    let req = GetBrokerLogsRequest {
        level: params.level.unwrap_or_default(),
        target: params.target.unwrap_or_default(),
        contains: params.contains.unwrap_or_default(),
        since_ms: 0,
        limit: params.lines.clamp(1, 2000),
    };

    match client.get_broker_logs(req).await {
        Ok(resp) => {
            let mut out = format!(
                "=== Broker {} Logs (gRPC Ring Buffer: {} returned, {} buffered) ===\n\n",
                resp.broker_id,
                resp.entries.len(),
                resp.total_buffered
            );
            if !params.broker_id.is_empty() && params.broker_id != resp.broker_id.to_string() {
                out.push_str(&format!(
                    "Note: Returned logs from connected admin endpoint (broker_id={}); requested '{}'.\n\n",
                    resp.broker_id, params.broker_id
                ));
            }
            if resp.entries.is_empty() {
                out.push_str("(No matching log entries in ring buffer.)\n");
            } else {
                for entry in resp.entries {
                    let fields = if entry.fields.is_empty() {
                        String::new()
                    } else {
                        let pairs: Vec<String> = entry
                            .fields
                            .iter()
                            .map(|(k, v)| format!("{k}={v}"))
                            .collect();
                        format!(" {{{}}}", pairs.join(", "))
                    };
                    out.push_str(&format!(
                        "[{}ms] {:5} {} - {}{}\n",
                        entry.timestamp_ms, entry.level, entry.target, entry.message, fields
                    ));
                }
            }
            out
        }
        Err(e) => format!("Error fetching broker logs via gRPC ring buffer: {}", e),
    }
}

/// Get logs from Docker container
fn get_docker_broker_logs(deployment: &DeploymentConfig, params: &BrokerLogsParams) -> String {
    let Some(docker) = &deployment.docker else {
        return "Error: Docker configuration not found".to_string();
    };

    // Find container by broker ID
    let container = docker
        .brokers
        .iter()
        .find(|b| b.id == params.broker_id)
        .or_else(|| {
            docker
                .other
                .as_ref()
                .and_then(|o| o.iter().find(|b| b.id == params.broker_id))
        });

    let Some(container) = container else {
        let available: Vec<_> = docker.brokers.iter().map(|b| b.id.as_str()).collect();
        return format!(
            "Error: Broker '{}' not found in configuration.\nAvailable brokers: {:?}",
            params.broker_id, available
        );
    };

    fetch_docker_logs(&container.container, params.lines)
}

/// Get logs from Kubernetes pod
async fn get_k8s_broker_logs(deployment: &DeploymentConfig, params: &BrokerLogsParams) -> String {
    let Some(k8s) = &deployment.kubernetes else {
        return "Error: Kubernetes configuration not found".to_string();
    };

    // Find pods by service or selector
    let pods = if let Some(service) = &k8s.broker_service {
        find_pods_by_service(&k8s.namespace, service)
    } else if let Some(selector) = &k8s.broker_selector {
        find_pods_by_selector(&k8s.namespace, selector)
    } else {
        return "Error: No broker_service or broker_selector configured".to_string();
    };

    let Ok(pods) = pods else {
        return format!("Error finding pods: {}", pods.unwrap_err());
    };

    // Find pod matching broker_id
    let pod = pods.iter().find(|p| p.contains(&params.broker_id));

    let Some(pod) = pod else {
        return format!(
            "Error: No pod found matching broker_id '{}'\nAvailable pods: {:?}",
            params.broker_id, pods
        );
    };

    fetch_k8s_logs(&k8s.namespace, pod, params.lines)
}

/// Get logs from local file
fn get_local_broker_logs(deployment: &DeploymentConfig, params: &BrokerLogsParams) -> String {
    let Some(local) = &deployment.local else {
        return "Error: Local configuration not found".to_string();
    };

    // Find log file by broker ID
    let mapping = local.brokers.iter().find(|b| b.id == params.broker_id);

    let Some(mapping) = mapping else {
        let available: Vec<_> = local.brokers.iter().map(|b| b.id.as_str()).collect();
        return format!(
            "Error: Broker '{}' not found in configuration.\nAvailable brokers: {:?}",
            params.broker_id, available
        );
    };

    let log_path = local.log_dir.join(&mapping.log_file);
    read_log_file(&log_path, params.lines)
}

/// List all configured brokers/containers
pub fn list_configured_brokers(config: &McpConfig) -> String {
    let Some(deployment) = &config.deployment else {
        return "No deployment configuration provided. Log fetching is not available.".to_string();
    };

    match deployment.deployment_type {
        DeploymentType::Docker => {
            if let Some(docker) = &deployment.docker {
                let mut output = String::from("Configured Docker containers:\n\n");
                output.push_str("Brokers:\n");
                for broker in &docker.brokers {
                    output.push_str(&format!(
                        "  - {} → container: {}\n",
                        broker.id, broker.container
                    ));
                }
                if let Some(other) = &docker.other {
                    output.push_str("\nOther services:\n");
                    for svc in other {
                        output
                            .push_str(&format!("  - {} → container: {}\n", svc.id, svc.container));
                    }
                }
                output
            } else {
                "Docker configuration not found".to_string()
            }
        }
        DeploymentType::Kubernetes => {
            if let Some(k8s) = &deployment.kubernetes {
                format!(
                    "Kubernetes deployment:\n\
                     Namespace: {}\n\
                     Service: {}\n\
                     Selector: {}\n\n\
                     Use list_k8s_pods to see running pods.",
                    k8s.namespace,
                    k8s.broker_service.as_deref().unwrap_or("not set"),
                    k8s.broker_selector.as_deref().unwrap_or("not set")
                )
            } else {
                "Kubernetes configuration not found".to_string()
            }
        }
        DeploymentType::Local => {
            if let Some(local) = &deployment.local {
                let mut output = format!("Local log files in: {:?}\n\n", local.log_dir);
                for broker in &local.brokers {
                    output.push_str(&format!("  - {} → {}\n", broker.id, broker.log_file));
                }
                output
            } else {
                "Local configuration not found".to_string()
            }
        }
    }
}

/// Fetch logs from a Docker container
pub fn fetch_docker_logs(container: &str, lines: u32) -> String {
    let output = Command::new("docker")
        .args(["logs", "--tail", &lines.to_string(), container])
        .output();

    match output {
        Ok(out) => {
            if out.status.success() {
                let stdout = String::from_utf8_lossy(&out.stdout);
                let stderr = String::from_utf8_lossy(&out.stderr);
                format!("{}{}", stdout, stderr)
            } else {
                format!(
                    "Error fetching logs from container '{}': {}",
                    container,
                    String::from_utf8_lossy(&out.stderr)
                )
            }
        }
        Err(e) => format!(
            "Failed to execute docker command: {}\nIs Docker installed and running?",
            e
        ),
    }
}

/// Fetch logs from a Kubernetes pod
pub fn fetch_k8s_logs(namespace: &str, pod: &str, lines: u32) -> String {
    let output = Command::new("kubectl")
        .args(["logs", "-n", namespace, pod, &format!("--tail={}", lines)])
        .output();

    match output {
        Ok(out) => {
            if out.status.success() {
                String::from_utf8_lossy(&out.stdout).to_string()
            } else {
                format!(
                    "Error fetching logs from pod '{}/{}': {}",
                    namespace,
                    pod,
                    String::from_utf8_lossy(&out.stderr)
                )
            }
        }
        Err(e) => format!(
            "Failed to execute kubectl command: {}\nIs kubectl installed and configured?",
            e
        ),
    }
}

/// Read last N lines from a local log file
fn read_log_file(path: &std::path::Path, lines: u32) -> String {
    match std::fs::read_to_string(path) {
        Ok(content) => {
            let all_lines: Vec<&str> = content.lines().collect();
            let start = all_lines.len().saturating_sub(lines as usize);
            all_lines[start..].join("\n")
        }
        Err(e) => format!("Error reading log file {:?}: {}", path, e),
    }
}

/// Find pods by service name
fn find_pods_by_service(namespace: &str, service: &str) -> Result<Vec<String>, String> {
    // Get endpoints for the service to find pod names
    let output = Command::new("kubectl")
        .args([
            "get",
            "pods",
            "-n",
            namespace,
            "-l",
            &format!("app={}", service),
            "-o",
            "jsonpath={.items[*].metadata.name}",
        ])
        .output()
        .map_err(|e| format!("Failed to execute kubectl: {}", e))?;

    if output.status.success() {
        let pods = String::from_utf8_lossy(&output.stdout)
            .split_whitespace()
            .map(String::from)
            .collect();
        Ok(pods)
    } else {
        Err(String::from_utf8_lossy(&output.stderr).to_string())
    }
}

/// Find pods by label selector
fn find_pods_by_selector(namespace: &str, selector: &str) -> Result<Vec<String>, String> {
    let output = Command::new("kubectl")
        .args([
            "get",
            "pods",
            "-n",
            namespace,
            "-l",
            selector,
            "-o",
            "jsonpath={.items[*].metadata.name}",
        ])
        .output()
        .map_err(|e| format!("Failed to execute kubectl: {}", e))?;

    if output.status.success() {
        let pods = String::from_utf8_lossy(&output.stdout)
            .split_whitespace()
            .map(String::from)
            .collect();
        Ok(pods)
    } else {
        Err(String::from_utf8_lossy(&output.stderr).to_string())
    }
}

/// List running Docker containers
pub fn list_docker_containers() -> String {
    let output = Command::new("docker")
        .args([
            "ps",
            "--format",
            "table {{.Names}}\t{{.Status}}\t{{.Ports}}",
        ])
        .output();

    match output {
        Ok(out) => {
            if out.status.success() {
                String::from_utf8_lossy(&out.stdout).to_string()
            } else {
                format!(
                    "Error listing containers: {}",
                    String::from_utf8_lossy(&out.stderr)
                )
            }
        }
        Err(e) => format!("Failed to execute docker command: {}", e),
    }
}

/// List Kubernetes pods in a namespace
pub fn list_k8s_pods(namespace: &str) -> String {
    let output = Command::new("kubectl")
        .args(["get", "pods", "-n", namespace, "-o", "wide"])
        .output();

    match output {
        Ok(out) => {
            if out.status.success() {
                String::from_utf8_lossy(&out.stdout).to_string()
            } else {
                format!(
                    "Error listing pods: {}",
                    String::from_utf8_lossy(&out.stderr)
                )
            }
        }
        Err(e) => format!("Failed to execute kubectl command: {}", e),
    }
}

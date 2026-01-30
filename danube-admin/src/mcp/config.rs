//! MCP configuration file parsing
//!
//! Supports Docker, Kubernetes, and local file deployments for log access.

use serde::Deserialize;
use std::path::PathBuf;

/// Main MCP configuration structure
#[derive(Debug, Clone, Deserialize)]
pub struct McpConfig {
    /// Broker gRPC endpoint
    pub broker_endpoint: String,

    /// Deployment configuration for log access
    pub deployment: Option<DeploymentConfig>,

    /// Resources to expose to AI (docs, config files) - future feature
    #[allow(dead_code)]
    pub resources: Option<ResourcesConfig>,
}

/// Deployment configuration - determines how to fetch logs
#[derive(Debug, Clone, Deserialize)]
pub struct DeploymentConfig {
    /// Deployment type: docker, kubernetes, or local
    #[serde(rename = "type")]
    pub deployment_type: DeploymentType,

    /// Docker-specific configuration
    pub docker: Option<DockerConfig>,

    /// Kubernetes-specific configuration
    pub kubernetes: Option<KubernetesConfig>,

    /// Local files configuration
    pub local: Option<LocalConfig>,
}

/// Supported deployment types
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum DeploymentType {
    Docker,
    Kubernetes,
    Local,
}

/// Docker deployment configuration
#[derive(Debug, Clone, Deserialize)]
pub struct DockerConfig {
    /// Broker container mappings
    pub brokers: Vec<ContainerMapping>,

    /// Other containers (etcd, minio, etc.)
    pub other: Option<Vec<ContainerMapping>>,
}

/// Maps a broker ID to a Docker container name
#[derive(Debug, Clone, Deserialize)]
pub struct ContainerMapping {
    /// Logical broker identifier (e.g., "broker1")
    pub id: String,

    /// Docker container name (e.g., "danube-broker1")
    pub container: String,
}

/// Kubernetes deployment configuration
#[derive(Debug, Clone, Deserialize)]
pub struct KubernetesConfig {
    /// Kubernetes namespace where brokers run
    pub namespace: String,

    /// Service name to find broker pods (recommended)
    pub broker_service: Option<String>,

    /// Label selector to find broker pods (alternative)
    pub broker_selector: Option<String>,
}

/// Local file-based log configuration
#[derive(Debug, Clone, Deserialize)]
pub struct LocalConfig {
    /// Directory containing log files
    pub log_dir: PathBuf,

    /// Broker log file mappings
    pub brokers: Vec<LogFileMapping>,
}

/// Maps a broker ID to a local log file
#[derive(Debug, Clone, Deserialize)]
pub struct LogFileMapping {
    /// Logical broker identifier (e.g., "broker1")
    pub id: String,

    /// Log file name (e.g., "broker_6650.log")
    pub log_file: String,
}

/// Resources to expose to AI assistants - future feature
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct ResourcesConfig {
    pub docs_dir: Option<PathBuf>,
    pub files: Option<Vec<FileMapping>>,
}

/// Maps a file path to a resource name - future feature
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct FileMapping {
    pub path: PathBuf,
    pub name: String,
}

impl McpConfig {
    /// Load configuration from a YAML file
    pub fn from_file(path: &PathBuf) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("Failed to read config file {:?}: {}", path, e))?;

        let config: McpConfig = serde_yaml::from_str(&content)
            .map_err(|e| anyhow::anyhow!("Failed to parse config file {:?}: {}", path, e))?;

        config.validate()?;
        Ok(config)
    }

    /// Create a minimal config with just the broker endpoint (no log access)
    pub fn minimal(broker_endpoint: String) -> Self {
        Self {
            broker_endpoint,
            deployment: None,
            resources: None,
        }
    }

    /// Validate the configuration
    pub fn validate(&self) -> anyhow::Result<()> {
        if let Some(deployment) = &self.deployment {
            match deployment.deployment_type {
                DeploymentType::Docker => {
                    let docker = deployment.docker.as_ref().ok_or_else(|| {
                        anyhow::anyhow!("Docker deployment type requires 'docker' configuration")
                    })?;
                    if docker.brokers.is_empty() {
                        return Err(anyhow::anyhow!(
                            "Docker configuration requires at least one broker mapping"
                        ));
                    }
                }
                DeploymentType::Kubernetes => {
                    let k8s = deployment.kubernetes.as_ref().ok_or_else(|| {
                        anyhow::anyhow!(
                            "Kubernetes deployment type requires 'kubernetes' configuration"
                        )
                    })?;
                    if k8s.broker_service.is_none() && k8s.broker_selector.is_none() {
                        return Err(anyhow::anyhow!("Kubernetes configuration requires either 'broker_service' or 'broker_selector'"));
                    }
                }
                DeploymentType::Local => {
                    let local = deployment.local.as_ref().ok_or_else(|| {
                        anyhow::anyhow!("Local deployment type requires 'local' configuration")
                    })?;
                    if !local.log_dir.exists() {
                        return Err(anyhow::anyhow!(
                            "Local log directory does not exist: {:?}",
                            local.log_dir
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    /// Check if log fetching is available
    pub fn has_log_access(&self) -> bool {
        self.deployment.is_some()
    }

    /// Check if resources are configured - future feature
    #[allow(dead_code)]
    pub fn has_resources(&self) -> bool {
        self.resources.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_docker_config() {
        let yaml = r#"
broker_endpoint: http://127.0.0.1:50051

deployment:
  type: docker
  docker:
    brokers:
      - id: broker1
        container: danube-broker1
      - id: broker2
        container: danube-broker2
"#;
        let config: McpConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.broker_endpoint, "http://127.0.0.1:50051");
        assert!(config.deployment.is_some());
        let deployment = config.deployment.unwrap();
        assert_eq!(deployment.deployment_type, DeploymentType::Docker);
        assert!(deployment.docker.is_some());
        let docker = deployment.docker.unwrap();
        assert_eq!(docker.brokers.len(), 2);
    }

    #[test]
    fn test_parse_kubernetes_config() {
        let yaml = r#"
broker_endpoint: http://k8s-lb:50051

deployment:
  type: kubernetes
  kubernetes:
    namespace: danube-system
    broker_service: danube-broker
"#;
        let config: McpConfig = serde_yaml::from_str(yaml).unwrap();
        let deployment = config.deployment.unwrap();
        assert_eq!(deployment.deployment_type, DeploymentType::Kubernetes);
        let k8s = deployment.kubernetes.unwrap();
        assert_eq!(k8s.namespace, "danube-system");
        assert_eq!(k8s.broker_service.unwrap(), "danube-broker");
    }

    #[test]
    fn test_minimal_config() {
        let config = McpConfig::minimal("http://127.0.0.1:50051".to_string());
        assert!(!config.has_log_access());
        assert!(!config.has_resources());
    }
}

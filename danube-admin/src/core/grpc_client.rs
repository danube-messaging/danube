use anyhow::{anyhow, Result};
use danube_core::admin_proto as admin;
use danube_core::proto::danube_schema;
use std::time::Duration;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};

use super::config::GrpcClientConfig;

/// Unified gRPC client for Danube admin operations
/// Consolidates logic from danube-admin-cli and danube-admin-gateway
pub struct AdminGrpcClient {
    channel: Channel,
    timeout: Duration,
}

impl AdminGrpcClient {
    /// Connect to a Danube broker using the provided configuration
    pub async fn connect(config: GrpcClientConfig) -> Result<Self> {
        // Accept either full URL (http/https) or host:port; default to http if no scheme
        let endpoint_url =
            if config.endpoint.starts_with("http://") || config.endpoint.starts_with("https://") {
                config.endpoint.clone()
            } else {
                format!("http://{}", config.endpoint)
            };

        let mut endpoint = Endpoint::from_shared(endpoint_url.clone())?.tcp_nodelay(true);

        // TLS enablement: https scheme OR enable_tls=true OR DANUBE_ADMIN_TLS=true
        let enable_tls = config.enable_tls.unwrap_or_else(|| {
            endpoint_url.starts_with("https://")
                || std::env::var("DANUBE_ADMIN_TLS")
                    .map(|v| v == "true")
                    .unwrap_or(false)
        });

        if enable_tls {
            let domain = config
                .domain
                .or_else(|| std::env::var("DANUBE_ADMIN_DOMAIN").ok())
                .unwrap_or_else(|| "localhost".to_string());

            let mut tls = ClientTlsConfig::new().domain_name(domain);

            // Optional Root CA
            if let Some(ca_path) = config
                .ca_path
                .or_else(|| std::env::var("DANUBE_ADMIN_CA").ok())
            {
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

    /// Helper: Execute request with timeout
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

    // ===== BROKER ADMIN METHODS =====

    pub async fn list_brokers(&self) -> Result<admin::BrokerListResponse> {
        let mut client = admin::broker_admin_client::BrokerAdminClient::new(self.channel.clone());
        let fut = async move { client.list_brokers(admin::Empty {}).await };
        self.execute_with_timeout(fut).await
    }

    pub async fn get_leader(&self) -> Result<admin::BrokerResponse> {
        let mut client = admin::broker_admin_client::BrokerAdminClient::new(self.channel.clone());
        let fut = async move { client.get_leader_broker(admin::Empty {}).await };
        self.execute_with_timeout(fut).await
    }

    pub async fn unload_broker(
        &self,
        req: admin::UnloadBrokerRequest,
    ) -> Result<admin::UnloadBrokerResponse> {
        let mut client = admin::broker_admin_client::BrokerAdminClient::new(self.channel.clone());
        let fut = async move { client.unload_broker(req).await };
        self.execute_with_timeout(fut).await
    }

    pub async fn activate_broker(
        &self,
        req: admin::ActivateBrokerRequest,
    ) -> Result<admin::ActivateBrokerResponse> {
        let mut client = admin::broker_admin_client::BrokerAdminClient::new(self.channel.clone());
        let fut = async move { client.activate_broker(req).await };
        self.execute_with_timeout(fut).await
    }

    pub async fn get_cluster_balance(
        &self,
        req: admin::ClusterBalanceRequest,
    ) -> Result<admin::ClusterBalanceResponse> {
        let mut client = admin::broker_admin_client::BrokerAdminClient::new(self.channel.clone());
        let fut = async move { client.get_cluster_balance(req).await };
        self.execute_with_timeout(fut).await
    }

    pub async fn trigger_rebalance(
        &self,
        req: admin::RebalanceRequest,
    ) -> Result<admin::RebalanceResponse> {
        let mut client = admin::broker_admin_client::BrokerAdminClient::new(self.channel.clone());
        let fut = async move { client.trigger_rebalance(req).await };
        self.execute_with_timeout(fut).await
    }

    // ===== NAMESPACE ADMIN METHODS =====

    pub async fn list_namespaces(&self) -> Result<admin::NamespaceListResponse> {
        let mut client = admin::broker_admin_client::BrokerAdminClient::new(self.channel.clone());
        let fut = async move { client.list_namespaces(admin::Empty {}).await };
        self.execute_with_timeout(fut).await
    }

    pub async fn create_namespace(
        &self,
        req: admin::NamespaceRequest,
    ) -> Result<admin::NamespaceResponse> {
        let mut client =
            admin::namespace_admin_client::NamespaceAdminClient::new(self.channel.clone());
        let fut = async move { client.create_namespace(req).await };
        self.execute_with_timeout(fut).await
    }

    pub async fn delete_namespace(
        &self,
        req: admin::NamespaceRequest,
    ) -> Result<admin::NamespaceResponse> {
        let mut client =
            admin::namespace_admin_client::NamespaceAdminClient::new(self.channel.clone());
        let fut = async move { client.delete_namespace(req).await };
        self.execute_with_timeout(fut).await
    }

    pub async fn get_namespace_policies(
        &self,
        req: admin::NamespaceRequest,
    ) -> Result<admin::PolicyResponse> {
        let mut client =
            admin::namespace_admin_client::NamespaceAdminClient::new(self.channel.clone());
        let fut = async move { client.get_namespace_policies(req).await };
        self.execute_with_timeout(fut).await
    }

    // ===== TOPIC ADMIN METHODS =====

    pub async fn list_namespace_topics(
        &self,
        req: admin::NamespaceRequest,
    ) -> Result<admin::TopicInfoListResponse> {
        let mut client = admin::topic_admin_client::TopicAdminClient::new(self.channel.clone());
        let fut = async move { client.list_namespace_topics(req).await };
        self.execute_with_timeout(fut).await
    }

    pub async fn list_broker_topics(
        &self,
        req: admin::BrokerRequest,
    ) -> Result<admin::TopicInfoListResponse> {
        let mut client = admin::topic_admin_client::TopicAdminClient::new(self.channel.clone());
        let fut = async move { client.list_broker_topics(req).await };
        self.execute_with_timeout(fut).await
    }

    pub async fn create_topic(&self, req: admin::NewTopicRequest) -> Result<admin::TopicResponse> {
        let mut client = admin::topic_admin_client::TopicAdminClient::new(self.channel.clone());
        let fut = async move { client.create_topic(req).await };
        self.execute_with_timeout(fut).await
    }

    pub async fn create_partitioned_topic(
        &self,
        req: admin::PartitionedTopicRequest,
    ) -> Result<admin::TopicResponse> {
        let mut client = admin::topic_admin_client::TopicAdminClient::new(self.channel.clone());
        let fut = async move { client.create_partitioned_topic(req).await };
        self.execute_with_timeout(fut).await
    }

    pub async fn delete_topic(&self, req: admin::TopicRequest) -> Result<admin::TopicResponse> {
        let mut client = admin::topic_admin_client::TopicAdminClient::new(self.channel.clone());
        let fut = async move { client.delete_topic(req).await };
        self.execute_with_timeout(fut).await
    }

    pub async fn unload_topic(&self, req: admin::TopicRequest) -> Result<admin::TopicResponse> {
        let mut client = admin::topic_admin_client::TopicAdminClient::new(self.channel.clone());
        let fut = async move { client.unload_topic(req).await };
        self.execute_with_timeout(fut).await
    }

    pub async fn describe_topic(
        &self,
        req: admin::DescribeTopicRequest,
    ) -> Result<admin::DescribeTopicResponse> {
        let mut client = admin::topic_admin_client::TopicAdminClient::new(self.channel.clone());
        let fut = async move { client.describe_topic(req).await };
        self.execute_with_timeout(fut).await
    }

    pub async fn list_subscriptions(
        &self,
        req: admin::TopicRequest,
    ) -> Result<admin::SubscriptionListResponse> {
        let mut client = admin::topic_admin_client::TopicAdminClient::new(self.channel.clone());
        let fut = async move { client.list_subscriptions(req).await };
        self.execute_with_timeout(fut).await
    }

    pub async fn unsubscribe(
        &self,
        req: admin::SubscriptionRequest,
    ) -> Result<admin::SubscriptionResponse> {
        let mut client = admin::topic_admin_client::TopicAdminClient::new(self.channel.clone());
        let fut = async move { client.unsubscribe(req).await };
        self.execute_with_timeout(fut).await
    }

    // ===== SCHEMA REGISTRY METHODS =====

    pub async fn register_schema(
        &self,
        req: danube_schema::RegisterSchemaRequest,
    ) -> Result<danube_schema::RegisterSchemaResponse> {
        let mut client =
            danube_schema::schema_registry_client::SchemaRegistryClient::new(self.channel.clone());
        let fut = async move { client.register_schema(req).await };
        self.execute_with_timeout(fut).await
    }

    pub async fn get_schema(
        &self,
        req: danube_schema::GetSchemaRequest,
    ) -> Result<danube_schema::GetSchemaResponse> {
        let mut client =
            danube_schema::schema_registry_client::SchemaRegistryClient::new(self.channel.clone());
        let fut = async move { client.get_schema(req).await };
        self.execute_with_timeout(fut).await
    }

    pub async fn get_latest_schema(
        &self,
        req: danube_schema::GetLatestSchemaRequest,
    ) -> Result<danube_schema::GetSchemaResponse> {
        let mut client =
            danube_schema::schema_registry_client::SchemaRegistryClient::new(self.channel.clone());
        let fut = async move { client.get_latest_schema(req).await };
        self.execute_with_timeout(fut).await
    }

    pub async fn list_versions(
        &self,
        req: danube_schema::ListVersionsRequest,
    ) -> Result<danube_schema::ListVersionsResponse> {
        let mut client =
            danube_schema::schema_registry_client::SchemaRegistryClient::new(self.channel.clone());
        let fut = async move { client.list_versions(req).await };
        self.execute_with_timeout(fut).await
    }

    pub async fn check_compatibility(
        &self,
        req: danube_schema::CheckCompatibilityRequest,
    ) -> Result<danube_schema::CheckCompatibilityResponse> {
        let mut client =
            danube_schema::schema_registry_client::SchemaRegistryClient::new(self.channel.clone());
        let fut = async move { client.check_compatibility(req).await };
        self.execute_with_timeout(fut).await
    }

    pub async fn set_compatibility_mode(
        &self,
        req: danube_schema::SetCompatibilityModeRequest,
    ) -> Result<danube_schema::SetCompatibilityModeResponse> {
        let mut client =
            danube_schema::schema_registry_client::SchemaRegistryClient::new(self.channel.clone());
        let fut = async move { client.set_compatibility_mode(req).await };
        self.execute_with_timeout(fut).await
    }

    pub async fn delete_schema_version(
        &self,
        req: danube_schema::DeleteSchemaVersionRequest,
    ) -> Result<danube_schema::DeleteSchemaVersionResponse> {
        let mut client =
            danube_schema::schema_registry_client::SchemaRegistryClient::new(self.channel.clone());
        let fut = async move { client.delete_schema_version(req).await };
        self.execute_with_timeout(fut).await
    }

    pub async fn configure_topic_schema(
        &self,
        req: danube_schema::ConfigureTopicSchemaRequest,
    ) -> Result<danube_schema::ConfigureTopicSchemaResponse> {
        let mut client =
            danube_schema::schema_registry_client::SchemaRegistryClient::new(self.channel.clone());
        let fut = async move { client.configure_topic_schema(req).await };
        self.execute_with_timeout(fut).await
    }

    pub async fn get_topic_schema_config(
        &self,
        req: danube_schema::GetTopicSchemaConfigRequest,
    ) -> Result<danube_schema::GetTopicSchemaConfigResponse> {
        let mut client =
            danube_schema::schema_registry_client::SchemaRegistryClient::new(self.channel.clone());
        let fut = async move { client.get_topic_schema_config(req).await };
        self.execute_with_timeout(fut).await
    }

    pub async fn update_topic_validation_policy(
        &self,
        req: danube_schema::UpdateTopicValidationPolicyRequest,
    ) -> Result<danube_schema::UpdateTopicValidationPolicyResponse> {
        let mut client =
            danube_schema::schema_registry_client::SchemaRegistryClient::new(self.channel.clone());
        let fut = async move { client.update_topic_validation_policy(req).await };
        self.execute_with_timeout(fut).await
    }
}

use anyhow::{anyhow, Result};
use danube_core::admin_proto as admin;
use std::time::Duration;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};

pub struct AdminGrpcClient {
    channel: Channel,
    timeout: Duration,
}

#[derive(Clone, Debug, Default)]
pub struct GrpcClientOptions {
    pub request_timeout_ms: u64,
    pub enable_tls: Option<bool>,
    pub domain: Option<String>,
    pub ca_path: Option<String>,
    pub cert_path: Option<String>,
    pub key_path: Option<String>,
}

impl AdminGrpcClient {
    pub async fn connect(seed_endpoint: String, opts: GrpcClientOptions) -> Result<Self> {
        // Accept either full URL (http/https) or host:port; default to http if no scheme
        let endpoint_url =
            if seed_endpoint.starts_with("http://") || seed_endpoint.starts_with("https://") {
                seed_endpoint
            } else {
                format!("http://{}", seed_endpoint)
            };

        let mut endpoint = Endpoint::from_shared(endpoint_url.clone())?.tcp_nodelay(true);

        // TLS enablement mimics danube-admin-cli: https scheme OR DANUBE_ADMIN_TLS=true
        let enable_tls = opts.enable_tls.unwrap_or_else(|| {
            endpoint_url.starts_with("https://")
                || std::env::var("DANUBE_ADMIN_TLS")
                    .map(|v| v == "true")
                    .unwrap_or(false)
        });

        if enable_tls {
            let domain = opts
                .domain
                .or_else(|| std::env::var("DANUBE_ADMIN_DOMAIN").ok())
                .unwrap_or_else(|| "localhost".to_string());
            let mut tls = ClientTlsConfig::new().domain_name(domain);

            // Optional Root CA
            if let Some(ca_path) = opts
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
                opts.cert_path.or(cert_path_env),
                opts.key_path.or(key_path_env),
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
            timeout: Duration::from_millis(opts.request_timeout_ms),
        })
    }

    pub async fn list_brokers(&self) -> Result<admin::BrokerListResponse> {
        let mut client = admin::broker_admin_client::BrokerAdminClient::new(self.channel.clone());
        let fut = async move { client.list_brokers(admin::Empty {}).await };
        match tokio::time::timeout(self.timeout, fut).await {
            Err(_) => Err(anyhow!("upstream timeout")),
            Ok(Err(status)) => Err(anyhow!(status)),
            Ok(Ok(resp)) => Ok(resp.into_inner()),
        }
    }

    pub async fn get_leader(&self) -> Result<admin::BrokerResponse> {
        let mut client = admin::broker_admin_client::BrokerAdminClient::new(self.channel.clone());
        let fut = async move { client.get_leader_broker(admin::Empty {}).await };
        match tokio::time::timeout(self.timeout, fut).await {
            Err(_) => Err(anyhow!("upstream timeout")),
            Ok(Err(status)) => Err(anyhow!(status)),
            Ok(Ok(resp)) => Ok(resp.into_inner()),
        }
    }

    pub async fn list_namespaces(&self) -> Result<admin::NamespaceListResponse> {
        let mut client = admin::broker_admin_client::BrokerAdminClient::new(self.channel.clone());
        let fut = async move { client.list_namespaces(admin::Empty {}).await };
        match tokio::time::timeout(self.timeout, fut).await {
            Err(_) => Err(anyhow!("upstream timeout")),
            Ok(Err(status)) => Err(anyhow!(status)),
            Ok(Ok(resp)) => Ok(resp.into_inner()),
        }
    }

    pub async fn list_topics(&self, namespace: &str) -> Result<admin::TopicListResponse> {
        let mut client = admin::topic_admin_client::TopicAdminClient::new(self.channel.clone());
        let req = admin::NamespaceRequest {
            name: namespace.to_string(),
        };
        let fut = async move { client.list_topics(req).await };
        match tokio::time::timeout(self.timeout, fut).await {
            Err(_) => Err(anyhow!("upstream timeout")),
            Ok(Err(status)) => Err(anyhow!(status)),
            Ok(Ok(resp)) => Ok(resp.into_inner()),
        }
    }

    pub async fn describe_topic(&self, topic: &str) -> Result<admin::DescribeTopicResponse> {
        let mut client = admin::topic_admin_client::TopicAdminClient::new(self.channel.clone());
        let req = admin::DescribeTopicRequest {
            name: topic.to_string(),
        };
        let fut = async move { client.describe_topic(req).await };
        match tokio::time::timeout(self.timeout, fut).await {
            Err(_) => Err(anyhow!("upstream timeout")),
            Ok(Err(status)) => Err(anyhow!(status)),
            Ok(Ok(resp)) => Ok(resp.into_inner()),
        }
    }

    pub async fn list_subscriptions(&self, topic: &str) -> Result<admin::SubscriptionListResponse> {
        let mut client = admin::topic_admin_client::TopicAdminClient::new(self.channel.clone());
        let req = admin::TopicRequest {
            name: topic.to_string(),
        };
        let fut = async move { client.list_subscriptions(req).await };
        match tokio::time::timeout(self.timeout, fut).await {
            Err(_) => Err(anyhow!("upstream timeout")),
            Ok(Err(status)) => Err(anyhow!(status)),
            Ok(Ok(resp)) => Ok(resp.into_inner()),
        }
    }

    pub async fn get_namespace_policies(&self, namespace: &str) -> Result<admin::PolicyResponse> {
        let mut client =
            admin::namespace_admin_client::NamespaceAdminClient::new(self.channel.clone());
        let req = admin::NamespaceRequest {
            name: namespace.to_string(),
        };
        let fut = async move { client.get_namespace_policies(req).await };
        match tokio::time::timeout(self.timeout, fut).await {
            Err(_) => Err(anyhow!("upstream timeout")),
            Ok(Err(status)) => Err(anyhow!(status.to_string())),
            Ok(Ok(resp)) => Ok(resp.into_inner()),
        }
    }
}

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
        let fut = async move { client.list_namespace_topics(req).await };
        match tokio::time::timeout(self.timeout, fut).await {
            Err(_) => Err(anyhow!("upstream timeout")),
            Ok(Err(status)) => Err(anyhow!(status.to_string())),
            Ok(Ok(resp)) => {
                let detailed = resp.into_inner();
                let topics: Vec<String> = detailed.topics.into_iter().map(|ti| ti.name).collect();
                Ok(admin::TopicListResponse { topics })
            }
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

    pub async fn list_broker_topics(
        &self,
        broker_id: &str,
    ) -> Result<admin::TopicInfoListResponse> {
        let mut client = admin::topic_admin_client::TopicAdminClient::new(self.channel.clone());
        let req = admin::BrokerRequest {
            broker_id: broker_id.to_string(),
        };
        let fut = async move { client.list_broker_topics(req).await };
        match tokio::time::timeout(self.timeout, fut).await {
            Err(_) => Err(anyhow!("upstream timeout")),
            Ok(Err(status)) => Err(anyhow!(status.to_string())),
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

    pub async fn create_topic(
        &self,
        name: &str,
        schema_type: &str,
        schema_data: &str,
        dispatch_strategy: i32,
    ) -> Result<admin::TopicResponse> {
        let mut client = admin::topic_admin_client::TopicAdminClient::new(self.channel.clone());
        let req = admin::NewTopicRequest {
            name: name.to_string(),
            schema_type: schema_type.to_string(),
            schema_data: schema_data.to_string(),
            dispatch_strategy,
        };
        let fut = async move { client.create_topic(req).await };
        match tokio::time::timeout(self.timeout, fut).await {
            Err(_) => Err(anyhow!("upstream timeout")),
            Ok(Err(status)) => Err(anyhow!(status)),
            Ok(Ok(resp)) => Ok(resp.into_inner()),
        }
    }

    pub async fn create_partitioned_topic(
        &self,
        base_name: &str,
        partitions: u32,
        schema_type: &str,
        schema_data: &str,
        dispatch_strategy: i32,
    ) -> Result<admin::TopicResponse> {
        let mut client = admin::topic_admin_client::TopicAdminClient::new(self.channel.clone());
        let req = admin::PartitionedTopicRequest {
            base_name: base_name.to_string(),
            partitions,
            schema_type: schema_type.to_string(),
            schema_data: schema_data.to_string(),
            dispatch_strategy,
        };
        let fut = async move { client.create_partitioned_topic(req).await };
        match tokio::time::timeout(self.timeout, fut).await {
            Err(_) => Err(anyhow!("upstream timeout")),
            Ok(Err(status)) => Err(anyhow!(status)),
            Ok(Ok(resp)) => Ok(resp.into_inner()),
        }
    }

    pub async fn delete_topic(&self, name: &str) -> Result<admin::TopicResponse> {
        let mut client = admin::topic_admin_client::TopicAdminClient::new(self.channel.clone());
        let req = admin::TopicRequest {
            name: name.to_string(),
        };
        let fut = async move { client.delete_topic(req).await };
        match tokio::time::timeout(self.timeout, fut).await {
            Err(_) => Err(anyhow!("upstream timeout")),
            Ok(Err(status)) => Err(anyhow!(status)),
            Ok(Ok(resp)) => Ok(resp.into_inner()),
        }
    }

    pub async fn unload_topic(&self, name: &str) -> Result<admin::TopicResponse> {
        let mut client = admin::topic_admin_client::TopicAdminClient::new(self.channel.clone());
        let req = admin::TopicRequest {
            name: name.to_string(),
        };
        let fut = async move { client.unload_topic(req).await };
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

    pub async fn unload_broker(
        &self,
        broker_id: &str,
        max_parallel: u32,
        namespaces_include: Vec<String>,
        namespaces_exclude: Vec<String>,
        dry_run: bool,
        timeout_seconds: u32,
    ) -> Result<admin::UnloadBrokerResponse> {
        let mut client = admin::broker_admin_client::BrokerAdminClient::new(self.channel.clone());
        let req = admin::UnloadBrokerRequest {
            broker_id: broker_id.to_string(),
            max_parallel,
            namespaces_include,
            namespaces_exclude,
            dry_run,
            timeout_seconds,
        };
        let fut = async move { client.unload_broker(req).await };
        match tokio::time::timeout(self.timeout, fut).await {
            Err(_) => Err(anyhow!("upstream timeout")),
            Ok(Err(status)) => Err(anyhow!(status)),
            Ok(Ok(resp)) => Ok(resp.into_inner()),
        }
    }

    pub async fn activate_broker(
        &self,
        broker_id: &str,
        reason: &str,
    ) -> Result<admin::ActivateBrokerResponse> {
        let mut client = admin::broker_admin_client::BrokerAdminClient::new(self.channel.clone());
        let req = admin::ActivateBrokerRequest {
            broker_id: broker_id.to_string(),
            reason: reason.to_string(),
        };
        let fut = async move { client.activate_broker(req).await };
        match tokio::time::timeout(self.timeout, fut).await {
            Err(_) => Err(anyhow!("upstream timeout")),
            Ok(Err(status)) => Err(anyhow!(status)),
            Ok(Ok(resp)) => Ok(resp.into_inner()),
        }
    }
}

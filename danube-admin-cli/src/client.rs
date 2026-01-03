use std::env;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint, Certificate, Identity};

use danube_core::admin_proto::{
    broker_admin_client::BrokerAdminClient,
    namespace_admin_client::NamespaceAdminClient,
    topic_admin_client::TopicAdminClient,
};
use danube_core::proto::danube_schema::schema_registry_client::SchemaRegistryClient;

const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:50051";

pub async fn admin_channel() -> Result<Channel, Box<dyn std::error::Error>> {
    let endpoint_url = env::var("DANUBE_ADMIN_ENDPOINT").unwrap_or_else(|_| DEFAULT_ENDPOINT.to_string());
    let mut endpoint = Endpoint::from_shared(endpoint_url.clone())?
        .tcp_nodelay(true);

    // Enable TLS if endpoint is https or DANUBE_ADMIN_TLS=true
    let enable_tls = endpoint_url.starts_with("https://") || env::var("DANUBE_ADMIN_TLS").map(|v| v == "true").unwrap_or(false);
    if enable_tls {
        // Domain name for TLS verification
        let domain = env::var("DANUBE_ADMIN_DOMAIN").unwrap_or_else(|_| "localhost".to_string());

        let mut tls = ClientTlsConfig::new().domain_name(domain);

        // Root CA (optional)
        if let Ok(ca_path) = env::var("DANUBE_ADMIN_CA") {
            let ca_pem = tokio::fs::read(ca_path).await?;
            let ca = Certificate::from_pem(ca_pem);
            tls = tls.ca_certificate(ca);
        }

        // Client identity for mTLS (optional)
        if let (Ok(cert_path), Ok(key_path)) = (env::var("DANUBE_ADMIN_CERT"), env::var("DANUBE_ADMIN_KEY")) {
            let cert = tokio::fs::read(cert_path).await?;
            let key = tokio::fs::read(key_path).await?;
            let identity = Identity::from_pem(cert, key);
            tls = tls.identity(identity);
        }

        endpoint = endpoint.tls_config(tls)?;
    }

    Ok(endpoint.connect().await?)
}

pub async fn broker_admin_client() -> Result<BrokerAdminClient<Channel>, Box<dyn std::error::Error>> {
    let ch = admin_channel().await?;
    Ok(BrokerAdminClient::new(ch))
}

pub async fn namespace_admin_client() -> Result<NamespaceAdminClient<Channel>, Box<dyn std::error::Error>> {
    let ch = admin_channel().await?;
    Ok(NamespaceAdminClient::new(ch))
}

pub async fn topic_admin_client() -> Result<TopicAdminClient<Channel>, Box<dyn std::error::Error>> {
    let ch = admin_channel().await?;
    Ok(TopicAdminClient::new(ch))
}

pub async fn schema_registry_client() -> Result<SchemaRegistryClient<Channel>, Box<dyn std::error::Error>> {
    let ch = admin_channel().await?;
    Ok(SchemaRegistryClient::new(ch))
}

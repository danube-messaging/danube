//! Shared test utilities for danube-broker integration tests

extern crate danube_client;

use anyhow::Result;
use danube_client::{DanubeClient, SchemaType, SubType};
use rustls::crypto;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::OnceCell;
use tokio::time::{sleep, timeout, Duration};

static CRYPTO_PROVIDER: OnceCell<()> = OnceCell::const_new();

pub async fn setup_client() -> Result<DanubeClient> {
    CRYPTO_PROVIDER
        .get_or_init(|| async {
            let crypto_provider = crypto::ring::default_provider();
            crypto_provider
                .install_default()
                .expect("Failed to install default CryptoProvider");
        })
        .await;

    let client = DanubeClient::builder()
        .service_url("https://127.0.0.1:6650")
        .with_tls("../cert/ca-cert.pem")?
        .build()
        .await?;

    Ok(client)
}

pub fn unique_topic(prefix: &str) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    format!("{}-{}", prefix, now)
}

pub async fn build_string_producer(
    client: &DanubeClient,
    topic: &str,
    name: &str,
) -> Result<danube_client::Producer> {
    let mut producer = client
        .new_producer()
        .with_topic(topic)
        .with_name(name)
        .with_schema("schema_str".into(), SchemaType::String)
        .build();
    producer.create().await?;
    Ok(producer)
}

pub async fn build_string_producer_partitioned(
    client: &DanubeClient,
    topic: &str,
    name: &str,
    partitions: usize,
) -> Result<danube_client::Producer> {
    let mut producer = client
        .new_producer()
        .with_topic(topic)
        .with_name(name)
        .with_schema("schema_str".into(), SchemaType::String)
        .with_partitions(partitions)
        .build();
    producer.create().await?;
    Ok(producer)
}

pub async fn build_consumer(
    client: &DanubeClient,
    topic: &str,
    consumer_name: &str,
    subscription: &str,
    sub_type: SubType,
) -> Result<danube_client::Consumer> {
    let mut consumer = client
        .new_consumer()
        .with_topic(topic.to_string())
        .with_consumer_name(consumer_name.to_string())
        .with_subscription(subscription.to_string())
        .with_subscription_type(sub_type)
        .build();
    consumer.subscribe().await?;
    Ok(consumer)
}

// (run_basic_subscription moved to subscription_basic.rs)

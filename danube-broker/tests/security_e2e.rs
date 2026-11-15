//! Security E2E integration test.
//!
//! Requires a broker started with TLS + JWT auth (`danube up ... --auth tls`).
//! The security-e2e CI workflow provisions RBAC via `danube auth` before running these tests.
//!
//! Environment variables (set by the workflow):
//!   DANUBE_BROKER_ENDPOINT  — e.g. "https://127.0.0.1:6650"
//!   DANUBE_CA_CERT           — path to CA cert, e.g. "./cert/ca-cert.pem"
//!   DANUBE_AUTHORIZED_TOKEN  — JWT for a principal with Produce+Consume on the test topic
//!   DANUBE_DENIED_TOKEN      — JWT for a principal with NO bindings

extern crate danube_client;

use anyhow::Result;
use danube_client::{DanubeClient, SubType};
use rustls::crypto;
use tokio::sync::OnceCell;
use tokio::time::{sleep, Duration};

static CRYPTO_PROVIDER: OnceCell<()> = OnceCell::const_new();

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{} env var must be set", name))
}

async fn build_client(token: &str) -> Result<DanubeClient> {
    CRYPTO_PROVIDER
        .get_or_init(|| async {
            let crypto_provider = crypto::ring::default_provider();
            crypto_provider
                .install_default()
                .expect("Failed to install default CryptoProvider");
        })
        .await;

    let endpoint = env("DANUBE_BROKER_ENDPOINT");
    let ca_cert = env("DANUBE_CA_CERT");

    let client = DanubeClient::builder()
        .service_url(&endpoint)
        .with_tls(&ca_cert)?
        .with_token(token)
        .build()
        .await?;

    Ok(client)
}

/// Tests that an authorized principal can create a producer and send a message
/// to a topic they have a Produce binding for.
///
/// **Precondition:** The CI workflow has created a `producer-role` (Produce, Lookup)
/// and bound the authorized principal to `/default/security-test`.
///
/// **Expected Outcome:** Producer creation and message send both succeed.
#[tokio::test]
#[ignore = "requires security e2e workflow"]
async fn security_authorized_produce() -> Result<()> {
    let token = env("DANUBE_AUTHORIZED_TOKEN");
    let client = build_client(&token).await?;

    let topic = "/default/security-test";

    let mut producer = client
        .new_producer()
        .with_topic(topic)
        .with_name("security-e2e-producer")
        .build()?;

    producer.create().await?;

    sleep(Duration::from_millis(300)).await;

    let receipt = producer
        .send("security-e2e-message".as_bytes().to_vec(), None)
        .await;

    assert!(
        receipt.is_ok(),
        "Authorized producer should be able to send a message, got: {:?}",
        receipt.err()
    );

    Ok(())
}

/// Tests that an authorized principal can subscribe and receive messages
/// from a topic they have a Consume binding for.
///
/// **Precondition:** The CI workflow has created a `consumer-role` (Consume, Lookup)
/// and bound the authorized principal to `/default/security-test`.
/// A message has been produced by the previous test.
///
/// **Expected Outcome:** Subscription creation succeeds.
#[tokio::test]
#[ignore = "requires security e2e workflow"]
async fn security_authorized_consume() -> Result<()> {
    let token = env("DANUBE_AUTHORIZED_TOKEN");
    let client = build_client(&token).await?;

    let topic = "/default/security-test";

    let mut consumer = client
        .new_consumer()
        .with_topic(topic)
        .with_consumer_name("security-e2e-consumer")
        .with_subscription("security-e2e-sub")
        .with_subscription_type(SubType::Exclusive)
        .build()?;

    let subscribe_result = consumer.subscribe().await;

    assert!(
        subscribe_result.is_ok(),
        "Authorized consumer should be able to subscribe, got: {:?}",
        subscribe_result.err()
    );

    Ok(())
}

/// Regression test: a principal with only a **topic-scoped** Consume binding must be
/// able to receive and ack, not just subscribe. `receive_messages`, `ack` and `nack`
/// used to authorize at cluster scope, so this flow failed after `subscribe`.
///
/// **Precondition:** The CI workflow bound the authorized principal to
/// `/default/security-test` with `producer-role` and `consumer-role` at topic scope.
///
/// **Expected Outcome:** The consumer receives the message and the ack succeeds.
#[tokio::test]
#[ignore = "requires security e2e workflow"]
async fn security_topic_scoped_consume_receive_and_ack() -> Result<()> {
    let token = env("DANUBE_AUTHORIZED_TOKEN");
    let client = build_client(&token).await?;
    let topic = "/default/security-test";

    // Start the consumer first: a new subscription starts at the end of the topic.
    let mut consumer = client
        .new_consumer()
        .with_topic(topic)
        .with_consumer_name("security-e2e-receiver")
        .with_subscription("security-e2e-receive-sub")
        .with_subscription_type(SubType::Exclusive)
        .build()?;
    consumer.subscribe().await?;
    let mut messages = consumer.receive().await?;
    sleep(Duration::from_millis(500)).await;

    let mut producer = client
        .new_producer()
        .with_topic(topic)
        .with_name("security-e2e-receive-producer")
        .build()?;
    producer.create().await?;
    producer
        .send("topic-scoped-receive".as_bytes().to_vec(), None)
        .await?;

    let message = tokio::time::timeout(Duration::from_secs(10), messages.recv())
        .await
        .expect("topic-scoped consumer should receive a message (receive_messages authz)")
        .expect("receive channel closed");
    assert_eq!(message.payload.as_ref(), b"topic-scoped-receive");

    let ack = consumer.ack(&message).await;
    assert!(
        ack.is_ok(),
        "topic-scoped consumer should be able to ack, got: {:?}",
        ack.err()
    );

    Ok(())
}

/// Tests that a principal with NO RBAC bindings is denied when trying to produce.
///
/// **Precondition:** The `DANUBE_DENIED_TOKEN` JWT belongs to a principal
/// with no roles or bindings configured.
///
/// **Expected Outcome:** Producer creation fails with a PermissionDenied error.
#[tokio::test]
#[ignore = "requires security e2e workflow"]
async fn security_denied_produce() -> Result<()> {
    let token = env("DANUBE_DENIED_TOKEN");
    let client = build_client(&token).await?;

    let topic = "/default/security-test";

    let mut producer = client
        .new_producer()
        .with_topic(topic)
        .with_name("security-e2e-denied-producer")
        .build()?;

    let result = producer.create().await;

    assert!(
        result.is_err(),
        "Unprivileged principal should be denied producer creation"
    );

    // Verify it's a permission error (not a transport error)
    let err_msg = format!("{}", result.unwrap_err());
    assert!(
        err_msg.contains("PermissionDenied") || err_msg.contains("permission"),
        "Error should be permission-related, got: {}",
        err_msg
    );

    Ok(())
}

extern crate danube_client;
extern crate futures_util;

use anyhow::Result;
use danube_client::{Consumer, DanubeClient, Producer, SchemaType, SubType};
use std::sync::Arc;
use tokio::time::{sleep, timeout, Duration};

struct TestSetup {
    client: Arc<DanubeClient>,
}

async fn setup() -> Result<TestSetup> {
    let client = Arc::new(
        DanubeClient::builder()
            .service_url("http://127.0.0.1:6650")
            .build()
            .await?,
    );

    Ok(TestSetup { client })
}

async fn setup_producer(
    client: Arc<DanubeClient>,
    topic: &str,
    producer_name: &str,
    partitions: usize,
) -> Result<Producer> {
    let mut producer = client
        .new_producer()
        .with_topic(topic)
        .with_name(producer_name)
        .with_schema("my_schema".into(), SchemaType::String)
        .with_partitions(partitions)
        .build();

    producer.create().await?;

    Ok(producer)
}

async fn setup_consumer(
    client: Arc<DanubeClient>,
    topic: &str,
    consumer_name: &str,
    sub_type: SubType,
) -> Result<Consumer> {
    let mut consumer = client
        .new_consumer()
        .with_topic(topic.to_string())
        .with_consumer_name(consumer_name.to_string())
        .with_subscription(format!("test_subscription_{}", consumer_name))
        .with_subscription_type(sub_type)
        .build();

    // Ensure the consumer is connected and subscribed
    consumer.subscribe().await?;
    Ok(consumer)
}

#[tokio::test]
async fn test_exclusive_subscription() -> Result<()> {
    let setup = setup().await?;
    let topic = "/default/topic_exclusive_subsc";
    let partitions = 3;

    let producer = setup_producer(
        setup.client.clone(),
        topic,
        "producer_exclusive",
        partitions,
    )
    .await?;

    let mut consumer = setup_consumer(
        setup.client.clone(),
        topic,
        "consumer_exclusive",
        SubType::Exclusive,
    )
    .await?;

    // Start receiving messages
    let mut message_stream = consumer.receive().await?;

    sleep(Duration::from_millis(500)).await;

    // Define messages to send
    let messages = vec!["Hello Danube 1", "Hello Danube 2", "Hello Danube 3"];

    // Produce messages
    for msg in &messages {
        producer.send(msg.as_bytes().into(), None).await?;
        println!("Message sent: {}", msg);
    }

    // Add a timeout to avoid blocking indefinitely
    let receive_future = async {
        let mut received_messages = vec![];

        while let Some(stream_message) = message_stream.recv().await {
            let payload = String::from_utf8(stream_message.payload).unwrap();
            println!("Message received: {}", payload);
            received_messages.push(payload);

            // Break early if we've received all messages
            if received_messages.len() == messages.len() {
                break;
            }
        }

        received_messages
    };

    let result = timeout(Duration::from_secs(10), receive_future).await?;
    let received_messages = result;

    // Check if all expected messages were received
    assert_eq!(
        received_messages.len(),
        messages.len(),
        "Not all messages were received"
    );
    for expected in &messages {
        assert!(
            received_messages.contains(&expected.to_string()),
            "Expected message '{}' not found",
            expected
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_shared_subscription() -> Result<()> {
    let setup = setup().await?;
    let topic = "/default/topic_shared_subsc";
    let partitions = 3;

    let producer =
        setup_producer(setup.client.clone(), topic, "producer_shared", partitions).await?;

    let mut consumer = setup_consumer(
        setup.client.clone(),
        topic,
        "consumer_shared",
        SubType::Shared,
    )
    .await?;

    // Start receiving messages
    let mut message_stream = consumer.receive().await?;

    sleep(Duration::from_millis(500)).await;

    // Define messages to send
    let messages = vec!["Hello Danube 1", "Hello Danube 2", "Hello Danube 3"];

    // Produce messages
    for msg in &messages {
        producer.send(msg.as_bytes().into(), None).await?;
        println!("Message sent: {}", msg);
    }

    // Add a timeout to avoid blocking indefinitely
    let receive_future = async {
        let mut received_messages = vec![];

        while let Some(stream_message) = message_stream.recv().await {
            let payload = String::from_utf8(stream_message.payload).unwrap();
            println!("Message received: {}", payload);
            received_messages.push(payload);

            // Break early if we've received all messages
            if received_messages.len() == messages.len() {
                break;
            }
        }

        received_messages
    };

    let result = timeout(Duration::from_secs(10), receive_future).await?;
    let received_messages = result;

    // Check if all expected messages were received
    assert_eq!(
        received_messages.len(),
        messages.len(),
        "Not all messages were received"
    );
    for expected in &messages {
        assert!(
            received_messages.contains(&expected.to_string()),
            "Expected message '{}' not found",
            expected
        );
    }

    Ok(())
}

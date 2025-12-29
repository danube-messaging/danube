use anyhow::Result;
use danube_client::{DanubeClient, SubType};
use serde::Deserialize;

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct MyMessage {
    field1: String,
    field2: i32,
}

#[tokio::main]
async fn main() -> Result<()> {
    let client = DanubeClient::builder()
        .service_url("http://127.0.0.1:6650")
        .build()
        .await?;

    let topic = "/default/test_topic";
    let consumer_name = "cons_json";
    let subscription_name = "subs_json";

    let mut consumer = client
        .new_consumer()
        .with_topic(topic)
        .with_consumer_name(consumer_name)
        .with_subscription(subscription_name)
        .with_subscription_type(SubType::Exclusive)
        .build();

    // Subscribe to the topic
    consumer.subscribe().await?;
    println!("The Consumer {} was created", consumer_name);

    // Phase 5: Start receiving typed messages with automatic deserialization
    let mut message_stream = consumer.receive_typed::<MyMessage>().await?;

    while let Some(result) = message_stream.recv().await {
        match result {
            Ok((message, decoded_message)) => {
                // Phase 5: Message is automatically deserialized
                println!("Received message: {:?}", decoded_message);

                // Acknowledge the message
                if let Err(e) = consumer.ack(&message).await {
                    eprintln!("Failed to acknowledge message: {}", e);
                }
            }
            Err(e) => {
                eprintln!("Failed to deserialize message: {}", e);
            }
        }
    }

    Ok(())
}

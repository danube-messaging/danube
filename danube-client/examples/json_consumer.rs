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
    // Setup tracing
    tracing_subscriber::fmt::init();

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

    let _schema = client.get_schema(topic).await.unwrap();

    // Start receiving messages
    let mut message_stream = consumer.receive().await?;

    while let Some(message) = message_stream.recv().await {
        let payload = message.payload.clone();
        // Deserialize the message using the schema
        match serde_json::from_slice::<MyMessage>(&payload) {
            Ok(decoded_message) => {
                println!("Received message: {:?}", decoded_message);

                // Acknowledge the message
                if let Err(e) = consumer.ack(&message).await {
                    eprintln!("Failed to acknowledge message: {}", e);
                }
            }
            Err(e) => {
                eprintln!("Failed to decode message: {}", e);
            }
        }
    }

    Ok(())
}

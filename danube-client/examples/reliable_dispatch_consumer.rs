use anyhow::Result;
use danube_client::{DanubeClient, SubType};

#[tokio::main]
async fn main() -> Result<()> {
    let client = DanubeClient::builder()
        .service_url("http://127.0.0.1:6650")
        .build()
        .await?;

    let topic = "/default/reliable_topic";
    let consumer_name = "cons_reliable";
    let subscription_name = "subs_reliable";

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

    // Start receiving messages
    let mut message_stream = consumer.receive().await?;
    let mut total_received_size = 0;

    while let Some(message) = message_stream.recv().await {
        let payload = message.payload.clone();
        total_received_size += payload.len();

        match String::from_utf8(payload) {
            Ok(message_str) => {
                println!(
                    "Received message: {} | offset: {} | total received bytes: {}",
                    message_str, &message.msg_id.topic_offset, total_received_size
                );

                consumer.ack(&message).await?;
            }
            Err(e) => println!("Failed to convert Payload to String: {}", e),
        }
    }

    Ok(())
}

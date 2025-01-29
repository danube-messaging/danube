use anyhow::Result;
use danube_client::DanubeClient;
use std::thread;
use std::time::Duration;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    // Setup tracing
    tracing_subscriber::fmt::init();

    let client = DanubeClient::builder()
        .service_url("http://127.0.0.1:6650")
        .build()
        .await?;

    let topic = "/default/partitioned_topic";
    let producer_name = "prod_part";

    let mut producer = client
        .new_producer()
        .with_topic(topic)
        .with_name(producer_name)
        .with_partitions(3)
        .build();

    producer.create().await?;
    info!("The Producer {} was created", producer_name);

    let mut i = 0;

    while i < 100 {
        let encoded_data = format!("Hello Danube {}", i).as_bytes().to_vec();

        let message_id = producer.send(encoded_data, None).await?;
        println!("The Message with id {} was sent", message_id);

        thread::sleep(Duration::from_secs(1));
        i += 1;
    }

    Ok(())
}

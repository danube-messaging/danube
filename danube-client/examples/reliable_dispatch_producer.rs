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

    let topic = "/default/reliable_topic";
    let producer_name = "prod_json_reliable";

    let mut producer = client
        .new_producer()
        .with_topic(topic)
        .with_name(producer_name)
        .with_reliable_dispatch()
        .build();

    producer.create().await?;
    info!("The Producer {} was created", producer_name);

    // Define word variations for dynamic message generation
    let subjects = [
        "The Danube system",
        "Danube platform",
        "Danube messaging service",
        "The Danube application",
    ];
    let verbs = ["processes", "handles", "manages", "delivers"];
    let objects = [
        "messages efficiently",
        "data reliably",
        "requests quickly",
        "events seamlessly",
    ];
    let conclusions = [
        "with high performance.",
        "at scale.",
        "with real-time.",
        "without issues.",
    ];

    let mut i = 0;

    while i < 100 {
        // Generate a dynamic message with 3-4 phrases
        let message = format!(
            "{} {} {}, with low latency and high throughput. It is designed for reliability, {}",
            subjects[i % subjects.len()],
            verbs[i % verbs.len()],
            objects[i % objects.len()],
            conclusions[i % conclusions.len()]
        );

        match producer.send(message.into_bytes(), None).await {
            Ok(message_id) => {
                println!("The Message with id {} was sent", message_id);
            }
            Err(e) => {
                eprintln!("Failed to send message: {}", e);
            }
        }

        thread::sleep(Duration::from_secs(1));
        i += 1;
    }

    Ok(())
}

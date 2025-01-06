# Danube-client

An async Rust client library for interacting with Danube Messaging platform.

[Danube](https://github.com/danube-messaging/danube) is an open-source **distributed** Messaging Broker platform written in Rust. Consult [the documentation](https://danube-docs.dev-state.com/) for supported concepts and the platform architecture.

## Example usage

Check out the [example files](https://github.com/danube-messaging/danube/tree/main/danube-client/examples).

### Producer

```rust
let client = DanubeClient::builder()
    .service_url("http://127.0.0.1:6650")
    .build()
    .unwrap();

let topic_name = "/default/test_topic";
let producer_name = "test_prod";

let mut producer = client
    .new_producer()
    .with_topic(topic_name)
    .with_name(producer_name)
    .build();

producer.create().await?;
println!("The Producer {} was created", producer_name);

let encoded_data = "Hello Danube".as_bytes().to_vec();

let message_id = producer.send(encoded_data, None).await?;
println!("The Message with id {} was sent", message_id);
```

### Consumer

```rust
let client = DanubeClient::builder()
        .service_url("http://127.0.0.1:6650")
        .build()
        .unwrap();

    let topic = "/default/test_topic";
    let consumer_name = "test_cons";
    let subscription_name = "test_subs";

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

    while let Some(message) = message_stream.recv().await {
        let payload = message.payload;

        match String::from_utf8(payload) {
            Ok(message_str) => {
                println!("Received message: {:?}", message_str);

                consumer.ack(&message).await?;
            }
            Err(e) => println!("Failed to convert Payload to String: {}", e),
        }
    }
```

## Contribution

Check [the documentation](https://danube-docs.dev-state.com/) on how to setup a Danube Broker.

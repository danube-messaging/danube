# Danube Client

An async Rust client library for interacting with Danube Messaging platform.

🌊 [Danube Messaging](https://github.com/danube-messaging/danube) is an open-source **distributed** Messaging Broker platform written in Rust. Consult [the documentation](https://danube-docs.dev-state.com/) for supported concepts and the platform architecture.

## Features

### 📤 Producer Capabilities

- **Basic Messaging** - Send messages with byte payloads and optional key-value attributes
- **Partitioned Topics** - Distribute messages across multiple partitions for horizontal scaling
- **Reliable Dispatch** - Guaranteed message delivery with persistence (WAL + cloud storage)
- **Schema Integration** - Type-safe messaging with automatic validation (Bytes, String, Number,Avro, JSON Schema, Protobuf)

### 📥 Consumer Capabilities

- **Flexible Subscriptions** - Three subscription types for different use cases:
  - **Exclusive** - Single active consumer, guaranteed ordering
  - **Shared** - Load balancing across multiple consumers, parallel processing
  - **Failover** - High availability with automatic standby promotion
- **Message Acknowledgment** - Reliable message processing with at-least-once delivery
- **Partitioned Consumption** - Automatic handling of messages from all partitions
- **Batch Processing** - Efficient batch consumption for high-throughput scenarios
- **Message Attributes** - Access metadata and custom headers

### 🔐 Schema Registry

- **Schema Management** - Register, version, and retrieve schemas (JSON Schema, Avro, Protobuf)
- **Compatibility Checking** - Validate schema evolution (Backward, Forward, Full, None modes)
- **Type Safety** - Automatic validation against registered schemas
- **Schema Evolution** - Safe schema updates with compatibility enforcement
- **Startup Validation** - Validate consumer structs against schemas before processing

### 🏗️ Client Features

- **Async/Await** - Built on Tokio for efficient async I/O
- **Connection Pooling** - Shared connection management across producers/consumers
- **Automatic Reconnection** - Resilient connection handling
- **Topic Namespaces** - Organize topics with namespace structure (`/namespace/topic-name`)

## Example Usage

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

## Advanced Features

For detailed guides and examples on advanced capabilities:

- **[Producer Advanced Features](https://danube-docs.dev-state.com/client_libraries/producer-advanced/)** - Partitions, reliable dispatch, and schema integration
- **[Consumer Advanced Features](https://danube-docs.dev-state.com/client_libraries/consumer-advanced/)** - Partitioned consumption, failover patterns, and batch processing
- **[Schema Registry Integration](https://danube-docs.dev-state.com/client_libraries/schema-registry/)** - Type-safe messaging with schema validation and evolution

Browse the [examples directory](https://github.com/danube-messaging/danube/tree/main/danube-client/examples) for complete working code.

## Contribution

Check [the documentation](https://danube-docs.dev-state.com/) on how to setup a Danube Broker.

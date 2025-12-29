# Danube Client Examples

This directory contains examples demonstrating various features of the Danube messaging platform and client library.

## Table of Contents

1. [Basic Examples](#basic-examples)
2. [Schema Registry Examples](#schema-registry-examples)
3. [Advanced Features](#advanced-features)
4. [Running Examples](#running-examples)

---

## Basic Examples

### 1. **simple_producer_consumer.rs**
**Purpose**: Demonstrates the simplest way to send and receive raw byte messages without schema validation.

**Key Features**:
- Basic producer/consumer setup
- Raw byte message passing
- Message acknowledgment
- Single producer-consumer pair

**Use Case**: Quick prototyping, simple message passing, when schema validation is not needed.

```bash
cargo run --example simple_producer_consumer
```

---

### 2. **partitions_producer.rs** & **partitions_consumer.rs**
**Purpose**: Shows how to use topic partitioning for horizontal scaling and parallel processing.

**Key Features**:
- Creating partitioned topics
- Automatic message distribution across partitions
- Parallel message consumption

**Use Case**: High-throughput scenarios where messages can be processed independently in parallel.

```bash
# Terminal 1 - Start consumer
cargo run --example partitions_consumer

# Terminal 2 - Start producer
cargo run --example partitions_producer
```

---

### 3. **reliable_dispatch_producer.rs** & **reliable_dispatch_consumer.rs**
**Purpose**: Demonstrates reliable message delivery with acknowledgments and retry mechanisms.

**Key Features**:
- Reliable dispatch mode for guaranteed delivery
- Automatic retry on failures
- Message acknowledgment tracking

**Use Case**: Critical messages that must be delivered (e.g., payment notifications, order processing).

```bash
# Terminal 1 - Start consumer
cargo run --example reliable_dispatch_consumer

# Terminal 2 - Start producer
cargo run --example reliable_dispatch_producer
```

---

## Schema Registry Examples

### 4. **json_producer.rs** & **json_consumer.rs**
**Purpose**: Shows how to use JSON Schema for message validation with typed data structures.

**Key Features**:
- JSON Schema registration in Schema Registry
- Automatic serialization/deserialization
- Type-safe message passing
- Schema validation on both producer and consumer sides

**Use Case**: Applications using JSON for structured data with schema evolution needs.

```bash
# Terminal 1 - Start consumer
cargo run --example json_consumer

# Terminal 2 - Start producer
cargo run --example json_producer
```

**Schema Type**: `json_schema`

---

### 5. **avro_producer.rs** & **avro_consumer.rs**
**Purpose**: Demonstrates Apache Avro schema usage for efficient binary serialization.

**Key Features**:
- Avro schema registration
- Compact binary encoding
- Schema evolution support
- Strongly-typed data structures

**Use Case**: High-performance applications requiring efficient serialization and schema evolution.

```bash
# Terminal 1 - Start consumer
cargo run --example avro_consumer

# Terminal 2 - Start producer
cargo run --example avro_producer
```

**Schema Type**: `avro`

**Example Schema**:
```json
{
    "type": "record",
    "name": "UserEvent",
    "namespace": "com.example.events",
    "fields": [
        {"name": "user_id", "type": "string"},
        {"name": "action", "type": "string"},
        {"name": "timestamp", "type": "long"},
        {"name": "metadata", "type": ["null", "string"], "default": null}
    ]
}
```

---

### 6. **schema_evolution.rs**
**Purpose**: Comprehensive demonstration of schema evolution and compatibility checking.

**Key Features**:
- Schema version management
- Compatibility checking (backward/forward/full)
- Safe schema evolution
- Listing all schema versions
- Retrieving latest schema

**Use Case**: Understanding how to evolve data schemas safely over time without breaking existing consumers.

```bash
cargo run --example schema_evolution
```

**Demonstrates**:
- ✅ **Compatible change**: Adding optional fields with defaults
- ❌ **Incompatible change**: Removing required fields
- Schema version history tracking
- Compatibility mode enforcement

---

## Advanced Features

### Topic Partitioning
Partitions allow horizontal scaling by distributing messages across multiple partitions:

```rust
let mut producer = client
    .new_producer()
    .with_topic("/default/my_topic")
    .with_name("my_producer")
    .with_partitions(3)  // Create 3 partitions
    .build();
```

### Reliable Dispatch
Ensures message delivery with automatic retries:

```rust
let mut producer = client
    .new_producer()
    .with_topic("/default/my_topic")
    .with_name("my_producer")
    .with_reliable_dispatch()  // Enable reliable delivery
    .build();
```

### Schema Validation
Register schemas and enable validation:

```rust
// 1. Register schema
let mut schema_client = SchemaRegistryClient::new(&client).await?;
let schema_id = schema_client
    .register_schema("my-subject")
    .with_type("avro")  // or "json_schema"
    .with_schema_data(schema_bytes)
    .execute()
    .await?;

// 2. Create producer with schema
let mut producer = client
    .new_producer()
    .with_topic("/default/my_topic")
    .with_name("my_producer")
    .with_schema_subject("my-subject")  // Link to schema
    .build();

// 3. Consumer automatically validates against schema
let mut message_stream = consumer.receive_typed::<MyType>().await?;
```

---

## Supported Schema Types

| Type | Description | Use Case |
|------|-------------|----------|
| `bytes` | Raw binary data (no validation) | Simple messaging, custom formats |
| `string` | UTF-8 text data | Plain text messages |
| `number` | Numeric data | Simple numeric values |
| `json_schema` | JSON with schema validation | Structured JSON data |
| `avro` | Apache Avro binary format | High-performance, schema evolution |
| `protobuf` | Protocol Buffers (future) | Cross-language compatibility |

---

## Schema Registry API

### Register Schema
```rust
let schema_id = schema_client
    .register_schema("my-subject")
    .with_type("avro")
    .with_schema_data(schema_bytes)
    .execute()
    .await?;
```

### Check Compatibility
```rust
let result = schema_client
    .check_compatibility("my-subject")
    .with_schema_data(new_schema_bytes)
    .execute()
    .await?;

if result.is_compatible {
    println!("✅ Safe to register!");
}
```

### List Versions
```rust
let versions = schema_client
    .list_versions("my-subject")
    .execute()
    .await?;
```

### Get Latest Schema
```rust
let schema = schema_client
    .get_latest_schema("my-subject")
    .await?;
```

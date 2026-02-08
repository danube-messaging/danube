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

### 5. **json_consumer_validated.rs**
**Purpose**: Demonstrates consumer-side schema validation against the Schema Registry at startup.

**Key Features**:
- Fetches schema from registry before consuming
- Validates Rust struct against JSON Schema definition
- Fails at startup if struct doesn't match schema
- Prevents runtime deserialization errors
- Schema version tracking and logging

**Use Case**: Production consumers that need to ensure their struct definitions match the producer's schema, preventing silent data loss or deserialization failures.

```bash
# Run the producer first to register schema
cargo run --example json_producer

# Then run the validated consumer
cargo run --example json_consumer_validated
```

**What it validates**:
- ✅ Field names match schema properties
- ✅ Field types are compatible (string, integer, etc.)
- ✅ Required fields are present
- ✅ No extra unexpected fields

**Dependencies**: Requires `jsonschema` crate for validation:
```toml
[dependencies]
jsonschema = "0.18"
```

**When to use**:
- **Development**: Catch schema mismatches early
- **CI/CD**: Validate before deployment
- **Production**: Ensure consumer compatibility

**Output Example**:
```
🔍 Fetching schema from registry for subject: my-app-events
📋 Retrieved schema version: 1
✅ Struct validated successfully against schema v1
✅ Consumer validated against schema version: 1
   Safe to proceed with typed deserialization
```

---

### 6. **avro_producer.rs** & **avro_consumer.rs**
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

### 7. **schema_evolution.rs**
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
use danube_client::{SchemaType, CompatibilityMode};

// 1. Get schema client from DanubeClient
let schema_client = client.schema();

// 2. Register schema
let schema_id = schema_client
    .register_schema("my-subject")
    .with_type(SchemaType::Avro)  // Type-safe enum: Avro, JsonSchema, Protobuf, etc.
    .with_schema_data(schema_bytes)
    .execute()
    .await?;

// 3. Check compatibility before evolution
let compat_result = schema_client
    .check_compatibility(
        "my-subject",
        new_schema_bytes,
        SchemaType::Avro,
        None,  // Use subject's default mode (or Some(CompatibilityMode::Full))
    )
    .await?;

// 4. Set compatibility mode for a subject
schema_client
    .set_compatibility_mode("critical-subject", CompatibilityMode::Full)
    .await?;

// 5. Create producer with schema subject
let mut producer = client
    .new_producer()
    .with_topic("/default/my_topic")
    .with_name("my_producer")
    .with_schema_subject("my-subject")  // Link to schema
    .build();

// 6. Consumer validates struct at startup (see json_consumer_validated.rs)
let schema_version = validate_struct_against_registry(
    &schema_client,
    "my-subject",
    &MyStruct::default(),
).await?;
```

---

## Supported Schema Types

| Type | Description | Use Case |
|------|-------------|----------|
| `SchemaType::Bytes` | Raw binary data (no validation) | Simple messaging, custom formats |
| `SchemaType::String` | UTF-8 text data | Plain text messages |
| `SchemaType::Number` | Numeric data (int, float, double) | Simple numeric values |
| `SchemaType::JsonSchema` | JSON with schema validation | Structured JSON data |
| `SchemaType::Avro` | Apache Avro binary format | High-performance, schema evolution |
| `SchemaType::Protobuf` | Protocol Buffers | Cross-language compatibility |

All schema types are available as type-safe enums for IDE auto-completion and compile-time validation.

---

## Compatibility Modes

Schema evolution is controlled by compatibility modes. Set these per-subject to define evolution rules:

| Mode | Description | Allows | Use Case |
|------|-------------|--------|----------|
| `CompatibilityMode::Backward` | New schema reads old data | Add optional fields, remove fields | **Default**. Consumers upgrade before producers |
| `CompatibilityMode::Forward` | Old schema reads new data | Add required fields, remove optional fields | Producers upgrade before consumers |
| `CompatibilityMode::Full` | Both directions | Only safe changes (add optional) | **Strictest**. Critical schemas |
| `CompatibilityMode::None` | No validation | Any change | Development/testing only |

**Example**:
```rust
// Set strict compatibility for critical schemas
schema_client
    .set_compatibility_mode("order-events", CompatibilityMode::Full)
    .await?;

// Development schemas can be flexible
schema_client
    .set_compatibility_mode("test-events", CompatibilityMode::None)
    .await?;
```

---

## Schema Registry API

### Register Schema
```rust
use danube_client::SchemaType;

let schema_id = schema_client
    .register_schema("my-subject")
    .with_type(SchemaType::Avro)  // Type-safe enum
    .with_schema_data(schema_bytes)
    .execute()
    .await?;
```

### Check Compatibility
```rust
use danube_client::{SchemaType, CompatibilityMode};

let result = schema_client
    .check_compatibility(
        "my-subject",
        new_schema_bytes,
        SchemaType::Avro,
        None,  // Optional: Some(CompatibilityMode::Full)
    )
    .await?;

if result.is_compatible {
    println!("✅ Safe to register!");
} else {
    eprintln!("❌ Incompatible: {:?}", result.errors);
}
```

### Set Compatibility Mode
```rust
use danube_client::CompatibilityMode;

schema_client
    .set_compatibility_mode("my-subject", CompatibilityMode::Full)
    .await?;
```

### List Versions
```rust
let versions = schema_client
    .list_versions("my-subject")
    .await?;

println!("Versions: {:?}", versions);  // e.g., [1, 2, 3]
```

### Get Latest Schema
```rust
let schema = schema_client
    .get_latest_schema("my-subject")
    .await?;

println!("Version: {}", schema.version);
println!("Type: {}", schema.schema_type);
```

---

## Running Examples

### Prerequisites

1. **Start the Danube broker**:
   ```bash
   # From the danube root directory
   cargo run --bin danube-broker
   ```

2. **For schema validation examples**, add `jsonschema` to your `Cargo.toml`:
   ```toml
   [dependencies]
   jsonschema = "0.18"
   ```

### Running Individual Examples

```bash
# Basic examples
cargo run --example simple_producer_consumer
cargo run --example partitions_producer
cargo run --example partitions_consumer

# Schema registry examples
cargo run --example json_producer
cargo run --example json_consumer
cargo run --example json_consumer_validated  # Requires jsonschema crate
cargo run --example avro_producer
cargo run --example avro_consumer
cargo run --example schema_evolution
```

### Recommended Learning Path

1. **Start simple**: `simple_producer_consumer.rs` - understand basic messaging
2. **Add schemas**: `json_producer.rs` + `json_consumer.rs` - learn schema registration
3. **Validate schemas**: `json_consumer_validated.rs` - production-ready validation
4. **Schema evolution**: `schema_evolution.rs` - understand compatibility rules
5. **High performance**: `avro_producer.rs` + `avro_consumer.rs` - binary serialization
6. **Scale up**: `partitions_producer.rs` + `partitions_consumer.rs` - horizontal scaling
7. **Reliability**: `reliable_dispatch_*` examples - guaranteed delivery

---

## Best Practices

### Schema Registry Usage

1. **Always register schemas** before producing messages
2. **Use type-safe enums** (`SchemaType`, `CompatibilityMode`) for API calls
3. **Validate consumer structs** at startup (see `json_consumer_validated.rs`)
4. **Set appropriate compatibility modes** per subject:
   - `Full` for critical schemas
   - `Backward` for most use cases (default)
   - `None` only for development

### Consumer Patterns

**Option 1: Manual Struct** (simple, fast)
- Define struct manually
- Serialize/deserialize with `serde_json`
- Good for: Prototypes, tightly-coupled services

**Option 2: Validated Struct** (recommended)
- Fetch schema from registry at startup
- Validate struct against schema
- Use typed struct for convenience
- Good for: Production services, schema evolution

**Option 3: Dynamic Validation** (flexible)
- Fetch schema and validate each message
- Use `serde_json::Value` for dynamic data
- Good for: Generic consumers, schema exploration

### Producer Patterns

1. **Register schema once** at startup
2. **Check compatibility** before schema updates
3. **Reuse schema_id** across restarts (idempotent registration)
4. **Set schema subject** on producer for automatic validation

# Danube CLI Documentation

Welcome to the **Danube CLI** - your command-line companion for interacting with Danube messaging system! 🚀

## What is Danube CLI?

Danube CLI is a powerful, easy-to-use command-line tool that lets you:
- 📤 **Produce** messages to topics with schema validation
- 📥 **Consume** messages from topics with automatic schema detection
- 📋 **Manage schemas** in the schema registry
- 🔄 **Test** your Danube deployment end-to-end
- 🛠️ **Develop** and debug messaging workflows

Whether you're testing a new deployment, debugging message flows, or building automation scripts, Danube CLI has you covered!

## Quick Start

### Installation

```bash
# Build from source
cargo build --release -p danube-cli

# The binary will be at: target/release/danube-cli
```

### Your First Message

```bash
# 1. Produce a message
danube-cli produce \
  -s http://localhost:6650 \
  -t /default/hello \
  -m "Hello Danube!"

# 2. Consume messages
danube-cli consume \
  -s http://localhost:6650 \
  -t /default/hello \
  -m my-subscription
```

That's it! You just sent and received your first message! 🎉

## Documentation Structure

### Getting Started
- **[Getting Started Guide](./getting-started.md)** - Installation, basic concepts, and first steps

### Producer Documentation
- **[Producer Basics](./producer-basics.md)** - Simple message production scenarios
- **[Producer Advanced](./producer-advanced.md)** - Schema registry, partitions, reliable delivery

### Consumer Documentation
- **[Consumer Basics](./consumer-basics.md)** - Simple message consumption scenarios
- **[Consumer Advanced](./consumer-advanced.md)** - Schema validation, subscription types, advanced patterns

### Schema Registry
- **[Schema Registry Guide](./schema-registry.md)** - Complete guide to managing schemas

### Examples & Recipes
- **[Common Patterns](./examples.md)** - Real-world examples and use cases

## Core Concepts

### Topics
Topics are logical channels where messages are published and consumed. Topic names follow a hierarchical structure:
```
/namespace/topic-name
```
Example: `/default/user-events`, `/production/orders`

### Producers
Producers send messages to topics. They can:
- Send messages with or without schemas
- Configure partitioning for scalability
- Enable reliable delivery for critical messages

### Consumers
Consumers receive messages from topics via subscriptions. They support:
- Multiple subscription types (Exclusive, Shared, Failover)
- Automatic schema validation
- Message acknowledgment

### Schema Registry
The schema registry provides:
- Centralized schema management
- Schema evolution with compatibility checking
- Automatic validation for producers and consumers

## Command Overview

```bash
danube-cli <COMMAND>

Commands:
  produce  Produce messages to a topic
  consume  Consume messages from a topic
  schema   Manage schemas in the schema registry
  help     Print help information
```

### Get Help Anytime

```bash
# General help
danube-cli --help

# Command-specific help
danube-cli produce --help
danube-cli consume --help
danube-cli schema --help

# Subcommand help
danube-cli schema register --help
```

## Quick Examples

### Produce with Schema
```bash
# Register a schema
danube-cli schema register user-events \
  --type json_schema \
  --file ./schemas/user.json

# Produce with the schema
danube-cli produce \
  -s http://localhost:6650 \
  -t /default/user-events \
  --schema-subject user-events \
  -m '{"user_id":"u123","action":"login"}'
```

### Consume with Validation
```bash
# Consumer automatically detects and validates against schemas
danube-cli consume \
  -s http://localhost:6650 \
  -t /default/user-events \
  -m my-subscription
```

### Check Schema Compatibility
```bash
# Test a new schema version before registering
danube-cli schema check user-events \
  --type json_schema \
  --file ./schemas/user_v2.json
```

## Common Use Cases

| Use Case | Documentation |
|----------|---------------|
| Simple pub/sub | [Producer Basics](./producer-basics.md), [Consumer Basics](./consumer-basics.md) |
| Schema-validated messaging | [Schema Registry Guide](./schema-registry.md) |
| High-throughput scenarios | [Producer Advanced](./producer-advanced.md) |
| Testing & debugging | [Examples](./examples.md) |
| Message routing | [Producer Advanced](./producer-advanced.md) |

## Configuration

### Default Values

```bash
Service URL:     http://127.0.0.1:6650
Topic:           /default/test_topic
Producer Name:   test_producer
Consumer Name:   consumer_pubsub
Subscription:    (required for consumers)
```

### Environment Variables

You can set defaults using environment variables:
```bash
export DANUBE_SERVICE_URL=http://localhost:6650
export DANUBE_TOPIC=/default/my-topic
```

## Troubleshooting

### Connection Issues
```bash
# Test connectivity
danube-cli produce -s http://localhost:6650 -m "test"
```

### Schema Errors
```bash
# Check schema details
danube-cli schema get <subject> --output json

# Validate schema format
danube-cli schema check <subject> --file <schema-file> --type <type>
```

### Message Not Received
- Verify topic name matches between producer and consumer
- Check subscription name is consistent
- Ensure consumer is running before producer sends messages (for exclusive subscriptions)

## Best Practices

1. **Use Schemas for Production** - Always use schema validation for production workloads
2. **Name Your Components** - Use descriptive names for producers, consumers, and subscriptions
3. **Test Compatibility** - Always check schema compatibility before evolving schemas
4. **Monitor Your Flows** - Use reliable delivery for critical messages
5. **Partition for Scale** - Use partitioned topics for high-throughput scenarios

## Next Steps

Ready to dive deeper? Here's your learning path:

1. 📖 [Getting Started Guide](./getting-started.md) - Set up and understand the basics
2. 📤 [Producer Basics](./producer-basics.md) - Learn to send messages
3. 📥 [Consumer Basics](./consumer-basics.md) - Learn to receive messages
4. 📋 [Schema Registry](./schema-registry.md) - Master schema management
5. 🎯 [Examples & Patterns](./examples.md) - See real-world use cases

## Getting Help

- **Documentation**: You're reading it! 📚
- **CLI Help**: Use `danube-cli --help` and `danube-cli <command> --help`
- **Examples**: Check out the [examples](./examples.md) section

---

**Happy Messaging with Danube!** 🌊

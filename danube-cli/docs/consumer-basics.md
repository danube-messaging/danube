# Consumer Basics

Learn how to consume messages from Danube topics! 📥

## Table of Contents
- [Simple Message Consumption](#simple-message-consumption)
- [Subscriptions](#subscriptions)
- [Subscription Types](#subscription-types)
- [Working with Consumers](#working-with-consumers)
- [Message Display](#message-display)
- [Best Practices](#best-practices)

## Simple Message Consumption

The simplest way to consume messages:

```bash
danube-cli consume \
  --service-addr http://localhost:6650 \
  --subscription my-subscription
```

This consumes messages from the default topic `/default/test_topic`.

### With Custom Topic

```bash
danube-cli consume \
  -s http://localhost:6650 \
  -t /default/my-topic \
  -m my-subscription
```

### Using Shortcuts

```bash
danube-cli consume -s http://localhost:6650 -t /default/events -m my-sub
```

## Understanding Subscriptions

### What is a Subscription?

A subscription is a **named cursor** that tracks which messages have been consumed. Think of it as a bookmark in a book.

**Key Benefits:**
- Multiple consumers can share a subscription
- Resume from where you left off
- Messages aren't lost if a consumer crashes

### Subscription Names

Choose meaningful names:

```bash
# ✅ Good names
-m user-service-processor
-m email-notification-handler
-m analytics-pipeline

# ❌ Bad names
-m sub1
-m test
-m abc
```

### How Subscriptions Work

```bash
# Consumer 1 starts reading
danube-cli consume -s http://localhost:6650 -t /default/events -m my-sub
# Reads messages 1, 2, 3...

# Stop Consumer 1 (Ctrl+C)

# Consumer 2 with same subscription continues from where Consumer 1 stopped
danube-cli consume -s http://localhost:6650 -t /default/events -m my-sub
# Continues from message 4, 5, 6...
```

## Subscription Types

Danube supports three subscription types that control how messages are distributed among consumers.

### 1. Shared Subscription (Default)

**Multiple consumers share the load.**

```bash
danube-cli consume \
  -s http://localhost:6650 \
  -t /default/events \
  -m shared-subscription \
  --sub-type shared
```

**How it works:**
- Messages are distributed among all active consumers
- Each message goes to exactly one consumer
- Great for parallel processing

**Example:**
```bash
# Terminal 1
danube-cli consume -s http://localhost:6650 -t /default/work -m workers --sub-type shared

# Terminal 2  
danube-cli consume -s http://localhost:6650 -t /default/work -m workers --sub-type shared

# Terminal 3
danube-cli consume -s http://localhost:6650 -t /default/work -m workers --sub-type shared

# Messages are distributed across all three terminals!
```

**Use Cases:**
- Load balancing
- Parallel processing
- Work queues
- High-throughput scenarios

### 2. Exclusive Subscription

**Only ONE consumer at a time.**

```bash
danube-cli consume \
  -s http://localhost:6650 \
  -t /default/events \
  -m exclusive-subscription \
  --sub-type exclusive
```

**How it works:**
- Only one consumer can be active
- If that consumer fails, another can take over
- Messages are processed in order

**Example:**
```bash
# Terminal 1 - This works
danube-cli consume -s http://localhost:6650 -t /default/orders -m processor --sub-type exclusive

# Terminal 2 - This will fail (subscription is exclusive!)
danube-cli consume -s http://localhost:6650 -t /default/orders -m processor --sub-type exclusive
# Error: Subscription is already exclusively owned
```

**Use Cases:**
- Sequential processing
- Single active processor
- Order-sensitive workflows

### 3. Failover Subscription

**Active/standby setup with automatic failover.**

```bash
danube-cli consume \
  -s http://localhost:6650 \
  -t /default/events \
  -m failover-subscription \
  --sub-type fail-over
```

**How it works:**
- One consumer is active, others are standby
- If active consumer fails, a standby takes over
- Automatic failover for high availability

**Example:**
```bash
# Terminal 1 - Active consumer
danube-cli consume -s http://localhost:6650 -t /default/critical -m ha --sub-type fail-over

# Terminal 2 - Standby (ready to take over)
danube-cli consume -s http://localhost:6650 -t /default/critical -m ha --sub-type fail-over

# If Terminal 1 crashes, Terminal 2 automatically becomes active!
```

**Use Cases:**
- High availability setups
- Critical processing
- Automatic failover needs

### Choosing the Right Type

| Type | Consumers | Distribution | Use Case |
|------|-----------|--------------|----------|
| **Shared** | Multiple active | Round-robin | Load balancing, parallel processing |
| **Exclusive** | One active | All to one | Sequential processing, ordering matters |
| **Failover** | One active + standbys | All to active | High availability, automatic recovery |

## Working with Consumers

### Consumer Names

Give your consumers meaningful names:

```bash
danube-cli consume \
  -s http://localhost:6650 \
  -t /default/events \
  --consumer-name event-processor-1 \
  -m my-subscription
```

**Benefits:**
- Easier debugging
- Better monitoring
- Identify which consumer processed what

### Multiple Topics with Different Consumers

```bash
# Consumer for orders
danube-cli consume \
  -s http://localhost:6650 \
  -t /default/orders \
  -n order-consumer \
  -m order-processor

# Consumer for events (different terminal)
danube-cli consume \
  -s http://localhost:6650 \
  -t /default/events \
  -n event-consumer \
  -m event-processor
```

## Message Display

### Default Output

Messages are displayed in a formatted block:

```
==================================================
Message Received
==================================================
Topic Offset: 42
Producer Name: test_producer
Message: {"event":"user_login","user_id":"u123"}
Size: 45 bytes
Attributes:
  priority: high
  region: us-west
==================================================
Total received: 15.2 KB
```

### JSON Messages

JSON messages are automatically pretty-printed:

```bash
# Producer sends
danube-cli produce -s http://localhost:6650 \
  -m '{"user_id":"u123","event":"login","timestamp":"2024-01-01T10:00:00Z"}'

# Consumer sees pretty JSON
{
  "user_id": "u123",
  "event": "login",
  "timestamp": "2024-01-01T10:00:00Z"
}
```

### With Attributes

Messages with attributes show them clearly:

```
==================================================
Message Received
==================================================
Topic Offset: 10
Producer Name: notification-service
Message: {"type":"email","to":"user@example.com"}
Size: 42 bytes
Attributes:
  priority: high
  queue: email
  retry_count: 0
==================================================
```

### Large Messages

Messages over 1KB show a truncation notice:

```
==================================================
Message Received
==================================================
Topic Offset: 5
Producer Name: data-pipeline
Message: [Message too large to display - 125.5 KB]
Size: 125543 bytes
==================================================
```

## Practical Examples

### Example 1: Simple Event Consumer

```bash
danube-cli consume \
  -s http://localhost:6650 \
  -t /production/user-events \
  -n user-event-processor \
  -m user-events-subscription
```

### Example 2: Load-Balanced Workers

```bash
# Start 5 workers to share the load
for i in {1..5}; do
  danube-cli consume \
    -s http://localhost:6650 \
    -t /default/work-queue \
    -n "worker-$i" \
    -m work-processors \
    --sub-type shared &
done

# All 5 workers share messages from the same subscription
```

### Example 3: Order Processor (Sequential)

```bash
# Only one processor to maintain order
danube-cli consume \
  -s http://localhost:6650 \
  -t /production/orders \
  -n order-processor \
  -m orders \
  --sub-type exclusive
```

### Example 4: High-Availability Setup

```bash
# Terminal 1 - Primary
danube-cli consume \
  -s http://localhost:6650 \
  -t /critical/payments \
  -n payment-processor-primary \
  -m payment-processing \
  --sub-type fail-over

# Terminal 2 - Backup
danube-cli consume \
  -s http://localhost:6650 \
  -t /critical/payments \
  -n payment-processor-backup \
  -m payment-processing \
  --sub-type fail-over
```

### Example 5: Multi-Environment Testing

```bash
# Development
danube-cli consume \
  -s http://localhost:6650 \
  -t /dev/test-events \
  -m dev-consumer

# Staging
danube-cli consume \
  -s http://staging:6650 \
  -t /staging/test-events \
  -m staging-consumer

# Production
danube-cli consume \
  -s http://production:6650 \
  -t /production/events \
  -m prod-consumer
```

## Controlling Consumption

### Stop Consuming

Press `Ctrl+C` to gracefully stop:

```bash
danube-cli consume -s http://localhost:6650 -m my-sub
# ... messages appear ...
# Press Ctrl+C
^C
# Consumer stops and cleans up
```

### Running in Background

```bash
# Start in background
danube-cli consume \
  -s http://localhost:6650 \
  -m background-consumer &

# Check background jobs
jobs

# Stop background consumer
kill %1
```

### Continuous Processing

Consumers run continuously until stopped:

```bash
danube-cli consume -s http://localhost:6650 -m continuous
# Runs forever, processing messages as they arrive
# Stop with Ctrl+C
```

## Best Practices

### 1. Use Descriptive Subscription Names

❌ Bad:
```bash
-m sub1
```

✅ Good:
```bash
-m analytics-pipeline-processor
```

### 2. Match Subscription Type to Use Case

```bash
# Need parallel processing?
--sub-type shared

# Need ordered processing?
--sub-type exclusive

# Need high availability?
--sub-type fail-over
```

### 3. Name Your Consumers

❌ Bad:
```bash
# Uses default name
danube-cli consume -s http://localhost:6650 -m my-sub
```

✅ Good:
```bash
danube-cli consume \
  -s http://localhost:6650 \
  --consumer-name analytics-worker-1 \
  -m my-sub
```

### 4. Test with Different Subscription Types

```bash
# Test shared
danube-cli consume -s http://localhost:6650 -m test --sub-type shared

# Test exclusive
danube-cli consume -s http://localhost:6650 -m test --sub-type exclusive

# Choose the one that fits your needs
```

### 5. Monitor Your Consumers

Watch the output to verify messages are being processed:

```
✅ Topic has schema:
   Subject: user-events
   Version: 1
   Type: json_schema

==================================================
Message Received
==================================================
...
```

## Common Patterns

### Pattern: Development Testing

```bash
# Terminal 1: Producer
danube-cli produce -s http://localhost:6650 -t /dev/test -m "test message"

# Terminal 2: Consumer
danube-cli consume -s http://localhost:6650 -t /dev/test -m dev-test
```

### Pattern: Load Testing

```bash
# Start multiple consumers
for i in {1..10}; do
  danube-cli consume \
    -s http://localhost:6650 \
    -t /default/load-test \
    -n "consumer-$i" \
    -m load-test-consumers \
    --sub-type shared &
done
```

### Pattern: Processing Pipeline

```bash
# Stage 1: Raw data consumer
danube-cli consume \
  -s http://localhost:6650 \
  -t /pipeline/raw-data \
  -m stage1-processor

# Stage 2: Processed data consumer (different terminal)
danube-cli consume \
  -s http://localhost:6650 \
  -t /pipeline/processed-data \
  -m stage2-processor
```

### Pattern: Monitoring & Alerting

```bash
# Monitor critical events
danube-cli consume \
  -s http://localhost:6650 \
  -t /monitoring/alerts \
  -n alert-monitor \
  -m alert-processor \
  --sub-type exclusive
```

## Troubleshooting

### No Messages Appearing

**Check these:**

1. **Is the producer running?**
```bash
# Test by producing a message
danube-cli produce -s http://localhost:6650 -t /default/test -m "test"
```

2. **Is the topic name correct?**
```bash
# Producer and consumer must use the same topic
# Producer: -t /default/events
# Consumer: -t /default/events ✅
```

3. **Is the consumer connected?**
```bash
# You should see:
🔍 Checking for schema associated with topic...
# If not, check service address
```

### Subscription Already Exclusive

```
Error: Subscription is already exclusively owned
```

**Solution:** Another consumer is using the exclusive subscription. Either:
- Stop the other consumer
- Use a different subscription name
- Use `--sub-type shared` instead

### Messages Out of Order

If message order matters:
```bash
# Use exclusive subscription
danube-cli consume \
  -s http://localhost:6650 \
  -m my-sub \
  --sub-type exclusive
```

## Next Steps

Ready for more advanced features?

- 📋 **[Schema Validation](./schema-registry.md)** - Automatic message validation
- 🚀 **[Consumer Advanced](./consumer-advanced.md)** - Advanced consumption patterns
- 📤 **[Producer Basics](./producer-basics.md)** - Learn to produce messages

---

**Happy consuming! 📥**

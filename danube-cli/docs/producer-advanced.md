# Producer Advanced

Master advanced producer features for production workloads! 🚀

## Table of Contents
- [Schema-Based Production](#schema-based-production)
- [Partitioned Topics](#partitioned-topics)
- [Reliable Delivery](#reliable-delivery)
- [Performance Optimization](#performance-optimization)
- [Production Patterns](#production-patterns)

## Schema-Based Production

Schema validation ensures messages conform to a defined structure, preventing bad data from entering your system.

### Using Pre-Registered Schemas

First, register a schema (see [Schema Registry Guide](./schema-registry.md)):

```bash
# 1. Register schema
danube-cli schema register user-events \
  --type json_schema \
  --file ./schemas/user-events.json

# 2. Produce with schema validation
danube-cli produce \
  -s http://localhost:6650 \
  -t /default/user-events \
  --schema-subject user-events \
  -m '{"user_id":"u123","action":"login","timestamp":"2024-01-01T10:00:00Z"}'
```

**What happens:**
- ✅ Message is validated against the schema
- ✅ Schema ID is attached to the message
- ✅ Consumers can automatically validate
- ❌ Invalid messages are rejected before sending

### Auto-Register Schemas from Files

For rapid development, auto-register schemas:

```bash
danube-cli produce \
  -s http://localhost:6650 \
  -t /default/orders \
  --schema-file ./schemas/order.json \
  --schema-type json_schema \
  -m '{"order_id":"ord_123","amount":99.99,"currency":"USD"}'
```

**What happens:**
```
📤 Auto-registering schema for subject 'default-orders' (type: JsonSchema)...
✅ Schema registered with ID: 1
📋 Using schema subject: default-orders
✅ Producer 'test_producer' created successfully
📤 Message 1/1 sent successfully (ID: ...)
```

The schema is automatically:
1. Derived from topic name (`/default/orders` → `default-orders`)
2. Registered in the schema registry
3. Applied to the producer

### Schema Types

#### JSON Schema

```bash
# Create JSON schema file
cat > user-schema.json << 'EOF'
{
  "type": "object",
  "properties": {
    "user_id": {"type": "string"},
    "email": {"type": "string", "format": "email"},
    "age": {"type": "integer", "minimum": 0}
  },
  "required": ["user_id", "email"]
}
EOF

# Produce with JSON schema
danube-cli produce \
  -s http://localhost:6650 \
  --schema-file user-schema.json \
  --schema-type json_schema \
  -m '{"user_id":"u123","email":"user@example.com","age":25}'
```

#### Avro Schema

```bash
# Create Avro schema file
cat > product-schema.avsc << 'EOF'
{
  "type": "record",
  "name": "Product",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "name", "type": "string"},
    {"name": "price", "type": "double"}
  ]
}
EOF

# Produce with Avro schema
danube-cli produce \
  -s http://localhost:6650 \
  --schema-file product-schema.avsc \
  --schema-type avro \
  -m '{"id":"prod_123","name":"Widget","price":29.99}'
```

#### Protobuf Schema

```bash
# Produce with Protobuf schema
danube-cli produce \
  -s http://localhost:6650 \
  --schema-file message.proto \
  --schema-type protobuf \
  --file compiled-protobuf-message.bin
```

### Schema Benefits

**Type Safety:**
```bash
# This will fail validation ❌
danube-cli produce \
  -s http://localhost:6650 \
  --schema-subject user-events \
  -m '{"user_id":123}'  # user_id should be string, not number
```

**Documentation:**
- Schema serves as living documentation
- Consumers know exactly what to expect
- No surprises in production

**Evolution:**
- Update schemas safely
- Compatibility checking prevents breaking changes
- Version history maintained

## Partitioned Topics

Partitions enable horizontal scaling and parallel processing.

### Creating Partitioned Topics

```bash
danube-cli produce \
  -s http://localhost:6650 \
  -t /default/high-throughput \
  --partitions 8 \
  -m "Partitioned message"
```

**What happens:**
- Topic is created with 8 partitions
- Messages are distributed across partitions
- Multiple consumers can process in parallel

### When to Use Partitions

**Use partitions when:**
- ✅ High message throughput (thousands of messages/sec)
- ✅ Multiple consumers need to share load
- ✅ Parallel processing is beneficial
- ✅ Order within partitions is sufficient

**Don't use partitions when:**
- ❌ Global message ordering is required
- ❌ Low message volume
- ❌ Simple testing/development

### Partition Count Guidelines

| Messages/sec | Recommended Partitions | Use Case |
|--------------|------------------------|----------|
| < 100 | 1 (no partitions) | Development, low volume |
| 100-1000 | 2-4 | Moderate volume |
| 1000-10000 | 4-16 | High volume |
| > 10000 | 16+ | Very high volume |

### Example: High-Throughput System

```bash
# Create partitioned topic for events
danube-cli produce \
  -s http://localhost:6650 \
  -t /production/events \
  --partitions 16 \
  --schema-subject events \
  -m '{"event":"page_view","user":"u123"}' \
  --count 10000 \
  --interval 100
```

Then run multiple consumers:
```bash
# Each consumer gets a subset of partitions
for i in {1..8}; do
  danube-cli consume \
    -s http://localhost:6650 \
    -t /production/events \
    -n "consumer-$i" \
    -m event-processors \
    --sub-type shared &
done
```

## Reliable Delivery

Reliable delivery guarantees at-least-once message delivery even if the broker crashes.

### Enable Reliable Delivery

```bash
danube-cli produce \
  -s http://localhost:6650 \
  -t /critical/orders \
  -m '{"order_id":"ord_123","amount":999.99}' \
  --reliable
```

**What happens:**
- Messages are persisted to disk before acknowledging
- Survives broker restarts
- Guarantees message delivery

### When to Use Reliable Delivery

**Use for:**
- ✅ Financial transactions
- ✅ Orders and payments
- ✅ Critical business events
- ✅ Audit logs
- ✅ Data that must not be lost

**Don't use for:**
- ❌ High-frequency metrics (lossy OK)
- ❌ Temporary data
- ❌ Easily reproducible data
- ❌ Maximum performance scenarios

### Example: Order Processing

```bash
danube-cli produce \
  -s http://localhost:6650 \
  -t /production/orders \
  --schema-subject orders \
  --reliable \
  -m '{
    "order_id": "ord_789",
    "customer_id": "cust_456",
    "items": [{"sku":"item_1","qty":2,"price":49.99}],
    "total": 99.98
  }' \
  -a "priority:high,source:web"
```

### Reliable + Partitioned

Combine for high-throughput critical data:

```bash
danube-cli produce \
  -s http://localhost:6650 \
  -t /production/transactions \
  --partitions 8 \
  --reliable \
  --schema-subject transactions \
  -m '{"transaction_id":"tx_123","amount":1000.00}' \
  --count 1000
```

## Performance Optimization

### Batch Production

Send multiple messages efficiently:

```bash
# High-throughput batch
danube-cli produce \
  -s http://localhost:6650 \
  -t /default/events \
  -m '{"event":"data"}' \
  --count 10000 \
  --interval 100  # Minimum interval for maximum speed
```

### Parallel Producers

Run multiple producers in parallel:

```bash
#!/bin/bash
# Start 5 parallel producers
for i in {1..5}; do
  danube-cli produce \
    -s http://localhost:6650 \
    -t /default/load-test \
    --producer-name "producer-$i" \
    -m "Message from producer $i" \
    --count 2000 \
    --interval 100 &
done

wait
echo "All producers finished!"
```

### Optimized Configuration

```bash
# Maximum performance configuration
danube-cli produce \
  -s http://localhost:6650 \
  -t /default/high-perf \
  --partitions 16 \
  -m '{"data":"value"}' \
  --count 100000 \
  --interval 100
```

**Performance tips:**
- Use minimum interval (100ms) for maximum throughput
- Enable partitions for parallel processing
- Consider multiple producers
- Avoid reliable delivery if not critical

### Load Testing

```bash
#!/bin/bash
# Load test script

DURATION=300  # 5 minutes
PRODUCERS=10
MESSAGES_PER_PRODUCER=10000

echo "Starting load test with $PRODUCERS producers..."

for i in $(seq 1 $PRODUCERS); do
  danube-cli produce \
    -s http://localhost:6650 \
    -t /load-test/messages \
    --producer-name "load-producer-$i" \
    --partitions 16 \
    -m "{\"producer\":$i,\"timestamp\":\"$(date -Iseconds)\"}" \
    --count $MESSAGES_PER_PRODUCER \
    --interval 100 &
done

wait
echo "Load test complete!"
```

## Production Patterns

### Pattern 1: Event Sourcing

```bash
# Produce domain events with schema validation
danube-cli produce \
  -s http://localhost:6650 \
  -t /events/user-domain \
  --schema-subject user-events \
  --reliable \
  -m '{
    "event_type": "UserRegistered",
    "aggregate_id": "user_123",
    "timestamp": "2024-01-01T10:00:00Z",
    "data": {
      "email": "user@example.com",
      "name": "John Doe"
    }
  }' \
  -a "event_version:1,aggregate_type:User"
```

### Pattern 2: CQRS Command Publishing

```bash
# Publish commands to command topic
danube-cli produce \
  -s http://localhost:6650 \
  -t /commands/orders \
  --schema-subject order-commands \
  --reliable \
  -m '{
    "command_type": "CreateOrder",
    "command_id": "cmd_789",
    "aggregate_id": "order_456",
    "payload": {
      "customer_id": "cust_123",
      "items": [{"sku":"item_1","quantity":2}]
    }
  }'
```

### Pattern 3: Data Pipeline

```bash
# Stage 1: Ingest raw data
danube-cli produce \
  -s http://localhost:6650 \
  -t /pipeline/raw-data \
  --partitions 8 \
  --file /data/raw/batch-001.json \
  -a "stage:ingest,batch:001"

# Stage 2: Produce processed data (after transformation)
danube-cli produce \
  -s http://localhost:6650 \
  -t /pipeline/processed-data \
  --schema-subject processed-events \
  --partitions 8 \
  -m '{"processed":true,"data":"transformed"}' \
  -a "stage:transform,batch:001"
```

### Pattern 4: Multi-Region Replication

```bash
# Produce to primary region
danube-cli produce \
  -s http://us-west.example.com:6650 \
  -t /global/events \
  --reliable \
  -m '{"event":"user_action","region":"us-west"}' \
  -a "region:us-west,replicate:true"

# Produce to secondary region
danube-cli produce \
  -s http://eu-west.example.com:6650 \
  -t /global/events \
  --reliable \
  -m '{"event":"user_action","region":"eu-west"}' \
  -a "region:eu-west,replicate:true"
```

### Pattern 5: Monitoring & Metrics

```bash
#!/bin/bash
# Continuous metrics publishing

while true; do
  TIMESTAMP=$(date -Iseconds)
  CPU=$(top -bn1 | grep "Cpu(s)" | awk '{print $2}' | cut -d'%' -f1)
  MEMORY=$(free | grep Mem | awk '{print ($3/$2) * 100.0}')
  
  danube-cli produce \
    -s http://localhost:6650 \
    -t /monitoring/system-metrics \
    --schema-subject system-metrics \
    -m "{
      \"timestamp\": \"$TIMESTAMP\",
      \"host\": \"$(hostname)\",
      \"cpu_percent\": $CPU,
      \"memory_percent\": $MEMORY
    }" \
    -a "host:$(hostname),metric_type:system"
  
  sleep 60
done
```

### Pattern 6: Retry with Dead Letter Queue

```bash
#!/bin/bash
# Producer with retry logic

MAX_RETRIES=3
MESSAGE='{"important":"data"}'

for attempt in $(seq 1 $MAX_RETRIES); do
  if danube-cli produce \
    -s http://localhost:6650 \
    -t /production/orders \
    --reliable \
    -m "$MESSAGE" \
    -a "attempt:$attempt"; then
    echo "Success on attempt $attempt"
    exit 0
  else
    echo "Failed attempt $attempt"
    if [ $attempt -eq $MAX_RETRIES ]; then
      # Send to dead letter queue
      danube-cli produce \
        -s http://localhost:6650 \
        -t /dlq/failed-orders \
        --reliable \
        -m "$MESSAGE" \
        -a "original_topic:orders,failures:$MAX_RETRIES"
      echo "Moved to DLQ"
    fi
    sleep 5
  fi
done
```

## Combining Features

### Example: Production-Ready Setup

```bash
danube-cli produce \
  -s http://production:6650 \
  -t /production/critical-events \
  --producer-name payment-service-v1 \
  --schema-subject payment-events \
  --partitions 16 \
  --reliable \
  -m '{
    "event_type": "PaymentProcessed",
    "payment_id": "pay_123",
    "amount": 999.99,
    "currency": "USD",
    "timestamp": "2024-01-01T10:00:00Z"
  }' \
  -a "priority:high,service:payment,version:v1,environment:production"
```

**This configuration provides:**
- ✅ Schema validation
- ✅ Reliable delivery
- ✅ Horizontal scaling (16 partitions)
- ✅ Descriptive naming
- ✅ Rich metadata (attributes)
- ✅ Production-ready setup

## Best Practices

### 1. Always Use Schemas in Production

❌ Without schema:
```bash
danube-cli produce -s http://localhost:6650 -m '{"data":"value"}'
```

✅ With schema:
```bash
danube-cli produce \
  -s http://localhost:6650 \
  --schema-subject my-events \
  -m '{"data":"value"}'
```

### 2. Enable Reliable Delivery for Critical Data

```bash
# Critical: orders, payments, audit logs
--reliable

# Non-critical: metrics, logs, temporary data
# (omit --reliable for better performance)
```

### 3. Partition for Scale

```bash
# Low volume (< 100 msg/sec)
# No partitions needed

# High volume (> 1000 msg/sec)
--partitions 8
```

### 4. Use Meaningful Attributes

```bash
-a "environment:production,service:orders,version:v2,priority:high"
```

### 5. Monitor Production

Add monitoring metadata:
```bash
-a "correlation_id:$(uuidgen),timestamp:$(date -Iseconds)"
```

## Troubleshooting

### Schema Validation Failures

```bash
# Check schema
danube-cli schema get my-subject

# Validate your message format
# Ensure it matches the schema exactly
```

### Performance Issues

```bash
# Increase partitions
--partitions 16

# Reduce interval
--interval 100

# Run multiple producers in parallel
```

### Reliability Concerns

```bash
# Ensure reliable delivery is enabled
--reliable

# Verify with attributes
-a "guaranteed:true,priority:critical"
```

## Next Steps

- 📋 **[Schema Registry](./schema-registry.md)** - Deep dive into schema management
- 📥 **[Consumer Advanced](./consumer-advanced.md)** - Advanced consumption patterns
- 🎯 **[Examples](./examples.md)** - Real-world end-to-end examples

---

**Build robust producers! 🚀**

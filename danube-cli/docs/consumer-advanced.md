# Consumer Advanced

Master advanced consumption patterns and schema validation! 🎯

## Table of Contents
- [Schema-Based Consumption](#schema-based-consumption)
- [Advanced Subscription Patterns](#advanced-subscription-patterns)
- [Message Processing Patterns](#message-processing-patterns)
- [Error Handling](#error-handling)
- [Production Patterns](#production-patterns)

## Schema-Based Consumption

The consumer automatically detects and validates messages against registered schemas.

### Automatic Schema Detection

```bash
# Consumer automatically checks for schemas
danube-cli consume \
  -s http://localhost:6650 \
  -t /default/user-events \
  -m my-subscription
```

**Output:**
```
🔍 Checking for schema associated with topic...
✅ Topic has schema:
   Subject: user-events
   Version: 1
   Type: json_schema
```

The consumer:
1. Derives subject name from topic (`/default/user-events` → `default-user-events`)
2. Fetches the latest schema version
3. Validates all incoming messages
4. Pretty-prints JSON messages

### JSON Schema Validation

Messages are automatically validated:

```bash
# Start consumer
danube-cli consume \
  -s http://localhost:6650 \
  -t /default/users \
  -m user-processor

# Valid message - processes successfully ✅
danube-cli produce \
  -s http://localhost:6650 \
  -t /default/users \
  --schema-subject users \
  -m '{"user_id":"u123","email":"user@example.com"}'

# Invalid message - validation fails ❌
danube-cli produce \
  -s http://localhost:6650 \
  -t /default/users \
  --schema-subject users \
  -m '{"user_id":123}'  # user_id should be string
```

**Consumer output for invalid message:**
```
Error processing message: JSON validation failed: [...]
```

### Schema Evolution

Consumers work with schema evolution:

```bash
# Producer uses v1 schema
danube-cli produce \
  -s http://localhost:6650 \
  --schema-subject orders \
  -m '{"order_id":"ord_1","amount":99.99}'

# Schema evolved to v2 (added optional "currency" field)
danube-cli schema register orders \
  --type json_schema \
  --file orders-v2.json

# Consumer still works with v1 messages ✅
# And also validates v2 messages ✅
danube-cli consume \
  -s http://localhost:6650 \
  -t /default/orders \
  -m order-processor
```

### Different Schema Types

#### JSON Schema Consumption

```bash
danube-cli consume \
  -s http://localhost:6650 \
  -t /default/json-events \
  -m json-consumer
```

**Display:**
```json
{
  "event_type": "user_login",
  "user_id": "u123",
  "timestamp": "2024-01-01T10:00:00Z"
}
```

#### Avro Schema Consumption

```bash
danube-cli consume \
  -s http://localhost:6650 \
  -t /default/avro-events \
  -m avro-consumer
```

**Display:** Avro messages are deserialized and displayed as JSON.

#### Protobuf Consumption

```bash
danube-cli consume \
  -s http://localhost:6650 \
  -t /default/proto-events \
  -m proto-consumer
```

**Display:**
```
[Protobuf binary data - 245 bytes]
```

### No Schema Topics

If a topic has no schema:

```bash
danube-cli consume \
  -s http://localhost:6650 \
  -t /default/raw-data \
  -m raw-consumer
```

**Output:**
```
🔍 Checking for schema associated with topic...
ℹ️  Topic has no schema - consuming raw bytes
```

Messages are displayed as-is (UTF-8 text or binary indication).

## Advanced Subscription Patterns

### Pattern 1: Fan-Out Processing

Multiple subscription groups process the same messages:

```bash
# Analytics team
danube-cli consume \
  -s http://localhost:6650 \
  -t /production/events \
  -m analytics-team \
  --sub-type shared

# Monitoring team (different subscription)
danube-cli consume \
  -s http://localhost:6650 \
  -t /production/events \
  -m monitoring-team \
  --sub-type shared

# Audit team (different subscription)
danube-cli consume \
  -s http://localhost:6650 \
  -t /production/events \
  -m audit-team \
  --sub-type exclusive
```

**Result:** Each team processes ALL messages independently.

### Pattern 2: Worker Pool

Multiple workers share the load:

```bash
#!/bin/bash
# Start a pool of 10 workers

for i in {1..10}; do
  danube-cli consume \
    -s http://localhost:6650 \
    -t /production/work-queue \
    -n "worker-$i" \
    -m work-processors \
    --sub-type shared &
done

echo "Worker pool started with 10 workers"
wait
```

### Pattern 3: Primary-Backup HA

High availability with automatic failover:

```bash
# Terminal 1: Primary processor
danube-cli consume \
  -s http://localhost:6650 \
  -t /critical/payments \
  -n payment-processor-primary \
  -m payment-processing \
  --sub-type fail-over

# Terminal 2: Backup processor
danube-cli consume \
  -s http://localhost:6650 \
  -t /critical/payments \
  -n payment-processor-backup \
  -m payment-processing \
  --sub-type fail-over

# If primary crashes, backup automatically takes over!
```

### Pattern 4: Topic Multiplexing

One consumer, multiple topics (via scripting):

```bash
#!/bin/bash
# Monitor multiple topics

TOPICS=(
  "/production/orders"
  "/production/payments"
  "/production/shipments"
)

for topic in "${TOPICS[@]}"; do
  danube-cli consume \
    -s http://localhost:6650 \
    -t "$topic" \
    -n "monitor-$(basename $topic)" \
    -m "monitoring-$(basename $topic)" &
done

wait
```

### Pattern 5: Staged Processing

Sequential processing stages:

```bash
# Stage 1: Raw event consumers
for i in {1..5}; do
  danube-cli consume \
    -s http://localhost:6650 \
    -t /pipeline/raw-events \
    -n "stage1-$i" \
    -m stage1-processors \
    --sub-type shared &
done

# Stage 2: Enriched event consumers (different terminal/script)
for i in {1..3}; do
  danube-cli consume \
    -s http://localhost:6650 \
    -t /pipeline/enriched-events \
    -n "stage2-$i" \
    -m stage2-processors \
    --sub-type shared &
done
```

## Message Processing Patterns

### Pattern 1: Filter and Process

```bash
#!/bin/bash
# Consume and filter by attributes

danube-cli consume \
  -s http://localhost:6650 \
  -t /production/events \
  -m event-filter | \
while IFS= read -r line; do
  # Process only high-priority messages
  if echo "$line" | grep -q "priority: high"; then
    echo "Processing high-priority: $line"
    # Your processing logic here
  fi
done
```

### Pattern 2: Batch Processing

```bash
#!/bin/bash
# Collect messages in batches

BATCH_SIZE=100
BATCH=()

danube-cli consume \
  -s http://localhost:6650 \
  -t /production/events \
  -m batch-processor | \
while IFS= read -r line; do
  BATCH+=("$line")
  
  if [ ${#BATCH[@]} -eq $BATCH_SIZE ]; then
    echo "Processing batch of $BATCH_SIZE messages..."
    # Process batch
    printf '%s\n' "${BATCH[@]}" | your-batch-processor
    BATCH=()
  fi
done
```

### Pattern 3: Dead Letter Queue

```bash
#!/bin/bash
# Consumer with DLQ for failed messages

danube-cli consume \
  -s http://localhost:6650 \
  -t /production/orders \
  -m order-processor 2>&1 | \
while IFS= read -r line; do
  if echo "$line" | grep -q "Error processing message"; then
    # Send to DLQ
    echo "$line" | danube-cli produce \
      -s http://localhost:6650 \
      -t /dlq/failed-orders \
      -m "$(cat)" \
      -a "error:processing_failed,timestamp:$(date -Iseconds)"
  else
    echo "$line"
  fi
done
```

### Pattern 4: Retry Logic

```bash
#!/bin/bash
# Consumer with retry logic

MAX_RETRIES=3

process_message() {
  local message="$1"
  local attempt=0
  
  while [ $attempt -lt $MAX_RETRIES ]; do
    if process_logic "$message"; then
      return 0
    fi
    ((attempt++))
    echo "Retry $attempt/$MAX_RETRIES for message"
    sleep 5
  done
  
  return 1
}

danube-cli consume \
  -s http://localhost:6650 \
  -t /production/events \
  -m resilient-processor | \
while IFS= read -r line; do
  if ! process_message "$line"; then
    echo "Failed after $MAX_RETRIES retries: $line"
  fi
done
```

### Pattern 5: Metric Collection

```bash
#!/bin/bash
# Track message processing metrics

PROCESSED=0
ERRORS=0
START_TIME=$(date +%s)

danube-cli consume \
  -s http://localhost:6650 \
  -t /production/events \
  -m metrics-tracker | \
while IFS= read -r line; do
  if echo "$line" | grep -q "Error processing"; then
    ((ERRORS++))
  elif echo "$line" | grep -q "Message Received"; then
    ((PROCESSED++))
  fi
  
  # Print metrics every 100 messages
  if [ $((PROCESSED % 100)) -eq 0 ] && [ $PROCESSED -gt 0 ]; then
    ELAPSED=$(($(date +%s) - START_TIME))
    RATE=$((PROCESSED / ELAPSED))
    echo "Metrics: Processed=$PROCESSED, Errors=$ERRORS, Rate=$RATE msg/sec"
  fi
done
```

## Error Handling

### Graceful Degradation

When schema validation fails:

```bash
danube-cli consume \
  -s http://localhost:6650 \
  -t /production/events \
  -m resilient-consumer

# Output for invalid message:
# Error processing message: JSON validation failed: [...]
# (Consumer continues processing next messages)
```

The consumer:
- ✅ Logs the error
- ✅ Continues processing
- ✅ Acknowledges the message (to avoid redelivery)
- ❌ Does not crash

### Connection Resilience

```bash
# Consumer automatically reconnects on connection loss
danube-cli consume \
  -s http://localhost:6650 \
  -t /production/events \
  -m persistent-consumer

# If connection drops:
# - Consumer attempts to reconnect
# - Subscription state is preserved
# - Resumes from last acknowledged message
```

### Message Acknowledgment

Messages are acknowledged after successful processing:

```
==================================================
Message Received
==================================================
Topic Offset: 42
...
==================================================
# Message auto-acknowledged here ✅
```

If processing fails before acknowledgment:
- Message will be redelivered
- Ensures at-least-once delivery

## Production Patterns

### Pattern 1: Event-Driven Microservice

```bash
#!/bin/bash
# Microservice consuming events

SERVICE_NAME="order-processor"
MAX_RETRIES=3

echo "Starting $SERVICE_NAME..."

danube-cli consume \
  -s http://production:6650 \
  -t /events/orders \
  -n "$SERVICE_NAME" \
  -m order-processing \
  --sub-type shared | \
while IFS= read -r line; do
  if echo "$line" | grep -q "Message Received"; then
    # Extract and process order
    # Call business logic
    # Update database
    # Publish result event
    echo "[$SERVICE_NAME] Processed order"
  fi
done
```

### Pattern 2: Data Aggregation Pipeline

```bash
#!/bin/bash
# Aggregate data from multiple partitions

AGGREGATION_WINDOW=60  # seconds
declare -A AGGREGATES

danube-cli consume \
  -s http://localhost:6650 \
  -t /analytics/raw-events \
  -m aggregation-processor \
  --sub-type shared | \
while IFS= read -r line; do
  # Aggregate logic
  # Every AGGREGATION_WINDOW seconds, flush aggregates
  # Publish aggregated data to next topic
done
```

### Pattern 3: Real-Time Alerting

```bash
#!/bin/bash
# Monitor for critical conditions

ALERT_THRESHOLD=1000

danube-cli consume \
  -s http://localhost:6650 \
  -t /monitoring/metrics \
  -m alert-monitor \
  --sub-type exclusive | \
while IFS= read -r line; do
  # Parse metric value
  if echo "$line" | grep -q "error_rate"; then
    VALUE=$(echo "$line" | extract_value)
    if [ "$VALUE" -gt "$ALERT_THRESHOLD" ]; then
      # Trigger alert
      send_alert "Error rate exceeded: $VALUE"
    fi
  fi
done
```

### Pattern 4: Stream Processing

```bash
#!/bin/bash
# Process stream with windowing

WINDOW_SIZE=10
WINDOW=()

danube-cli consume \
  -s http://localhost:6650 \
  -t /streams/data \
  -m stream-processor | \
while IFS= read -r line; do
  WINDOW+=("$line")
  
  if [ ${#WINDOW[@]} -ge $WINDOW_SIZE ]; then
    # Process window
    process_window "${WINDOW[@]}"
    # Slide window
    WINDOW=("${WINDOW[@]:1}")
  fi
done
```

### Pattern 5: Multi-Environment Consumer

```bash
#!/bin/bash
# Consume from different environments

ENVIRONMENT=${1:-development}

case $ENVIRONMENT in
  development)
    SERVICE_ADDR="http://localhost:6650"
    TOPIC="/dev/events"
    ;;
  staging)
    SERVICE_ADDR="http://staging:6650"
    TOPIC="/staging/events"
    ;;
  production)
    SERVICE_ADDR="http://production:6650"
    TOPIC="/production/events"
    ;;
esac

echo "Starting consumer for $ENVIRONMENT environment..."

danube-cli consume \
  -s "$SERVICE_ADDR" \
  -t "$TOPIC" \
  -n "consumer-$ENVIRONMENT" \
  -m "events-$ENVIRONMENT"
```

## Performance Optimization

### Parallel Consumption

```bash
#!/bin/bash
# Scale consumers horizontally

NUM_CONSUMERS=16

echo "Starting $NUM_CONSUMERS parallel consumers..."

for i in $(seq 1 $NUM_CONSUMERS); do
  danube-cli consume \
    -s http://localhost:6650 \
    -t /high-volume/events \
    -n "consumer-$i" \
    -m high-volume-processors \
    --sub-type shared &
done

wait
```

### Resource Management

```bash
#!/bin/bash
# Monitor resource usage

while true; do
  danube-cli consume \
    -s http://localhost:6650 \
    -t /production/events \
    -m managed-consumer &
  
  CONSUMER_PID=$!
  
  # Monitor memory usage
  while kill -0 $CONSUMER_PID 2>/dev/null; do
    MEM=$(ps -p $CONSUMER_PID -o rss= | awk '{print $1/1024}')
    if (( $(echo "$MEM > 1000" | bc -l) )); then
      echo "Memory limit exceeded, restarting consumer..."
      kill $CONSUMER_PID
      break
    fi
    sleep 10
  done
  
  sleep 5
done
```

## Best Practices

### 1. Use Shared Subscriptions for Load Balancing

```bash
# Scale horizontally with shared subscriptions
--sub-type shared
```

### 2. Use Exclusive for Ordered Processing

```bash
# Maintain order with exclusive subscription
--sub-type exclusive
```

### 3. Use Failover for High Availability

```bash
# Automatic failover with standby consumers
--sub-type fail-over
```

### 4. Name Your Consumers

```bash
# Descriptive consumer names for debugging
--consumer-name order-processor-v2
```

### 5. Monitor Schema Evolution

```bash
# Check schema version on startup
# Validate against expected schema
```

## Troubleshooting

### Schema Validation Errors

```bash
# Check current schema
danube-cli schema get <subject>

# Verify message format matches schema
```

### No Messages Received

- Verify topic name matches producer
- Check subscription name consistency
- Ensure broker is reachable

### High Memory Usage

- Reduce number of parallel consumers
- Process messages faster
- Check for message processing leaks

## Next Steps

- 📋 **[Schema Registry](./schema-registry.md)** - Master schema management
- 🚀 **[Producer Advanced](./producer-advanced.md)** - Advanced production patterns
- 🎯 **[Examples](./examples.md)** - Complete end-to-end examples

---

**Master advanced consumption! 🎯**

# Producer Basics

Learn how to produce messages to Danube topics with ease! 📤

## Table of Contents
- [Simple Message Production](#simple-message-production)
- [Working with Topics](#working-with-topics)
- [Message Content](#message-content)
- [Sending Multiple Messages](#sending-multiple-messages)
- [Message Attributes](#message-attributes)
- [Best Practices](#best-practices)

## Simple Message Production

The simplest way to produce a message:

```bash
danube-cli produce \
  --service-addr http://localhost:6650 \
  --message "Hello, World!"
```

This sends one message to the default topic `/default/test_topic`.

### With Custom Topic

```bash
danube-cli produce \
  -s http://localhost:6650 \
  -t /default/my-topic \
  -m "Custom topic message"
```

### Shortcuts

Use short flags for faster typing:

```bash
danube-cli produce -s http://localhost:6650 -t /default/events -m "Quick message"
```

## Working with Topics

### Topic Naming

Topics follow a hierarchical structure:

```
/namespace/topic-name
```

**Examples:**
```bash
/default/events
/production/orders
/staging/user-activity
/dev/test-messages
```

**Good Practices:**
- Use descriptive names: `/production/order-confirmations`
- Separate environments: `/dev/...`, `/staging/...`, `/production/...`
- Group related topics: `/default/user-events`, `/default/user-notifications`

### Creating Topics

Topics are created automatically on first use:

```bash
# This creates the topic if it doesn't exist
danube-cli produce -s http://localhost:6650 -t /default/new-topic -m "First message"
```

## Message Content

### Text Messages

The most common way - simple strings:

```bash
danube-cli produce \
  -s http://localhost:6650 \
  -m "This is a text message"
```

### JSON Messages

For structured data:

```bash
danube-cli produce \
  -s http://localhost:6650 \
  -m '{"event":"user_login","user_id":"user_123","timestamp":"2024-01-01T10:00:00Z"}'
```

**Tip:** Use single quotes around JSON to avoid shell escaping issues!

### Binary Files

Send files directly:

```bash
danube-cli produce \
  -s http://localhost:6650 \
  --file /path/to/data.bin
```

**Use Cases:**
- Images
- Documents
- Serialized data
- Compressed files

### From stdin

You can also pipe content:

```bash
echo "Piped message" | danube-cli produce -s http://localhost:6650 -m "$(cat)"
```

```bash
cat large-file.json | danube-cli produce -s http://localhost:6650 -m "$(cat)"
```

## Sending Multiple Messages

### Bulk Send with Count

Send the same message multiple times:

```bash
danube-cli produce \
  -s http://localhost:6650 \
  -m "Repeated message" \
  --count 100
```

This sends 100 copies of the message.

### With Intervals

Control the rate of messages:

```bash
danube-cli produce \
  -s http://localhost:6650 \
  -m "Throttled message" \
  --count 50 \
  --interval 1000
```

This sends 50 messages with a 1-second delay between each.

**Note:** Minimum interval is 100ms.

### Load Testing Example

```bash
# Send 1000 messages as fast as possible (100ms intervals)
danube-cli produce \
  -s http://localhost:6650 \
  -t /default/load-test \
  -m "Load test message" \
  --count 1000 \
  --interval 100
```

### Monitoring Progress

Watch the progress as messages are sent:

```bash
danube-cli produce \
  -s http://localhost:6650 \
  -m "Message" \
  --count 100

# Output:
# ✅ Producer 'test_producer' created successfully
# 📤 Message 1/100 sent successfully (ID: ...)
# 📤 Message 2/100 sent successfully (ID: ...)
# ...
# 📊 Summary:
#    ✅ Success: 100
```

## Message Attributes

Attributes are key-value pairs attached to messages for metadata, routing, or filtering.

### Adding Attributes

```bash
danube-cli produce \
  -s http://localhost:6650 \
  -m "Alert message" \
  --attributes "priority:high,region:us-west,team:ops"
```

### Common Use Cases

#### 1. Priority Routing
```bash
danube-cli produce \
  -s http://localhost:6650 \
  -m "Critical alert" \
  -a "priority:critical,service:payment"
```

#### 2. Geographic Routing
```bash
danube-cli produce \
  -s http://localhost:6650 \
  -m "Regional update" \
  -a "region:eu-west,country:germany"
```

#### 3. Message Tracking
```bash
danube-cli produce \
  -s http://localhost:6650 \
  -m "Order created" \
  -a "correlation_id:order-123,source:web-app"
```

#### 4. Content Type
```bash
danube-cli produce \
  -s http://localhost:6650 \
  -m '{"data":"value"}' \
  -a "content_type:application/json,version:v1"
```

### Attribute Format

- Use colon (`:`) between key and value
- Use comma (`,`) to separate pairs
- No spaces around separators
- Quote the entire string

```bash
# ✅ Correct
-a "key1:value1,key2:value2"

# ❌ Incorrect
-a "key1: value1, key2: value2"
```

## Producer Configuration

### Custom Producer Name

Give your producer a meaningful name:

```bash
danube-cli produce \
  -s http://localhost:6650 \
  --producer-name order-service-prod \
  -m "Order event"
```

**Benefits:**
- Easier debugging
- Better monitoring
- Clear message origin

### Default Values

If not specified, these defaults are used:

```bash
--service-addr http://127.0.0.1:6650
--topic /default/test_topic
--producer-name test_producer
--count 1
--interval 500
```

## Practical Examples

### Example 1: Event Logging

```bash
danube-cli produce \
  -s http://localhost:6650 \
  -t /production/application-events \
  --producer-name app-logger \
  -m '{"level":"INFO","message":"User logged in","user_id":"u123"}' \
  -a "severity:info,component:auth"
```

### Example 2: Metrics Publishing

```bash
danube-cli produce \
  -s http://localhost:6650 \
  -t /monitoring/metrics \
  --producer-name metrics-collector \
  -m '{"cpu":45.2,"memory":78.5,"timestamp":"2024-01-01T10:00:00Z"}' \
  --count 60 \
  --interval 1000
```

This publishes a metric every second for one minute.

### Example 3: Data Pipeline

```bash
danube-cli produce \
  -s http://localhost:6650 \
  -t /data/raw-events \
  --file /tmp/events.ndjson \
  -a "source:kafka,format:ndjson,date:2024-01-01"
```

### Example 4: Notification Service

```bash
danube-cli produce \
  -s http://localhost:6650 \
  -t /notifications/email \
  -m '{"to":"user@example.com","subject":"Welcome!","template":"welcome_email"}' \
  -a "priority:high,type:email"
```

### Example 5: Batch Processing

```bash
# Send 1000 messages for batch processing
danube-cli produce \
  -s http://localhost:6650 \
  -t /batch/processing-queue \
  -m '{"job_id":"batch-001","data":"process_this"}' \
  --count 1000 \
  --interval 100
```

## Best Practices

### 1. Use Descriptive Topics

❌ Bad:
```bash
-t /default/topic1
```

✅ Good:
```bash
-t /production/user-registration-events
```

### 2. Name Your Producers

❌ Bad:
```bash
# Uses default name "test_producer"
danube-cli produce -s http://localhost:6650 -m "message"
```

✅ Good:
```bash
danube-cli produce \
  -s http://localhost:6650 \
  --producer-name registration-service \
  -m "message"
```

### 3. Add Context with Attributes

❌ Bad:
```bash
-m "Error occurred"
```

✅ Good:
```bash
-m "Error occurred" \
-a "severity:error,service:api,environment:production"
```

### 4. Structure Your JSON

❌ Bad:
```bash
-m '{"a":"b","c":"d"}' # No structure
```

✅ Good:
```bash
-m '{
  "event_type": "user_action",
  "timestamp": "2024-01-01T10:00:00Z",
  "user_id": "u123",
  "action": "login"
}'
```

### 5. Test Before Production

```bash
# Test in dev first
danube-cli produce -s http://localhost:6650 -t /dev/test -m "test"

# Then move to production
danube-cli produce -s http://prod:6650 -t /production/events -m "real"
```

## Common Patterns

### Pattern: Health Check Messages

```bash
danube-cli produce \
  -s http://localhost:6650 \
  -t /monitoring/health \
  --producer-name health-checker \
  -m '{"status":"healthy","timestamp":"'$(date -Iseconds)'"}' \
  --count 1
```

### Pattern: Periodic Data Collection

```bash
#!/bin/bash
# Script to send metrics every minute
while true; do
  danube-cli produce \
    -s http://localhost:6650 \
    -t /metrics/system \
    -m "{\"cpu\":$(top -bn1 | grep Cpu | awk '{print $2}')}" \
    -a "host:$(hostname)"
  sleep 60
done
```

### Pattern: File Processing Pipeline

```bash
#!/bin/bash
# Process all JSON files in a directory
for file in /data/*.json; do
  danube-cli produce \
    -s http://localhost:6650 \
    -t /pipeline/input \
    --file "$file" \
    -a "filename:$(basename $file),size:$(stat -f%z $file)"
done
```

## Troubleshooting

### Message Too Large

If your message is too large:

```bash
# Split into smaller messages
# Or compress before sending
gzip -c large-file.json > /tmp/compressed.gz
danube-cli produce -s http://localhost:6650 --file /tmp/compressed.gz
```

### Rate Limiting

If you need to control the rate:

```bash
# Slow down with --interval
danube-cli produce \
  -s http://localhost:6650 \
  -m "Throttled" \
  --count 100 \
  --interval 500  # 2 messages per second
```

### Connection Timeouts

For unstable connections, split into smaller batches:

```bash
# Instead of --count 10000
# Do multiple smaller batches
for i in {1..10}; do
  danube-cli produce -s http://localhost:6650 -m "Batch $i" --count 1000
done
```

## Next Steps

Ready for more advanced features?

- 📋 **[Schema Registry](./schema-registry.md)** - Add schema validation to your messages
- 🚀 **[Producer Advanced](./producer-advanced.md)** - Partitioning, reliable delivery, and more
- 📥 **[Consumer Basics](./consumer-basics.md)** - Learn to consume your messages

---

**Keep producing! 📤**

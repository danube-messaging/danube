# Quick Reference

Fast command lookup for Danube CLI! ⚡

## Installation

```bash
cargo build --release -p danube-cli
# Binary: target/release/danube-cli
```

## Common Commands

### Producer

```bash
# Basic
danube-cli produce -s http://localhost:6650 -m "Hello"

# With schema
danube-cli produce -s http://localhost:6650 \
  --schema-subject my-schema -m '{"key":"value"}'

# Auto-register schema
danube-cli produce -s http://localhost:6650 \
  --schema-file schema.json --schema-type json_schema -m '{"key":"value"}'

# Bulk messages
danube-cli produce -s http://localhost:6650 -m "Message" -c 100 -i 500

# Reliable delivery
danube-cli produce -s http://localhost:6650 -m "Important" --reliable

# With partitions
danube-cli produce -s http://localhost:6650 -m "Message" --partitions 8

# With attributes
danube-cli produce -s http://localhost:6650 -m "Message" -a "key:value,priority:high"

# Binary file
danube-cli produce -s http://localhost:6650 --file data.bin
```

### Consumer

```bash
# Basic
danube-cli consume -s http://localhost:6650 -m my-subscription

# Custom topic
danube-cli consume -s http://localhost:6650 -t /my/topic -m my-sub

# Shared (load balanced)
danube-cli consume -s http://localhost:6650 -m shared-sub --sub-type shared

# Exclusive (ordered)
danube-cli consume -s http://localhost:6650 -m exclusive-sub --sub-type exclusive

# Failover (HA)
danube-cli consume -s http://localhost:6650 -m ha-sub --sub-type fail-over

# Custom consumer name
danube-cli consume -s http://localhost:6650 -n my-consumer -m my-sub
```

### Schema Registry

```bash
# Register schema
danube-cli schema register my-schema \
  --type json_schema --file schema.json

# Get schema
danube-cli schema get my-schema

# List versions
danube-cli schema versions my-schema

# Check compatibility
danube-cli schema check my-schema \
  --type json_schema --file new-schema.json

# JSON output
danube-cli schema get my-schema --output json
```

## Flags & Options

### Producer Flags

| Flag | Short | Description | Default |
|------|-------|-------------|---------|
| `--service-addr` | `-s` | Broker URL | `http://127.0.0.1:6650` |
| `--topic` | `-t` | Topic name | `/default/test_topic` |
| `--message` | `-m` | Message content | Required* |
| `--file` | `-f` | Binary file path | - |
| `--producer-name` | `-n` | Producer name | `test_producer` |
| `--schema-subject` | - | Schema subject | - |
| `--schema-file` | - | Schema file | - |
| `--schema-type` | - | Schema type | - |
| `--count` | `-c` | Number of messages | `1` |
| `--interval` | `-i` | Interval (ms) | `500` |
| `--partitions` | `-p` | Number of partitions | - |
| `--attributes` | `-a` | Message attributes | - |
| `--reliable` | - | Reliable delivery | `false` |

*Required unless `--file` is provided

### Consumer Flags

| Flag | Short | Description | Default |
|------|-------|-------------|---------|
| `--service-addr` | `-s` | Broker URL | Required |
| `--topic` | `-t` | Topic name | `/default/test_topic` |
| `--subscription` | `-m` | Subscription name | Required |
| `--consumer-name` | `-n` | Consumer name | `consumer_pubsub` |
| `--sub-type` | - | Subscription type | `shared` |

### Schema Flags

| Flag | Short | Description | Default |
|------|-------|-------------|---------|
| `--service-addr` | `-s` | Broker URL | `http://127.0.0.1:6650` |
| `--type` | `-t` | Schema type | Required |
| `--file` | `-f` | Schema file | Required |
| `--output` | `-o` | Output format | `table` |
| `--mode` | - | Compatibility mode | - |

## Schema Types

- `json_schema` - JSON Schema validation
- `avro` - Apache Avro
- `protobuf` - Protocol Buffers

## Subscription Types

- `shared` - Load balanced across consumers
- `exclusive` - One active consumer
- `fail-over` - Active/standby with failover

## Output Formats

- `table` - Human-readable table (default)
- `json` - JSON output for scripting

## Examples by Use Case

### Testing & Development

```bash
# Quick test
danube-cli produce -s http://localhost:6650 -m "test"
danube-cli consume -s http://localhost:6650 -m test-sub

# With schema validation
danube-cli schema register test --type json_schema --file test.json
danube-cli produce -s http://localhost:6650 --schema-subject test -m '{"test":true}'
danube-cli consume -s http://localhost:6650 -m test-sub
```

### Production Workloads

```bash
# High-throughput with partitions
danube-cli produce -s http://prod:6650 \
  --topic /production/events \
  --partitions 16 \
  --schema-subject events \
  --reliable \
  -m '{"event":"data"}' \
  -c 10000 -i 100

# Load-balanced consumers
for i in {1..8}; do
  danube-cli consume -s http://prod:6650 \
    -t /production/events \
    -n "consumer-$i" \
    -m event-processors \
    --sub-type shared &
done
```

### Monitoring & Metrics

```bash
# Metrics collection
while true; do
  danube-cli produce -s http://localhost:6650 \
    -t /monitoring/metrics \
    --schema-subject metrics \
    -m "{\"timestamp\":\"$(date -Iseconds)\",\"cpu\":$(get_cpu)}"
  sleep 60
done

# Metrics consumer
danube-cli consume -s http://localhost:6650 \
  -t /monitoring/metrics \
  -m metrics-processor \
  --sub-type exclusive
```

### Schema Evolution

```bash
# 1. Check compatibility
danube-cli schema check my-subject \
  --type json_schema \
  --file schema-v2.json

# 2. Register if compatible
danube-cli schema register my-subject \
  --type json_schema \
  --file schema-v2.json

# 3. Verify versions
danube-cli schema versions my-subject
```

## Environment Variables

```bash
# Set defaults
export DANUBE_SERVICE_URL=http://localhost:6650
export DANUBE_TOPIC=/default/my-topic
```

## Error Handling

```bash
# Check connectivity
danube-cli produce -s http://localhost:6650 -m "test" || echo "Connection failed"

# Verify schema
danube-cli schema get my-subject || echo "Schema not found"

# Test subscription
danube-cli consume -s http://localhost:6650 -m test-sub &
CONSUMER_PID=$!
sleep 2
kill $CONSUMER_PID
```

## Scripting Examples

### Bash Loop

```bash
#!/bin/bash
for i in {1..100}; do
  danube-cli produce -s http://localhost:6650 -m "Message $i"
  sleep 1
done
```

### Parallel Processing

```bash
#!/bin/bash
for i in {1..10}; do
  danube-cli produce -s http://localhost:6650 -m "Parallel $i" &
done
wait
```

### Error Handling

```bash
#!/bin/bash
if danube-cli produce -s http://localhost:6650 -m "test"; then
  echo "Success"
else
  echo "Failed"
  exit 1
fi
```

### JSON Parsing

```bash
#!/bin/bash
SCHEMA_ID=$(danube-cli schema get my-subject --output json | jq -r '.schema_id')
echo "Schema ID: $SCHEMA_ID"
```

## Troubleshooting

| Problem | Solution |
|---------|----------|
| Connection refused | Check broker URL and ensure broker is running |
| Schema validation failed | Verify message matches schema with `schema get` |
| Subscription already exclusive | Use different subscription or change type |
| Message too large | Split message or use file upload |
| No messages received | Verify topic name matches producer |

## Performance Tips

1. **Use partitions for high throughput**: `--partitions 16`
2. **Enable reliable delivery for critical data**: `--reliable`
3. **Use shared subscriptions for parallelism**: `--sub-type shared`
4. **Minimize interval for max speed**: `--interval 100`
5. **Run multiple producers**: Parallel processes
6. **Use binary format for large data**: `--file` instead of `-m`

## Help Commands

```bash
# General help
danube-cli --help

# Command help
danube-cli produce --help
danube-cli consume --help
danube-cli schema --help

# Subcommand help
danube-cli schema register --help
danube-cli schema check --help
```

## Additional Resources

- **[Getting Started](./getting-started.md)** - First steps
- **[Producer Basics](./producer-basics.md)** - Producer guide
- **[Consumer Basics](./consumer-basics.md)** - Consumer guide
- **[Schema Registry](./schema-registry.md)** - Schema management
- **[Examples](./examples.md)** - Real-world examples

---

**Quick reference for daily use! ⚡**

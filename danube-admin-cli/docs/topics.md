# Topics Management

Create and manage topics in your Danube cluster.

## Overview

Topics are the fundamental messaging primitive in Danube. They provide:
- Named channels for publishing and subscribing to messages
- Schema enforcement via Schema Registry
- Partitioning for horizontal scaling
- Reliable or non-reliable delivery modes

## Commands

### List Topics

View topics in a namespace or on a specific broker.

```bash
danube-admin-cli topics list [OPTIONS]
```

**By Namespace:**
```bash
# List all topics in a namespace
danube-admin-cli topics list --namespace default

# JSON output for automation
danube-admin-cli topics list --namespace default --output json
```

**By Broker:**
```bash
# List topics on a specific broker
danube-admin-cli topics list --broker broker-001

# JSON output
danube-admin-cli topics list --broker broker-001 --output json
```

**Example Output (Plain Text):**
```
Topics in namespace 'default':
  /default/user-events
  /default/payment-transactions
  /default/analytics-stream
```

**Example Output (JSON):**
```json
[
  "/default/user-events",
  "/default/payment-transactions",
  "/default/analytics-stream"
]
```

---

### Create a Topic

Create a new topic with optional schema validation.

```bash
danube-admin-cli topics create <TOPIC> [OPTIONS]
```

#### Basic Topic Creation

**Simple Topic (No Schema):**
```bash
# Create topic without schema
danube-admin-cli topics create /default/logs

# Create with reliable delivery
danube-admin-cli topics create /default/events --dispatch-strategy reliable
```

**Using Namespace Flag:**
```bash
# Specify namespace separately
danube-admin-cli topics create my-topic --namespace default

# Equivalent to
danube-admin-cli topics create /default/my-topic
```

#### Schema-Validated Topics

**With Schema Registry:**
```bash
# First, register a schema
danube-admin-cli schemas register user-events \
  --schema-type json_schema \
  --file user-schema.json

# Create topic with schema validation
danube-admin-cli topics create /default/user-events \
  --schema-subject user-events \
  --dispatch-strategy reliable
```

**Example Output:**
```
✅ Topic created: /default/user-events
   Schema subject: user-events
```

#### Partitioned Topics

**Create with Partitions:**
```bash
# Create partitioned topic (3 partitions)
danube-admin-cli topics create /default/high-throughput \
  --partitions 3

# With schema and partitions
danube-admin-cli topics create /default/user-events \
  --partitions 5 \
  --schema-subject user-events \
  --dispatch-strategy reliable
```

**Example Output:**
```
✅ Partitioned topic created: /default/high-throughput
   Schema subject: user-events
   Partitions: 5
```

**Partitioning Guidelines:**

| Throughput | Recommended Partitions |
|------------|----------------------|
| Low (< 1K msg/s) | 1 (non-partitioned) |
| Medium (1K-10K msg/s) | 3-5 |
| High (10K-100K msg/s) | 10-20 |
| Very High (> 100K msg/s) | 50+ |

#### Options Reference

| Option | Description | Default | Example |
|--------|-------------|---------|---------|
| `--namespace` | Namespace (if not in topic path) | - | `--namespace default` |
| `--partitions` | Number of partitions | 1 | `--partitions 3` |
| `--schema-subject` | Schema subject from registry | None | `--schema-subject user-events` |
| `--dispatch-strategy` | Delivery mode | `non_reliable` | `--dispatch-strategy reliable` |

**Dispatch Strategies:**

- **non_reliable**: Fast, at-most-once delivery (fire-and-forget)
  - Use for: Logs, metrics, non-critical events
  - Pros: Low latency, high throughput
  - Cons: Messages may be lost

- **reliable**: Slower, at-least-once delivery (with acknowledgments)
  - Use for: Transactions, orders, critical events
  - Pros: Guaranteed delivery
  - Cons: Higher latency

---

### Describe a Topic

View detailed information about a topic including schema and subscriptions.

```bash
danube-admin-cli topics describe <TOPIC> [OPTIONS]
```

**Basic Usage:**
```bash
danube-admin-cli topics describe /default/user-events
```

**Output Formats:**
```bash
# Plain text (default) - human-readable
danube-admin-cli topics describe /default/user-events

# JSON format - for automation
danube-admin-cli topics describe /default/user-events --output json
```

**Example Output (Plain Text):**
```
Topic: /default/user-events
Broker ID: broker-001
Delivery: Reliable

📋 Schema Registry:
  Subject: user-events
  Schema ID: 12345
  Version: 2
  Type: json_schema
  Compatibility: BACKWARD

Subscriptions: ["analytics-consumer", "audit-logger"]
```

**Example Output (JSON):**
```json
{
  "topic": "/default/user-events",
  "broker_id": "broker-001",
  "delivery": "Reliable",
  "schema_subject": "user-events",
  "schema_id": 12345,
  "schema_version": 2,
  "schema_type": "json_schema",
  "compatibility_mode": "BACKWARD",
  "subscriptions": [
    "analytics-consumer",
    "audit-logger"
  ]
}
```

**Without Schema:**
```
Topic: /default/logs
Broker ID: broker-002
Delivery: NonReliable

📋 Schema: None

Subscriptions: []
```

---

### List Subscriptions

View all active subscriptions for a topic.

```bash
danube-admin-cli topics subscriptions <TOPIC> [OPTIONS]
```

**Basic Usage:**
```bash
danube-admin-cli topics subscriptions /default/user-events
```

**Output Formats:**
```bash
# Plain text
danube-admin-cli topics subscriptions /default/user-events

# JSON format
danube-admin-cli topics subscriptions /default/user-events --output json
```

**Example Output:**
```
Subscriptions: ["consumer-1", "consumer-2", "analytics-team"]
```

---

### Delete a Topic

Permanently remove a topic and all its messages.

```bash
danube-admin-cli topics delete <TOPIC> [OPTIONS]
```

**Basic Usage:**
```bash
danube-admin-cli topics delete /default/old-topic
```

**With Namespace:**
```bash
danube-admin-cli topics delete old-topic --namespace default
```

**Example Output:**
```
✅ Topic deleted: /default/old-topic
```

**⚠️ Important Warnings:**

1. **Data Loss**: All messages in the topic are permanently deleted
2. **No Confirmation**: Operation is immediate and irreversible
3. **Active Subscriptions**: All consumers will be disconnected
4. **Schema Intact**: The schema in the registry is NOT deleted

**Safety Checklist:**
```bash
# 1. Check subscriptions
danube-admin-cli topics subscriptions /default/my-topic

# 2. Verify topic details
danube-admin-cli topics describe /default/my-topic

# 3. Backup if needed (application-level)

# 4. Delete topic
danube-admin-cli topics delete /default/my-topic
```

---

### Unsubscribe

Remove a specific subscription from a topic.

```bash
danube-admin-cli topics unsubscribe <TOPIC> --subscription <NAME> [OPTIONS]
```

**Basic Usage:**
```bash
danube-admin-cli topics unsubscribe /default/user-events \
  --subscription old-consumer
```

**With Namespace:**
```bash
danube-admin-cli topics unsubscribe my-topic \
  --namespace default \
  --subscription old-consumer
```

**Example Output:**
```
✅ Unsubscribed: true
```

**Use Cases:**
- Remove inactive consumers
- Clean up test subscriptions
- Force consumer reconnection

---

### Unload a Topic

Gracefully unload a topic from its current broker.

```bash
danube-admin-cli topics unload <TOPIC> [OPTIONS]
```

**Basic Usage:**
```bash
danube-admin-cli topics unload /default/user-events
```

**Example Output:**
```
✅ Topic unloaded: /default/user-events
```

**Use Cases:**
- Rebalance topics across brokers
- Prepare for broker maintenance
- Move topic to different broker

---

## Connection Configuration

All topic commands use these environment variables:

```bash
# Broker admin endpoint (default: http://127.0.0.1:50051)
export DANUBE_ADMIN_ENDPOINT="http://broker.example.com:50051"

# Enable TLS
export DANUBE_ADMIN_TLS=true
export DANUBE_ADMIN_DOMAIN="broker.example.com"

# TLS certificates (optional)
export DANUBE_ADMIN_CA="/path/to/ca.crt"
export DANUBE_ADMIN_CERT="/path/to/client.crt"
export DANUBE_ADMIN_KEY="/path/to/client.key"
```

---

## Common Workflows

### 1. Create Topic with Schema Validation

**Step-by-step:**
```bash
# Step 1: Register schema
danube-admin-cli schemas register user-events \
  --schema-type json_schema \
  --file schemas/user-events.json \
  --description "User event schema" \
  --tags users analytics

# Step 2: Verify schema
danube-admin-cli schemas get --subject user-events

# Step 3: Create topic
danube-admin-cli topics create /production/user-events \
  --schema-subject user-events \
  --dispatch-strategy reliable \
  --partitions 5

# Step 4: Verify topic
danube-admin-cli topics describe /production/user-events
```

### 2. Schema Evolution

**Update schema for existing topic:**
```bash
# Step 1: Check current compatibility mode
danube-admin-cli schemas get --subject user-events

# Step 2: Test new schema compatibility
danube-admin-cli schemas check user-events \
  --file schemas/user-events-v2.json \
  --schema-type json_schema

# Step 3: If compatible, register new version
danube-admin-cli schemas register user-events \
  --schema-type json_schema \
  --file schemas/user-events-v2.json \
  --description "Added email field"

# Step 4: Verify topic picked up new version
danube-admin-cli topics describe /production/user-events
```

### 3. Multi-Environment Deployment

**Create same topics across environments:**
```bash
# Production
danube-admin-cli topics create /production/user-events \
  --schema-subject user-events \
  --dispatch-strategy reliable \
  --partitions 10

# Staging
danube-admin-cli topics create /staging/user-events \
  --schema-subject user-events \
  --dispatch-strategy reliable \
  --partitions 3

# Development
danube-admin-cli topics create /development/user-events \
  --schema-subject user-events-dev \
  --partitions 1
```

### 4. Topic Cleanup

**Remove old topics:**
```bash
# List topics
danube-admin-cli topics list --namespace old-project

# Check each topic
for topic in $(danube-admin-cli topics list --namespace old-project --output json | jq -r '.[]'); do
  echo "=== $topic ==="
  danube-admin-cli topics subscriptions $topic
  danube-admin-cli topics describe $topic
done

# Delete if safe
danube-admin-cli topics delete /old-project/deprecated-topic
```

### 5. Topic Migration

**Move topic to new namespace:**
```bash
# Step 1: Create new namespace
danube-admin-cli namespaces create new-namespace

# Step 2: Get old topic schema
OLD_SCHEMA=$(danube-admin-cli topics describe /old/topic --output json | jq -r '.schema_subject')

# Step 3: Create new topic with same schema
danube-admin-cli topics create /new-namespace/topic \
  --schema-subject $OLD_SCHEMA \
  --dispatch-strategy reliable

# Step 4: Migrate consumers (application-level)

# Step 5: Delete old topic
danube-admin-cli topics delete /old/topic
```

---

## Topic Naming Best Practices

### ✅ Good Names

```bash
/production/user-events
/staging/payment-transactions
/analytics/clickstream
/logs/application-errors
/metrics/system-health
```

### ❌ Bad Names

```bash
/topic1              # Not descriptive
/PROD/Events         # Inconsistent casing
/my_topic            # Use hyphens, not underscores
/test-test-test      # Not meaningful
```

### Naming Conventions

**Pattern**: `/namespace/domain-event-type`

**Examples:**
```bash
# By domain
/payment/transactions
/payment/refunds
/user/registrations
/user/profile-updates

# By event type
/analytics/clickstream
/analytics/conversions
/logs/application
/logs/security
```

---

## Schema Validation Strategies

### Strategy 1: Strict Validation (Recommended)

**Always use schema subjects:**
```bash
# Register schema first
danube-admin-cli schemas register strict-events \
  --schema-type json_schema \
  --file schema.json

# Create topic with schema
danube-admin-cli topics create /production/events \
  --schema-subject strict-events \
  --dispatch-strategy reliable
```

**Pros:**
- Type safety
- Schema evolution control
- Documentation via schema

**Cons:**
- Extra setup step
- Schema must be defined upfront

### Strategy 2: No Validation

**For logs, metrics, or prototyping:**
```bash
# Create topic without schema
danube-admin-cli topics create /logs/application
```

**Pros:**
- Quick setup
- Flexible data

**Cons:**
- No type safety
- Harder to maintain

---

## Partitioning Guidelines

### When to Use Partitions

**Use Partitions When:**
- Throughput > 1000 messages/second
- Need parallel processing
- Have multiple producers
- Data can be partitioned by key (user ID, region, etc.)

**Don't Use Partitions When:**
- Low throughput (< 100 msg/s)
- Need strict ordering
- Simple use case
- Testing/development

### Partition Count Selection

```bash
# Calculate partitions
# Formula: (Peak msg/s × Message size KB) / (1000 × Target latency ms)

# Example: 10K msg/s, 1KB messages, 100ms latency
# Partitions = (10000 × 1) / (1000 × 100) = ~3-5 partitions
danube-admin-cli topics create /high-throughput/events \
  --partitions 5
```

### Partition Key Strategy

When producing to partitioned topics:
- **User ID**: For user-specific events
- **Region**: For geo-distributed data
- **Tenant ID**: For multi-tenant systems
- **Round-robin**: For load balancing

---

## Troubleshooting

### Topic Creation Fails

```bash
# Check namespace exists
danube-admin-cli brokers namespaces | grep my-namespace

# Verify schema exists
danube-admin-cli schemas get --subject my-schema

# Check namespace policies
danube-admin-cli namespaces policies my-namespace
```

### Schema Not Found

```bash
# List available schemas
danube-admin-cli schemas list

# Register schema first
danube-admin-cli schemas register my-schema \
  --schema-type json_schema \
  --file schema.json
```

### Cannot Delete Topic

```bash
# Check subscriptions
danube-admin-cli topics subscriptions /my/topic

# Unsubscribe all
danube-admin-cli topics unsubscribe /my/topic --subscription consumer-1

# Then delete
danube-admin-cli topics delete /my/topic
```

---

## Performance Tips

### 1. Choose Right Dispatch Strategy

```bash
# For high-throughput, non-critical: non_reliable
danube-admin-cli topics create /metrics/system --dispatch-strategy non_reliable

# For critical data: reliable
danube-admin-cli topics create /orders/payments --dispatch-strategy reliable
```

### 2. Use Partitions for Scale

```bash
# Single partition: ~1K msg/s
danube-admin-cli topics create /low-volume/events

# Multiple partitions: ~10K+ msg/s
danube-admin-cli topics create /high-volume/events --partitions 10
```

### 3. Monitor Topic Health

```bash
# Check topic details
danube-admin-cli topics describe /my/topic --output json

# Monitor subscriptions
danube-admin-cli topics subscriptions /my/topic

# Track schema version
danube-admin-cli schemas get --subject my-schema
```

---

## Quick Reference

```bash
# List topics
danube-admin-cli topics list --namespace <namespace>
danube-admin-cli topics list --broker <broker-id>

# Create topic
danube-admin-cli topics create /namespace/topic
danube-admin-cli topics create /ns/topic --schema-subject <schema>
danube-admin-cli topics create /ns/topic --partitions 3

# Describe topic
danube-admin-cli topics describe /namespace/topic

# Manage subscriptions
danube-admin-cli topics subscriptions /namespace/topic
danube-admin-cli topics unsubscribe /ns/topic --subscription <name>

# Delete topic
danube-admin-cli topics delete /namespace/topic

# Unload topic
danube-admin-cli topics unload /namespace/topic
```

---

## Related Commands

- `danube-admin-cli schemas register` - Register schemas for validation
- `danube-admin-cli schemas get` - View schema details
- `danube-admin-cli namespaces create` - Create namespaces for topics
- `danube-admin-cli brokers list` - View broker topology

---

## Next Steps

1. **Create Schema**: `danube-admin-cli schemas register my-schema --schema-type json_schema --file schema.json`
2. **Create Topic**: `danube-admin-cli topics create /default/my-topic --schema-subject my-schema`
3. **Verify Setup**: `danube-admin-cli topics describe /default/my-topic`
4. **Start Producing**: Use Danube client to publish messages

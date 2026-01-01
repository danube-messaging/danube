# Danube-admin-cli

The danube-admin-cli is a command-line interface designed for interacting with and managing the Danube cluster.

## Danube Admin CLI Commands

### Brokers

```bash
# List active brokers
danube-admin-cli brokers list

# Show leader broker
danube-admin-cli brokers leader-broker

# List namespaces in cluster
danube-admin-cli brokers namespaces
```

**Examples:**

```bash
target/debug/danube-admin-cli brokers list
 BROKER ID            | BROKER STATUS | BROKER ADDRESS      | BROKER ROLE      | ADMIN ADDR           | METRICS ADDR 
 16769495701206859101 | active        | http://0.0.0.0:6650 | Cluster_Leader   | http://0.0.0.0:50051 | 0.0.0.0:9040 
 3823634821110504384  | active        | http://0.0.0.0:6651 | Cluster_Follower | http://0.0.0.0:50052 | 0.0.0.0:9041 
 9235619178526211712  | active        | http://0.0.0.0:6652 | Cluster_Follower | http://0.0.0.0:50053 | 0.0.0.0:9042
```

### Namespaces

```bash
# List topics in a namespace
danube-admin-cli namespaces topics default

# Get namespace policies
danube-admin-cli namespaces policies default

# Create a namespace
danube-admin-cli namespaces create default

# Delete a namespace (must be empty)
danube-admin-cli namespaces delete default
```

**Examples:**

```bash
target/debug/danube-admin-cli namespaces policies default
Policies Configuration:
-----------------------
Max Producers per Topic: 0
Max Subscriptions per Topic: 0
Max Consumers per Topic: 0
Max Consumers per Subscription: 0
Max publish rate: 0
Dispatch rate for subscription: 0
Max message size: 10485760
-----------------------
```

### Topic management

```bash
# List topics in a namespace
danube-admin-cli topics list default
# JSON output
danube-admin-cli topics list default --output json

# Create non-partitioned topic (non-reliable)
danube-admin-cli topics create /default/mytopic --dispatch-strategy non_reliable

# Create non-partitioned topic (reliable with defaults)
danube-admin-cli topics create /default/mytopic --dispatch-strategy reliable

# Create partitioned topic (unified create with --partitions)
danube-admin-cli topics create /default/mytopic \
  --partitions 3 \
  --dispatch-strategy reliable

# You can omit namespace in the topic and provide it via --namespace
danube-admin-cli topics create mytopic --namespace default --dispatch-strategy reliable

# Schema ergonomics: provide type and schema file for Json
danube-admin-cli topics create mytopic \
  --namespace default \
  --schema Json \
  --schema-file ./schema.json

# Delete a topic
danube-admin-cli topics delete /default/mytopic

# List subscriptions on a topic
danube-admin-cli topics subscriptions /default/mytopic
# JSON output
danube-admin-cli topics subscriptions /default/mytopic --output json

# Unsubscribe a subscription from a topic
danube-admin-cli topics unsubscribe --subscription sub1 /default/mytopic

# Describe a topic (schema + subscriptions)
danube-admin-cli topics describe /default/mytopic
# JSON output (describe uses Admin endpoint at DANUBE_ADMIN_ENDPOINT; default http://127.0.0.1:50051)
danube-admin-cli topics describe /default/mytopic --output json
```

**Examples:**

```bash
target/debug/danube-admin-cli topics list --broker 3823634821110504384
Topic: /default/pattern_4-part-2 (broker_id: 3823634821110504384, delivery: Reliable)
Topic: /default/pattern_2-part-0 (broker_id: 3823634821110504384, delivery: NonReliable)
Topic: /default/pattern_2-part-2 (broker_id: 3823634821110504384, delivery: NonReliable)
Topic: /default/pattern_5-part-1 (broker_id: 3823634821110504384, delivery: Reliable)
Topic: /default/pattern_5-part-2 (broker_id: 3823634821110504384, delivery: Reliable)
Topic: /default/pattern_5-part-0 (broker_id: 3823634821110504384, delivery: Reliable)
```

```bash
target/debug/danube-admin-cli topics list --namespace default
Topic: /default/pattern_4-part-2 (broker_id: 3823634821110504384, delivery: Reliable)
Topic: /default/pattern_4-part-0 (broker_id: 9235619178526211712, delivery: Reliable)
Topic: /default/pattern_4-part-1 (broker_id: 16769495701206859101, delivery: Reliable)
Topic: /default/pattern_3 (broker_id: 9235619178526211712, delivery: Reliable)
Topic: /default/pattern_2-part-2 (broker_id: 3823634821110504384, delivery: NonReliable)
Topic: /default/pattern_2-part-1 (broker_id: 9235619178526211712, delivery: NonReliable)
Topic: /default/pattern_2-part-0 (broker_id: 3823634821110504384, delivery: NonReliable)
Topic: /default/pattern_5-part-0 (broker_id: 3823634821110504384, delivery: Reliable)
Topic: /default/pattern_5-part-2 (broker_id: 3823634821110504384, delivery: Reliable)
Topic: /default/pattern_5-part-1 (broker_id: 3823634821110504384, delivery: Reliable)
Topic: /default/pattern_1 (broker_id: 9235619178526211712, delivery: NonReliable)
```

```bash
target/debug/danube-admin-cli topics describe /default/pattern_3
Topic: /default/pattern_3
Broker ID: 9235619178526211712
Delivery: Reliable
Schema: String
Subscriptions: ["s_pattern_3_excl_1", "s_pattern_3_excl_1", "s_pattern_3_shared_1", "s_pattern_3_shared_1"]
```

### Schema Registry

```bash
# Register schema
danube-admin-cli schemas register user-events \
  --schema-type json_schema \
  --file schemas/user-events.json \
  --description "User event schema v1" \
  --tags production users

# Get schema by subject
danube-admin-cli schemas get --subject user-events
# JSON output
danube-admin-cli schemas get --subject user-events --output json

# Get schema by ID
danube-admin-cli schemas get --id 123

# List versions for a subject
danube-admin-cli schemas versions user-events

# Check schema compatibility before registering
danube-admin-cli schemas check user-events \
  --file schemas/user-events-v2.json \
  --schema-type json_schema

# Set compatibility mode (none, backward, forward, full)
danube-admin-cli schemas set-compatibility user-events --mode backward

# Delete specific version
danube-admin-cli schemas delete user-events --version 1 --confirm
```

**Examples:**

```bash
target/debug/danube-admin-cli schemas register payment-events \
  --schema-type json_schema \
  --file schemas/payment.json \
  --description "Payment transaction events"
✅ Schema registered successfully
   Subject: payment-events
   Schema ID: 1
   Version: 1
```

```bash
target/debug/danube-admin-cli schemas versions user-events
📋 Listing versions for subject 'user-events'...
✅ Found 2 version(s):
   • v1
   • v2
```

```bash
target/debug/danube-admin-cli schemas check user-events \
  --file schemas/user-events-v3.json \
  --schema-type json_schema
🔍 Checking compatibility for subject 'user-events'...
✅ Schema is COMPATIBLE
   Safe to register as a new version for 'user-events'
```

## Reliable dispatch options

The `topics create` command accepts a `--dispatch-strategy` flag.

Values:

- `non_reliable` (default)
- `reliable`

## TLS and mTLS

The CLI uses a TLS-aware client factory. Configure via environment variables or by using an HTTPS endpoint.

Environment variables:

- DANUBE_ADMIN_ENDPOINT: Admin endpoint (default: http://127.0.0.1:50051)
- DANUBE_ADMIN_TLS: "true" to force TLS even for http endpoints
- DANUBE_ADMIN_DOMAIN: TLS server name (SNI/verification)
- DANUBE_ADMIN_CA: Path to CA PEM file (optional; system roots used if omitted)
- DANUBE_ADMIN_CERT: Path to client certificate (PEM) for mTLS (optional)
- DANUBE_ADMIN_KEY: Path to client private key (PEM) for mTLS (optional)

Examples:

```bash
# TLS using HTTPS endpoint and custom domain/CA
export DANUBE_ADMIN_ENDPOINT=https://broker.example.com:50051
export DANUBE_ADMIN_DOMAIN=broker.example.com
export DANUBE_ADMIN_CA=./cert/ca-cert.pem

# optional mTLS
export DANUBE_ADMIN_CERT=./cert/client-cert.pem
export DANUBE_ADMIN_KEY=./cert/client-key.pem

danube-admin-cli brokers list
```

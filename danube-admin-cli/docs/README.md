# Danube Admin CLI Documentation

Complete guide to managing your Danube cluster with the admin CLI.

## 📚 Documentation

### Core Components
- **[Brokers](./brokers.md)** - Manage brokers, unload topics, monitor cluster health
- **[Namespaces](./namespaces.md)** - Organize topics with namespaces, manage policies
- **[Topics](./topics.md)** - Create and manage topics with schema validation
- **[Schema Registry](./schema_registry.md)** - Manage schemas, versions, and compatibility

---

## 🚀 Quick Start

### Installation

```bash
# Build from source
cd danube-admin-cli
cargo build --release

# Binary will be at: target/release/danube-admin-cli
```

### First Steps

```bash
# 1. Check cluster health
danube-admin-cli brokers list

# 2. Create a namespace
danube-admin-cli namespaces create my-app

# 3. Register a schema
danube-admin-cli schemas register my-events \
  --schema-type json_schema \
  --file schema.json

# 4. Create a topic
danube-admin-cli topics create /my-app/events \
  --schema-subject my-events \
  --dispatch-strategy reliable

# 5. Verify setup
danube-admin-cli topics describe /my-app/events
```

---

## 📖 Command Reference

### Brokers Management

```bash
# List all brokers
danube-admin-cli brokers list

# Get leader broker
danube-admin-cli brokers leader

# Unload topics from broker
danube-admin-cli brokers unload <broker-id> --dry-run

# Activate broker
danube-admin-cli brokers activate <broker-id>
```

👉 **[Full Brokers Guide](./brokers.md)**

---

### Namespaces Management

```bash
# Create namespace
danube-admin-cli namespaces create <namespace>

# List topics in namespace
danube-admin-cli namespaces topics <namespace>

# View namespace policies
danube-admin-cli namespaces policies <namespace>

# Delete namespace
danube-admin-cli namespaces delete <namespace>
```

👉 **[Full Namespaces Guide](./namespaces.md)**

---

### Topics Management

```bash
# Create topic
danube-admin-cli topics create /namespace/topic

# Create with schema validation
danube-admin-cli topics create /namespace/topic \
  --schema-subject my-schema

# Create partitioned topic
danube-admin-cli topics create /namespace/topic \
  --partitions 5

# Describe topic
danube-admin-cli topics describe /namespace/topic

# List subscriptions
danube-admin-cli topics subscriptions /namespace/topic

# Delete topic
danube-admin-cli topics delete /namespace/topic
```

👉 **[Full Topics Guide](./topics.md)**

---

### Schema Registry

```bash
# Register schema
danube-admin-cli schemas register <subject> \
  --schema-type json_schema \
  --file schema.json

# Get schema
danube-admin-cli schemas get --subject <subject>

# List versions
danube-admin-cli schemas versions <subject>

# Check compatibility
danube-admin-cli schemas check <subject> \
  --file new-schema.json \
  --schema-type json_schema

# Set compatibility mode
danube-admin-cli schemas set-compatibility <subject> \
  --mode backward

# Delete version
danube-admin-cli schemas delete <subject> \
  --version 1 \
  --confirm
```

👉 **[Full Schema Registry Guide](./schema_registry.md)**

---

## 🔧 Configuration

### Environment Variables

All commands support these environment variables:

```bash
# Broker endpoint (default: http://127.0.0.1:50051)
export DANUBE_ADMIN_ENDPOINT="http://broker.example.com:50051"

# TLS Configuration
export DANUBE_ADMIN_TLS=true
export DANUBE_ADMIN_DOMAIN="broker.example.com"

# TLS Certificates (optional)
export DANUBE_ADMIN_CA="/path/to/ca.crt"
export DANUBE_ADMIN_CERT="/path/to/client.crt"
export DANUBE_ADMIN_KEY="/path/to/client.key"
```

### Configuration File (Future)

Coming soon: YAML/TOML configuration file support.

---

## 💡 Common Workflows

### 1. New Application Setup

**Create complete environment for a new application:**

```bash
# Step 1: Create namespace
danube-admin-cli namespaces create payment-service

# Step 2: Register schemas
danube-admin-cli schemas register payment-events \
  --schema-type json_schema \
  --file schemas/payment-events.json \
  --description "Payment transaction events" \
  --tags production payments

danube-admin-cli schemas register refund-events \
  --schema-type json_schema \
  --file schemas/refund-events.json \
  --description "Refund events" \
  --tags production payments

# Step 3: Set compatibility
danube-admin-cli schemas set-compatibility payment-events --mode backward
danube-admin-cli schemas set-compatibility refund-events --mode backward

# Step 4: Create topics
danube-admin-cli topics create /payment-service/transactions \
  --schema-subject payment-events \
  --dispatch-strategy reliable \
  --partitions 5

danube-admin-cli topics create /payment-service/refunds \
  --schema-subject refund-events \
  --dispatch-strategy reliable

# Step 5: Verify
danube-admin-cli namespaces topics payment-service
danube-admin-cli topics describe /payment-service/transactions
```

---

### 2. Schema Evolution

**Safely evolve a schema:**

```bash
# Step 1: Create new schema version
vim schemas/user-events-v2.json

# Step 2: Test compatibility
danube-admin-cli schemas check user-events \
  --file schemas/user-events-v2.json \
  --schema-type json_schema

# Step 3: If compatible (✅), register
danube-admin-cli schemas register user-events \
  --schema-type json_schema \
  --file schemas/user-events-v2.json \
  --description "Added email field"

# Step 4: Verify
danube-admin-cli schemas versions user-events
danube-admin-cli topics describe /production/user-events
```

---

### 3. Multi-Environment Deployment

**Deploy same topics across environments:**

```bash
# Production
danube-admin-cli namespaces create production
danube-admin-cli schemas register prod-events \
  --schema-type json_schema \
  --file schema.json \
  --tags production
danube-admin-cli schemas set-compatibility prod-events --mode full
danube-admin-cli topics create /production/events \
  --schema-subject prod-events \
  --dispatch-strategy reliable \
  --partitions 10

# Staging
danube-admin-cli namespaces create staging
danube-admin-cli schemas register staging-events \
  --schema-type json_schema \
  --file schema.json \
  --tags staging
danube-admin-cli schemas set-compatibility staging-events --mode backward
danube-admin-cli topics create /staging/events \
  --schema-subject staging-events \
  --dispatch-strategy reliable \
  --partitions 3

# Development
danube-admin-cli namespaces create development
danube-admin-cli schemas register dev-events \
  --schema-type json_schema \
  --file schema.json \
  --tags development
danube-admin-cli schemas set-compatibility dev-events --mode none
danube-admin-cli topics create /development/events \
  --schema-subject dev-events \
  --partitions 1
```

---

### 4. Broker Maintenance

**Safely maintain a broker:**

```bash
# Step 1: Preview unload
danube-admin-cli brokers unload broker-001 --dry-run

# Step 2: Unload topics
danube-admin-cli brokers unload broker-001 \
  --max-parallel 5 \
  --timeout 60

# Step 3: Perform maintenance
# (External: restart, upgrade, etc.)

# Step 4: Reactivate
danube-admin-cli brokers activate broker-001 \
  --reason "Maintenance completed"

# Step 5: Verify
danube-admin-cli brokers list
```

---

### 5. Cluster Audit

**Generate cluster report:**

```bash
#!/bin/bash

echo "=== Danube Cluster Audit Report ==="
echo "Date: $(date)"
echo ""

echo "=== Brokers ==="
danube-admin-cli brokers list
echo ""

echo "=== Namespaces ==="
for ns in $(danube-admin-cli brokers namespaces --output json | jq -r '.[]'); do
  echo "Namespace: $ns"
  echo "Topics:"
  danube-admin-cli namespaces topics $ns
  echo "Policies:"
  danube-admin-cli namespaces policies $ns
  echo ""
done

echo "=== Topics with Schemas ==="
danube-admin-cli topics list --namespace production --output json | \
  jq -r '.[]' | \
  while read topic; do
    echo "Topic: $topic"
    danube-admin-cli topics describe $topic | grep -E "Subject|Schema ID|Version|Compatibility"
    echo ""
  done
```

---

## 🎓 Learning Path

### Beginner

1. **[Brokers Guide](./brokers.md)** - Understand cluster topology
2. **[Namespaces Guide](./namespaces.md)** - Organize your topics
3. **[Topics Guide](./topics.md)** - Create your first topic

### Intermediate

4. **[Schema Registry Guide](./schema_registry.md)** - Add type safety
5. Topics Guide: Partitioning section
6. Schema Registry: Compatibility modes

### Advanced

7. Schema Registry: Schema evolution workflows
8. Brokers: Load balancing and maintenance
9. Topics: Performance tuning with partitions

---

## 🔍 Troubleshooting

### Common Issues

#### "Connection refused"
```bash
# Check endpoint
echo $DANUBE_ADMIN_ENDPOINT

# Test connectivity
curl -v http://127.0.0.1:50051

# Verify broker is running
danube-admin-cli brokers list
```

#### "Schema not found"
```bash
# List available schemas (via topics)
danube-admin-cli topics list --namespace production --output json

# Register schema first
danube-admin-cli schemas register my-schema \
  --schema-type json_schema \
  --file schema.json
```

#### "Namespace does not exist"
```bash
# List namespaces
danube-admin-cli brokers namespaces

# Create namespace
danube-admin-cli namespaces create my-namespace
```

#### "Compatibility check failed"
```bash
# View current schema
danube-admin-cli schemas get --subject my-events

# Understand the error message
danube-admin-cli schemas check my-events \
  --file new-schema.json \
  --schema-type json_schema

# Fix schema based on error message
```

---

## 📊 Output Formats

All commands support multiple output formats:

### Plain Text (Default)
Human-readable, formatted output
```bash
danube-admin-cli brokers list
```

### JSON
Machine-readable, for scripting
```bash
danube-admin-cli brokers list --output json | jq .
```

### Example: Process JSON Output
```bash
# Count active brokers
danube-admin-cli brokers list --output json | \
  jq '[.[] | select(.broker_status == "active")] | length'

# List all topics with schemas
danube-admin-cli topics list --namespace production --output json | \
  jq -r '.[]' | \
  while read topic; do
    schema=$(danube-admin-cli topics describe $topic --output json | jq -r '.schema_subject')
    echo "$topic -> $schema"
  done
```

---

## 🛡️ Best Practices

### Security

1. **Use TLS in Production**
   ```bash
   export DANUBE_ADMIN_TLS=true
   export DANUBE_ADMIN_DOMAIN="broker.example.com"
   ```

2. **Protect Credentials**
   ```bash
   # Don't commit certificates to git
   echo "*.crt" >> .gitignore
   echo "*.key" >> .gitignore
   ```

3. **Audit Trail**
   ```bash
   # Always log admin operations
   danube-admin-cli brokers activate broker-001 \
     --reason "Deployed v2.0.1" | tee -a admin-ops.log
   ```

### Operations

1. **Always Dry-Run First**
   ```bash
   danube-admin-cli brokers unload broker-001 --dry-run
   ```

2. **Use JSON for Automation**
   ```bash
   danube-admin-cli topics list --output json | jq .
   ```

3. **Monitor Changes**
   ```bash
   # Before
   danube-admin-cli topics describe /my/topic > before.txt
   
   # Make changes
   
   # After
   danube-admin-cli topics describe /my/topic > after.txt
   diff before.txt after.txt
   ```

### Schema Management

1. **Version Control Schemas**
   ```bash
   git add schemas/
   git commit -m "Added email field to user-events schema"
   ```

2. **Test Compatibility**
   ```bash
   danube-admin-cli schemas check my-events \
     --file new-schema.json \
     --schema-type json_schema
   ```

3. **Document Changes**
   ```bash
   danube-admin-cli schemas register my-events \
     --file schema.json \
     --description "Added email field for v2.1.0"
   ```

---

## 📞 Support

- **Documentation**: This directory
- **Examples**: `/examples` directory
- **Issues**: GitHub Issues
- **Community**: Discord/Slack (coming soon)

---

## 🗺️ Roadmap

### Coming Soon

- [ ] Batch operations (create multiple topics)
- [ ] Topic templates
- [ ] Schema migration tools
- [ ] Configuration file support
- [ ] Interactive mode
- [ ] Shell completion (bash/zsh/fish)

---

## 📝 Quick Reference Card

```bash
# Cluster Health
danube-admin-cli brokers list
danube-admin-cli brokers leader

# Namespace
danube-admin-cli namespaces create <ns>
danube-admin-cli namespaces topics <ns>

# Schema
danube-admin-cli schemas register <subject> --schema-type json_schema --file <file>
danube-admin-cli schemas get --subject <subject>
danube-admin-cli schemas check <subject> --file <file> --schema-type json_schema

# Topic
danube-admin-cli topics create /<ns>/<topic> --schema-subject <subject>
danube-admin-cli topics describe /<ns>/<topic>
danube-admin-cli topics delete /<ns>/<topic>

# Maintenance
danube-admin-cli brokers unload <broker-id> --dry-run
danube-admin-cli brokers activate <broker-id>
```

---

**Ready to get started?** Pick a guide above and dive in! 🚀

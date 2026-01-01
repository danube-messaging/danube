# Schema Registry Guide

Master schema management for reliable, validated messaging! 📋

## Table of Contents
- [What is a Schema Registry?](#what-is-a-schema-registry)
- [Schema Management](#schema-management)
- [Schema Types](#schema-types)
- [Schema Evolution](#schema-evolution)
- [Compatibility Modes](#compatibility-modes)
- [Best Practices](#best-practices)
- [Complete Workflows](#complete-workflows)

## What is a Schema Registry?

The Schema Registry is a centralized repository that stores and manages schemas for your messages.

### Why Use Schemas?

**Without Schemas:**
```bash
# Producer sends anything
danube-cli produce -s http://localhost:6650 -m '{"user":123}'  # number
danube-cli produce -s http://localhost:6650 -m '{"user":"abc"}' # string
danube-cli produce -s http://localhost:6650 -m '{"usr":"xyz"}'  # typo!

# Consumer has no idea what to expect! ❌
```

**With Schemas:**
```bash
# Schema defines the contract
{
  "type": "object",
  "properties": {
    "user_id": {"type": "string"},
    "email": {"type": "string"}
  },
  "required": ["user_id", "email"]
}

# Only valid messages are accepted ✅
# Consumers know exactly what to expect ✅
# Breaking changes are prevented ✅
```

### Benefits

| Benefit | Description |
|---------|-------------|
| **Type Safety** | Prevent invalid data at the source |
| **Documentation** | Schema serves as living documentation |
| **Evolution** | Safe schema updates with compatibility checking |
| **Validation** | Automatic validation for producers and consumers |
| **Versioning** | Track schema changes over time |

## Schema Management

### Register a Schema

```bash
danube-cli schema register <subject> \
  --type <schema-type> \
  --file <schema-file>
```

**Example:**
```bash
# Create a JSON schema file
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

# Register the schema
danube-cli schema register user-events \
  --type json_schema \
  --file user-schema.json
```

**Output:**
```
📤 Registering schema 'user-events' (type: JsonSchema)...
✅ Schema registered successfully!
   Subject: user-events
   Schema ID: 1
   Version: 1
```

### Get Schema Details

```bash
danube-cli schema get <subject>
```

**Example:**
```bash
danube-cli schema get user-events
```

**Output:**
```
✅ Schema Details
==================================================
Subject:       user-events
Version:       1
Schema ID:     1
Type:          json_schema
==================================================
Schema Definition:
{
  "type": "object",
  "properties": {
    "user_id": {"type": "string"},
    "email": {"type": "string", "format": "email"},
    "age": {"type": "integer", "minimum": 0}
  },
  "required": ["user_id", "email"]
}
==================================================
```

### List Schema Versions

```bash
danube-cli schema versions <subject>
```

**Example:**
```bash
danube-cli schema versions user-events
```

**Output:**
```
✅ Schema Versions for 'user-events'
==================================================
Version 1 (ID: 1) - Current
Version 2 (ID: 2)
Version 3 (ID: 3) - Latest
==================================================
Total versions: 3
```

### Check Compatibility

Before registering a new version, check compatibility:

```bash
danube-cli schema check <subject> \
  --type <schema-type> \
  --file <new-schema-file>
```

**Example:**
```bash
# Create updated schema (v2)
cat > user-schema-v2.json << 'EOF'
{
  "type": "object",
  "properties": {
    "user_id": {"type": "string"},
    "email": {"type": "string", "format": "email"},
    "age": {"type": "integer", "minimum": 0},
    "name": {"type": "string"}
  },
  "required": ["user_id", "email"]
}
EOF

# Check compatibility
danube-cli schema check user-events \
  --type json_schema \
  --file user-schema-v2.json
```

**Output (Compatible):**
```
✅ Schema is compatible!
   Subject: user-events
   Compatibility Mode: backward
   
Schema can be safely registered.
```

**Output (Incompatible):**
```
❌ Schema is NOT compatible!
   Subject: user-events
   Compatibility Mode: backward
   
Compatibility errors:
- Required field 'name' added (breaks backward compatibility)

Cannot register this schema version.
```

## Schema Types

### JSON Schema

Most common for JSON messages.

**Create Schema:**
```json
{
  "type": "object",
  "properties": {
    "event_type": {"type": "string"},
    "timestamp": {"type": "string", "format": "date-time"},
    "user_id": {"type": "string"}
  },
  "required": ["event_type", "timestamp"]
}
```

**Register:**
```bash
danube-cli schema register events \
  --type json_schema \
  --file events-schema.json
```

**Use:**
```bash
# Produce with validation
danube-cli produce \
  -s http://localhost:6650 \
  --schema-subject events \
  -m '{"event_type":"login","timestamp":"2024-01-01T10:00:00Z","user_id":"u123"}'
```

### Avro Schema

For compact binary serialization.

**Create Schema:**
```json
{
  "type": "record",
  "name": "User",
  "namespace": "com.example",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "email", "type": "string"},
    {"name": "age", "type": ["null", "int"], "default": null}
  ]
}
```

**Register:**
```bash
danube-cli schema register users \
  --type avro \
  --file user-schema.avsc
```

**Use:**
```bash
danube-cli produce \
  -s http://localhost:6650 \
  --schema-subject users \
  -m '{"id":"u123","email":"user@example.com","age":25}'
```

### Protobuf Schema

For Google Protocol Buffers.

**Create Schema (message.proto):**
```protobuf
syntax = "proto3";

message User {
  string id = 1;
  string email = 2;
  int32 age = 3;
}
```

**Register:**
```bash
danube-cli schema register users \
  --type protobuf \
  --file message.proto
```

**Use:**
```bash
# Send compiled protobuf binary
danube-cli produce \
  -s http://localhost:6650 \
  --schema-subject users \
  --file compiled-message.bin
```

## Schema Evolution

### Evolution Scenarios

#### Adding Optional Fields (Safe)

**V1:**
```json
{
  "type": "object",
  "properties": {
    "user_id": {"type": "string"}
  },
  "required": ["user_id"]
}
```

**V2 (Add optional field):**
```json
{
  "type": "object",
  "properties": {
    "user_id": {"type": "string"},
    "email": {"type": "string"}
  },
  "required": ["user_id"]
}
```

✅ **Backward compatible** - Old consumers can read new messages
✅ **Forward compatible** - New consumers can read old messages

#### Removing Optional Fields (Safe)

**V1:**
```json
{
  "type": "object",
  "properties": {
    "user_id": {"type": "string"},
    "temp_field": {"type": "string"}
  },
  "required": ["user_id"]
}
```

**V2 (Remove optional field):**
```json
{
  "type": "object",
  "properties": {
    "user_id": {"type": "string"}
  },
  "required": ["user_id"]
}
```

✅ **Backward compatible** - Old consumers still work

#### Adding Required Fields (Unsafe)

**V1:**
```json
{
  "type": "object",
  "properties": {
    "user_id": {"type": "string"}
  },
  "required": ["user_id"]
}
```

**V2 (Add required field):**
```json
{
  "type": "object",
  "properties": {
    "user_id": {"type": "string"},
    "email": {"type": "string"}
  },
  "required": ["user_id", "email"]
}
```

❌ **NOT backward compatible** - Old producers can't provide required field

### Safe Evolution Workflow

```bash
# Step 1: Check current schema
danube-cli schema get orders

# Step 2: Create new schema version
cat > orders-v2.json << 'EOF'
{
  "type": "object",
  "properties": {
    "order_id": {"type": "string"},
    "amount": {"type": "number"},
    "currency": {"type": "string", "default": "USD"}
  },
  "required": ["order_id", "amount"]
}
EOF

# Step 3: Check compatibility
danube-cli schema check orders \
  --type json_schema \
  --file orders-v2.json

# Step 4: If compatible, register
danube-cli schema register orders \
  --type json_schema \
  --file orders-v2.json

# Step 5: Verify versions
danube-cli schema versions orders
```

## Compatibility Modes

Compatibility modes control how schemas can evolve.

### Backward (Default)

New schema can read data written with old schema.

**Use when:** Consumers are upgraded before producers

```bash
# Check backward compatibility
danube-cli schema check orders \
  --type json_schema \
  --file orders-v2.json \
  --mode backward
```

**Allowed changes:**
- ✅ Add optional fields
- ✅ Remove required fields

**Forbidden changes:**
- ❌ Add required fields
- ❌ Remove optional fields

### Forward

Old schema can read data written with new schema.

**Use when:** Producers are upgraded before consumers

**Allowed changes:**
- ✅ Remove optional fields
- ✅ Add required fields

**Forbidden changes:**
- ❌ Add optional fields
- ❌ Remove required fields

### Full

Both backward and forward compatible.

**Use when:** Consumers and producers upgrade independently

**Allowed changes:**
- ✅ Add optional fields with defaults
- ✅ Remove optional fields

**Forbidden changes:**
- ❌ Add required fields
- ❌ Remove required fields
- ❌ Change field types

### None

No compatibility checking.

**Use when:** Breaking changes are acceptable

⚠️ **Warning:** Can break consumers!

## Best Practices

### 1. Always Use Schemas in Production

```bash
# ❌ Don't do this in production
danube-cli produce -s http://prod:6650 -m '{"data":"value"}'

# ✅ Do this
danube-cli produce \
  -s http://prod:6650 \
  --schema-subject my-events \
  -m '{"data":"value"}'
```

### 2. Check Compatibility Before Evolving

```bash
# Always check first!
danube-cli schema check orders \
  --type json_schema \
  --file orders-v2.json

# Then register if compatible
danube-cli schema register orders \
  --type json_schema \
  --file orders-v2.json
```

### 3. Use Descriptive Subject Names

```bash
# ❌ Bad
danube-cli schema register schema1 ...

# ✅ Good
danube-cli schema register user-registration-events ...
```

### 4. Keep Schemas in Version Control

```bash
schemas/
├── users/
│   ├── v1.json
│   ├── v2.json
│   └── v3.json
├── orders/
│   ├── v1.json
│   └── v2.json
└── events/
    └── v1.json
```

### 5. Document Breaking Changes

Add comments in schema files:

```json
{
  "type": "object",
  "title": "User Event v2",
  "description": "Added optional 'name' field. Backward compatible with v1.",
  "properties": {
    "user_id": {"type": "string"},
    "name": {"type": "string"}
  }
}
```

### 6. Use Default Values for New Fields

```json
{
  "type": "object",
  "properties": {
    "user_id": {"type": "string"},
    "status": {"type": "string", "default": "active"}
  }
}
```

### 7. Test Schema Changes

```bash
# Test in development first
danube-cli schema check --mode backward test-subject --file new-schema.json

# Then staging
danube-cli schema check --mode backward staging-subject --file new-schema.json

# Finally production
danube-cli schema check --mode backward prod-subject --file new-schema.json
```

## Complete Workflows

### Workflow 1: New Schema from Scratch

```bash
# Step 1: Create schema file
cat > payment-events.json << 'EOF'
{
  "type": "object",
  "properties": {
    "payment_id": {"type": "string"},
    "amount": {"type": "number", "minimum": 0},
    "currency": {"type": "string"},
    "status": {"type": "string", "enum": ["pending", "completed", "failed"]}
  },
  "required": ["payment_id", "amount", "currency", "status"]
}
EOF

# Step 2: Register schema
danube-cli schema register payment-events \
  --type json_schema \
  --file payment-events.json

# Step 3: Verify registration
danube-cli schema get payment-events

# Step 4: Start producer with schema
danube-cli produce \
  -s http://localhost:6650 \
  -t /production/payments \
  --schema-subject payment-events \
  -m '{"payment_id":"pay_123","amount":99.99,"currency":"USD","status":"completed"}'

# Step 5: Start consumer (automatic validation)
danube-cli consume \
  -s http://localhost:6650 \
  -t /production/payments \
  -m payment-processor
```

### Workflow 2: Schema Evolution

```bash
# Step 1: Check current schema
danube-cli schema get user-events
danube-cli schema versions user-events

# Step 2: Create new schema version
cat > user-events-v2.json << 'EOF'
{
  "type": "object",
  "properties": {
    "user_id": {"type": "string"},
    "event": {"type": "string"},
    "timestamp": {"type": "string"},
    "metadata": {"type": "object"}
  },
  "required": ["user_id", "event", "timestamp"]
}
EOF

# Step 3: Check compatibility
danube-cli schema check user-events \
  --type json_schema \
  --file user-events-v2.json

# Step 4: Register if compatible
danube-cli schema register user-events \
  --type json_schema \
  --file user-events-v2.json

# Step 5: Verify new version
danube-cli schema versions user-events

# Step 6: Test with new schema
danube-cli produce \
  -s http://localhost:6650 \
  --schema-subject user-events \
  -m '{"user_id":"u123","event":"login","timestamp":"2024-01-01T10:00:00Z","metadata":{"ip":"127.0.0.1"}}'
```

### Workflow 3: Multi-Environment Schema Management

```bash
#!/bin/bash
# Promote schema across environments

SCHEMA_FILE="orders-schema.json"
SUBJECT="orders"

# Development
echo "Registering in development..."
danube-cli schema register $SUBJECT \
  -s http://dev:6650 \
  --type json_schema \
  --file $SCHEMA_FILE

# Staging
echo "Checking compatibility in staging..."
danube-cli schema check $SUBJECT \
  -s http://staging:6650 \
  --type json_schema \
  --file $SCHEMA_FILE

if [ $? -eq 0 ]; then
  echo "Registering in staging..."
  danube-cli schema register $SUBJECT \
    -s http://staging:6650 \
    --type json_schema \
    --file $SCHEMA_FILE
else
  echo "Not compatible with staging! Aborting."
  exit 1
fi

# Production (with approval)
read -p "Promote to production? (yes/no): " approval
if [ "$approval" == "yes" ]; then
  echo "Registering in production..."
  danube-cli schema register $SUBJECT \
    -s http://production:6650 \
    --type json_schema \
    --file $SCHEMA_FILE
fi
```

### Workflow 4: Automated Schema Testing

```bash
#!/bin/bash
# CI/CD schema validation

SCHEMA_DIR="./schemas"
ENVIRONMENT=${1:-development}

case $ENVIRONMENT in
  development)
    SERVICE_ADDR="http://localhost:6650"
    ;;
  staging)
    SERVICE_ADDR="http://staging:6650"
    ;;
  production)
    SERVICE_ADDR="http://production:6650"
    ;;
esac

echo "Testing schemas in $ENVIRONMENT..."

for schema_file in $SCHEMA_DIR/*.json; do
  subject=$(basename $schema_file .json)
  
  echo "Checking $subject..."
  
  danube-cli schema check $subject \
    -s $SERVICE_ADDR \
    --type json_schema \
    --file $schema_file
  
  if [ $? -ne 0 ]; then
    echo "❌ Schema $subject failed compatibility check!"
    exit 1
  fi
  
  echo "✅ Schema $subject is compatible"
done

echo "All schemas passed validation!"
```

## Troubleshooting

### Schema Not Found

```bash
# Check if schema is registered
danube-cli schema get my-subject

# If not found, register it
danube-cli schema register my-subject --type json_schema --file schema.json
```

### Validation Failures

```bash
# Get current schema
danube-cli schema get my-subject --output json

# Verify your message matches the schema
# Check required fields, types, formats
```

### Compatibility Issues

```bash
# Check what compatibility mode is set
danube-cli schema get my-subject

# Test with different modes
danube-cli schema check my-subject --mode backward --file new-schema.json
danube-cli schema check my-subject --mode forward --file new-schema.json
danube-cli schema check my-subject --mode full --file new-schema.json
```

## JSON Output for Automation

All schema commands support JSON output:

```bash
# Get schema as JSON
danube-cli schema get user-events --output json | jq .

# List versions as JSON
danube-cli schema versions user-events --output json | jq .

# Check compatibility with JSON output
danube-cli schema check user-events \
  --type json_schema \
  --file new-schema.json \
  --output json | jq .
```

**Example automation:**
```bash
#!/bin/bash
# Automated schema deployment

SCHEMA_ID=$(danube-cli schema get orders --output json | jq -r '.schema_id')
echo "Current schema ID: $SCHEMA_ID"

# Deploy based on schema version
if [ "$SCHEMA_ID" == "1" ]; then
  echo "Deploying v1 consumer..."
else
  echo "Deploying v$SCHEMA_ID consumer..."
fi
```

## Next Steps

- 📤 **[Producer Advanced](./producer-advanced.md)** - Use schemas in production
- 📥 **[Consumer Advanced](./consumer-advanced.md)** - Consume with schema validation
- 🎯 **[Examples](./examples.md)** - Complete end-to-end examples

---

**Master schema management! 📋**

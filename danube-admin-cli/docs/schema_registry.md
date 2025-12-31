# Schema Registry Management

Manage schemas for data validation and evolution in Danube.

## Overview

The Schema Registry provides centralized schema management for your Danube topics. It enables:
- **Type Safety**: Validate messages against defined schemas
- **Schema Evolution**: Track and manage schema versions over time
- **Compatibility Checking**: Ensure new schemas don't break existing consumers
- **Documentation**: Schemas serve as living documentation for your data

### Why Use Schema Registry?

**Without Schema Registry:**
```bash
# No validation - anything goes
producer.send('{"nam": "John"}')  # Typo: "nam" instead of "name"
# Message accepted ❌ - consumers break
```

**With Schema Registry:**
```bash
# Schema enforces structure
producer.send('{"nam": "John"}')  # Typo detected
# Error: Field 'name' is required ✅
```

---

## Commands

### Register a Schema

Register a new schema or create a new version of an existing schema.

```bash
danube-admin-cli schemas register <SUBJECT> [OPTIONS]
```

#### Basic Schema Registration

**From File (Recommended):**
```bash
danube-admin-cli schemas register user-events \
  --schema-type json_schema \
  --file schemas/user-events.json
```

**Inline Schema:**
```bash
danube-admin-cli schemas register simple-events \
  --schema-type json_schema \
  --schema '{"type": "object", "properties": {"id": {"type": "string"}}}'
```

**Example Output:**
```
✅ Registered new schema version
Subject: user-events
Schema ID: 12345
Version: 1
Fingerprint: sha256:abc123...
```

#### With Metadata

**Add Description and Tags:**
```bash
danube-admin-cli schemas register user-events \
  --schema-type json_schema \
  --file schemas/user-events.json \
  --description "User registration and login events" \
  --tags users \
  --tags authentication \
  --tags analytics
```

**Tags for Organization:**
- `production`, `staging`, `development` - Environment
- `team-analytics`, `team-platform` - Ownership
- `pii`, `sensitive` - Data classification
- `v1`, `v2` - Version tracking
- `deprecated` - Lifecycle status

#### Schema Types

| Type | Description | Use Cases | Extension |
|------|-------------|-----------|-----------|
| `json_schema` | JSON Schema (Draft 7) | Web APIs, JavaScript/TypeScript | `.json` |
| `avro` | Apache Avro | Big data, Kafka integration | `.avsc` |
| `protobuf` | Protocol Buffers | gRPC, high performance | `.proto` |
| `string` | Plain string (no validation) | Simple text messages | `.txt` |
| `bytes` | Raw bytes (no validation) | Binary data | - |

#### JSON Schema Example

**schemas/user-events.json:**
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "UserEvent",
  "type": "object",
  "required": ["event_type", "user_id", "timestamp"],
  "properties": {
    "event_type": {
      "type": "string",
      "enum": ["login", "logout", "register"]
    },
    "user_id": {
      "type": "string",
      "format": "uuid"
    },
    "timestamp": {
      "type": "string",
      "format": "date-time"
    },
    "metadata": {
      "type": "object",
      "properties": {
        "ip_address": { "type": "string" },
        "user_agent": { "type": "string" }
      }
    }
  }
}
```

**Register:**
```bash
danube-admin-cli schemas register user-events \
  --schema-type json_schema \
  --file schemas/user-events.json \
  --description "User authentication events" \
  --tags users authentication
```

#### Avro Schema Example

**schemas/payment.avsc:**
```json
{
  "type": "record",
  "name": "Payment",
  "namespace": "com.example.payments",
  "fields": [
    {"name": "payment_id", "type": "string"},
    {"name": "amount", "type": "double"},
    {"name": "currency", "type": "string"},
    {"name": "timestamp", "type": "long"}
  ]
}
```

**Register:**
```bash
danube-admin-cli schemas register payment-events \
  --schema-type avro \
  --file schemas/payment.avsc \
  --description "Payment transaction events"
```

---

### Get a Schema

Retrieve schema details by subject or ID.

```bash
danube-admin-cli schemas get [OPTIONS]
```

#### By Subject (Latest Version)

```bash
# Get latest version
danube-admin-cli schemas get --subject user-events
```

**Example Output:**
```
Schema ID: 12345
Version: 2
Subject: user-events
Type: json_schema
Compatibility Mode: BACKWARD
Description: User registration and login events
Tags: users, authentication, analytics

Schema Definition:
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "UserEvent",
  "type": "object",
  "required": ["event_type", "user_id", "timestamp"],
  ...
}
```

#### By Schema ID

```bash
# Get specific schema by ID
danube-admin-cli schemas get --id 12345

# Get specific version of a schema
danube-admin-cli schemas get --id 12345 --version 1
```

#### JSON Output

```bash
danube-admin-cli schemas get --subject user-events --output json
```

**Example JSON Output:**
```json
{
  "schema_id": 12345,
  "version": 2,
  "subject": "user-events",
  "schema_type": "json_schema",
  "schema_definition": "{ ... }",
  "description": "User registration and login events",
  "created_at": 1704067200,
  "created_by": "admin",
  "tags": ["users", "authentication", "analytics"],
  "fingerprint": "sha256:abc123...",
  "compatibility_mode": "BACKWARD"
}
```

---

### List Schema Versions

View all versions for a schema subject.

```bash
danube-admin-cli schemas versions <SUBJECT> [OPTIONS]
```

**Basic Usage:**
```bash
danube-admin-cli schemas versions user-events
```

**Example Output:**
```
Versions for subject 'user-events':
  Version 1: schema_id=12344, fingerprint=sha256:old123...
    Created by: alice
    Description: Initial schema
  Version 2: schema_id=12345, fingerprint=sha256:abc123...
    Created by: bob
    Description: Added email field
  Version 3: schema_id=12346, fingerprint=sha256:new456...
    Created by: charlie
    Description: Made phone optional
```

**JSON Output:**
```bash
danube-admin-cli schemas versions user-events --output json
```

**Example JSON:**
```json
[
  {
    "version": 1,
    "schema_id": 12344,
    "created_at": 1704067200,
    "created_by": "alice",
    "description": "Initial schema",
    "fingerprint": "sha256:old123..."
  },
  {
    "version": 2,
    "schema_id": 12345,
    "created_at": 1704153600,
    "created_by": "bob",
    "description": "Added email field",
    "fingerprint": "sha256:abc123..."
  }
]
```

---

### Check Schema Compatibility

Verify if a new schema is compatible with existing versions.

```bash
danube-admin-cli schemas check <SUBJECT> [OPTIONS]
```

**Basic Usage:**
```bash
danube-admin-cli schemas check user-events \
  --file schemas/user-events-v2.json \
  --schema-type json_schema
```

**Example Output (Compatible):**
```
✅ Schema is compatible with subject 'user-events'
```

**Example Output (Incompatible):**
```
❌ Schema is NOT compatible with subject 'user-events'

Compatibility errors:
  - Field 'user_id' was removed (breaking change)
  - Field 'email' is now required (breaking change for existing consumers)
```

#### Override Compatibility Mode

```bash
# Check with specific mode (overrides subject's default)
danube-admin-cli schemas check user-events \
  --file schemas/user-events-v2.json \
  --schema-type json_schema \
  --mode full
```

#### Workflow Example

```bash
# Step 1: Create new schema version
vim schemas/user-events-v2.json

# Step 2: Check compatibility BEFORE registering
danube-admin-cli schemas check user-events \
  --file schemas/user-events-v2.json \
  --schema-type json_schema

# Step 3: If compatible, register it
if [ $? -eq 0 ]; then
  danube-admin-cli schemas register user-events \
    --schema-type json_schema \
    --file schemas/user-events-v2.json \
    --description "Added email field"
fi
```

---

### Set Compatibility Mode

Configure how schema evolution is enforced.

```bash
danube-admin-cli schemas set-compatibility <SUBJECT> --mode <MODE>
```

**Compatibility Modes:**

| Mode | Description | Allows | Use Case |
|------|-------------|--------|----------|
| `none` | No compatibility checks | Any changes | Development, testing |
| `backward` | New schema can read old data | Add optional fields, remove fields | Most common - new consumers, old producers |
| `forward` | Old schema can read new data | Remove optional fields, add fields | New producers, old consumers |
| `full` | Both backward and forward | Add/remove optional fields only | Strict compatibility |

**Examples:**

```bash
# Set backward compatibility (most common)
danube-admin-cli schemas set-compatibility user-events --mode backward

# Set full compatibility (strict)
danube-admin-cli schemas set-compatibility payment-events --mode full

# Disable compatibility (development only)
danube-admin-cli schemas set-compatibility test-events --mode none
```

**Example Output:**
```
✅ Compatibility mode set for subject 'user-events'
Mode: BACKWARD
```

#### Backward Compatibility (Recommended)

**Allows:**
- ✅ Adding optional fields
- ✅ Removing fields
- ✅ Adding enum values

**Prevents:**
- ❌ Removing required fields
- ❌ Changing field types
- ❌ Making optional fields required

**Example:**
```bash
# Old schema
{
  "properties": {
    "user_id": {"type": "string"},
    "name": {"type": "string"}
  },
  "required": ["user_id", "name"]
}

# New schema (backward compatible)
{
  "properties": {
    "user_id": {"type": "string"},
    "name": {"type": "string"},
    "email": {"type": "string"}  // ✅ Added optional field
  },
  "required": ["user_id", "name"]
}
```

#### Forward Compatibility

**Allows:**
- ✅ Removing optional fields
- ✅ Adding fields

**Prevents:**
- ❌ Adding required fields
- ❌ Changing field types

#### Full Compatibility (Strictest)

**Allows:**
- ✅ Only changes that are both backward AND forward compatible
- ✅ Adding optional fields with defaults
- ✅ Removing optional fields

**Prevents:**
- ❌ Most breaking changes
- ❌ Required field modifications

---

### Delete Schema Version

Remove a specific version of a schema.

```bash
danube-admin-cli schemas delete <SUBJECT> --version <VERSION> --confirm
```

**Basic Usage:**
```bash
danube-admin-cli schemas delete user-events --version 1 --confirm
```

**Example Output:**
```
✅ Deleted version 1 of subject 'user-events'
```

**⚠️ Important Notes:**

1. **Requires Confirmation**: Must use `--confirm` flag to prevent accidents
2. **Cannot Delete Active**: Cannot delete version currently used by topics
3. **No Undo**: Deletion is permanent
4. **Version History**: Gaps in version numbers are normal after deletion

**Safety Checks:**
```bash
# Step 1: List all versions
danube-admin-cli schemas versions user-events

# Step 2: Check which version is active
danube-admin-cli topics describe /production/events | grep "Version:"

# Step 3: Only delete if not active and confirmed safe
danube-admin-cli schemas delete user-events --version 1 --confirm
```

---

## Connection Configuration

All schema commands use these environment variables:

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

### 1. Initial Schema Setup

```bash
# Step 1: Create schema file
cat > schemas/user-events.json << 'EOF'
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": ["event_type", "user_id"],
  "properties": {
    "event_type": {"type": "string"},
    "user_id": {"type": "string"}
  }
}
EOF

# Step 2: Register schema
danube-admin-cli schemas register user-events \
  --schema-type json_schema \
  --file schemas/user-events.json \
  --description "User event schema v1" \
  --tags production users

# Step 3: Set compatibility mode
danube-admin-cli schemas set-compatibility user-events --mode backward

# Step 4: Create topic with schema
danube-admin-cli topics create /production/user-events \
  --schema-subject user-events \
  --dispatch-strategy reliable

# Step 5: Verify setup
danube-admin-cli topics describe /production/user-events
```

### 2. Schema Evolution (Add Optional Field)

```bash
# Step 1: Update schema file
cat > schemas/user-events-v2.json << 'EOF'
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": ["event_type", "user_id"],
  "properties": {
    "event_type": {"type": "string"},
    "user_id": {"type": "string"},
    "email": {"type": "string"}  // New optional field
  }
}
EOF

# Step 2: Test compatibility
danube-admin-cli schemas check user-events \
  --file schemas/user-events-v2.json \
  --schema-type json_schema

# Step 3: If compatible, register
danube-admin-cli schemas register user-events \
  --schema-type json_schema \
  --file schemas/user-events-v2.json \
  --description "Added optional email field"

# Step 4: Verify new version
danube-admin-cli schemas versions user-events
```

### 3. Multi-Environment Schema Management

```bash
# Development: No compatibility checks
danube-admin-cli schemas register dev-events \
  --schema-type json_schema \
  --file schemas/events.json \
  --tags development

danube-admin-cli schemas set-compatibility dev-events --mode none

# Staging: Test compatibility
danube-admin-cli schemas register staging-events \
  --schema-type json_schema \
  --file schemas/events.json \
  --tags staging

danube-admin-cli schemas set-compatibility staging-events --mode backward

# Production: Strict compatibility
danube-admin-cli schemas register prod-events \
  --schema-type json_schema \
  --file schemas/events.json \
  --tags production

danube-admin-cli schemas set-compatibility prod-events --mode full
```

### 4. Schema Audit

```bash
# List all schemas (via topics)
danube-admin-cli topics list --namespace production --output json | \
  jq -r '.[]' | \
  while read topic; do
    echo "=== $topic ==="
    danube-admin-cli topics describe $topic | grep "Subject:"
  done

# Get details for each schema
danube-admin-cli schemas get --subject user-events
danube-admin-cli schemas versions user-events
```

### 5. Schema Cleanup

```bash
# Step 1: List all versions
danube-admin-cli schemas versions old-events

# Step 2: Check which versions are in use
danube-admin-cli topics describe /production/events | grep "Version:"

# Step 3: Delete unused versions
danube-admin-cli schemas delete old-events --version 1 --confirm
danube-admin-cli schemas delete old-events --version 2 --confirm
```

---

## Best Practices

### 1. Schema Design

**✅ Do:**
- Use descriptive field names (`user_id` not `uid`)
- Include field descriptions in schema
- Use standard formats (`date-time`, `email`, `uuid`)
- Version schema files (`user-events-v1.json`, `user-events-v2.json`)
- Keep schemas simple and focused

**❌ Don't:**
- Use generic names (`data`, `payload`, `info`)
- Make everything required
- Use complex nested structures without reason
- Skip documentation

**Good Schema Example:**
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "UserRegistrationEvent",
  "description": "Event emitted when a user registers",
  "type": "object",
  "required": ["event_id", "user_id", "timestamp"],
  "properties": {
    "event_id": {
      "type": "string",
      "format": "uuid",
      "description": "Unique event identifier"
    },
    "user_id": {
      "type": "string",
      "format": "uuid",
      "description": "Unique user identifier"
    },
    "email": {
      "type": "string",
      "format": "email",
      "description": "User email address"
    },
    "timestamp": {
      "type": "string",
      "format": "date-time",
      "description": "Event timestamp in ISO 8601"
    }
  }
}
```

### 2. Compatibility Mode Selection

**Development**: `none`
- Fast iteration
- No restrictions
- Switch to `backward` before production

**Staging**: `backward`
- Test compatibility
- Catch issues early
- Same as production

**Production**: `backward` or `full`
- `backward`: Most flexible (recommended)
- `full`: Strictest safety

### 3. Versioning Strategy

**Option 1: Subject Per Version**
```bash
danube-admin-cli schemas register user-events-v1 ...
danube-admin-cli schemas register user-events-v2 ...
```

**Option 2: Subject with Versions (Recommended)**
```bash
danube-admin-cli schemas register user-events ...  # v1
danube-admin-cli schemas register user-events ...  # v2 (same subject)
```

### 4. Testing Changes

```bash
# Always test before registering
danube-admin-cli schemas check user-events \
  --file new-schema.json \
  --schema-type json_schema

# Test with different modes
danube-admin-cli schemas check user-events \
  --file new-schema.json \
  --schema-type json_schema \
  --mode full
```

### 5. Documentation

**Maintain a schema catalog:**
```markdown
# Schema Catalog

## user-events
- **Type**: json_schema
- **Owner**: Platform Team
- **Compatibility**: BACKWARD
- **Current Version**: 3
- **Topics**: /production/user-events, /staging/user-events
- **Description**: User authentication and registration events
- **Last Updated**: 2024-01-15
```

---

## Troubleshooting

### Schema Registration Fails

```bash
# Check schema syntax (JSON)
cat schemas/my-schema.json | jq .

# Validate JSON Schema
# Use online validator: https://www.jsonschemavalidator.net/

# Check file path
ls -la schemas/my-schema.json
```

### Compatibility Check Fails

```bash
# View current schema
danube-admin-cli schemas get --subject my-events

# Compare with new schema
diff <(danube-admin-cli schemas get --subject my-events --output json | jq .schema_definition) \
     <(cat new-schema.json)

# Understand the error
danube-admin-cli schemas check my-events \
  --file new-schema.json \
  --schema-type json_schema
# Read the error messages carefully
```

### Cannot Delete Version

```bash
# Check if version is in use
danube-admin-cli topics describe /production/events | grep "Version:"

# If in use, you cannot delete
# Solution: Update topic to newer version first, then delete
```

---

## Quick Reference

```bash
# Register schema
danube-admin-cli schemas register <subject> \
  --schema-type <type> \
  --file <file>

# Get schema
danube-admin-cli schemas get --subject <subject>
danube-admin-cli schemas get --id <id>

# List versions
danube-admin-cli schemas versions <subject>

# Check compatibility
danube-admin-cli schemas check <subject> \
  --file <file> \
  --schema-type <type>

# Set compatibility mode
danube-admin-cli schemas set-compatibility <subject> --mode <mode>

# Delete version
danube-admin-cli schemas delete <subject> --version <n> --confirm
```

---

## Schema Evolution Examples

### Adding Optional Field (Backward Compatible) ✅

**Before:**
```json
{
  "required": ["id", "name"],
  "properties": {
    "id": {"type": "string"},
    "name": {"type": "string"}
  }
}
```

**After:**
```json
{
  "required": ["id", "name"],
  "properties": {
    "id": {"type": "string"},
    "name": {"type": "string"},
    "email": {"type": "string"}  // Added optional
  }
}
```

### Removing Field (Backward Compatible) ✅

**Before:**
```json
{
  "required": ["id", "name", "temp"],
  "properties": {
    "id": {"type": "string"},
    "name": {"type": "string"},
    "temp": {"type": "string"}
  }
}
```

**After:**
```json
{
  "required": ["id", "name"],
  "properties": {
    "id": {"type": "string"},
    "name": {"type": "string"}
    // Removed temp
  }
}
```

### Making Field Required (Breaking) ❌

**Before:**
```json
{
  "required": ["id"],
  "properties": {
    "id": {"type": "string"},
    "email": {"type": "string"}
  }
}
```

**After:**
```json
{
  "required": ["id", "email"],  // Made email required ❌
  "properties": {
    "id": {"type": "string"},
    "email": {"type": "string"}
  }
}
```

**Why Breaking**: Old data without `email` field won't validate

### Changing Field Type (Breaking) ❌

**Before:**
```json
{
  "properties": {
    "age": {"type": "string"}
  }
}
```

**After:**
```json
{
  "properties": {
    "age": {"type": "number"}  // Changed type ❌
  }
}
```

**Why Breaking**: Old string values won't parse as numbers

---

## Next Steps

1. **Create Your First Schema**: Design JSON schema for your events
2. **Register Schema**: `danube-admin-cli schemas register my-events --schema-type json_schema --file schema.json`
3. **Set Compatibility**: `danube-admin-cli schemas set-compatibility my-events --mode backward`
4. **Create Topic**: `danube-admin-cli topics create /default/events --schema-subject my-events`
5. **Evolve Schema**: Test compatibility before registering new versions

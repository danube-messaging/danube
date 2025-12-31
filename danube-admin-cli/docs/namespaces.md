# Namespaces Management

Organize your topics with namespaces in Danube.

## Overview

Namespaces provide logical isolation for topics in your Danube cluster. Use namespaces to:
- Organize topics by application, team, or environment
- Apply policies at the namespace level
- Control access and resource allocation
- Separate production, staging, and development workloads

## Commands

### List Topics in a Namespace

View all topics within a specific namespace.

```bash
danube-admin-cli namespaces topics <NAMESPACE>
```

**Basic Usage:**
```bash
danube-admin-cli namespaces topics default
```

**Output Formats:**
```bash
# Plain text (default)
danube-admin-cli namespaces topics default

# JSON format - for automation
danube-admin-cli namespaces topics default --output json
```

**Example Output (Plain Text):**
```
Topics in namespace 'default':
  /default/user-events
  /default/payment-logs
  /default/analytics
```

**Example Output (JSON):**
```json
[
  "/default/user-events",
  "/default/payment-logs",
  "/default/analytics"
]
```

---

### View Namespace Policies

Get the policies configured for a namespace.

```bash
danube-admin-cli namespaces policies <NAMESPACE>
```

**Basic Usage:**
```bash
danube-admin-cli namespaces policies default
```

**Output Formats:**
```bash
# Plain text (default) - pretty printed
danube-admin-cli namespaces policies default

# JSON format
danube-admin-cli namespaces policies default --output json
```

**Example Output (Plain Text):**
```
Policies for namespace 'default':
{
  "max_topics_per_namespace": 1000,
  "max_producers_per_topic": 100,
  "max_consumers_per_topic": 100,
  "message_ttl_seconds": 604800,
  "retention_policy": "time_based"
}
```

**Example Output (JSON):**
```json
{
  "max_topics_per_namespace": 1000,
  "max_producers_per_topic": 100,
  "max_consumers_per_topic": 100,
  "message_ttl_seconds": 604800,
  "retention_policy": "time_based"
}
```

**Common Policies:**

| Policy | Description | Typical Values |
|--------|-------------|----------------|
| `max_topics_per_namespace` | Maximum number of topics | `100` - `10000` |
| `max_producers_per_topic` | Maximum producers per topic | `10` - `1000` |
| `max_consumers_per_topic` | Maximum consumers per topic | `10` - `1000` |
| `message_ttl_seconds` | Message time-to-live | `3600` (1h) - `604800` (7d) |
| `retention_policy` | How messages are retained | `time_based`, `size_based` |

---

### Create a Namespace

Create a new namespace in the cluster.

```bash
danube-admin-cli namespaces create <NAMESPACE>
```

**Basic Usage:**
```bash
# Create namespace
danube-admin-cli namespaces create production
```

**Example Output:**
```
✅ Namespace created: production
```

**Naming Guidelines:**
- Use lowercase letters and hyphens
- Keep names descriptive: `production`, `staging`, `dev`
- Avoid special characters
- Use consistent naming: `team-app-env` pattern

**Examples:**
```bash
# By environment
danube-admin-cli namespaces create production
danube-admin-cli namespaces create staging
danube-admin-cli namespaces create development

# By team
danube-admin-cli namespaces create analytics-team
danube-admin-cli namespaces create platform-team

# By application
danube-admin-cli namespaces create payment-service
danube-admin-cli namespaces create user-service
```

---

### Delete a Namespace

Remove a namespace from the cluster.

```bash
danube-admin-cli namespaces delete <NAMESPACE>
```

**Basic Usage:**
```bash
danube-admin-cli namespaces delete old-namespace
```

**Example Output:**
```
✅ Namespace deleted: old-namespace
```

**⚠️ Important Warnings:**

1. **All Topics Deleted**: Deleting a namespace removes ALL topics within it
2. **No Confirmation**: This operation is immediate and irreversible
3. **Active Connections**: Connected producers/consumers will be disconnected
4. **Data Loss**: All messages in the namespace are permanently deleted

**Safety Checklist:**
```bash
# 1. List topics before deletion
danube-admin-cli namespaces topics my-namespace

# 2. Verify no critical topics
danube-admin-cli namespaces topics my-namespace --output json | grep -i critical

# 3. Check policies to understand impact
danube-admin-cli namespaces policies my-namespace

# 4. Only then delete
danube-admin-cli namespaces delete my-namespace
```

---

## Connection Configuration

All namespace commands use these environment variables:

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

### 1. Namespace Setup for New Application

```bash
# Create namespace
danube-admin-cli namespaces create payment-service

# Verify creation
danube-admin-cli brokers namespaces | grep payment-service

# Check default policies
danube-admin-cli namespaces policies payment-service

# Create topics in namespace
danube-admin-cli topics create /payment-service/transactions
danube-admin-cli topics create /payment-service/refunds
danube-admin-cli topics create /payment-service/notifications
```

### 2. Multi-Environment Setup

```bash
# Create environments
danube-admin-cli namespaces create production
danube-admin-cli namespaces create staging
danube-admin-cli namespaces create development

# List all namespaces
danube-admin-cli brokers namespaces

# Create same topics in each environment
for env in production staging development; do
  danube-admin-cli topics create /$env/user-events
  danube-admin-cli topics create /$env/order-events
done
```

### 3. Namespace Audit

```bash
# List all namespaces
danube-admin-cli brokers namespaces --output json > namespaces.json

# For each namespace, get details
for ns in $(cat namespaces.json | jq -r '.[]'); do
  echo "=== Namespace: $ns ==="
  danube-admin-cli namespaces topics $ns
  danube-admin-cli namespaces policies $ns
  echo ""
done
```

### 4. Cleanup Old Namespace

```bash
# Check what's in the namespace
danube-admin-cli namespaces topics old-project

# Check if any topics are active
danube-admin-cli namespaces topics old-project --output json | \
  xargs -I {} danube-admin-cli topics describe {}

# Delete namespace if safe
danube-admin-cli namespaces delete old-project
```

---

## Namespace Organization Patterns

### Pattern 1: By Environment

Separate production, staging, and development workloads.

```
production/
  ├── user-events
  ├── order-events
  └── payment-events

staging/
  ├── user-events
  ├── order-events
  └── payment-events

development/
  ├── user-events
  ├── order-events
  └── payment-events
```

**Pros**: Clear environment separation, easy to apply different policies  
**Cons**: Topic duplication across environments

### Pattern 2: By Team/Service

Organize by ownership and service boundaries.

```
analytics-team/
  ├── clickstream
  ├── user-behavior
  └── reports

payment-service/
  ├── transactions
  ├── refunds
  └── notifications

user-service/
  ├── registrations
  ├── profiles
  └── preferences
```

**Pros**: Clear ownership, aligns with microservices  
**Cons**: Cross-team topics need coordination

### Pattern 3: By Data Type

Group by data classification or purpose.

```
realtime/
  ├── stock-quotes
  ├── sensor-data
  └── live-metrics

batch/
  ├── daily-reports
  ├── etl-jobs
  └── backups

critical/
  ├── fraud-alerts
  ├── security-events
  └── system-errors
```

**Pros**: Easy to apply retention/TTL policies  
**Cons**: May mix different services

### Pattern 4: Hybrid (Recommended)

Combine environment and service for maximum flexibility.

```
production-payment/
production-user/
production-analytics/

staging-payment/
staging-user/

development/
```

**Pros**: Best of both worlds  
**Cons**: More namespaces to manage

---

## Best Practices

### 1. Namespace Naming

✅ **Do:**
```bash
production
staging-analytics
payment-service
team-platform-prod
```

❌ **Don't:**
```bash
Prod123
my_namespace
test-test-test
namespace
```

### 2. Policy Management

```bash
# Review policies before creating topics
danube-admin-cli namespaces policies production

# Understand limits before hitting them
# max_topics_per_namespace: 1000
# max_producers_per_topic: 100
```

### 3. Access Control

- Create separate namespaces for different security zones
- Use namespace-level policies to enforce quotas
- Keep sensitive data in dedicated namespaces

### 4. Lifecycle Management

```bash
# Regular audits
danube-admin-cli brokers namespaces --output json > audit-$(date +%Y%m%d).json

# Clean up unused namespaces
danube-admin-cli namespaces topics old-namespace
# If empty, consider deletion
```

### 5. Documentation

Maintain a registry of namespaces:
```markdown
| Namespace | Owner | Purpose | Environment | Created |
|-----------|-------|---------|-------------|---------|
| production-payment | Payment Team | Payment processing | Production | 2024-01-15 |
| staging-analytics | Analytics Team | Event analysis | Staging | 2024-02-01 |
```

---

## Troubleshooting

### Namespace Not Found
```bash
# List all available namespaces
danube-admin-cli brokers namespaces

# Check spelling
danube-admin-cli namespaces topics production  # not "prod"
```

### Cannot Delete Namespace
```bash
# Check if topics exist
danube-admin-cli namespaces topics my-namespace

# Delete all topics first
for topic in $(danube-admin-cli namespaces topics my-namespace --output json | jq -r '.[]'); do
  danube-admin-cli topics delete $topic
done

# Then delete namespace
danube-admin-cli namespaces delete my-namespace
```

### Policy Limits Exceeded
```bash
# Check current policies
danube-admin-cli namespaces policies my-namespace

# Example: max_topics_per_namespace = 100
# Solution: Create topics in a new namespace or request policy increase
```

---

## Quick Reference

```bash
# List topics in namespace
danube-admin-cli namespaces topics <namespace>

# View policies
danube-admin-cli namespaces policies <namespace>

# Create namespace
danube-admin-cli namespaces create <namespace>

# Delete namespace (⚠️ destructive)
danube-admin-cli namespaces delete <namespace>

# List all namespaces
danube-admin-cli brokers namespaces
```

---

## Related Commands

- `danube-admin-cli topics create /namespace/topic` - Create topics in a namespace
- `danube-admin-cli topics list --namespace <ns>` - List topics by namespace
- `danube-admin-cli brokers namespaces` - List all namespaces in cluster

---

## Next Steps

1. **Create Your First Namespace**: `danube-admin-cli namespaces create my-app`
2. **Review Policies**: `danube-admin-cli namespaces policies my-app`
3. **Create Topics**: `danube-admin-cli topics create /my-app/events`
4. **Monitor Usage**: Regular audits with `namespaces topics` and `policies`

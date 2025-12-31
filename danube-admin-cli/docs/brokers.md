# Brokers Management

Manage and view broker information in your Danube cluster.

## Overview

The `brokers` command provides visibility and control over the brokers in your Danube cluster. Use it to:
- List all brokers with their status
- Identify the leader broker
- View broker namespaces
- Unload topics from brokers
- Activate brokers

## Commands

### List All Brokers

Display all brokers in the cluster with their details.

```bash
danube-admin-cli brokers list
```

**Output Formats:**
```bash
# Plain text (default) - easy to read
danube-admin-cli brokers list

# JSON format - for scripting/automation
danube-admin-cli brokers list --output json
```

**Example Output (Plain Text):**
```
┌──────────────┬─────────────────────┬──────────┬─────────────────────┬─────────────────────┬────────┐
│ Broker ID    │ Address             │ Role     │ Admin Address       │ Metrics Address     │ Status │
├──────────────┼─────────────────────┼──────────┼─────────────────────┼─────────────────────┼────────┤
│ broker-001   │ 127.0.0.1:6650      │ leader   │ 127.0.0.1:50051     │ 127.0.0.1:9090      │ active │
│ broker-002   │ 127.0.0.1:6651      │ follower │ 127.0.0.1:50052     │ 127.0.0.1:9091      │ active │
└──────────────┴─────────────────────┴──────────┴─────────────────────┴─────────────────────┴────────┘
```

**Example Output (JSON):**
```json
[
  {
    "broker_id": "broker-001",
    "broker_addr": "127.0.0.1:6650",
    "broker_role": "leader",
    "admin_addr": "127.0.0.1:50051",
    "metrics_addr": "127.0.0.1:9090",
    "broker_status": "active"
  },
  {
    "broker_id": "broker-002",
    "broker_addr": "127.0.0.1:6651",
    "broker_role": "follower",
    "admin_addr": "127.0.0.1:50052",
    "metrics_addr": "127.0.0.1:9091",
    "broker_status": "active"
  }
]
```

---

### Get Leader Broker

Identify which broker is currently the cluster leader.

```bash
danube-admin-cli brokers leader
```

**Example Output:**
```
Leader: broker-001
```

**Why This Matters:**
- The leader broker coordinates cluster operations
- Useful for debugging cluster issues
- Important for understanding cluster topology

---

### List Broker Namespaces

View all namespaces managed by the cluster.

```bash
danube-admin-cli brokers namespaces
```

**Output Formats:**
```bash
# Plain text
danube-admin-cli brokers namespaces

# JSON format
danube-admin-cli brokers namespaces --output json
```

**Example Output (Plain Text):**
```
Namespaces: ["default", "analytics", "logs"]
```

**Example Output (JSON):**
```json
["default", "analytics", "logs"]
```

---

### Unload Broker Topics

Gracefully unload topics from a broker (useful for maintenance or rebalancing).

```bash
danube-admin-cli brokers unload <BROKER_ID> [OPTIONS]
```

**Basic Usage:**
```bash
# Unload all topics from broker-001
danube-admin-cli brokers unload broker-001

# Dry-run to see what would be unloaded
danube-admin-cli brokers unload broker-001 --dry-run
```

**Advanced Options:**
```bash
# Unload with custom parallelism
danube-admin-cli brokers unload broker-001 --max-parallel 5

# Unload only specific namespaces
danube-admin-cli brokers unload broker-001 \
  --namespace-include default \
  --namespace-include analytics

# Exclude certain namespaces
danube-admin-cli brokers unload broker-001 \
  --namespace-exclude system

# Set custom timeout per topic (seconds)
danube-admin-cli brokers unload broker-001 --timeout 30
```

**Options:**

| Option | Description | Default |
|--------|-------------|---------|
| `--dry-run` | Preview topics to be unloaded without making changes | `false` |
| `--max-parallel` | Number of topics to unload concurrently | `1` |
| `--namespace-include` | Only unload topics from these namespaces (repeatable) | All |
| `--namespace-exclude` | Skip topics from these namespaces (repeatable) | None |
| `--timeout` | Timeout in seconds for each topic unload | `30` |

**Example Output:**
```
Unload Started: true
Total Topics: 45
Succeeded: 45
Failed: 0
Pending: 0
```

**Use Cases:**
- **Broker Maintenance**: Drain topics before shutting down a broker
- **Load Rebalancing**: Move topics to other brokers
- **Rolling Upgrades**: Safely upgrade brokers one at a time

---

### Activate Broker

Mark a broker as active, allowing it to receive traffic.

```bash
danube-admin-cli brokers activate <BROKER_ID> [OPTIONS]
```

**Basic Usage:**
```bash
danube-admin-cli brokers activate broker-002
```

**With Audit Reason:**
```bash
danube-admin-cli brokers activate broker-002 \
  --reason "Maintenance completed"
```

**Example Output:**
```
Activated: true
```

**Use Cases:**
- **After Maintenance**: Re-enable a broker after maintenance
- **After Unload**: Activate broker to start receiving topics again
- **Cluster Expansion**: Activate newly added brokers

---

## Connection Configuration

All broker commands use these environment variables:

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

### 1. Health Check
```bash
# Check cluster health
danube-admin-cli brokers list
danube-admin-cli brokers leader

# Verify all brokers are active
danube-admin-cli brokers list | grep -c active
```

### 2. Broker Maintenance
```bash
# Step 1: Dry-run to preview unload
danube-admin-cli brokers unload broker-001 --dry-run

# Step 2: Unload topics
danube-admin-cli brokers unload broker-001

# Step 3: Perform maintenance (external)
# ...

# Step 4: Reactivate broker
danube-admin-cli brokers activate broker-001 --reason "Maintenance completed"
```

### 3. Cluster Expansion
```bash
# List current brokers
danube-admin-cli brokers list

# Add new broker (external process)
# ...

# Activate new broker
danube-admin-cli brokers activate broker-003 --reason "New broker added"

# Verify
danube-admin-cli brokers list
```

### 4. Load Rebalancing
```bash
# Unload topics from overloaded broker
danube-admin-cli brokers unload broker-001 --max-parallel 10

# Topics will automatically be assigned to other brokers
danube-admin-cli brokers list --output json | jq '.[].broker_id' | sort | uniq -c
```

---

## Troubleshooting

### Broker Not Listed
```bash
# Check if broker is registered in cluster
danube-admin-cli brokers list

# Verify endpoint
echo $DANUBE_ADMIN_ENDPOINT
```

### Unload Fails
```bash
# Check for failed topics
danube-admin-cli brokers unload broker-001 --dry-run

# Increase timeout for slow topics
danube-admin-cli brokers unload broker-001 --timeout 60

# Reduce parallelism to avoid overload
danube-admin-cli brokers unload broker-001 --max-parallel 1
```

### Connection Issues
```bash
# Test connectivity
curl -v $DANUBE_ADMIN_ENDPOINT

# Check TLS configuration
echo $DANUBE_ADMIN_TLS
echo $DANUBE_ADMIN_DOMAIN
```

---

## Best Practices

1. **Always Dry-Run First**: Use `--dry-run` before unloading topics to preview changes
2. **Monitor Progress**: Watch unload operations with `--output json` for automation
3. **Gradual Unloading**: Use `--max-parallel 1` for critical brokers
4. **Namespace Filtering**: Use include/exclude to protect system namespaces
5. **Audit Trail**: Always provide `--reason` when activating brokers
6. **Health Checks**: Regularly run `brokers list` to monitor cluster health

---

## Quick Reference

```bash
# List all brokers
danube-admin-cli brokers list

# Get leader
danube-admin-cli brokers leader

# List namespaces
danube-admin-cli brokers namespaces

# Unload topics (dry-run)
danube-admin-cli brokers unload <broker-id> --dry-run

# Unload topics (execute)
danube-admin-cli brokers unload <broker-id> --max-parallel 5

# Activate broker
danube-admin-cli brokers activate <broker-id> --reason "Ready"
```

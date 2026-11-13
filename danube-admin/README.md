# Danube Admin (`danube ctl` & `danube mcp`)

**Unified control-plane, data-plane CLI, UI API server, and MCP gateway built directly into the `danube` binary.**

Instead of maintaining a separate `danube-admin` executable, Danube v2 exposes all administration and AI agent interfaces from the `danube` binary (`danube-broker` package) backed by the `danube-admin` library crate:

- **`danube ctl`**: CLI for cluster management, topics, StreamIntents, declarative `apply`, schemas, RBAC, and `produce`/`consume`
- **`danube ctl serve`**: HTTP/JSON API server for the Admin UI and Streamable HTTP MCP endpoint
- **`danube mcp`**: Model Context Protocol (MCP) server (stdio by default, or HTTP with `--http`) for AI coding & ops assistants

By default, `danube ctl` and `danube mcp` target **`http://127.0.0.1:6650`** (override with `--endpoint` or `DANUBE_ADMIN_ENDPOINT`).

---

## Quick Start

### Control-Plane & Topic Operations (`danube ctl`)

```bash
# Cluster & brokers
danube ctl cluster status
danube ctl brokers list
danube ctl brokers balance

# Create topics explicitly or from a StreamIntent preset
danube ctl topics create /default/orders --partitions 3 --dispatch-strategy reliable
danube ctl topics create /default/tasks --intent agent_task_queue

# Peek payloads, infer JSON Schema Draft-07, seek subscriptions, or replay DLQ
danube ctl topics peek /default/orders --limit 5
danube ctl topics infer-schema /default/orders --sample-size 20
danube ctl topics seek /default/orders --subscription workers --offset 100
danube ctl topics replay-dlq /default/orders --subscription workers --max-messages 50

# Stream branching from offset or timestamp
danube ctl topics branch /default/orders /default/orders-exp
danube ctl topics branch /default/orders /default/orders-replay --at-offset 1000

# Declarative reconciliation (additive by default; plan shown first)
danube ctl apply -f cluster-state.yaml --dry-run
danube ctl apply -f cluster-state.yaml
danube ctl apply -f cluster-state.yaml --prune   # Opt-in deletion of unmentioned user topics

# Schemas & RBAC
danube ctl schemas register user-events --file schema.json --schema-type json_schema
danube ctl security roles create reader --permissions Lookup,Consume,ReadPayload
```

### Data-Plane Produce & Consume (`danube ctl`)

```bash
danube ctl produce --service-addr http://127.0.0.1:6650 --topic /default/demo -c 10 -m "hello"
danube ctl consume --service-addr http://127.0.0.1:6650 --topic /default/demo --subscription sub1
```

### UI / HTTP Server

```bash
# Start the HTTP API server (default listen addr: 0.0.0.0:8080)
danube ctl serve --mode ui --broker-endpoint http://127.0.0.1:6650

# With Prometheus metrics integration
danube ctl serve --mode ui \
  --broker-endpoint http://127.0.0.1:6650 \
  --prometheus-url http://prometheus:9090
```

### MCP Server (`danube mcp`)

```bash
# Stdio MCP transport (for Claude Code, Cursor, VS Code, Antigravity)
danube mcp --broker-endpoint http://127.0.0.1:6650

# Streamable HTTP MCP transport
danube mcp --http --listen-addr 0.0.0.0:8080 --broker-endpoint http://127.0.0.1:6650
```

Built-in MCP capabilities include:
- **Cluster & Brokers**: `list_brokers`, `get_leader`, `cluster_status`, `trigger_rebalance`, `get_cluster_balance`, `add_cluster_node`, `promote_cluster_node`, `remove_cluster_node`, `apply_desired_state`, `configure_object_store`
- **Topics & Agent Workflows**: `list_topics`, `describe_topic`, `create_topic` (including `StreamIntent`), `delete_topic`, `peek_messages` (requires `ReadPayload` permission), `seek_subscription`, `replay_dlq`, `explain_consumer_stall`, `infer_topic_schema`, `branch_topic`
- **Schema Registry**: `register_schema`, `get_schema`, `list_schema_versions`, `check_compatibility`, `set_compatibility_mode`, `list_subjects`
- **Security**: `create_role`, `get_role`, `list_roles`, `delete_role`, `create_binding`, `list_bindings`, `delete_binding`
- **Diagnostics & Built-in Telemetry/Logs**: `health_check`, `analyze_consumer_lag`, `get_recommendations`, `get_cluster_metrics` / `get_broker_metrics` / `get_topic_metrics` (with `GetMetricsSnapshot` gRPC fallback), and `get_broker_logs` (with `GetBrokerLogs` gRPC ring-buffer fallback when no external Prometheus/Docker log source is configured)
- **Safety Guardrails**: Destructive MCP mutations require explicit `confirm_destructive: true` (and `dry_run` preview support on rebalance, unload, and `apply_desired_state`), errors surface cleanly via MCP `is_error`, RBAC separates payload inspection (`ReadPayload`) from administrative actions, and control-plane changes are recorded in the broker audit log.

---

## Configuration & Environment Variables

```bash
DANUBE_ADMIN_ENDPOINT=http://127.0.0.1:6650    # Broker gRPC endpoint (or --endpoint)
DANUBE_ADMIN_LISTEN_ADDR=0.0.0.0:8080          # HTTP listen address for serve/--http
DANUBE_ADMIN_TOKEN=<jwt>                       # Optional JWT bearer token
```

### TLS / mTLS Flags & Env

```bash
DANUBE_ADMIN_TLS=true
DANUBE_ADMIN_DOMAIN=broker.example.com
DANUBE_ADMIN_CA=/path/to/ca-cert.pem
DANUBE_ADMIN_CERT=/path/to/client-cert.pem
DANUBE_ADMIN_KEY=/path/to/client-key.pem
```

## MCP IDE Integration

**Claude Code** (`.mcp.json`):
```json
{
  "mcpServers": {
    "danube": {
      "command": "/usr/local/bin/danube",
      "args": ["mcp", "--broker-endpoint", "http://127.0.0.1:6650"],
      "env": { "NO_COLOR": "1", "RUST_LOG": "error" }
    }
  }
}
```

**VS Code** (`.vscode/mcp.json`):
```json
{
  "servers": {
    "danube": {
      "type": "stdio",
      "command": "/usr/local/bin/danube",
      "args": ["mcp", "--broker-endpoint", "http://127.0.0.1:6650"],
      "env": { "NO_COLOR": "1", "RUST_LOG": "error" }
    }
  }
}
```

## Development

```bash
cargo test --package danube-admin
cargo run -p danube-broker --bin danube -- ctl --help
cargo run -p danube-broker --bin danube -- ctl topics --help
```

---

**[Documentation](https://danube-messaging.com/)** · **[Issues](https://github.com/danube-messaging/danube/issues)**

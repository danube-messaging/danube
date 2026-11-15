# Danube Admin (`danube` CLI, `danube console` & `danube mcp`)

**Unified control-plane, data-plane CLI, web console backend, and MCP gateway built directly into the `danube` binary.**

Instead of maintaining a separate admin executable, Danube v2 exposes all administration and AI agent interfaces from the `danube` binary (`danube-broker` package) backed by the `danube-admin` library crate:

- **Resource commands** (`danube topic`, `namespace`, `schema`, `cluster`, `broker`, `auth`, `apply`): cluster management, topics, StreamIntents, declarative `apply`, schemas and RBAC
- **Messaging commands** (`danube produce`, `danube consume`): publish and stream messages from the terminal
- **`danube console`**: HTTP/JSON API server for the Admin UI, which also serves the Streamable HTTP MCP endpoint at `/mcp`
- **`danube mcp`**: Model Context Protocol (MCP) server (stdio by default, or streamable HTTP with `--http ADDR`) for AI coding & ops assistants
- **Utilities**: `danube context` (saved connection profiles), `danube completions <shell>`, `danube version`

Run bare `danube` for an overview and `danube <command> --help` for details. By default, every client command targets **`http://127.0.0.1:6650`** (override with `--endpoint`, `DANUBE_ENDPOINT`, or a saved context).

---

## Quick Start

### Control-Plane & Topic Operations

```bash
# Cluster & brokers
danube cluster status
danube broker list
danube broker leader
danube broker balance

# Create topics explicitly or from a StreamIntent preset
danube topic create /default/orders --partitions 3 --dispatch-strategy reliable
danube topic create /default/tasks --intent task_queue

# Peek payloads, infer JSON Schema Draft-07, seek subscriptions, or replay DLQ
danube topic peek /default/orders --count 5
danube topic schema infer /default/orders --sample-size 20
danube topic subscription list /default/orders
danube topic subscription seek /default/orders --subscription workers --to-offset 100
danube topic dlq replay /default/orders --subscription workers --max-messages 50

# Stream branching from offset or timestamp
danube topic branch /default/orders /default/orders-exp
danube topic branch /default/orders /default/orders-replay --at-offset 1000

# Raft membership & storage
danube cluster node add --node-addr http://broker4:6650
danube cluster storage show

# Declarative reconciliation (additive by default; plan shown first)
danube apply -f cluster-state.yaml --dry-run
danube apply -f cluster-state.yaml
danube apply -f cluster-state.yaml --prune   # Opt-in deletion of unmentioned user topics

# Schemas & RBAC
danube schema register user-events --file schema.json --schema-type json_schema
danube auth role create reader --permissions Lookup,Consume,ReadPayload
danube auth token create --subject my-app --jwt-secret "$DANUBE_JWT_SECRET"
danube auth token validate "$TOKEN" --jwt-secret "$DANUBE_JWT_SECRET"
```

### Data-Plane Produce & Consume

```bash
danube produce /default/demo -c 10 -m "hello" --attr source:cli,env:dev
danube produce /default/orders -f ./order.json --key customer-42
danube consume /default/demo -s sub1 --type exclusive
```

### Web Console Backend (`danube console`)

```bash
# Start the HTTP API server (default listen addr: 0.0.0.0:8080); also serves MCP at /mcp
danube console --endpoint http://127.0.0.1:6650

# With Prometheus metrics integration and HTTP bearer auth
danube console --listen 0.0.0.0:8080 \
  --endpoint http://127.0.0.1:6650 \
  --prometheus-url http://prometheus:9090 \
  --require-auth --http-token "$CONSOLE_TOKEN"
```

### MCP Server (`danube mcp`)

```bash
# Stdio MCP transport (for Claude Code, Cursor, VS Code, Antigravity)
danube mcp --endpoint http://127.0.0.1:6650

# Streamable HTTP MCP transport (served at /mcp)
danube mcp --http 0.0.0.0:8080 --endpoint http://127.0.0.1:6650
```

Built-in MCP capabilities include:
- **Cluster & Brokers**: `list_brokers`, `get_leader`, `cluster_status`, `trigger_rebalance`, `get_cluster_balance`, `add_cluster_node`, `promote_cluster_node`, `remove_cluster_node`, `apply_desired_state`, `configure_object_store`
- **Topics & Agent Workflows**: `list_topics`, `describe_topic`, `create_topic` (including `StreamIntent`), `delete_topic`, `peek_messages` (requires `ReadPayload` permission), `seek_subscription`, `replay_dlq`, `explain_consumer_stall`, `infer_topic_schema`, `branch_topic`
- **Schema Registry**: `register_schema`, `get_schema`, `list_schema_versions`, `check_compatibility`, `set_compatibility_mode`, `list_subjects`
- **Security**: `create_role`, `get_role`, `list_roles`, `delete_role`, `create_binding`, `list_bindings`, `delete_binding`
- **Diagnostics & Built-in Telemetry/Logs**: `health_check`, `analyze_consumer_lag`, `get_recommendations`, `get_cluster_metrics` / `get_broker_metrics` / `get_topic_metrics` (with `GetMetricsSnapshot` gRPC fallback), and `get_broker_logs` (with `GetBrokerLogs` gRPC ring-buffer fallback when no external Prometheus/Docker log source is configured)
- **Safety Guardrails**: Destructive MCP mutations require explicit `confirm_destructive: true` (and `dry_run` preview support on rebalance, unload, and `apply_desired_state`), errors surface cleanly via MCP `is_error`, RBAC separates payload inspection (`ReadPayload`) from administrative actions, and control-plane changes are recorded in the broker audit log.

---

## Connection Profiles (`danube context`)

Save endpoint, TLS and token settings once and switch between clusters. Profiles live client-side in `$DANUBE_CONFIG_DIR/contexts.toml` (default `~/.config/danube/contexts.toml`, file mode `0600`):

```bash
danube context add local --endpoint http://127.0.0.1:6650 --use
danube context add prod --endpoint https://danube.example.com:6650 --tls-ca ./ca.pem --token "$TOKEN"
danube context use prod          # switch the current context
danube context list              # * marks the current one (alias: ls)
danube context current
danube context show prod         # token masked
danube context delete local      # alias: rm
danube topic list --namespace default --context local   # one-off override
```

Every client command uses the current context unless `--context`, `--endpoint`, or the `DANUBE_*` environment variables override it.

## Configuration & Environment Variables

Global connection flags accepted by every client command (`topic`, `produce`, `console`, `mcp`, ...):

| Flag | Env | Description |
| :--- | :--- | :--- |
| `--endpoint <URL>` | `DANUBE_ENDPOINT` | Broker gRPC endpoint (default `http://127.0.0.1:6650`) |
| `--context <NAME>` | `DANUBE_CONTEXT` | Saved connection profile |
| `--token <JWT>` | `DANUBE_TOKEN` | JWT bearer token |
| `--tls` | `DANUBE_TLS` | Connect with TLS (implied by `https://` endpoints) |
| `--tls-ca <FILE>` | `DANUBE_TLS_CA` | CA certificate used to verify the broker |
| `--tls-cert <FILE>` | `DANUBE_TLS_CERT` | Client certificate for mTLS |
| `--tls-key <FILE>` | `DANUBE_TLS_KEY` | Client private key for mTLS |
| `--tls-domain <NAME>` | `DANUBE_TLS_DOMAIN` | TLS server name to verify (default `localhost`) |

Console-specific: `DANUBE_CONSOLE_LISTEN` (`--listen`), `DANUBE_HTTP_TOKEN` (`--http-token`), `DANUBE_HTTP_REQUIRE_AUTH` (`--require-auth`). Offline token commands read `DANUBE_JWT_SECRET` (`--jwt-secret`).

## Shell Completions & Version

```bash
danube completions bash > /etc/bash_completion.d/danube
danube completions zsh  > "${fpath[1]}/_danube"
danube completions fish > ~/.config/fish/completions/danube.fish
danube version
```

## MCP IDE Integration

**Claude Code** (`.mcp.json`):
```json
{
  "mcpServers": {
    "danube": {
      "command": "/usr/local/bin/danube",
      "args": ["mcp", "--endpoint", "http://127.0.0.1:6650"],
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
      "args": ["mcp", "--endpoint", "http://127.0.0.1:6650"],
      "env": { "NO_COLOR": "1", "RUST_LOG": "error" }
    }
  }
}
```

## Development

```bash
cargo test --package danube-admin
cargo run -p danube-broker --bin danube -- --help
cargo run -p danube-broker --bin danube -- topic --help
```

---

**[Documentation](https://danube-messaging.com/)** · **[Issues](https://github.com/danube-messaging/danube/issues)**

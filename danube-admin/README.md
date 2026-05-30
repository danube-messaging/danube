# Danube Admin

**Unified administration tool for Danube messaging clusters.**

A single binary that provides three interfaces for managing your Danube cluster:

* **>_ CLI Mode**: Interactive command-line tool for cluster operations
* **🌐 Server Mode**: HTTP/JSON API server for the Danube Admin UI
* **🤖 MCP Mode**: Model Context Protocol server for AI assistants

## Quick Start

### CLI

```bash
danube-admin brokers list
danube-admin topics create /default/my-topic --partitions 3
danube-admin schemas register user-events --file schema.json --schema-type json_schema
danube-admin security roles list
```

### UI Server

```bash
# Start the HTTP API server (default: 0.0.0.0:8080)
danube-admin serve --mode ui --broker-endpoint http://broker1:50051

# With Prometheus metrics
danube-admin serve --mode ui \
  --broker-endpoint http://broker1:50051 \
  --prometheus-url http://prometheus:9090
```

### MCP Server

```bash
# Start the MCP server for AI assistants (stdio transport)
danube-admin serve --mode mcp --broker-endpoint http://localhost:50051
```

Connect to any MCP-compatible assistant, ex. Claude Code, Cursor, VS Code Copilot, Windsurf, Antigravity.

The MCP server exposes **58 tools** across 8 categories and **4 guided workflow prompts**:

- **Cluster & Brokers**: broker lifecycle, namespaces, Raft membership, load balancing, rebalancing
- **Topics**: CRUD, subscriptions, schema binding, validation and failure policies
- **Schema Registry**: register, evolve, and manage schemas with compatibility checks
- **Security**: RBAC roles and bindings, offline JWT token creation and validation
- **Diagnostics**: health checks, consumer lag analysis, prioritized recommendations
- **Metrics**: cluster, broker, and topic metrics via Prometheus, plus raw PromQL
- **Logs**: broker logs from Docker, Kubernetes, or local deployments

## Configuration

### Environment Variables

```bash
DANUBE_ADMIN_ENDPOINT=http://broker1:50051      # Broker gRPC endpoint
DANUBE_ADMIN_LISTEN_ADDR=0.0.0.0:8080           # HTTP listen address (UI mode)
DANUBE_ADMIN_TOKEN=<jwt>                         # Auth token (optional)
```

### TLS

```bash
DANUBE_ADMIN_TLS=true
DANUBE_ADMIN_DOMAIN=broker.example.com
DANUBE_ADMIN_CA=/path/to/ca-cert.pem
DANUBE_ADMIN_CERT=/path/to/client-cert.pem
DANUBE_ADMIN_KEY=/path/to/client-key.pem
```

### Server Options

```
--mode               ui | mcp | all
--listen-addr        HTTP address (default: 0.0.0.0:8080)
--broker-endpoint    Broker gRPC endpoint (default: http://127.0.0.1:50051)
--prometheus-url     Prometheus URL for metrics
--config             MCP config file for log access and deployment mapping
--request-timeout-ms gRPC timeout (default: 5000)
--cache-ttl-ms       Response cache TTL (default: 3000)
--cors-allow-origin  CORS origin for Admin UI
```

## MCP IDE Setup

Add `danube-admin` to your AI assistant's MCP configuration:

**Claude Code** (`.mcp.json` in project root):
```json
{
  "mcpServers": {
    "danube-admin": {
      "command": "/usr/local/bin/danube-admin",
      "args": ["serve", "--mode", "mcp", "--broker-endpoint", "http://localhost:50051"],
      "env": { "NO_COLOR": "1", "RUST_LOG": "error" }
    }
  }
}
```

**VS Code** (`.vscode/mcp.json`):
```json
{
  "servers": {
    "danube-admin": {
      "type": "stdio",
      "command": "/usr/local/bin/danube-admin",
      "args": ["serve", "--mode", "mcp", "--broker-endpoint", "http://localhost:50051"],
      "env": { "NO_COLOR": "1", "RUST_LOG": "error" }
    }
  }
}
```

For log access and metrics in MCP mode, create a config file:

```yaml
# mcp-config.yml
broker_endpoint: http://localhost:50051
prometheus_url: http://localhost:9090

deployment:
  type: docker    # docker | kubernetes | local
  docker:
    container_mappings:
      - id: "broker1"
        container: "danube-broker1"
      - id: "broker2"
        container: "danube-broker2"
```

```bash
danube-admin serve --mode mcp --config /path/to/mcp-config.yml
```

## Docker

```bash
# CLI
docker run --rm ghcr.io/danube-messaging/danube-admin:latest brokers list

# UI Server
docker run -p 8080:8080 \
  -e DANUBE_ADMIN_ENDPOINT=http://broker:50051 \
  ghcr.io/danube-messaging/danube-admin:latest \
  serve --mode ui --listen-addr 0.0.0.0:8080
```

## Development

```bash
cargo build --release --package danube-admin
cargo test --package danube-admin
cargo run --package danube-admin -- brokers list
cargo run --package danube-admin -- serve --mode ui
```

## Help

Every command has built-in help:

```bash
danube-admin --help
danube-admin brokers --help
danube-admin topics create --help
```

---

**[Documentation](https://danube-messaging.com/)** · **[Issues](https://github.com/danube-messaging/danube/issues)**

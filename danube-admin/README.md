# Danube Admin

**Unified administration tool for Danube clusters**

`danube-admin` is a versatile tool that provides both command-line and HTTP server interfaces for managing Danube messaging clusters.

## 🎯 What is Danube Admin?

Danube Admin is a single binary that operates in three modes:

- **>_ CLI Mode** - Interactive command-line tool for cluster operations
- **🌐 Server Mode** - HTTP/JSON API server for the Danube Admin UI
- **🤖 MCP Mode** - Model Context Protocol server for AI assistants

## 🚀 Quick Start

### CLI Mode

Manage your Danube cluster from the command line:

```bash
# List brokers in the cluster
danube-admin brokers list

# Check cluster balance
danube-admin brokers balance

# Create a topic
danube-admin topics create /default/my-topic --partitions 3

# Register a schema
danube-admin schemas register user-events --file schema.json --schema-type json_schema
```

### Server Mode

Start the HTTP API server for the Admin UI:

```bash
# Start server with defaults (localhost:8080)
danube-admin serve

# Custom configuration
danube-admin serve \
  --listen-addr 0.0.0.0:8080 \
  --broker-endpoint http://broker1:50051 \
  --prometheus-url http://prometheus:9090
```

The server provides REST endpoints for:
- Cluster overview and broker status
- Topic and namespace management
- Schema registry operations
- Metrics integration with Prometheus

### MCP Mode

Start the MCP server for AI assistant integration (Claude Desktop, Cursor, Windsurf):

```bash
# Start MCP server (uses stdio transport)
danube-admin serve --mode mcp

# With custom broker endpoint
DANUBE_ADMIN_ENDPOINT=http://broker1:50051 danube-admin serve --mode mcp
```

The MCP server provides **32 tools** for AI assistants to:
- **Cluster Management** - List brokers, check balance, trigger rebalancing
- **Topic Operations** - Create, delete, describe topics and subscriptions
- **Namespace Management** - Create, delete, manage namespace policies
- **Schema Registry** - Register, validate, and manage schemas with compatibility checking
- **Diagnostics** - Health checks, consumer lag analysis, performance recommendations
- **Metrics** - Query Prometheus metrics for cluster/broker/topic health
- **Log Access** - Fetch broker logs from Docker/Kubernetes deployments

Plus **4 troubleshooting prompts** and **1 metrics resource catalog** for context-aware assistance.

## 📖 Command Categories

### Brokers

Manage cluster brokers, view load distribution, and trigger rebalancing:

```bash
danube-admin brokers list                    # List all brokers
danube-admin brokers balance                 # View load distribution
danube-admin brokers rebalance --dry-run     # Preview rebalancing
```

### Topics

Create, delete, and manage topics:

```bash
danube-admin topics create /default/events --partitions 4
danube-admin topics list --namespace default
danube-admin topics describe /default/events
```

### Schemas

Register and manage schemas:

```bash
danube-admin schemas register user-events --schema-type json_schema --file schema.json
danube-admin schemas get --subject user-events
danube-admin schemas check user-events --file new-schema.json --schema-type json_schema
```

## ⚙️ Configuration

### Environment Variables

```bash
# gRPC endpoint for broker communication
export DANUBE_ADMIN_ENDPOINT=http://broker1:50051

# Server listen address (server mode only)
export DANUBE_ADMIN_LISTEN_ADDR=0.0.0.0:8080

# TLS configuration (optional)
export DANUBE_ADMIN_TLS=true
export DANUBE_ADMIN_DOMAIN=broker.example.com
export DANUBE_ADMIN_CA=/path/to/ca-cert.pem
export DANUBE_ADMIN_CERT=/path/to/client-cert.pem
export DANUBE_ADMIN_KEY=/path/to/client-key.pem
```

### Server Options

```bash
--listen-addr              # HTTP server address (default: 0.0.0.0:8080)
--broker-endpoint          # Broker gRPC endpoint (default: http://127.0.0.1:50051)
--prometheus-url           # Prometheus server URL for metrics
--request-timeout-ms       # gRPC request timeout (default: 5000)
--cache-ttl-ms            # Response cache TTL (default: 3000)
--cors-allow-origin       # CORS allowed origin for Admin UI
```

## 🐳 Docker Usage

### CLI Operations

```bash
# Using pre-built image
docker run --rm ghcr.io/danube-messaging/danube-admin:latest brokers list

# With custom endpoint
docker run --rm \
  -e DANUBE_ADMIN_ENDPOINT=http://broker:50051 \
  ghcr.io/danube-messaging/danube-admin:latest topics list --namespace default
```

### HTTP Server

```bash
# Start server in container
docker run -p 8080:8080 \
  -e DANUBE_ADMIN_ENDPOINT=http://broker:50051 \
  ghcr.io/danube-messaging/danube-admin:latest \
  serve --listen-addr 0.0.0.0:8080
```

## 🤖 MCP Configuration

Integrate Danube Admin with AI assistants using the Model Context Protocol.

### Claude Desktop

Add to `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "danube": {
      "command": "danube-admin",
      "args": ["serve", "--mode", "mcp"],
      "env": {
        "DANUBE_ADMIN_ENDPOINT": "http://localhost:50051"
      }
    }
  }
}
```

### Windsurf

Windsurf MCP config (`~/.codeium/windsurf/mcp_config.json`):

```json
{
  "mcpServers": {
    "danube-admin": {
      "command": "sh",
      "args": [
        "-c",
        "/path/to/danube-admin serve --mode mcp --broker-endpoint http://127.0.0.1:50051"
      ],
      "env": {
        "PATH": "/usr/local/bin:/usr/bin:/bin",
        "NO_COLOR": "1",
        "RUST_LOG": "error"
      }
    }
  }
}
```

### VSCode

VSCode MCP config:

```json
{
  "servers": {
    "danube-admin": {
      "type": "stdio",
      "command": "/path/to/danube-admin",
      "args": [
        "serve",
        "--mode",
        "mcp",
        "--broker-endpoint",
        "http://127.0.0.1:50051"
      ],
      "env": {
        "NO_COLOR": "1",
        "RUST_LOG": "error"
      }
    }
  }
}
```

### Optional: Advanced Configuration

For log access and metrics, create `mcp-config.yml`:

```yaml
broker_endpoint: http://127.0.0.1:50051
prometheus_url: http://localhost:9090  # If running Prometheus

# To read Logs from Docker deployments
deployment:
  type: docker
  docker:
    container_mappings:
      - id: "broker1"
        container: "danube-broker1"
      - id: "broker2"
        container: "danube-broker2"
```

Then update the MCP config to include `--config /path/to/mcp-config.yml` in the args.

### Available Capabilities

- **32 Tools** - Full cluster administration via AI
- **4 Prompts** - Guided troubleshooting workflows
- **1 Resource** - Prometheus metrics catalog for observability

AI assistants can now manage your Danube cluster through natural language!

## 🛠️ Development

```bash
# Build from source
cargo build --release --package danube-admin

# Run tests
cargo test --package danube-admin

# Run locally
cargo run --package danube-admin -- brokers list
cargo run --package danube-admin -- serve
```

## 📚 Getting Help

Each command has detailed help text with examples:

```bash
danube-admin --help                          # General help
danube-admin brokers --help                  # Brokers subcommands
danube-admin topics create --help            # Specific command help
```

---

**Need more help?** Check out the [Danube Documentation](https://danube-docs.dev-state.com/) or [open an issue](https://github.com/danube-messaging/danube/issues).

# Danube Admin

**Unified administration tool for Danube clusters**

`danube-admin` is a versatile tool that provides both command-line and HTTP server interfaces for managing Danube messaging clusters.

## 🎯 What is Danube Admin?

Danube Admin is a single binary that operates in three modes:

- **CLI Mode** - Interactive command-line tool for cluster operations
- **Server Mode** - HTTP/JSON API server for the Danube Admin UI
- **MCP Mode** *(coming soon)* - Model Context Protocol server for AI assistants

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

## 📖 Command Categories

### Brokers

Manage cluster brokers, view load distribution, and trigger rebalancing:

```bash
danube-admin brokers list                    # List all brokers
danube-admin brokers balance                 # View load distribution
danube-admin brokers rebalance --dry-run     # Preview rebalancing
```

[Full broker command documentation →](--help)

### Topics

Create, delete, and manage topics:

```bash
danube-admin topics create /default/events --partitions 4
danube-admin topics list --namespace default
danube-admin topics describe /default/events
```

[Full topic command documentation →](--help)

### Namespaces

Organize topics with namespaces and policies:

```bash
danube-admin namespaces create production
danube-admin namespaces topics production
danube-admin namespaces policies production
```

[Full namespace command documentation →](--help)

### Schemas

Register and manage schemas:

```bash
danube-admin schemas register user-events --schema-type json_schema --file schema.json
danube-admin schemas get --subject user-events
danube-admin schemas check user-events --file new-schema.json --schema-type json_schema
```

[Full schema command documentation →](--help)

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

## 🔮 Future: MCP Support

Model Context Protocol integration is planned to enable AI assistants (Claude, Cursor, Windsurf) to interact with Danube clusters:

```bash
# Coming soon
danube-admin serve --mode mcp --stdio
```

This will allow AI assistants to:
- Query cluster status and metrics
- Manage topics and schemas
- Diagnose issues and suggest optimizations
- Generate configuration templates

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

## 📝 License

Apache 2.0

---

**Need more help?** Check out the [Danube Documentation](https://danube-docs.dev-state.com/) or [open an issue](https://github.com/danube-messaging/danube/issues).

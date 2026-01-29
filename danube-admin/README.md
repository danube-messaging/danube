# danube-admin

Unified CLI and HTTP server for managing Danube clusters.

## Overview

`danube-admin` consolidates the functionality of `danube-admin-cli` and `danube-admin-gateway` into a single binary that supports both CLI and server modes. This reduces code duplication, simplifies distribution, and provides a foundation for future MCP (Model Context Protocol) integration.

## Architecture

```
danube-admin/
├── src/
│   ├── main.rs          # Entry point & mode router
│   ├── core/            # Shared core (DRY)
│   │   ├── grpc_client.rs   # Unified gRPC client
│   │   ├── error.rs         # Standard error types
│   │   ├── types.rs         # Common response types
│   │   └── config.rs        # Client configuration
│   ├── cli/             # CLI mode
│   │   ├── brokers.rs
│   │   ├── topics.rs
│   │   ├── namespaces.rs
│   │   └── schemas.rs
│   └── server/          # Server mode
│       ├── app.rs
│       ├── handlers/
│       └── ui/
```

## Usage

### CLI Mode (default)

```bash
# List brokers
danube-admin brokers list

# Create topic
danube-admin topics create /default/mytopic --partitions 3

# Manage schemas
danube-admin schemas register user-events --file schema.json

# Get cluster balance
danube-admin brokers balance
```

### Server Mode

```bash
# Start HTTP server
danube-admin serve --listen-addr 0.0.0.0:8080

# With custom broker endpoint
danube-admin serve --broker-endpoint https://broker.example.com:50051

# With TLS
danube-admin serve --grpc-enable-tls --grpc-domain broker.example.com
```

## Configuration

Environment variables:
- `DANUBE_ADMIN_ENDPOINT`: Broker gRPC endpoint (default: `http://127.0.0.1:50051`)
- `DANUBE_ADMIN_LISTEN_ADDR`: Server listen address (default: `0.0.0.0:8080`)
- TLS: `DANUBE_ADMIN_TLS`, `DANUBE_ADMIN_DOMAIN`, `DANUBE_ADMIN_CA`, `DANUBE_ADMIN_CERT`, `DANUBE_ADMIN_KEY`

## Features

- **Unified Binary**: Both CLI and server in one executable
- **Shared Core**: Single gRPC client implementation (no duplication)
- **Standard Errors**: Consistent error handling across modes
- **TLS/mTLS Support**: Secure connections to brokers
- **Extensible**: Foundation for MCP server mode

## Current Status

**✅ Phase 1: Core Infrastructure (COMPLETED)**

- [x] Project structure created
- [x] Cargo.toml with all dependencies
- [x] Core module with unified gRPC client
- [x] Standard error and response types
- [x] Configuration types
- [x] Main entry point with mode routing
- [x] CLI module stubs (ready for migration)
- [x] Server module stubs (ready for migration)
- [x] Project compiles successfully

**⏳ Phase 2: CLI Migration (IN PROGRESS)**

- [ ] Migrate brokers commands
- [ ] Migrate topics commands
- [ ] Migrate namespaces commands
- [ ] Migrate schemas commands
- [ ] Add output formatting utilities
- [ ] Integration tests

**⏳ Phase 3: Server Migration (PENDING)**

- [ ] Migrate HTTP handlers
- [ ] Migrate metrics integration
- [ ] Migrate UI pages
- [ ] Add middleware (CORS, logging, request ID)
- [ ] Integration tests

**⏳ Phase 4: Documentation & Testing (PENDING)**

- [ ] Complete README
- [ ] API documentation
- [ ] Migration guide for users
- [ ] Add deprecation warnings to old binaries

## Migration from Old Binaries

`danube-admin` replaces both `danube-admin-cli` and `danube-admin-gateway`:

```bash
# Old (danube-admin-cli)
danube-admin-cli brokers list

# New (danube-admin)
danube-admin brokers list
```

```bash
# Old (danube-admin-gateway)
danube-admin-gateway --listen-addr 0.0.0.0:8080

# New (danube-admin)
danube-admin serve --listen-addr 0.0.0.0:8080
```

## Development

### Build

```bash
cargo build --package danube-admin
```

### Test

```bash
cargo test --package danube-admin
```

### Run

```bash
# CLI mode
cargo run --package danube-admin -- brokers list

# Server mode
cargo run --package danube-admin -- serve
```

## Future: MCP Support

Once CLI and server modes are complete, MCP (Model Context Protocol) support will be added:

```bash
# Build with MCP feature
cargo build --package danube-admin --features mcp

# Run MCP server
danube-admin serve --mode mcp --stdio
```

This will enable AI assistants (Claude, Cursor, Windsurf) to interact with Danube clusters.

## License

Apache 2.0

# AGENTS.md: danube-admin

Client side of the `danube` binary: resource and messaging commands, connection handling, contexts, web console backend and MCP server. The clap root lives in `danube-broker/src/cli`; this crate provides the `Args` structs and handlers. Read ADR 0014.

## Entry points

- `src/cli/connection.rs`: `ClientArgs` (global `--endpoint --context --token --tls*`) and `Connection` (resolution order flag > env > context > default, `grpc_config()`, `danube_client()`). Contexts live in `contexts.toml`.
- `src/cli/{topics,namespaces,schemas,cluster,brokers,security,apply,produce,consume,context}.rs`: one module per command group. Each handler is `handle(cmd)`.
- `src/core/`: `AdminGrpcClient` and `GrpcClientConfig` (no env fallbacks; settings come from `Connection`).
- `src/server/`: `danube console` (axum, `/ui/v1/...` + `/mcp`), `ConsoleArgs`, `McpArgs`.
- `src/mcp/`: MCP server (`server.rs` has the `#[tool]` methods), prompts, the `danube://metrics` resource, log access config.

## Invariants

- Every command group flattens `ClientArgs` and resolves it once. Never read connection env vars directly.
- Flag names must not collide with the global connection flags (`--token`, `--tls-*`, `--endpoint`, `--context`). `debug_assert` tests catch this.
- Mutating MCP tools need confirmation or dry run, and must go through admin gRPC so RBAC applies.
- Help text: singular nouns, one-line `about`, examples using real syntax.

## Pitfalls

- `--output json` is per command, not global.
- Leader-only admin calls may need a redirect to the leader. Use `AdminGrpcClient::config_for_endpoint()` to keep TLS and token settings.
- In stdio MCP, `broker_endpoint` from `--config` overrides `--endpoint`.

## Tests

```bash
cargo test -p danube-admin --lib          # parsing/debug_assert, contexts
cargo test -p danube-admin --test <name>  # lifecycle tests need a running broker (DANUBE_BROKER_ENDPOINT)
```

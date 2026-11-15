# 0006: Zero-YAML configuration layers

**Status:** Accepted

## Context

v1 required a long `danube_broker.yml` on every node. Copies drifted between nodes, and changing a cluster-wide setting meant editing files and restarting.

## Decision

Configuration is split by **who owns it**:

1. **Node-local (startup):** `danube up standalone|cluster|edge` flags and env vars: data dir, listeners, advertised address, Raft port, role, auth mode and TLS files, membership. Only things that differ per node or cannot change at runtime.
2. **Cluster-wide (live, in Raft):** storage mode, WAL retention, write buffer, load manager, namespaces, policies, RBAC. Changed with `danube cluster ...`, or declaratively with `danube apply -f desired_state.yaml [--dry-run] [--prune]`.
3. **Per-topic:** explicit structure (`--partitions`, `--schema-subject`) plus implicit behaviour from `--intent` (0007).
4. **Break-glass:** `danube cluster export-config` writes a recovery file, and `danube up ... --recovery-config FILE` applies it locally when quorum is lost.

Safety rails at startup:

- `--dry-run` prints the resolved node config and exits.
- Each data dir records its mode in `danube-node.json`; starting it in another mode is refused.
- `cluster` and `edge` require `--data-dir`.

## Why

- No config drift between nodes: there is one copy, in Raft.
- Settings change without restarts, and agents can manage them through the CLI or MCP.
- Declarative `apply` is additive by default; pruning needs `--prune`.

## Trade-offs and exceptions

- The lakehouse runtime still reads an optional YAML file (`DANUBE_LAKEHOUSE_CONFIG`). Move it into Raft when that area is next reworked.
- The MCP server accepts an optional `--config` file for log-access settings.
- Client connection profiles (`~/.config/danube/contexts.toml`) are client-side only and never read by the server.

## Rules for changes

- A new setting goes in layer 1 only if it is truly node-specific. Otherwise put it in Raft, expose it via `danube cluster ...` and the manifest (`desired_state` in the broker), and include it in `export-config`.
- Never add a field that is accepted but not enforced.


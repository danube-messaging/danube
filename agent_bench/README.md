# Danube Agent Benchmark (`agent_bench`)

`agent_bench` tests whether an AI agent can operate Danube end to end using the public agent stack:

- **`danube` binary:** single binary providing the broker (`danube up`), data-plane CLI (`danube produce`, `danube consume`), and MCP server (`danube mcp`).
- **`danube mcp`:** 71 admin, schema, RBAC, diagnostic, log, and metric tools with `dry_run` and `confirm_destructive` guardrails.
- **[`danube-agent-skills`](https://github.com/danube-messaging/danube-agent-skills):** two lightweight skills (`danube-bootstrap` and `danube-messaging`) covering node startup and data-plane messaging.

## What this proves

Unit and integration tests verify code behavior. `agent_bench` verifies **agent usability**:

1. **Bootstrap:** start a standalone node or multi-node Raft cluster from CLI.
2. **Operate via MCP:** create namespaces and topics (`--intent`), manage schemas, inspect metrics/logs, and verify guardrails.
3. **Produce & consume reliably:** send and receive messages over reliable topics using `danube produce` and `danube consume`.
4. **Recover & debug:** branch and replay streams, explain consumer stalls, and analyze lag.
5. **Secure:** run `--auth auto-tls`, configure scoped RBAC roles/bindings/tokens, and confirm secrets stay redacted in MCP exports.

## Prerequisites

1. Build or install `danube` and put it on `PATH` (or export `DANUBE_BIN=/path/to/danube`):
   ```bash
   cargo build --release -p danube-broker
   export PATH="$PWD/target/release:$PATH"
   ```
2. Configure `danube mcp` in your agent's MCP settings (example for unsecured local tasks 01-05):
   ```json
   {
     "mcpServers": {
       "danube": {
         "command": "danube",
         "args": ["mcp", "--endpoint", "http://127.0.0.1:6650"]
       }
     }
   }
   ```
3. Give the agent access to the two skills from `danube-agent-skills` (`danube-bootstrap` and `danube-messaging`).

## How to run a task

1. Start from a clean temporary working directory (for example `/tmp/danube-bench`) so data directories never collide across runs.
2. Give the agent the prompt in `tasks/<NN>_*.md`.
3. After the agent finishes, run the matching check script from `danube`:
   ```bash
   bash agent_bench/verify/01_bootstrap_and_cluster_ops.sh
   ```
   (For task 06 with `--auth auto-tls`, export `DANUBE_ENDPOINT`, `DANUBE_TLS_CA`, and `DANUBE_TOKEN` or point `--context` before running the verify script.)
4. Record the exact outcome (including failures or guardrail friction) in `reports/<version>.md` using [reports/TEMPLATE.md](reports/TEMPLATE.md).

## Task index

Tasks `01` through `06` require only the `danube` binary (`danube up`, CLI, `danube mcp`). Task `07` also requires Cargo/Rust to compile a small worker against `danube-client`:

- [01_bootstrap_and_cluster_ops.md](tasks/01_bootstrap_and_cluster_ops.md): start node, connect MCP, health check, create namespace, inspect metrics/logs.
- [02_reliable_messaging_and_dlq.md](tasks/02_reliable_messaging_and_dlq.md): `task_queue` topic, reliable produce/consume, subscription and DLQ failure policy inspection.
- [03_schemas.md](tasks/03_schemas.md): JSON Schema registration, compatibility checks, enforcement, and schema inference.
- [04_replay_and_branching.md](tasks/04_replay_and_branching.md): peek messages, zero-copy `branch_topic`, `seek_subscription`, and full replay.
- [05_troubleshooting_and_lag.md](tasks/05_troubleshooting_and_lag.md): simulate backlog/stall, run `explain_consumer_stall`, `analyze_consumer_lag`, and drain.
- [06_security_and_guardrails.md](tasks/06_security_and_guardrails.md): `--auth auto-tls`, scoped RBAC roles/bindings/tokens, destructive confirmation, secret redaction.
- [07_agent_primitives_sdk.md](tasks/07_agent_primitives_sdk.md): Rust SDK (`danube-client`) `extend_lease`, `report_progress` checkpoints, `cancel_task_tree`, `request_reply`, `x-danube-compute-cost-units`, and NACK -> DLQ -> MCP `replay_dlq`.


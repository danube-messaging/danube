# Task 01: Bootstrap and Cluster Ops

## Goal

Verify the agent can bootstrap a fresh Danube node via CLI (`danube-bootstrap`) and inspect/manage it using `danube mcp` tools instead of admin CLI commands.

## Preconditions

- `danube` binary is on `PATH`.
- No Danube broker is listening on `127.0.0.1:6650`.
- Working directory `/tmp/danube-bench-01` is empty.

## Prompt for the agent

> Start a fresh standalone Danube node in `/tmp/danube-bench-01/data` listening on `127.0.0.1:6650` (preview with `--dry-run` first).
> Then, using Danube MCP tools:
> 1. Run `health_check` and `list_brokers` to verify the broker is active.
> 2. Create a namespace called `bench`.
> 3. Fetch recent broker logs (`get_broker_logs`) and cluster metrics (`get_cluster_metrics`).
> Report the broker ID, namespace status, and keep the node running for verification.

## Expected surface usage

- **CLI (`danube-bootstrap`):** `danube up standalone --data-dir /tmp/danube-bench-01/data --dry-run`, then start `danube up standalone --data-dir /tmp/danube-bench-01/data` in background.
- **MCP:** `health_check`, `list_brokers`, `create_namespace`, `get_broker_logs`, `get_cluster_metrics`. (Note: `cluster_status` and `get_cluster_balance` are only available in `cluster` mode, not `standalone`.)

## Verification

Run while the node is still up:

```bash
bash agent_bench/verify/01_bootstrap_and_cluster_ops.sh
```


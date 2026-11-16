# AGENTS.md

Instructions for AI agents asked to run or update `agent_bench`.

## When asked to "run `agent_bench`"

1. **Confirm the binary:** build if needed (`cargo build -p danube-broker`), then verify `./target/debug/danube version` and `git rev-parse --short HEAD`. Set `DANUBE_BIN=./target/debug/danube` (or point to the binary under test).
2. **Use isolated temp dirs:** run inside fresh directories under `/tmp/danube-bench-*`. Never share or reuse a `--data-dir` across tasks with different auth or node modes.
3. **Respect the surface boundary:**
   - Use **CLI** for node lifecycle (`danube up ...`) and data-plane messaging (`danube produce`, `danube consume --limit <N> -o jsonl`, `danube auth token issue`).
   - Use **`danube mcp`** (either via your connected MCP client or stdio JSON-RPC over `danube mcp`) for all admin, topic, schema, RBAC, replay/branch, and diagnostic steps specified in `tasks/0N_*.md`. Do not silently fall back to CLI admin commands when a task requires MCP.
4. **Keep node & checks in one session when sandboxed:** if your shell isolates background processes or loopback across commands, launch the broker, run the task steps, and execute `agent_bench/verify/0N_*.sh` in one script/session (cleaning up the broker PID on exit).
5. **Run all 7 tasks and verifiers:**
   - Tasks `01` through `05` and Task `07`: run against an unsecured `standalone` node on `http://127.0.0.1:6650` (Task `07` uses Cargo / `danube-client` for Rust SDK agent primitives).
   - Task `06`: run against a fresh `standalone --auth auto-tls` node on `https://127.0.0.1:6650` with `DANUBE_ENDPOINT`, `DANUBE_TLS_CA`, and `DANUBE_TOKEN` set.
   - Run `DANUBE_BIN=./target/debug/danube bash agent_bench/verify/0N_*.sh` while the node is live.
6. **Report honestly:** write or update `agent_bench/reports/<version>.md` from [reports/TEMPLATE.md](reports/TEMPLATE.md). Record exact `PASS`, `PARTIAL`, or `FAIL` results, real MCP/CLI errors, and product friction observed. Never edit verify assertions to hide real broker or MCP gaps.

## Reference files

- Suite overview: [README.md](README.md)
- Task specs: `tasks/01_bootstrap_and_cluster_ops.md` through `tasks/07_agent_primitives_sdk.md`
- Deterministic checks: `verify/01_bootstrap_and_cluster_ops.sh` through `verify/07_agent_primitives_sdk.sh`
- Repo-wide rules and architecture: [../AGENTS.md](../AGENTS.md)
- Latest baseline report: [reports/v0.17.0.md](reports/v0.17.0.md)


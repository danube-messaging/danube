# Agent Benchmark Report - `<version>`

- **Date:** `YYYY-MM-DD`
- **Danube version / commit:** `<e.g. v0.17.0 / abcdef1>`
- **Agent / Client:** `<e.g. Claude Code / Cursor / Antigravity / Codex>`
- **Model:** `<e.g. model name>`
- **Skills loaded:** `danube-bootstrap`, `danube-messaging` (`Yes / No / Baseline without skills`)
- **MCP config:** `danube mcp` (`stdio` / `http`)

## Summary

- **Passed:** `X / 6`
- **Partial / With friction:** `X / 6`
- **Failed:** `X / 6`

## Results by task

- **01 Bootstrap & Cluster Ops:** `PASS | FAIL` - `<notes: startup time, MCP tools used, verify exit code>`
- **02 Reliable Messaging & DLQ:** `PASS | FAIL` - `<notes: reliable produce/consume ordering, JSONL check, verify exit code>`
- **03 Schemas:** `PASS | FAIL` - `<notes: schema registration, compat check, enforcement, infer_topic_schema>`
- **04 Replay & Branching:** `PASS | FAIL` - `<notes: branch_topic, seek_subscription, replayed message count>`
- **05 Troubleshooting & Lag:** `PASS | FAIL` - `<notes: explain_consumer_stall, analyze_consumer_lag, backlog drained>`
- **06 Security & Guardrails:** `PASS | FAIL` - `<notes: auto-tls, confirm_destructive refusal, secret redaction, RBAC allow/deny>`

## Friction & findings

List every command error, MCP guardrail refusal, missing skill detail, or manual intervention encountered during the run:

1. `<finding 1>`
2. `<finding 2>`

## Raw verification output

Paste or link the `agent_bench/verify/*.sh` run output so results can be audited.


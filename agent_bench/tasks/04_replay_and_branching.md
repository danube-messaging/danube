# Task 04: Replay and Zero-Copy Branching

## Goal

Verify the agent can inspect stored messages with MCP `peek_messages`, fork history with `branch_topic`, rewind cursors with `seek_subscription`, and replay messages via `danube consume`.

## Preconditions

- Running Danube node on `http://127.0.0.1:6650` with namespace `bench`.
- `danube mcp` connected.

## Prompt for the agent

> 1. Using Danube MCP, create topic `/bench/events` with `intent: "event_stream"`.
> 2. Using Danube CLI, start an initial subscription `ingest` (`--limit 5`) and produce 5 messages (`event-1`..`event-5` or JSON payloads) so all 5 messages are persisted in the WAL.
> 3. Using Danube MCP, call `peek_messages` on `/bench/events` starting at offset `0` (count `5`) to confirm stored payloads.
> 4. Using Danube MCP, branch `/bench/events` into `/bench/events-replay` using `branch_topic` (from head / latest state).
> 5. On `/bench/events`, create/start a fresh subscription `audit` or seek an existing subscription back to offset `0` with MCP `seek_subscription`, then consume all 5 messages from offset `0` using `danube consume --limit 5 -o jsonl`.

## Expected surface usage

- **MCP:** `create_topic`, `peek_messages`, `branch_topic`, `seek_subscription`, `describe_topic`.
- **CLI (`danube-messaging`):** `danube produce`, `danube consume --limit 5 -o jsonl`.

## Verification

```bash
bash agent_bench/verify/04_replay_and_branching.sh
```


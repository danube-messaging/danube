# Task 05: Troubleshooting Consumer Stall and Lag

## Goal

Verify the agent can use Danube MCP diagnostic tools (`explain_consumer_stall`, `analyze_consumer_lag`, `get_recommendations`, `get_broker_logs`) to diagnose a stalled subscription and resolve the backlog.

## Preconditions

- Running Danube node on `http://127.0.0.1:6650` with namespace `bench`.
- `danube mcp` connected.

## Prompt for the agent

> 1. Create reliable topic `/bench/backlog` (`intent: "event_stream"`) via MCP.
> 2. Establish subscription `slow-reader` by consuming 1 warm-up message (`danube consume /bench/backlog -s slow-reader --limit 1`), then stop the consumer.
> 3. Produce 10 messages to `/bench/backlog` while no consumer is connected to `slow-reader` so a backlog builds up.
> 4. Using Danube MCP diagnostics (`explain_consumer_stall`, `analyze_consumer_lag`, `get_recommendations`), diagnose why `slow-reader` has unconsumed backlog and confirm `consumer_count=0` and `backlog=10`.
> 5. Fix the stall by starting `danube consume /bench/backlog -s slow-reader --limit 10 -o jsonl`, then re-run `explain_consumer_stall` to confirm backlog is `0`.

## Expected surface usage

- **MCP:** `create_topic`, `explain_consumer_stall`, `analyze_consumer_lag`, `get_recommendations`, `get_broker_logs`.
- **CLI (`danube-messaging`):** `danube consume`, `danube produce`.

## Verification

```bash
bash agent_bench/verify/05_troubleshooting_and_lag.sh
```


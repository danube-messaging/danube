# Task 02: Reliable Messaging and DLQ Policy

## Goal

Verify the agent can create a `task_queue` topic via MCP, produce and consume messages reliably via CLI (`danube-messaging`), and inspect/verify subscription and failure-policy metadata via MCP.

## Preconditions

- Running Danube node on `http://127.0.0.1:6650` with namespace `bench` (from Task 01 or fresh standalone node).
- `danube mcp` connected.

## Prompt for the agent

> Using Danube MCP:
> 1. Create a topic `/bench/jobs` with intent `task_queue`.
> 2. Verify its dispatch strategy is reliable and inspect its details.
>
> Next, using the Danube CLI:
> 3. Start a consumer on `/bench/jobs` with subscription `workers` (`shared` type, `--limit 5`, `-o jsonl`) and produce 5 JSON job messages `{"job":1}`..`{"job":5}` (make sure the consumer receives all 5 messages without hanging).
>
> Finally, using Danube MCP:
> 4. List subscriptions on `/bench/jobs` and inspect the failure policy of subscription `workers` (confirm DLQ topic and retry settings).

## Expected surface usage

- **MCP:** `create_topic` (`intent: "task_queue"`), `describe_topic`, `list_subscriptions`, `get_subscription_failure_policy`.
- **CLI (`danube-messaging`):** background `danube consume /bench/jobs -s workers --type shared --limit 5 -o jsonl`, then `danube produce /bench/jobs ... --count 5`.

## Verification

```bash
bash agent_bench/verify/02_reliable_messaging_and_dlq.sh
```


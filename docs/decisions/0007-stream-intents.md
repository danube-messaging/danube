# 0007: Stream intents expand once, at creation

**Status:** Accepted

## Context

Choosing dispatch strategy, WAL sync mode, retries, backoff and DLQ per topic is error-prone for humans and agents. Earlier intent profiles also carried fields (default partitions, retention, storage tier) that were never enforced per topic.

## Decision

`danube topic create /ns/t --intent <name>` (or `intent:` in a manifest) resolves a preset in `danube-broker/src/stream_intent.rs` into `ExpandedStreamSettings` and persists them at `/topics/{ns}/{topic}/intent`:

- `realtime` (aliases include `telemetry`, `firehose`, `ephemeral`): non-reliable, in memory, no disk I/O
- `event_stream` (aliases include `events`, `event_mesh`, `lakehouse_feed`): reliable, `os_cache_flush`, 3 retries at 500 ms exponential, DLQ `<topic>-dlq`
- `task_queue` (`tasks`, `work_queue`, `agent_task_queue`): reliable, `fsync_batch`, 5 retries at 1000 ms, DLQ `<topic>-dlq`
- `audit_log` (`audit`, `tool_log`, `state_log`): reliable, `fsync_batch`, 10 retries at 500 ms, poison policy `block` (no DLQ, strict order)

When a subscription is created without an explicit failure policy, the topic's intent settings are copied into `/topics/{ns}/{topic}/subscriptions/{sub}/failure_policy`.

Partitions and schema subject stay **explicit**: partitioning changes ordering semantics and cannot be undone.

## Why

- Users describe the workload, and Danube picks the settings that actually exist in the broker.
- Settings are frozen at creation, so a broker upgrade that changes a preset never silently changes live topics.
- Every field maps to enforced behaviour.

## Rules for changes

- Changing a preset affects **new** topics only. Never re-read the intent name to derive behaviour at runtime.
- Do not add fields to `ExpandedStreamSettings` unless the broker enforces them per topic.
- Keep aliases, CLI help, MCP tool docs and the docs page `concepts/stream-intents` in sync.

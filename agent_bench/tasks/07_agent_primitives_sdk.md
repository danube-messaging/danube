# Task 07: Agent Runtime Primitives and DLQ Replay (Rust SDK)

## Goal

Verify an AI agent can use the `danube-sdk` skill (Rust `danube-client`) alongside `danube mcp` to run Danube's native agent-coordination primitives and dead-letter recovery end to end:

1. Adaptive leases (`extend_lease`) and progress checkpoints (`report_progress` -> redelivery carrying `x-danube-checkpoint-b64`).
2. NACK retry exhaustion -> `<topic>-dlq` -> MCP `replay_dlq`.
3. Cascading task-tree cancellation (`x-danube-parent-msg-id`, `x-danube-root-msg-id`, `cancel_task_tree`) and compute-cost metadata (`x-danube-compute-cost-units`).
4. Correlated RPC (`request_reply` on a `Non-Reliable` reply topic).

*Note: Unlike Tasks 01-06 (binary-only), Task 07 requires Cargo / Rust to compile a small client against `danube-client`.*

## Preconditions

- Running standalone Danube node on `http://127.0.0.1:6650`.
- `danube mcp` connected.
- Rust toolchain (`cargo`) with workspace access to `danube-client` and `danube-core`.

## Prompt for the agent

> Using Danube MCP:
> 1. Create namespace `agents` (if it does not exist), plus topics:
>    - `/agents/tasks` (`intent: "task_queue"`)
>    - `/agents/subtasks` (`intent: "task_queue"`)
>    - `/agents/rpc` (`intent: "realtime"`)
>    - `/agents/rpc-replies` (`intent: "realtime"`)
>
> Using `danube-client` in Rust (`danube-sdk` skill / `references/rust.md`):
> 2. **Lease + Checkpoint:** Subscribe `worker-cp` on `/agents/tasks`, send `"task-cp"`, call `extend_lease` and `report_progress` with `{"step":3}`, call `nack` so the broker redelivers it, verify `msg.checkpoint_b64()` decodes to `{"step":3}`, then `ack`.
> 3. **DLQ + MCP Replay:** Use MCP `set_subscription_failure_policy` for subscription `worker-dlq` on `/agents/tasks` (`max_redelivery_count: 1`, `ack_timeout_ms: 5000`, `base_redelivery_delay_ms: 50`, `max_redelivery_delay_ms: 100`, `poison_policy: "dead_letter"`, `dead_letter_topic: "/agents/tasks-dlq"`). Then subscribe `worker-dlq`, send `"poison-task"`, `nack` it twice so it moves to `/agents/tasks-dlq`, verify via MCP `peek_messages` on `/agents/tasks-dlq`, then replay it back to `/agents/tasks` via MCP `replay_dlq` (`confirm_destructive: true`) and consume the replayed message.
> 4. **Task-tree cancellation + Compute cost:** Subscribe on `/agents/subtasks`, call `cancel_task_tree("/agents/subtasks", "root-cancel-1", ...)`, publish one subtask with `x-danube-root-msg-id: "root-cancel-1"` and one with `x-danube-root-msg-id: "root-keep-2"` + `x-danube-compute-cost-units: "42"`, and confirm `"root-cancel-1"` is skipped while `"root-keep-2"` arrives with `compute_cost_units() == Some(42)`.
> 5. **Request/Reply RPC:** Run a responder on `/agents/rpc` that replies to `msg.reply_to()` with matching `x-danube-correlation-id`, call `client.request_reply("/agents/rpc", "/agents/rpc-replies", ...)` and verify the reply returns.

## Expected surface usage

- **MCP:** `create_namespace`, `create_topic`, `set_subscription_failure_policy`, `peek_messages`, `replay_dlq`.
- **Rust SDK (`danube-sdk` / `references/rust.md`):** `extend_lease`, `report_progress`, `nack`, `ack`, `cancel_task_tree`, `request_reply`, `ATTR_PARENT_MSG_ID`, `ATTR_ROOT_MSG_ID`, `ATTR_COMPUTE_COST_UNITS`.

## Verification

```bash
bash agent_bench/verify/07_agent_primitives_sdk.sh
```


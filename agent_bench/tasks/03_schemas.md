# Task 03: Schema Registry and Inference

## Goal

Verify the agent can manage schema evolution through MCP (register, compatibility check, topic assignment, enforcement) and validate live messages via CLI.

## Preconditions

- Running Danube node on `http://127.0.0.1:6650` with namespace `bench`.
- `danube mcp` connected.

## Prompt for the agent

> Using Danube MCP:
> 1. Register a JSON Schema under subject `bench-orders-value` requiring object properties `order_id` (string) and `amount` (number), with both required.
> 2. Check compatibility of a breaking v2 schema that adds a new required field `currency` (confirm compatibility fails under default `BACKWARD` mode), then check compatibility of an optional `currency` field (confirm it passes) and register v2.
> 3. Create topic `/bench/orders` (`intent: "event_stream"`, `schema_subject: "bench-orders-value"`) and set its validation policy to `enforce`.
>
> Using Danube CLI:
> 4. Verify an invalid payload `{"order_id":"ord-1"}` (missing `amount`) fails or is rejected when published with `--schema-subject bench-orders-value`, and a valid payload `{"order_id":"ord-1","amount":42.5}` succeeds.
>
> Bonus MCP check:
> 5. Produce 3 JSON messages to an un-schematized reliable topic `/bench/raw-events` and run MCP `infer_topic_schema` on `/bench/raw-events`.

## Expected surface usage

- **MCP:** `register_schema`, `check_schema_compatibility`, `create_topic`, `set_topic_validation_policy`, `get_topic_schema_config`, `infer_topic_schema`.
- **CLI (`danube-messaging`):** `danube produce` with `--schema-subject bench-orders-value` and valid/invalid payloads.

## Verification

```bash
bash agent_bench/verify/03_schemas.sh
```


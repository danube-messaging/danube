# 0011: Embedded schema registry

**Status:** Accepted

## Decision

- The schema registry is part of the broker (`danube-schema` logic, served by `danube-broker` on the same port as client gRPC, 6650).
- Subjects and versions are metadata stored in Raft (`/schemas/{subject}/versions/{v}`). Topics reference a subject (`/topics/.../schema_subject`, `schema_config`).
- Supported types: bytes, string, number, JSON Schema, Avro, Protobuf. Compatibility modes: none, backward, forward, full.
- Per-topic validation policy: `none`, `warn`, `enforce`, with optional payload validation (`danube topic schema configure|validation-policy`).
- `danube topic schema infer` (and the MCP tool) samples live messages and proposes a JSON Schema.

## Why

- No separate registry service to deploy, secure and keep consistent.
- Schemas are replicated and versioned with the rest of the cluster state.
- The lakehouse export (0013) and edge validation reuse the same schemas.

## Rules for changes

- Schema bodies are small and change rarely, which is fine for Raft (0001). Never store per-message schema data.
- Changing compatibility mode requires `ManageSchema`.

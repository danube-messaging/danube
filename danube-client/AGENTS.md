# AGENTS.md: danube-client

Async Rust SDK used by applications, the CLI (`produce` / `consume`), edge and tests. Other language SDKs (Go, Java, Python) live in separate repos and should mirror this API.

## Entry points

- `src/client.rs`: `DanubeClient::builder()` (service URL, TLS / mTLS, token), `request_reply`.
- `src/lookup_service.rs`: topic lookup and redirect handling (ADR 0002).
- `src/producer.rs`, `src/topic_producer.rs`: builders, partitions, reliable dispatch flag, schema selection, retry and re-lookup on failure.
- `src/consumer.rs`, `src/topic_consumer.rs`: subscription types, ack / nack, key filters, agent primitives `extend_lease`, `report_progress` (checkpoints up to 64 KiB), `cancel_task_tree`.
- `src/schema_registry_client.rs`: register, check compatibility, fetch schemas.
- `src/retry_manager.rs`, `src/connection_manager.rs`, `src/health_check.rs`.

## Invariants

- Always follow redirects and re-lookup after a topic move. Never assume a fixed broker per topic.
- The producer's dispatch strategy must match the topic (ADR 0008).
- Public builder APIs are mirrored in the docs (`client-libraries/*`) and the other SDKs. Treat renames as breaking changes.

## Pitfalls

- `send()` returns a sequence id, not a message id.
- The default subscription type is Shared. A new subscription on a reliable topic starts at the latest offset.

## Tests

The client is exercised by broker integration tests (`danube-broker/tests`). Run `cargo test -p danube-client` for unit tests.

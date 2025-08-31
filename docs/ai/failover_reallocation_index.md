# Feature: Failover Reallocation and Reconnect — Index

This index points to the three distinct phases of work and their detailed plans. Each phase doc includes an introduction (why needed), scope, goals, implementation steps, testing, and acceptance criteria.

- Phase 1 — Broker Failover Reallocation (Non-Reliable)
  - File: `docs/ai/failover_reallocation_phase1_broker_failover.md`
  - Focus: Preserve `/topics/**` and `/namespaces/**`, move dead-broker assignments to `/cluster/unassigned/**`, create topic locally on new broker.

- Phase 2 — Client Reconnect Improvements
  - File: `docs/ai/failover_reallocation_phase2_client_reconnect.md`
  - Focus: Robust retries with exponential backoff + jitter, redirect hint usage, idempotent creates, mid-flight routing validation.

- Phase 3 — Reliable Dispatch with Persistent Subscription Cursors
  - File: `docs/ai/failover_reallocation_phase3_reliable_cursors.md`
  - Focus: Persist subscription cursor and hydrate on reassignment/reattach to resume exactly from last ack.


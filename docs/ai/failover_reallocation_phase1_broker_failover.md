# Phase 1: Broker Failover Reallocation (Non-Reliable)

## Introduction: Why this is needed
When a broker dies or is taken out of service, any topics assigned to it must be promptly reallocated to healthy brokers without losing durable metadata. Today, parts of the reallocation logic delete namespace/topic records, causing clients to see inconsistent state and delaying recovery. Phase 1 ensures topic ownership is rebalanced cleanly while preserving metadata so producers/consumers can reconnect and resume (for non-reliable, resume from "now").

## Current Broker Behavior (context)
- In `danube-broker/src/danube_service.rs`, the broker already watches for PUT events on `/cluster/brokers/{this}/{ns}/{topic}` and calls `BrokerService.create_topic_locally()` to build the in-memory topic from `LocalCache` (policies/schema/dispatch config).
- Subscription runtime materialization is deferred to the first consumer attach in non-reliable mode.
- This is default behavior; no code changes are required here for Phase 1.

## Scope
- Only non-reliable dispatch semantics.
- Server-side (broker + metadata) changes to reassign topics and prepare the new broker.
- Clients continue to reconnect as they do today (Phase 2 improves client behavior).

## Design Goals
- Preserve durable metadata under `/topics/**` and `/namespaces/**`.
- Transition ownership via `/cluster/unassigned/**` to trigger reassignment.
- On assignment, ensure the broker can immediately serve topic creation and accept new attachments.

## Implementation Plan

- __[Reallocation path]__
  - On dead broker detection, remove assignment key only:
    - From: `/cluster/brokers/{dead-broker}/{ns}/{topic}`
    - To: create `/cluster/unassigned/{ns}/{topic}` (empty value).
  - Do not delete `/topics/**` or `/namespaces/**`.

- __[Load Manager changes]__
  - In `danube-broker/src/danube_service/load_manager.rs`:
    - Update `delete_topic_allocation(broker_id)` to:
      - Delete only broker assignment keys.
      - Create `/cluster/unassigned/{ns}/{topic}` for each.
    - Ensure the load balancing loop picks unassigned topics and allocates to a new broker.


- __[Cache readiness verification]__
  - `LocalCache` is global and eventually consistent; metadata under `/topics/**` and `/namespaces/**` remains available.
  - During assignment handling, verify required entries (policy/schema/delivery) are present in `LocalCache` before calling `create_topic_locally()`; if not present within a short wait, perform a one-time direct fetch from `MetadataStorage` as a fallback.
  - This avoids reliance on fixed sleeps and handles rare watcher ordering races.

- __[Config knobs]__
  - Optional: Consider making broker registration TTL configurable to balance failover speed vs flapping.

## Preserved metadata and reallocation-only changes
- __[Preserved metadata]__
  - Namespace: `/namespaces/{ns}/policy`, `/namespaces/{ns}/topics/{ns}/{topic}` remain intact.
  - Topic: `/topics/{ns}/{topic}/policy`, `/topics/{ns}/{topic}/schema`, `/topics/{ns}/{topic}/producers/*`, `/topics/{ns}/{topic}/subscriptions/*` remain intact.

- __[Reallocation-only changes]__
  - Delete: `/cluster/brokers/{dead}/{ns}/{topic}` (remove dead broker assignment).
  - Put: `/cluster/unassigned/{ns}/{topic}` (signals Load Manager to reassign).

## Testing Strategy
- __[Unit tests]__ (in-memory metadata store)
  - Verify that when a broker is removed:
    - Assignment entry `/cluster/brokers/{dead}/{ns}/{topic}` is deleted.
    - `/cluster/unassigned/{ns}/{topic}` is created.
    - `/namespaces/**` and `/topics/**` are untouched.
  - Optional: exercise Load Manager code path that picks from `/cluster/unassigned/**` and assigns to `get_next_broker()` to ensure reassignment path is triggered.

-- __[Integration tests]__ (separate CI workflow)
  - Stand up etcd.
  - Start only Broker A.
  - Use `danube-admin-cli` (see `danube-admin-cli/README.md`) to:
    - Create namespace and topic, e.g. `/default/failover-test`.
    - Verify lookup/broker ownership returns Broker A for `/default/failover-test`.
  - Baseline functional check on Broker A:
    - Run `danube-broker/tests/subscription_basic.rs` (or a simple pub/sub using `danube-client`) to confirm produce/consume works and topic resources exist.
  - Introduce Broker B, then kill Broker A:
    - Start Broker B (so it can be the reassignment target).
    - Kill Broker A to simulate failure, ensuring A was the initial owner.
  - Validation steps:
    - Wait for reassignment (consider broker TTL in `danube-broker/src/danube_service.rs`).
    - Use `danube-admin-cli` to confirm the topic is now assigned to Broker B (lookup returns B's address).
    - Re-run `subscription_basic.rs` (or continue client) to validate produce/consume works in non-reliable mode (resume from now on Broker B).
  - Artifacts: collect broker logs and etcd keys on failure for troubleshooting.

### CI Workflow outline (new file)
- `.github/workflows/broker-failover-e2e.yml`
  - Jobs:
    - Setup Rust + cache
    - Start etcd service (container or local)
    - Build workspace
    - Launch Broker A (background)
    - Use `danube-admin-cli` to provision namespace/topic
    - Run baseline pub/sub test (`subscription_basic.rs`) to ensure topic exists and Broker A owns it
    - Launch Broker B (background)
    - Kill Broker A (ensure A was initial owner)
    - Poll `danube-admin-cli` lookup until reassignment observed
    - Run pub/sub test again; assert success
    - Upload logs on failure

## Acceptance Criteria
- Reallocation removes only broker-assignment keys and creates unassigned entries.
- New broker creates local topic on assignment and accepts producers/consumers.
- No durable metadata is removed during failover.

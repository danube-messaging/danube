# Reliable Topic Unload/Delete: Implementation Plan

Goal: Safely remove a Reliable topic from one broker and allow reassignment to another without data loss, preserving at-least-once delivery. Admin CLI wiring is out of scope for now; we focus on broker core.


## Invariants and Requirements

- Schema immutability for Reliable topics:
  - Once data exists for `/ns/topic`, subsequent producers must supply identical schema. Reject mismatches.
- At-least-once delivery:
  - Persist subscription cursors (offsets) before unassigning.
  - Ensure all WAL data is durably uploaded to cloud before local teardown.
- No writes after seal:
  - Quiesce producers and prevent new publishes before sealing.
- Metadata scope:
  - Unassign moves only cluster assignment; `/topics/**` (schema, delivery, subscriptions, cursors) must remain intact.


## Phased Implementation

### Phase 1 — Quiesce and Pause

- Topic state machine: `ACTIVE -> DRAINING -> CLOSED`
  - Add to `Topic` and guard publish paths.
- BrokerService API:
  - `unload_reliable_topic(topic: &str, force: bool) -> Result<()>`
- TopicManager/Topic:
  - `quiesce_producers(topic)` sets state DRAINING; new publishes return retriable/moved error.
- Dispatcher:
  - `commit_and_pause_all(topic) -> Result<()>` commits cursors to etcd and pauses polling/dispatch.

Deliverables:
- State enum and guards in producer publish path.
- Dispatcher method committing `/topics/<ns>/<topic>/subscriptions/<sub>/cursor`.


### Phase 2 — Flush and Seal Storage

- WAL/WAL Storage:
  - `fsync_and_rotate() -> Result<Offsets>` ensures all local data is fsync’d and segment finalized.
- Cloud Uploader:
  - `drain_and_stop(topic) -> Result<DrainStats>` flushes pending uploads and updates object index and `cur` pointer.
- Etcd Metadata (Storage):
  - Write `/danube-data/storage/topics/<ns>/<topic>/state`:
    `{ state: "sealed", last_committed_offset, broker_id, ts }`.
- WalFactory orchestration:
  - `flush_and_seal(topic: &str) -> Result<SealInfo>` calling the above components and stopping per-topic tasks.

Deliverables:
- New methods in wal_storage/uploader/etcd_metadata and a coordinator in wal_factory.


### Phase 3 — Unassign and Local Cleanup

- TopicCluster:
  - `unassign_topic(topic: &str) -> Result<()>`:
    - Delete `/cluster/brokers/{this_broker}/{ns}/{topic}`.
    - Create `/cluster/unassigned/{ns}/{topic}`.
- TopicManager:
  - `delete_local(topic)` already disconnects producers/consumers and removes worker topic.
- Load report:
  - Emit immediate local load update after add/remove to reduce staleness (optional now, recommended).

Deliverables:
- New unassign helper and BrokerService wiring within `unload_reliable_topic`.


### Phase 4 — Resume on New Broker

- No code changes required for now; rely on existing watch Put -> `ensure_local()`.
- Ensure `WalFactory::for_topic()` detects existing storage state (sealed and objects/cur) and resumes appropriately.
- Dispatcher reads cursors from etcd and resumes consumers.

Deliverables:
- Verify `for_topic()` resume path; add any missing checks for sealed state and checkpoint.


## Detailed Module Changes

### danube-broker

- `topic.rs` / `topic_worker.rs`
  - Add `TopicState { ACTIVE, DRAINING, CLOSED }`.
  - Guard publish path: if DRAINING, return retriable/moved error.

- `dispatcher/subscription_engine.rs` and `dispatcher/unified_single.rs`
  - Implement `commit_and_pause_all(topic)`:
    - For each subscription on topic:
      - Flush internal cursor/accounting to a consistent offset.
      - Persist via `TopicResources::set_subscription_cursor(sub, topic, offset)`.
      - Pause dispatcher and stop polling.

- `broker_service.rs`
  - Add `unload_reliable_topic(topic, force)` orchestrating:
    1) validate + check local ownership
    2) TopicManager.quiesce_producers(topic)
    3) dispatcher.commit_and_pause_all(topic)
    4) wal_factory.flush_and_seal(topic)
    5) topic_cluster.unassign_topic(topic)
    6) topic_manager.delete_local(topic)

- `topic_control.rs`
  - Expose `quiesce_producers(topic)` on TopicManager/Topic.

- `topic_cluster.rs`
  - Add `unassign_topic(topic)`; do not touch `/topics/**` metadata.

- Schema validation on producer create (reliable only):
  - When topic is Reliable, compare incoming producer schema against `/topics/<ns>/<topic>/schema`. Reject on mismatch.


### danube-persistent-storage

- `wal_storage.rs`
  - `fsync_and_rotate() -> Result<Offsets>`: ensure data durable and segment boundary finalized; return last committed offset.

- `cloud/uploader.rs` and `cloud/uploader_stream.rs`
  - `drain_and_stop(topic) -> Result<DrainStats>`: flush all pending uploads, update object index and `cur`, stop scheduling further uploads for the topic.

- `checkpoint.rs`
  - Ensure checkpoint consistency after rotation; expose retrieval for last_committed_offset if not already.

- `etcd_metadata.rs`
  - Helpers to write/read storage state:
    - `/danube-data/storage/topics/<ns>/<topic>/state` with `{ sealed, last_committed_offset, broker_id, ts }`.

- `wal_factory.rs`
  - `flush_and_seal(topic) -> Result<SealInfo>`: orchestrates `fsync_and_rotate`, `drain_and_stop`, writes `state`, stops per-topic tasks; reuse `normalize_topic_path`.
  - On `for_topic()` open path, if sealed state exists and objects present, ensure consistent resume rules.


## Error Handling and Idempotency

- Re-running unload:
  - If topic already DRAINING, proceed; if CLOSED, no-op.
  - `flush_and_seal` returns success if already sealed.
  - `unassign_topic` tolerates missing broker assignment and existing unassigned entry.
  - `delete_local` tolerates missing topic (already removed).

- `force` flag:
  - Skip active consumer checks; still commit known cursors and proceed with sealing.


## Observability

- Add tracing spans for each phase: quiesce, commit, seal, unassign, cleanup.
- Emit final event: `topic_unloaded { topic, from_broker, last_committed_offset }`.


## Test Plan

- Integration: 3 brokers, Reliable topic.
  1) Produce N messages, consume some, leave unconsumed tail, verify cursors.
  2) Call `unload_reliable_topic` on current broker.
  3) Verify etcd shows unassigned then reassigned to a different broker.
  4) Start producer/consumer on new broker; producer schema must match; consumer resumes from stored cursor and continues.
  5) Validate cloud objects exist and uploader stopped on old broker.

- Negative:
  - Schema mismatch on producer create post-move (reject).
  - Unload with active consumers and `force=false` (fail with clear error).


## Milestones

- M1: Topic state + producer quiesce + dispatcher commit/pause.
- M2: WAL `fsync_and_rotate` + uploader `drain_and_stop` + etcd sealed state + wal_factory `flush_and_seal`.
- M3: TopicCluster `unassign_topic` + BrokerService `unload_reliable_topic` wiring + local delete.
- M4: Schema immutability checks for Reliable producers.
- M5: Integration tests and observability.

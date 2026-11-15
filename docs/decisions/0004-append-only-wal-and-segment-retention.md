# 0004: Append-only WAL, segment-level retention

**Status:** Accepted. "Delete on ack" retention is planned, not implemented.

## Context

Users with JMS / IBM MQ / Tibco EMS backgrounds expect queue semantics: one consumer of a group gets each message, many producers, and the message is **deleted once acknowledged**, which keeps the store small.

## What Danube already provides

- **Competing consumers:** a `Shared` subscription delivers each message to exactly one consumer of the subscription. `Key-Shared` adds per-key ordering, `Exclusive` / `Failover` give single active consumers.
- **Many producers** per topic.
- **Ack, nack, retries, DLQ:** per-subscription failure policies with backoff and poison handling (`dead_letter`, `block`, `drop`), usually set by the `task_queue` intent.
- **Independent subscriptions:** several subscriptions read the same data without copying it (pub/sub and queue on the same topic).

## Decision

Storage is an **append-only WAL** split into segments. Acks move a subscription's cursor; they do not delete messages. Space is reclaimed by **whole segments**:

- local WAL files are pruned by time/size (`danube cluster wal`), and only once they have been exported to durable storage
- durable segments are reference-counted, so `danube topic branch` can share history without copying

## Why

- Deleting individual messages means random I/O, index maintenance and fragmentation. Sequential appends and whole-file deletes keep throughput high and predictable.
- One copy of the data serves many subscriptions, replay (`topic subscription seek`), branching and lakehouse export. A destructive queue cannot do any of that.
- Recovery is simple: segments are immutable once sealed.

## Planned: delete-on-ack retention

When all subscriptions have acknowledged past a sealed segment, the broker may drop that segment immediately instead of waiting for time/size retention. This gives queue-like storage footprints without per-message deletes. It belongs in `danube-persistent-storage` retention, driven by subscription cursors stored in Raft. It is **not implemented yet**: do not document it as available.

## Trade-offs

- Storage is freed in segment-sized steps, not per message.
- A slow or abandoned subscription holds back reclamation under delete-on-ack. It will need a policy (max lag, or ignore inactive subscriptions).

## Rejected alternatives

- **Per-message delete / compaction on ack:** throughput cost, and it breaks replay and branching.

## Related ideas (not decided)

A JMS adapter on top of `danube-java` could map JMS queues onto shared subscriptions and JMS topics onto exclusive or failover subscriptions.


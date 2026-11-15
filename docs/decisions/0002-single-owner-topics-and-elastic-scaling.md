# 0002: Single-owner topics, client redirect, tiered segments

**Status:** Accepted

## Context

Users coming from IBM MQ / Tibco EMS ask for "replicated queues": a message lands on node 1 while the consumer listens on node 2. Operators ask how Danube scales up and down quickly.

## Decision

- Every topic (or partition `-part-N`) has **exactly one owning broker**, recorded in Raft and chosen by the load manager (0010).
- Clients do a **lookup**: if they reach a broker that does not own the topic, it answers with a redirect to the owner (`danube-client/src/lookup_service.rs`). Brokers do **not** proxy or forward message data to each other.
- The owner writes to its local WAL. Sealed segments are exported to shared FS or object storage and indexed in Raft, so any broker can read the history.

## How scaling works because of this

- **Scale out:** a new broker joins Raft (`--seed-nodes` auto-join or `--join` + `danube cluster node add`), registers, and becomes active. The load manager places new topics there; `danube broker rebalance` moves existing ones.
- **Moving a topic:** the old owner seals its active segment and releases ownership, and the new owner claims it in Raft and continues from the durable history. No historical bytes are copied, so a move is fast (well under a second) whatever the topic size. See `architecture/reliable-topic-move` in the docs.
- **Scale in / failure:** `danube broker unload` (or SIGTERM drain) moves topics away. On a crash, ownership expires (TTL) and topics are reassigned. They recover from sealed segments, then local WAL if the disk survived, then Valkey (0003).
- **Many brokers, small Raft:** brokers beyond the voter cap join as non-voting learners, so adding serving capacity does not slow metadata commits (0010).

## Why

- No inter-broker data mesh: network cost stays proportional to client traffic.
- Ordering and acks are decided in one place per topic, which keeps the dispatcher simple and correct.
- Topic mobility is what makes automatic rebalancing safe to run.

## Trade-offs

- A topic's write throughput is bounded by one broker. Use partitions (`--partitions N`) to spread a hot topic.
- Clients must handle redirects and re-lookup after moves (the SDKs do).
- In `local` storage mode there is no shared history, so topic moves and failover cannot preserve data. Multi-broker clusters should use `shared_fs` or `object_store`.

## Rejected alternatives

- **Broker-side forwarding / replicated queues:** doubles network traffic and adds a second delivery path to keep consistent.
- **Multi-writer topics:** needs distributed ordering per message.

## Revisit if

Clients that cannot follow redirects become a requirement. A thin stateless proxy in front would then be preferable to broker-side forwarding.


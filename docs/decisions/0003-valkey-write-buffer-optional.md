# 0003: The Valkey write buffer is optional, and stays

**Status:** Accepted

## Context

Because of 0001/0002, the active WAL segment of a reliable topic lives on one broker until it rotates (512 MiB by default) or is exported. Two questions keep coming up: "why keep Valkey at all?" and "why not make it mandatory?"

## Decision

`danube cluster write-buffer enable --endpoints redis://...` turns on a **warm tier** in `danube-persistent-storage/src/valkey`:

- every reliable message is also appended to a Valkey stream before the producer is acknowledged, optionally waiting for Valkey replicas (`--wait-replicas`, `--wait-timeout-ms`, `--on-wait-timeout`)
- a rotation listener trims Valkey once a WAL segment is safely exported, keeping a few closed segments (`--max-cached-segments`)
- recovery replays the unexported tail from Valkey when the local WAL is gone
- the stream reader serves recent reads from Valkey (XRANGE) as a warm read path

It is **off by default**.

## Why keep it

- `fsync` (the `task_queue` and `audit_log` intents) protects against process crashes and power loss, **not against losing the disk**: Kubernetes `emptyDir`, spot or preemptible nodes, a dead host.
- Without it, the loss window on disk loss is "everything since the last export", which on a busy topic is minutes of data.
- It holds only the short tail, not history, so memory stays bounded.

## Why Valkey and not replication to peer brokers

Replicating the tail between brokers means building per-topic replica sets, in-sync tracking, replica election and catch-up inside Danube. That is the complexity 0001 avoids. Valkey streams plus `WAIT` give synchronous replicas with off-the-shelf tooling, and Valkey can be operated or bought as a managed service.

## Why optional

- The default deployment must have zero external dependencies (invariant 6 in `AGENTS.md`).
- Many deployments have durable disks (EBS, PVs) or accept a bounded loss window on disk failure.
- Non-reliable topics (`realtime`) never touch it.

## Trade-offs

- An extra network round trip per reliable write (more with `--wait-replicas`).
- Memory of roughly `max_cached_segments × rotation size` per topic in Valkey.
- One more system to run when enabled.

## Revisit if

Export becomes continuous with near-zero lag (for example small segments streamed to object storage), or brokers gain native tail replication. Either could make Valkey unnecessary.

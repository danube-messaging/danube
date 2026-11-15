# Architecture decision records

Each record explains one decision that is not obvious from the code: why it was made, what it costs, and when to revisit it. Agents should read the relevant record before changing the area it covers, and update or supersede it in the same PR when the decision changes.

Format: **Status, Context, Decision, Why, Trade-offs, Rejected alternatives, Revisit if.** Keep each record under about 80 lines. Number new records sequentially and add them to this list and to the index in the root `AGENTS.md`.

- [0001](0001-raft-for-metadata-only.md) Raft replicates metadata, never message data
- [0002](0002-single-owner-topics-and-elastic-scaling.md) Single-owner topics, client redirect, tiered segments
- [0003](0003-valkey-write-buffer-optional.md) Optional Valkey write buffer for the unexported tail
- [0004](0004-append-only-wal-and-segment-retention.md) Append-only WAL, segment-level retention (delete-on-ack planned)
- [0005](0005-embedded-openraft-redb.md) Embedded openraft + redb
- [0006](0006-zero-yaml-configuration-layers.md) Zero-YAML configuration layers
- [0007](0007-stream-intents.md) Stream intents expand at creation
- [0008](0008-dual-dispatch.md) Non-reliable and reliable dispatch
- [0009](0009-security-architecture.md) Security architecture (known gaps listed)
- [0010](0010-load-manager-and-voter-cap.md) Load manager and Raft voter cap
- [0011](0011-embedded-schema-registry.md) Embedded schema registry
- [0012](0012-edge-gateway.md) Edge gateway consolidates MQTT
- [0013](0013-lakehouse-isolation.md) Lakehouse export isolation
- [0014](0014-single-binary-cli-and-mcp.md) Single binary, one CLI tree, guarded MCP

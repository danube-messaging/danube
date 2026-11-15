# AGENTS.md: danube-edge

MQTT v3.1.1/v5.0 gateway and WAL-backed replicator behind `danube up edge`. Read ADR 0012.

## Entry points

- `src/edge_service.rs`: wiring of the MQTT server, local WAL ingestion and the replicator.
- `src/config.rs`: `EdgeConfig::zero_yaml` (flags only) and topic mappings. The default mapping is `/{edge-name}/ingest`.
- `src/mqtt/` (`server.rs`, `session.rs`, `bridge.rs`, `ingester.rs`): MQTT protocol handling and the bridge to Danube messages (attributes `mqtt.topic`, `edge.node_id`, `mqtt.*` for v5 properties).
- `src/replicator/`: streams local WAL data to the cloud cluster with checkpoints (`/edge/checkpoints/...`).
- `src/readiness.rs`: topic readiness. PUBACK is held back until a topic is provisioned.

## Invariants

- Device topics are consolidated. Never create one cloud topic per device or per MQTT topic.
- Writes go to the local WAL before PUBACK, so the edge survives WAN loss.
- Replication is resumable from checkpoints and must not duplicate or skip offsets.

## Pitfalls

- The cloud namespace for the edge must exist before replication starts.
- An edge node uses the same default ports as a broker (6650, 7650), so pick others when running next to a local cluster.
- With an enforce schema policy, invalid messages get reason code 0x99 on MQTT v5. On v3.1.1 they get a normal PUBACK and are dropped.

## Tests

```bash
cargo test -p danube-edge --lib
cargo test -p danube-edge --test edge_replication_basic   # needs a running cloud broker
```


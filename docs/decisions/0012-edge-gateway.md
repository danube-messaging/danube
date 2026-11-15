# 0012: Edge gateway consolidates MQTT into one ingest topic

**Status:** Accepted

## Decision

`danube up edge --name <edge> --cloud-endpoint <url>` runs the same binary as an MQTT v3.1.1/v5.0 gateway (`danube-edge`):

- device publishes are written to a **local WAL** first, so the edge survives WAN outages
- unmatched MQTT topics are consolidated into **one Danube topic per edge node**: `/{edge-name}/ingest`. The original MQTT topic is kept in the `mqtt.topic` attribute, alongside `edge.node_id` (MQTT 5 properties become `mqtt.*` attributes)
- a replicator streams the WAL to the cloud cluster over gRPC with checkpoints (`/edge/checkpoints/...` on the edge, `/edge/replicated/...` in the cloud)
- PUBACK is held back for topics that are not ready yet, so QoS 1 devices retry instead of losing data

## Why

- Millions of device topics must not become millions of Raft topics: the cloud metadata would explode (0001).
- Local-first writes make the edge independent of WAN availability.
- Same binary, same CLI, same security model as the cloud.

## Rules for changes

- Anything that creates cloud topics per device or per MQTT topic needs a new decision record.
- The cloud namespace for the edge must exist (`danube namespace create <edge-name>`) before replication.


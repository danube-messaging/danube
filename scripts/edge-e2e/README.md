# Edge End-to-End Testing

This folder contains scripts to test the full Danube Edge broker pipeline locally:

```
MQTT Client → Edge MQTT Gateway → Schema Validation → WAL → Replicator → Cluster → Consumer
```

## Contents

- `edge-e2e-local.sh` — Spins up a 3-node Danube cluster + edge broker with MQTT gateway, namespace provisioning, and schema registration. Infrastructure-only — does not run tests itself.
- `test_mqtt_ingestion.py` — Python E2E test suite using `paho-mqtt` (MQTT client) and `danube-client` (cluster consumer) to verify the full pipeline.
- `requirements.txt` — Python dependencies.

## Quick Start

### 1. Start the Infrastructure

```bash
# From the repo root
./scripts/edge-e2e/edge-e2e-local.sh --keep-alive
```

This starts:
- **3 Cluster Brokers** — ports `6650`, `6651`, `6652`
- **1 Edge Broker** — port `6653`, MQTT on `1883`
- Provisions `edge1` namespace and `telemetry-events` JSON schema

Flags:
- `--keep-alive` — Keep brokers running after startup (required for manual testing)
- `--skip-build` — Reuse existing binary (faster restarts)
- `--debug` — Use debug build instead of release

### 2. Install Python Dependencies

```bash
pip install -r scripts/edge-e2e/requirements.txt
```

### 3. Run the E2E Tests

```bash
python3 scripts/edge-e2e/test_mqtt_ingestion.py
```

## What the Tests Verify

| Test | Protocol | What it validates |
|------|----------|-------------------|
| **Test 1** — Valid payload | paho-mqtt | MQTT publish → topic routing → schema validation passes → PUBACK |
| **Test 2** — Raw bytes | paho-mqtt | MQTT publish to catch-all topic → PUBACK (no schema) |
| **Test 3** — Invalid payload | paho-mqtt | MQTT publish → PUBACK sent (v3.1.1 has no rejection) → message silently dropped |
| **Test 4** — Full pipeline | paho-mqtt + danube-client | Valid message arrives on cluster + invalid message from Test 3 was dropped |

### Test 4: Full Pipeline

This is the true end-to-end test. It:
1. Creates a `danube-client` consumer on the **cluster** (`127.0.0.1:6650`) subscribed to `/edge1/telemetry`
2. Publishes a valid JSON payload via MQTT to `device/sensor-1/telemetry`
3. Waits up to 15s for the message to arrive on the cluster consumer
4. Asserts the received payload matches what was published

## CI Integration

These same tests also run in CI via `.github/workflows/edge-replication-e2e.yml`, which:
1. Starts the same infrastructure
2. Runs Rust replication tests (`danube-edge/tests/edge_replication_basic.rs`)
3. Runs the Python MQTT E2E tests (`test_mqtt_ingestion.py`)

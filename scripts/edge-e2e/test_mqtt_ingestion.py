"""
Edge MQTT E2E Tests — Full Pipeline Verification

Tests the complete edge broker pipeline using real clients:
  - paho-mqtt: Simulates an IoT device publishing to the MQTT gateway
  - danube-client: Subscribes on the cluster to verify replication

Pipeline under test:
  MQTT Client → Edge MQTT Gateway → Schema Validation → WAL → Replicator → Cluster → Consumer

Prerequisites:
  - Run ./scripts/edge-e2e/edge-e2e-local.sh --keep-alive [--skip-build]
  - pip install -r scripts/edge-e2e/requirements.txt
"""

import asyncio
import json
import sys
import time

import paho.mqtt.client as mqtt

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
MQTT_BROKER = "127.0.0.1"
MQTT_PORT = 1883
CLUSTER_URL = "http://127.0.0.1:6650"

MQTT_TOPIC_TELEMETRY = "device/sensor-1/telemetry"
MQTT_TOPIC_RAW = "device/sensor-1/raw"
DANUBE_TOPIC_TELEMETRY = "/edge1/telemetry"

VALID_PAYLOAD = {"temperature": 25.5, "device_id": "sensor-1"}
INVALID_PAYLOAD = {"humidity": 60}  # missing required "temperature"

# Timeouts
MQTT_CONNECT_WAIT = 1  # seconds to wait after MQTT connect
MQTT_PUBACK_WAIT = 5  # seconds to wait for PUBACK
REPLICATION_TIMEOUT = 15  # seconds to wait for message on cluster


# ---------------------------------------------------------------------------
# MQTT Client Helpers
# ---------------------------------------------------------------------------
class MqttTestClient:
    """Thin wrapper around paho-mqtt for test assertions."""

    def __init__(self, client_id: str = "test-e2e-client"):
        self.client = mqtt.Client(client_id=client_id)
        self.connected = False
        self.published: dict[int, str] = {}  # mid -> status

        self.client.on_connect = self._on_connect
        self.client.on_publish = self._on_publish

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.connected = True

    def _on_publish(self, client, userdata, mid):
        if mid in self.published:
            self.published[mid] = "SUCCESS"

    def connect(self):
        self.client.connect(MQTT_BROKER, MQTT_PORT, 60)
        self.client.loop_start()
        time.sleep(MQTT_CONNECT_WAIT)
        if not self.connected:
            raise ConnectionError(
                f"Failed to connect to {MQTT_BROKER}:{MQTT_PORT}. "
                "Is the edge broker running? (./scripts/edge-e2e/edge-e2e-local.sh --keep-alive)"
            )

    def publish_and_wait(self, topic: str, payload, qos: int = 1) -> bool:
        """Publish and wait for PUBACK. Returns True if acknowledged."""
        msg_info = self.client.publish(topic, payload, qos=qos)
        self.published[msg_info.mid] = "PENDING"
        try:
            msg_info.wait_for_publish(timeout=MQTT_PUBACK_WAIT)
        except Exception:
            pass
        return self.published.get(msg_info.mid) == "SUCCESS"

    def close(self):
        self.client.disconnect()
        self.client.loop_stop()


# ---------------------------------------------------------------------------
# Test Functions
# ---------------------------------------------------------------------------
def test_valid_payload(mqtt_client: MqttTestClient) -> bool:
    """Test 1: Valid JSON payload on an enforced topic should be accepted."""
    print("\n--- Test 1: Valid Schema Payload ---")
    payload = json.dumps(VALID_PAYLOAD)
    print(f"Publishing valid JSON to {MQTT_TOPIC_TELEMETRY}: {payload}")

    if mqtt_client.publish_and_wait(MQTT_TOPIC_TELEMETRY, payload):
        print("✅ Test 1 PASSED: Valid payload was accepted.")
        return True
    else:
        print("❌ Test 1 FAILED: Valid payload was not accepted.")
        return False


def test_raw_payload(mqtt_client: MqttTestClient) -> bool:
    """Test 2: Raw bytes on a non-enforced topic should be accepted."""
    print("\n--- Test 2: Raw Bytes Payload ---")
    raw = b"raw binary data 123"
    print(f"Publishing raw bytes to {MQTT_TOPIC_RAW}: {raw}")

    if mqtt_client.publish_and_wait(MQTT_TOPIC_RAW, raw):
        print("✅ Test 2 PASSED: Raw payload was accepted.")
        return True
    else:
        print("❌ Test 2 FAILED: Raw payload was not accepted.")
        return False


def test_invalid_payload(mqtt_client: MqttTestClient) -> bool:
    """Test 3: Invalid JSON acknowledged (MQTT v3.1.1) but silently dropped.

    MQTT v3.1.1 has no way to signal rejection in PUBACK, so the broker
    sends a normal PUBACK (to stop the device from retrying forever) but
    drops the invalid message instead of ingesting it.

    We verify the drop by publishing invalid + valid, then checking that
    only the valid message arrives on the cluster.
    """
    print("\n--- Test 3: Invalid Schema Payload (Accept-but-Drop) ---")
    payload = json.dumps(INVALID_PAYLOAD)
    print(f"Publishing invalid JSON to {MQTT_TOPIC_TELEMETRY}: {payload}")

    if mqtt_client.publish_and_wait(MQTT_TOPIC_TELEMETRY, payload):
        print("  ✅ PUBACK received (client stops retrying).")
        print("  → Message is silently dropped by the broker (verified in Test 4).")
        print("✅ Test 3 PASSED: Invalid payload was acknowledged but will be dropped.")
        return True
    else:
        print("❌ Test 3 FAILED: No PUBACK received for invalid payload.")
        return False


async def test_full_pipeline(mqtt_client: MqttTestClient) -> bool:
    """Test 4: Full pipeline — MQTT → edge → cluster consumer.

    Also verifies that invalid payloads (from Test 3) were dropped:
    publishes a valid message and checks that the cluster consumer
    only receives the valid messages (not the invalid one from Test 3).
    """
    print("\n--- Test 4: Full Pipeline (MQTT → Edge → Cluster Consumer) ---")

    # Import danube here so Tests 1-2 can run without it installed
    try:
        from danube import DanubeClientBuilder, SubType
    except ImportError:
        print("⚠️  Test 4 SKIPPED: danube-client not installed (pip install danube-client)")
        return True  # Don't fail if danube-client isn't available

    # Step 1: Create a Danube consumer on the CLUSTER (not the edge)
    print(f"  Subscribing on cluster ({CLUSTER_URL}) to topic {DANUBE_TOPIC_TELEMETRY}...")
    danube_client = await DanubeClientBuilder().service_url(CLUSTER_URL).build()

    consumer = (
        danube_client.new_consumer()
        .with_topic(DANUBE_TOPIC_TELEMETRY)
        .with_consumer_name("e2e-pipeline-consumer")
        .with_subscription("e2e-pipeline-sub")
        .with_subscription_type(SubType.EXCLUSIVE)
        .build()
    )

    await consumer.subscribe()
    queue = await consumer.receive()
    print("  ✅ Consumer subscribed on cluster.")

    # Step 2: Publish a unique valid payload via MQTT
    marker = f"e2e-pipeline-{int(time.time() * 1000)}"
    valid_payload = json.dumps({"temperature": 42.0, "device_id": marker})
    print(f"  Publishing valid payload via MQTT: {valid_payload}")

    if not mqtt_client.publish_and_wait(MQTT_TOPIC_TELEMETRY, valid_payload):
        print("❌ Test 4 FAILED: MQTT publish was not acknowledged by edge broker.")
        return False

    # Step 3: Drain messages until we find our marker (or timeout).
    # The consumer starts from the earliest offset, so older messages
    # from Test 1 may arrive first. We drain them all, checking:
    # - Did our marker arrive? (proves full pipeline works)
    # - Did the invalid payload from Test 3 leak? (should never happen)
    print(f"  Waiting up to {REPLICATION_TIMEOUT}s for marker on cluster...")
    found_marker = False
    invalid_leaked = False
    deadline = time.time() + REPLICATION_TIMEOUT

    try:
        while time.time() < deadline:
            remaining = max(0.1, deadline - time.time())
            message = await asyncio.wait_for(queue.get(), timeout=remaining)
            received_payload = message.payload.decode()
            await consumer.ack(message)

            if marker in received_payload:
                print(f"  Received marker on cluster: {received_payload}")
                found_marker = True
                break
            elif "humidity" in received_payload and "temperature" not in received_payload:
                print(f"  ❌ Invalid payload leaked to cluster: {received_payload}")
                invalid_leaked = True
            else:
                print(f"  (drained earlier message: {received_payload})")
    except asyncio.TimeoutError:
        pass

    if not found_marker:
        print(
            f"❌ Test 4 FAILED: Marker not received on cluster within {REPLICATION_TIMEOUT}s.\n"
            "   Check edge broker logs for replication errors."
        )
        return False

    print("✅ Test 4 PASSED: Valid message arrived on cluster via full pipeline!")

    # Step 4: Final drain — check for any trailing invalid payloads
    # (invalid_leaked may already be set from the marker search drain)
    print("  Checking that no invalid payload leaked to cluster (2s drain)...")
    try:
        while True:
            extra = await asyncio.wait_for(queue.get(), timeout=2)
            extra_payload = extra.payload.decode()
            await consumer.ack(extra)
            if "humidity" in extra_payload and "temperature" not in extra_payload:
                print(f"  ❌ Invalid payload leaked to cluster: {extra_payload}")
                invalid_leaked = True
            else:
                print(f"  (drained extra message: {extra_payload})")
    except asyncio.TimeoutError:
        pass  # Expected: queue is empty

    if invalid_leaked:
        print("❌ Test 4 FAILED: Invalid payload was NOT dropped by the edge broker.")
        return False

    print("  ✅ No invalid payloads leaked to cluster.")
    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
async def main():
    mqtt_client = MqttTestClient()

    print(f"Connecting to MQTT broker at {MQTT_BROKER}:{MQTT_PORT}...")
    try:
        mqtt_client.connect()
        print("✅ Connected to Edge Broker successfully.")
    except Exception as e:
        print(f"❌ {e}")
        sys.exit(1)

    passed = True

    # Tests 1-2: MQTT gateway tests (sync, paho-mqtt only)
    passed = test_valid_payload(mqtt_client) and passed
    passed = test_raw_payload(mqtt_client) and passed

    # Test 3: Invalid payload — PUBACK received (accept-but-drop)
    passed = test_invalid_payload(mqtt_client) and passed

    # Test 4: Full pipeline + verify Test 3's invalid payload was dropped
    passed = await test_full_pipeline(mqtt_client) and passed

    mqtt_client.close()

    if passed:
        print("\n🎉 All MQTT E2E tests passed successfully!")
    else:
        print("\n💥 Some tests FAILED.")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())


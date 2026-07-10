"""
IoT Device Simulator — Danube Lakehouse Demo

Simulates IoT sensor devices publishing data via MQTT across 3 topic categories:

  1. telemetry  — temperature, humidity, pressure readings
  2. sensors    — machine vibration / RPM readings
  3. alerts     — threshold-based alerts (fires every round per device)

Each topic gets roughly equal message volume (~3 msg/s each ≈ 9 msg/s total).

Pipeline:
  This script → MQTT (1883) → Edge Broker → Cluster Brokers → danube-iceberg → Iceberg/Parquet

Environment variables:
  MQTT_BROKER      — MQTT broker hostname (default: edge-broker)
  MQTT_PORT        — MQTT broker port (default: 1883)
  NUM_DEVICES      — Number of simulated devices (default: 9, 3 per topic)
  PUBLISH_INTERVAL — Seconds between publish rounds (default: 1)
  DURATION         — Total runtime in seconds (default: 0 = infinite)
"""

import json
import os
import random
import signal
import sys
import time

import paho.mqtt.client as mqtt

# ---------------------------------------------------------------------------
# Configuration (from environment or defaults)
# ---------------------------------------------------------------------------
MQTT_BROKER = os.getenv("MQTT_BROKER", "edge-broker")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
NUM_DEVICES = int(os.getenv("NUM_DEVICES", "9"))
PUBLISH_INTERVAL = float(os.getenv("PUBLISH_INTERVAL", "1"))
DURATION = int(os.getenv("DURATION", "0"))  # 0 = run forever

# Simulated device locations
LOCATIONS = [
    "warehouse-A", "warehouse-B", "factory-floor",
    "cold-storage", "rooftop", "loading-dock",
    "clean-room", "server-room", "lab-1",
]

# ---------------------------------------------------------------------------
# Signal handling for graceful shutdown
# ---------------------------------------------------------------------------
shutdown = False


def handle_signal(signum, frame):
    global shutdown
    print(f"\n🛑 Received signal {signum}, shutting down gracefully...")
    shutdown = True


signal.signal(signal.SIGTERM, handle_signal)
signal.signal(signal.SIGINT, handle_signal)

# ---------------------------------------------------------------------------
# MQTT callbacks
# ---------------------------------------------------------------------------
connected = False
publish_count = 0
error_count = 0
topic_counts = {}


def on_connect(client, userdata, flags, rc):
    global connected
    if rc == 0:
        connected = True
        print(f"✅ Connected to MQTT broker at {MQTT_BROKER}:{MQTT_PORT}")
    else:
        print(f"❌ MQTT connection failed with code {rc}")


def on_publish(client, userdata, mid):
    global publish_count
    publish_count += 1


def on_disconnect(client, userdata, rc):
    global connected
    connected = False
    if rc != 0:
        print(f"⚠️  Unexpected MQTT disconnect (rc={rc}), will retry...")


# ---------------------------------------------------------------------------
# Device classes — each topic category has a distinct payload shape
# ---------------------------------------------------------------------------

class TelemetryDevice:
    """Temperature / humidity / pressure sensor."""

    def __init__(self, device_id: str, location: str):
        self.device_id = device_id
        self.location = location
        self.base_temp = random.uniform(18.0, 35.0)
        self.base_humidity = random.uniform(30.0, 80.0)
        self.base_pressure = random.uniform(1010.0, 1025.0)

    def mqtt_topic(self) -> str:
        return f"device/{self.device_id}/telemetry"

    def reading(self) -> dict:
        self.base_temp += random.gauss(0, 0.1)
        self.base_humidity += random.gauss(0, 0.3)
        self.base_pressure += random.gauss(0, 0.05)
        return {
            "device_id": self.device_id,
            "location": self.location,
            "temperature": round(self.base_temp + random.gauss(0, 0.5), 2),
            "humidity": round(self.base_humidity + random.gauss(0, 2.0), 2),
            "pressure": round(self.base_pressure + random.gauss(0, 0.3), 2),
            "battery_pct": round(random.uniform(20.0, 100.0), 1),
            "timestamp": int(time.time()),
        }


class SensorDevice:
    """Machine vibration / RPM sensor."""

    def __init__(self, device_id: str, location: str):
        self.device_id = device_id
        self.location = location
        self.base_rpm = random.uniform(1200.0, 3600.0)
        self.base_vibration = random.uniform(0.5, 3.0)

    def mqtt_topic(self) -> str:
        return f"sensors/{self.device_id}/vibration"

    def reading(self) -> dict:
        self.base_rpm += random.gauss(0, 5.0)
        self.base_vibration += random.gauss(0, 0.05)
        return {
            "device_id": self.device_id,
            "location": self.location,
            "rpm": round(self.base_rpm + random.gauss(0, 20.0), 1),
            "vibration_mm_s": round(max(0, self.base_vibration + random.gauss(0, 0.2)), 3),
            "motor_temp_c": round(random.uniform(40.0, 90.0), 1),
            "power_watts": round(random.uniform(100.0, 2000.0), 0),
            "timestamp": int(time.time()),
        }


class AlertDevice:
    """Threshold-based alert generator. Fires every round for fair volume."""

    ALERT_TYPES = ["over_temp", "low_battery", "high_vibration", "door_open", "leak_detected"]
    SEVERITIES = ["info", "warning", "critical"]

    def __init__(self, device_id: str, location: str):
        self.device_id = device_id
        self.location = location

    def mqtt_topic(self) -> str:
        return f"device/{self.device_id}/alerts"

    def reading(self) -> dict:
        return {
            "device_id": self.device_id,
            "location": self.location,
            "alert_type": random.choice(self.ALERT_TYPES),
            "severity": random.choice(self.SEVERITIES),
            "value": round(random.uniform(0, 100), 2),
            "message": f"Alert from {self.device_id} at {self.location}",
            "acknowledged": random.choice([True, False]),
            "timestamp": int(time.time()),
        }


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
def main():
    global shutdown, connected, publish_count, error_count, topic_counts

    print("=" * 60)
    print("  🌡️  IoT Device Simulator — Danube Lakehouse Demo")
    print("=" * 60)
    print(f"  MQTT Broker:    {MQTT_BROKER}:{MQTT_PORT}")
    print(f"  Devices:        {NUM_DEVICES}")
    print(f"  Interval:       {PUBLISH_INTERVAL}s")
    print(f"  Duration:       {'♾️  infinite' if DURATION == 0 else f'{DURATION}s'}")
    print(f"  Topics:         telemetry, sensors, alerts")
    print("=" * 60)

    # Create virtual devices — distribute evenly across 3 categories
    telemetry_devices = []
    sensor_devices = []
    alert_devices = []

    for i in range(NUM_DEVICES):
        device_id = f"sensor-{i + 1:02d}"
        location = LOCATIONS[i % len(LOCATIONS)]
        category = i % 3  # Round-robin across 3 categories

        if category == 0:
            dev = TelemetryDevice(device_id, location)
            telemetry_devices.append(dev)
            label = "telemetry"
        elif category == 1:
            dev = SensorDevice(device_id, location)
            sensor_devices.append(dev)
            label = "sensors"
        else:
            dev = AlertDevice(device_id, location)
            alert_devices.append(dev)
            label = "alerts"

        print(f"  📡 {device_id:>12s} @ {location:<16s} [{label}]")

    print(f"\n  Summary: {len(telemetry_devices)} telemetry, "
          f"{len(sensor_devices)} sensors, "
          f"{len(alert_devices)} alerts")

    # Connect to MQTT broker with retry
    client = mqtt.Client(client_id=f"iot-simulator-{int(time.time())}")
    client.on_connect = on_connect
    client.on_publish = on_publish
    client.on_disconnect = on_disconnect

    print(f"\n⏳ Connecting to MQTT broker at {MQTT_BROKER}:{MQTT_PORT}...")

    max_retries = 30
    for attempt in range(1, max_retries + 1):
        try:
            client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
            client.loop_start()
            break
        except Exception as e:
            if attempt == max_retries:
                print(f"❌ Failed to connect after {max_retries} attempts: {e}")
                sys.exit(1)
            print(f"  Attempt {attempt}/{max_retries} failed: {e}. Retrying in 5s...")
            time.sleep(5)

    # Wait for connection
    deadline = time.time() + 30
    while not connected and time.time() < deadline:
        time.sleep(0.5)

    if not connected:
        print("❌ MQTT connection timeout after 30s")
        sys.exit(1)

    # Publish loop
    start_time = time.time()
    last_status = start_time

    print(f"\n🚀 Starting publish loop (3 topics, ~{NUM_DEVICES} msg/s)...\n")

    try:
        while not shutdown:
            # Check duration limit
            if DURATION > 0 and (time.time() - start_time) >= DURATION:
                print(f"\n⏱️  Duration limit ({DURATION}s) reached.")
                break

            # All devices publish every round for equal volume
            for dev in telemetry_devices:
                if shutdown:
                    break
                _publish(client, dev.mqtt_topic(), dev.reading())

            for dev in sensor_devices:
                if shutdown:
                    break
                _publish(client, dev.mqtt_topic(), dev.reading())

            for dev in alert_devices:
                if shutdown:
                    break
                _publish(client, dev.mqtt_topic(), dev.reading())

            # Status update every 30 seconds
            now = time.time()
            if now - last_status >= 30:
                elapsed = int(now - start_time)
                rate = publish_count / max(elapsed, 1)
                topics_str = ", ".join(
                    f"{t}={c}" for t, c in sorted(topic_counts.items())
                )
                print(
                    f"  📊 {elapsed:>6d}s | {publish_count:>8d} msgs | "
                    f"{rate:.1f} msg/s | {topics_str}"
                )
                last_status = now

            time.sleep(PUBLISH_INTERVAL)

    except KeyboardInterrupt:
        pass

    # Summary
    elapsed = int(time.time() - start_time)
    print(f"\n{'=' * 60}")
    print(f"  📈 Simulation {'Stopped' if shutdown else 'Complete'}")
    print(f"  Total published: {publish_count}")
    print(f"  Errors:          {error_count}")
    print(f"  Duration:        {elapsed}s")
    print(f"  Avg rate:        {publish_count / max(elapsed, 1):.1f} msg/s")
    for topic, count in sorted(topic_counts.items()):
        print(f"    {topic}: {count}")
    print(f"{'=' * 60}")

    client.disconnect()
    client.loop_stop()

    if error_count > 0:
        print(f"⚠️  Completed with {error_count} errors")
        sys.exit(1)
    else:
        print("✅ Simulator stopped cleanly")


def _publish(client, topic: str, payload: dict):
    """Publish a JSON payload to an MQTT topic."""
    global error_count, topic_counts
    result = client.publish(topic, json.dumps(payload), qos=1)
    if result.rc != mqtt.MQTT_ERR_SUCCESS:
        error_count += 1

    # Track per-topic counts
    category = topic.split("/")[-1]  # telemetry, vibration, alerts
    topic_counts[category] = topic_counts.get(category, 0) + 1


if __name__ == "__main__":
    main()

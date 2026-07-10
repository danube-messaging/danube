"""
IoT Device Simulator — Lakehouse Pipeline Demo

Simulates multiple IoT sensor devices publishing telemetry data via MQTT
to the Danube Edge Broker. Each device sends temperature, humidity, and
pressure readings every few seconds.

Pipeline:
  This script → MQTT (1883) → Edge Broker → Cluster Brokers → danube-iceberg → Iceberg/Parquet

Environment variables:
  MQTT_BROKER     — MQTT broker hostname (default: edge-broker)
  MQTT_PORT       — MQTT broker port (default: 1883)
  NUM_DEVICES     — Number of simulated devices (default: 3)
  PUBLISH_INTERVAL— Seconds between publishes per device (default: 2)
  DURATION        — Total runtime in seconds (default: 300, 0 = infinite)
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
NUM_DEVICES = int(os.getenv("NUM_DEVICES", "3"))
PUBLISH_INTERVAL = float(os.getenv("PUBLISH_INTERVAL", "2"))
DURATION = int(os.getenv("DURATION", "300"))  # 0 = run forever

# MQTT topic pattern: device/{device_id}/telemetry
MQTT_TOPIC_TEMPLATE = "device/{device_id}/telemetry"

# Simulated device locations
LOCATIONS = ["warehouse-A", "warehouse-B", "factory-floor", "cold-storage", "rooftop"]

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
# Sensor data generation
# ---------------------------------------------------------------------------
class VirtualDevice:
    """Simulates an IoT sensor device with realistic drifting readings."""

    def __init__(self, device_id: str, location: str):
        self.device_id = device_id
        self.location = location
        # Base values with some per-device variation
        self.base_temp = random.uniform(18.0, 35.0)
        self.base_humidity = random.uniform(30.0, 80.0)
        self.base_pressure = random.uniform(1010.0, 1025.0)

    def reading(self) -> dict:
        """Generate a telemetry reading with realistic noise."""
        return {
            "device_id": self.device_id,
            "location": self.location,
            "temperature": round(self.base_temp + random.gauss(0, 0.5), 2),
            "humidity": round(self.base_humidity + random.gauss(0, 2.0), 2),
            "pressure": round(self.base_pressure + random.gauss(0, 0.3), 2),
            "battery_pct": round(random.uniform(20.0, 100.0), 1),
            "timestamp": int(time.time()),
        }

    def drift(self):
        """Simulate slow environmental drift over time."""
        self.base_temp += random.gauss(0, 0.1)
        self.base_humidity += random.gauss(0, 0.3)
        self.base_pressure += random.gauss(0, 0.05)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
def main():
    global shutdown, connected, publish_count, error_count

    print("=" * 60)
    print("  🌡️  IoT Device Simulator — Danube Lakehouse Demo")
    print("=" * 60)
    print(f"  MQTT Broker:    {MQTT_BROKER}:{MQTT_PORT}")
    print(f"  Devices:        {NUM_DEVICES}")
    print(f"  Interval:       {PUBLISH_INTERVAL}s")
    print(f"  Duration:       {'infinite' if DURATION == 0 else f'{DURATION}s'}")
    print("=" * 60)

    # Create virtual devices
    devices = []
    for i in range(NUM_DEVICES):
        device_id = f"sensor-{i + 1}"
        location = LOCATIONS[i % len(LOCATIONS)]
        devices.append(VirtualDevice(device_id, location))
        print(f"  📡 Device: {device_id} @ {location}")

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

    print(f"\n🚀 Starting telemetry publish loop...")

    try:
        while not shutdown:
            # Check duration limit
            if DURATION > 0 and (time.time() - start_time) >= DURATION:
                print(f"\n⏱️  Duration limit ({DURATION}s) reached.")
                break

            for device in devices:
                if shutdown:
                    break

                reading = device.reading()
                topic = MQTT_TOPIC_TEMPLATE.format(device_id=device.device_id)
                payload = json.dumps(reading)

                result = client.publish(topic, payload, qos=1)
                if result.rc != mqtt.MQTT_ERR_SUCCESS:
                    error_count += 1

                device.drift()

            # Status update every 30 seconds
            now = time.time()
            if now - last_status >= 30:
                elapsed = int(now - start_time)
                rate = publish_count / max(elapsed, 1)
                print(
                    f"  📊 Status: {publish_count} published, "
                    f"{error_count} errors, "
                    f"{rate:.1f} msg/s, "
                    f"{elapsed}s elapsed"
                )
                last_status = now

            time.sleep(PUBLISH_INTERVAL)

    except KeyboardInterrupt:
        pass

    # Summary
    elapsed = int(time.time() - start_time)
    print(f"\n{'=' * 60}")
    print(f"  📈 Simulation Complete")
    print(f"  Total published: {publish_count}")
    print(f"  Errors:          {error_count}")
    print(f"  Duration:        {elapsed}s")
    print(f"  Avg rate:        {publish_count / max(elapsed, 1):.1f} msg/s")
    print(f"{'=' * 60}")

    client.disconnect()
    client.loop_stop()

    if error_count > 0:
        print(f"⚠️  Completed with {error_count} errors")
        sys.exit(1)
    else:
        print("✅ Simulator completed successfully")


if __name__ == "__main__":
    main()

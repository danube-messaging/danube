#!/usr/bin/env bash
# =============================================================================
# Edge Replication E2E — Local Test Runner
#
# Mirrors the edge-replication-e2e.yml CI workflow for local development.
#
# Usage:
#   ./scripts/edge-e2e-local.sh          # build + run tests
#   ./scripts/edge-e2e-local.sh --skip-build  # skip cargo build (reuse existing binary)
#   ./scripts/edge-e2e-local.sh --debug       # use debug build instead of release
#
# Prerequisites:
#   - Ports 1883, 6650-6653, 7650-7653, 50051-50054 must be free
#   - No other danube-broker instances running
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$(dirname "$SCRIPT_DIR")")"
cd "$PROJECT_DIR"

# Parse flags
SKIP_BUILD=false
BUILD_MODE="release"
BUILD_FLAG="--release"
KEEP_ALIVE=false
while [[ $# -gt 0 ]]; do
    case "$1" in
        --skip-build) SKIP_BUILD=true; shift ;;
        --debug)      BUILD_MODE="debug"; BUILD_FLAG=""; shift ;;
        --keep-alive) KEEP_ALIVE=true; shift ;;
        *)            echo "Unknown flag: $1"; exit 1 ;;
    esac
done

BIN="./target/${BUILD_MODE}/danube-broker"
ADMIN_BIN="./target/${BUILD_MODE}/danube-admin"
LOG_LEVEL="${RUST_LOG:-info,openraft::replication=off,openraft=error}"

SEED_NODES="0.0.0.0:7650,0.0.0.0:7651,0.0.0.0:7652"
CONFIG_FILE="./config/danube_broker.yml"
TEMP_DIR="./temp"
DATA_DIR="./danube-data"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

cleanup() {
    echo -e "\n${YELLOW}Cleaning up...${NC}"
    pkill -f "danube-broker" 2>/dev/null || true
    sleep 1
    echo -e "${GREEN}All brokers stopped.${NC}"
}

trap cleanup EXIT

# =============================================================================
# Step 0: Kill any existing brokers
# =============================================================================
echo -e "${YELLOW}Stopping any existing danube-broker instances...${NC}"
pkill -f "danube-broker" 2>/dev/null || true
sleep 1

# =============================================================================
# Step 1: Build
# =============================================================================
if [ "$SKIP_BUILD" = false ]; then
    echo -e "${YELLOW}Building workspace (${BUILD_MODE})...${NC}"
    cargo build --workspace ${BUILD_FLAG}
    echo -e "${GREEN}Build complete.${NC}"
else
    echo -e "${YELLOW}Skipping build (--skip-build).${NC}"
fi

if [ ! -f "$BIN" ]; then
    echo -e "${RED}Binary not found: $BIN${NC}"
    exit 1
fi

# =============================================================================
# Step 2: Clean data dirs + create temp
# =============================================================================
echo -e "${YELLOW}Cleaning data directories...${NC}"
rm -rf "${DATA_DIR}/raft-1" "${DATA_DIR}/raft-2" "${DATA_DIR}/raft-3" "${DATA_DIR}/edge-1"
mkdir -p "$TEMP_DIR" "${DATA_DIR}/raft-1" "${DATA_DIR}/raft-2" "${DATA_DIR}/raft-3" "${DATA_DIR}/edge-1"

# =============================================================================
# Step 3: Start 3-broker cluster
# =============================================================================
echo -e "${YELLOW}Starting 3-broker cluster...${NC}"

for i in 0 1 2; do
    broker_port=$((6650 + i))
    admin_port=$((50051 + i))
    raft_port=$((7650 + i))
    prom_port=$((9040 + i))
    data_dir="${DATA_DIR}/raft-$((i + 1))"
    log_file="${TEMP_DIR}/broker_${broker_port}.log"

    RUST_LOG="$LOG_LEVEL" "$BIN" \
        --config-file "$CONFIG_FILE" \
        --broker-addr "0.0.0.0:${broker_port}" \
        --admin-addr "0.0.0.0:${admin_port}" \
        --raft-addr "0.0.0.0:${raft_port}" \
        --data-dir "$data_dir" \
        --seed-nodes "$SEED_NODES" \
        --prom-exporter "0.0.0.0:${prom_port}" \
        > "$log_file" 2>&1 &

    echo "  Broker $i: client=${broker_port} admin=${admin_port} raft=${raft_port} prom=${prom_port} (log: ${log_file})"
done

# Wait for cluster brokers
echo -e "${YELLOW}Waiting for cluster brokers to be ready...${NC}"
for port in 6650 6651 6652; do
    for attempt in $(seq 1 15); do
        if nc -zv 127.0.0.1 "$port" 2>/dev/null; then
            echo -e "  ${GREEN}✅ Broker on port ${port} is ready.${NC}"
            break
        elif [ "$attempt" -eq 15 ]; then
            echo -e "  ${RED}❌ Broker on port ${port} failed to start.${NC}"
            echo "--- Log ---"
            cat "${TEMP_DIR}/broker_${port}.log" || true
            exit 1
        fi
        sleep 2
    done
done

# =============================================================================
# Step 4: Provision edge namespace
# =============================================================================
echo -e "${YELLOW}Provisioning edge1 namespace on cluster...${NC}"
DANUBE_ADMIN_ENDPOINT="http://127.0.0.1:50051" "$ADMIN_BIN" namespaces create edge1 2>/dev/null || true
echo -e "${GREEN}✅ Namespace 'edge1' provisioned.${NC}"

# =============================================================================
# Step 5: Register schema on cluster
# =============================================================================
echo -e "${YELLOW}Registering 'telemetry-events' schema on cluster...${NC}"
DANUBE_ADMIN_ENDPOINT="http://127.0.0.1:50051" "$ADMIN_BIN" schemas register \
    telemetry-events \
    --schema-type json_schema \
    --schema '{"type":"object","properties":{"temperature":{"type":"number"},"device_id":{"type":"string"}},"required":["temperature"]}' \
    --description "Edge telemetry events schema"
echo -e "${GREEN}✅ Schema 'telemetry-events' registered.${NC}"

# =============================================================================
# Step 6: Start edge broker
# =============================================================================
echo -e "${YELLOW}Starting edge broker...${NC}"

# Generate edge config for the test
cat > "${TEMP_DIR}/edge.yaml" <<EOF
edge:
  edge_name: "edge1"
  cluster_url: "http://127.0.0.1:6650"
  token: ""
  heartbeat_interval_ms: 10000

replicator:
  batch_size: 100
  batch_timeout_ms: 1000

mqtt:
  listener: "0.0.0.0:1883"
  topic_mappings:
    - mqtt_pattern: "device/+/telemetry"
      danube_topic: "/edge1/telemetry"
      schema_subject: "telemetry-events"
      validation_policy: "enforce"
      extract_attributes:
        device_id: "\$1"
    - mqtt_pattern: "#"
      danube_topic: "/edge1/raw"
  ingestion:
    batch_size: 100
    batch_timeout_ms: 500
EOF

RUST_LOG="$LOG_LEVEL" "$BIN" \
    --mode edge \
    --broker-addr 0.0.0.0:6653 \
    --admin-addr 0.0.0.0:50054 \
    --raft-addr 0.0.0.0:7653 \
    --data-dir "${DATA_DIR}/edge-1" \
    --edge-config "${TEMP_DIR}/edge.yaml" \
    > "${TEMP_DIR}/edge_broker.log" 2>&1 &

echo "  Edge broker: client=6653 admin=50054 mqtt=1883 cluster=6650 (log: ${TEMP_DIR}/edge_broker.log)"

for attempt in $(seq 1 15); do
    if nc -zv 127.0.0.1 6653 2>/dev/null; then
        echo -e "  ${GREEN}✅ Edge broker on port 6653 is ready.${NC}"
        break
    elif [ "$attempt" -eq 15 ]; then
        echo -e "  ${RED}❌ Edge broker failed to start.${NC}"
        echo "--- Edge Broker Log ---"
        cat "${TEMP_DIR}/edge_broker.log" || true
        exit 1
    fi
    sleep 2
done

# =============================================================================
# Step 7: Infrastructure ready
# =============================================================================
echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  Edge E2E infrastructure is READY ✅${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "  Cluster brokers: 127.0.0.1:{6650,6651,6652}"
echo "  Edge broker:     127.0.0.1:6653"
echo "  MQTT gateway:    127.0.0.1:1883"
echo ""
echo "Run the Python E2E tests:"
echo "  python3 scripts/edge-e2e/test_mqtt_ingestion.py"
echo ""

if [ "$KEEP_ALIVE" = true ]; then
    echo -e "${YELLOW}  Brokers are running in the background.${NC}"
    echo -e "${YELLOW}  Press [ENTER] to stop the brokers and exit...${NC}"
    read -r
fi


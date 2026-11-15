#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/common.sh"

echo "=== Verifying Task 03: Schemas and Inference ==="

versions_out="$("$DANUBE_BIN" schema versions bench-orders-value --output json)"
assert_contains "$versions_out" '1' "Schema subject bench-orders-value has v1"
assert_contains "$versions_out" '2' "Schema subject bench-orders-value evolved to v2"

schema_cfg="$("$DANUBE_BIN" topic schema show /bench/orders --output json)"
assert_contains "$schema_cfg" '"schema_subject": "bench-orders-value"' "Topic /bench/orders is bound to bench-orders-value"
assert_contains "$schema_cfg" "ENFORCE" "Topic /bench/orders enforces schema validation"

peek_orders="$("$DANUBE_BIN" topic peek /bench/orders --start-offset 0 --count 10)"
assert_contains "$peek_orders" 'ord-1' "Valid schema-validated message persisted in /bench/orders"

raw_topics="$("$DANUBE_BIN" namespace topics bench --output json)"
assert_contains "$raw_topics" '/bench/raw-events' "Topic /bench/raw-events exists for schema inference"

echo "Task 03 verification passed."

#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/common.sh"

echo "=== Verifying Task 01: Bootstrap and Cluster Ops ==="

cluster_out="$("$DANUBE_BIN" cluster status)"
assert_contains "$cluster_out" "Leader" "Cluster has an active Raft leader"

brokers_out="$("$DANUBE_BIN" broker list --output json)"
assert_contains "$brokers_out" "active" "At least one active broker is registered"

ns_out="$("$DANUBE_BIN" namespace policies bench --output json)"
assert_contains "$ns_out" "{" "Namespace 'bench' exists"

echo "Task 01 verification passed."


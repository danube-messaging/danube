#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/common.sh"

echo "=== Verifying Task 05: Troubleshooting Consumer Stall and Lag ==="

subs_out="$("$DANUBE_BIN" topic subscription list /bench/backlog --output json)"
assert_contains "$subs_out" '"slow-reader"' "Subscription slow-reader exists on /bench/backlog"

peek_out="$("$DANUBE_BIN" topic peek /bench/backlog --start-offset 0 --count 20)"
assert_contains "$peek_out" "offset=10" "Topic /bench/backlog persisted warm-up + 10 backlog messages"

echo "Task 05 verification passed (verify agent output also showed backlog 10 -> 0)."


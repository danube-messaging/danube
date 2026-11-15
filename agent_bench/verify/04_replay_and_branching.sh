#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/common.sh"

echo "=== Verifying Task 04: Replay and Branching ==="

src_peek="$("$DANUBE_BIN" topic peek /bench/events --start-offset 0 --count 10)"
assert_contains "$src_peek" "offset=4" "Source topic /bench/events holds at least 5 messages (offsets 0..4)"

topics_out="$("$DANUBE_BIN" namespace topics bench --output json)"
assert_contains "$topics_out" '/bench/events-replay' "Branched topic /bench/events-replay exists"

branch_peek="$("$DANUBE_BIN" topic peek /bench/events-replay --start-offset 0 --count 10)"
assert_contains "$branch_peek" "offset=4" "Branched topic /bench/events-replay replays at least 5 messages"

echo "Task 04 verification passed."


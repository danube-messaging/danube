#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/common.sh"

echo "=== Verifying Task 07: Agent Runtime Primitives and DLQ Replay ==="

ns_topics="$("$DANUBE_BIN" namespace topics agents --output json)"
assert_contains "$ns_topics" '/agents/tasks' "Topic /agents/tasks exists"
assert_contains "$ns_topics" '/agents/tasks-dlq' "Dead-letter topic /agents/tasks-dlq exists"
assert_contains "$ns_topics" '/agents/subtasks' "Topic /agents/subtasks exists"
assert_contains "$ns_topics" '/agents/rpc' "Topic /agents/rpc exists"
assert_contains "$ns_topics" '/agents/rpc-replies' "Topic /agents/rpc-replies exists"

tasks_desc="$("$DANUBE_BIN" topic describe /agents/tasks --output json)"
assert_contains "$tasks_desc" '"delivery": "Reliable"' "Topic /agents/tasks uses Reliable delivery"

rpc_desc="$("$DANUBE_BIN" topic describe /agents/rpc-replies --output json)"
assert_contains "$rpc_desc" '"delivery": "NonReliable"' "Topic /agents/rpc-replies uses NonReliable delivery"

dlq_peek="$("$DANUBE_BIN" topic peek /agents/tasks-dlq --start-offset 0 --count 5)"
assert_contains "$dlq_peek" "poison-task" "Poison message landed in /agents/tasks-dlq"

tasks_peek="$("$DANUBE_BIN" topic peek /agents/tasks --start-offset 0 --count 10)"
assert_contains "$tasks_peek" "poison-task" "Replayed poison message returned to /agents/tasks"

echo "Task 07 verification passed."


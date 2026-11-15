#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/common.sh"

echo "=== Verifying Task 02: Reliable Messaging and DLQ Policy ==="

desc_out="$("$DANUBE_BIN" topic describe /bench/jobs --output json)"
assert_contains "$desc_out" '"delivery": "Reliable"' "Topic /bench/jobs uses Reliable delivery"
assert_contains "$desc_out" 'task_queue' "Topic /bench/jobs records intent task_queue"

subs_out="$("$DANUBE_BIN" topic subscription list /bench/jobs --output json)"
assert_contains "$subs_out" '"workers"' "Subscription 'workers' exists on /bench/jobs"

policy_out="$("$DANUBE_BIN" topic subscription failure-policy get /bench/jobs --subscription workers --output json)"
assert_contains "$policy_out" '"poison_policy": "dead_letter"' "Failure policy sets dead_letter poison policy"
assert_contains "$policy_out" '/bench/jobs-dlq' "Failure policy targets /bench/jobs-dlq"

peek_out="$("$DANUBE_BIN" topic peek /bench/jobs --start-offset 0 --count 10)"
assert_contains "$peek_out" "job" "Stored job payloads are present in /bench/jobs WAL"

echo "Task 02 verification passed."


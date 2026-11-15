#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/common.sh"

echo "=== Verifying Task 06: Security, RBAC, and MCP Guardrails ==="

ns_out="$("$DANUBE_BIN" namespace topics secure --output json)"
assert_contains "$ns_out" '/secure/payments' "Secured topic /secure/payments exists"

writer_role="$("$DANUBE_BIN" auth role get pay-writer --output json)"
assert_contains "$writer_role" 'Produce' "Role pay-writer grants Produce"

reader_role="$("$DANUBE_BIN" auth role get pay-reader --output json)"
assert_contains "$reader_role" 'Consume' "Role pay-reader grants Consume"

bindings_out="$("$DANUBE_BIN" auth binding list --scope topic --resource /secure/payments --output json)"
assert_contains "$bindings_out" 'checkout-svc' "Topic binding exists for checkout-svc"
assert_contains "$bindings_out" 'ledger-svc' "Topic binding exists for ledger-svc"

export_out="$("$DANUBE_BIN" cluster export-config)"
assert_contains "$export_out" '<redacted>' "Redacted export hides secrets by default"

echo "Task 06 verification passed."


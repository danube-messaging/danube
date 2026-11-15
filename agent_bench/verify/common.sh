#!/usr/bin/env bash
set -euo pipefail

DANUBE_BIN="${DANUBE_BIN:-danube}"

if ! command -v "$DANUBE_BIN" >/dev/null 2>&1; then
  if [ -x "./target/release/danube" ]; then
    DANUBE_BIN="./target/release/danube"
  elif [ -x "./target/debug/danube" ]; then
    DANUBE_BIN="./target/debug/danube"
  else
    echo "FAIL: danube binary not found on PATH (or ./target/{release,debug}/danube)" >&2
    exit 1
  fi
fi

pass() {
  echo "PASS: $1"
}

fail() {
  echo "FAIL: $1" >&2
  exit 1
}

assert_contains() {
  local haystack="$1"
  local needle="$2"
  local label="$3"
  if echo "$haystack" | grep -qF "$needle"; then
    pass "$label"
  else
    echo "--- output ---" >&2
    echo "$haystack" >&2
    echo "--------------" >&2
    fail "$label (expected substring: $needle)"
  fi
}


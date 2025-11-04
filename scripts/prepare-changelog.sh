#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: $0 <next_tag> [previous_tag]" >&2
  exit 1
}

NEXT_TAG=${1-}
PREV_TAG=${2-}

[ -n "${NEXT_TAG}" ] || usage

if [ -z "${PREV_TAG}" ]; then
  PREV_TAG=$(git tag -l 'v*.*.*' --sort=-v:refname | sed -n '1p' || true)
fi

# Resolve repo root and output file path independent of current working dir
ROOT_DIR=$(git rev-parse --show-toplevel)
OUT_FILE="${ROOT_DIR}/CHANGELOG.md"

# Determine range of commits for this release
if [ -n "${PREV_TAG}" ]; then
  RANGE="${PREV_TAG}..HEAD"
else
  FIRST_COMMIT=$(git rev-list --max-parents=0 HEAD | tail -n1)
  RANGE="${FIRST_COMMIT}..HEAD"
fi

# Build new section into a temp file, then prepend to existing changelog
TMP_FILE=$(mktemp)
{
  echo "<!-- ${NEXT_TAG} START -->"
  echo "## ${NEXT_TAG} - $(date +%Y-%m-%d)"
  echo "### What's Changed"
  git log "${RANGE}" --no-merges --pretty='format:* %s by @%an in %h'
  echo
  echo "<!-- ${NEXT_TAG} END -->"
  echo
} >"${TMP_FILE}"

# Append previous changelog content if it exists, but avoid duplicate section for same NEXT_TAG
if [ -f "${OUT_FILE}" ] && [ -s "${OUT_FILE}" ]; then
  if head -n 3 "${OUT_FILE}" | grep -q "<!-- ${NEXT_TAG} START -->"; then
    echo "A section for ${NEXT_TAG} already exists at the top of ${OUT_FILE}. Aborting to avoid duplication." >&2
    rm -f "${TMP_FILE}"
    exit 1
  fi
  cat "${OUT_FILE}" >> "${TMP_FILE}"
fi

mv "${TMP_FILE}" "${OUT_FILE}"

if [ ! -s "${OUT_FILE}" ]; then
  echo "Generated changelog is empty" >&2
  exit 1
fi

echo "Wrote ${OUT_FILE}"

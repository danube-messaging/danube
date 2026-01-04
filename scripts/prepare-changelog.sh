#!/usr/bin/env bash
#
# prepare-changelog.sh - Generate changelog entries from git commits
#
# DESCRIPTION:
#   Automatically generates changelog entries by extracting commit messages
#   between git tags and prepends them to CHANGELOG.md in the repository root.
#
# USAGE:
#   ./scripts/prepare-changelog.sh <next_tag> [previous_tag]
#
# ARGUMENTS:
#   next_tag       - The upcoming release version (e.g., v0.5.3)
#   previous_tag   - (Optional) The previous release tag to compare against.
#                    If omitted, uses the most recent tag matching v*.*.* pattern.
#                    If no tags exist, uses the first commit in the repository.
#
# EXAMPLES:
#   # Generate changelog for v0.5.3 (auto-detects previous tag)
#   ./scripts/prepare-changelog.sh v0.5.3
#
#   # Generate changelog between specific tags
#   ./scripts/prepare-changelog.sh v0.5.3 v0.5.2
#
#   # First release (no previous tags)
#   ./scripts/prepare-changelog.sh v0.1.0
#
# OUTPUT:
#   Updates CHANGELOG.md in the repository root with a new section at the top
#   containing all commits since the previous tag.
#
# NOTES:
#   - Merge commits are excluded from the changelog
#   - Commit format: "* <commit message> by @<author> in <hash>"
#   - Prevents duplicate sections if the same tag already exists
#   - Script can be run from any directory within the repository
#
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
  git log "${RANGE}" --no-merges --pretty='format:* %s by @%an in %h' | sed -e 's/by @Dan Rusei/by @danrusei/g'
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

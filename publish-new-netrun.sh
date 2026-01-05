#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

# netrun is a pure Python package

# Extract current version from netrun/pyproject.toml
CURRENT_VERSION=$(grep -E '^[[:space:]]*version[[:space:]]*=' netrun/pyproject.toml | head -n 1 | sed -E 's/^[^"]*"([^"]*)".*$/\1/')

if [[ -z "${CURRENT_VERSION:-}" ]]; then
  echo "Could not determine current version from netrun/pyproject.toml"
  exit 1
fi

echo "Current netrun version: ${CURRENT_VERSION}"
read -r -p "Next version (empty to cancel): " NEXT_VERSION

if [[ -z "${NEXT_VERSION}" ]]; then
  echo "No version entered, exiting."
  exit 0
fi

VERSION="${NEXT_VERSION}"

set -x
sed -i '' -E "s/^([[:space:]]*version[[:space:]]*=[[:space:]]*\")[^\"]*(\".*)$/\1${VERSION}\2/" netrun/pyproject.toml
git add netrun/pyproject.toml
if git commit -m "chore: bump netrun to version v${VERSION}" && git push; then
  git tag "netrun-v${VERSION}"
  git push origin "netrun-v${VERSION}"
else
  echo "Commit failed, not tagging or pushing."
  exit 1
fi

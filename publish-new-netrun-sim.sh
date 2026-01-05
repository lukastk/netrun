#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

# netrun-sim has both a Rust crate and Python bindings that share the same version

# Extract current version from netrun-sim/python/pyproject.toml
CURRENT_PY_VERSION=$(grep -E '^[[:space:]]*version[[:space:]]*=' netrun-sim/python/pyproject.toml | head -n 1 | sed -E 's/^[^"]*"([^"]*)".*$/\1/')

# Extract current version from netrun-sim/core/Cargo.toml
CURRENT_CARGO_VERSION=$(grep -E '^[[:space:]]*version[[:space:]]*=' netrun-sim/core/Cargo.toml | head -n 1 | sed -E 's/^[^"]*"([^"]*)".*$/\1/')

if [[ -z "${CURRENT_PY_VERSION:-}" || -z "${CURRENT_CARGO_VERSION:-}" ]]; then
  echo "Could not determine current version from pyproject.toml or Cargo.toml"
  exit 1
fi

if [[ "${CURRENT_PY_VERSION}" != "${CURRENT_CARGO_VERSION}" ]]; then
  echo "Version mismatch between pyproject.toml (${CURRENT_PY_VERSION}) and Cargo.toml (${CURRENT_CARGO_VERSION})."
  exit 1
fi

CURRENT_VERSION="${CURRENT_PY_VERSION}"

echo "Current netrun-sim version: ${CURRENT_VERSION}"
read -r -p "Next version (empty to cancel): " NEXT_VERSION

if [[ -z "${NEXT_VERSION}" ]]; then
  echo "No version entered, exiting."
  exit 0
fi

VERSION="${NEXT_VERSION}"

set -x
sed -i '' -E "s/^([[:space:]]*version[[:space:]]*=[[:space:]]*\")[^\"]*(\".*)$/\1${VERSION}\2/" netrun-sim/python/pyproject.toml
sed -i '' -E "s/^([[:space:]]*version[[:space:]]*=[[:space:]]*\")[^\"]*(\".*)$/\1${VERSION}\2/" netrun-sim/core/Cargo.toml
git add netrun-sim/python/pyproject.toml netrun-sim/core/Cargo.toml
if git commit -m "chore: bump netrun-sim to version v${VERSION}" && git push; then
  git tag "netrun-sim-v${VERSION}"
  git push origin "netrun-sim-v${VERSION}"
else
  echo "Commit failed, not tagging or pushing."
  exit 1
fi

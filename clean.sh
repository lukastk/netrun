#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

echo "=== Formatting and linting all projects ==="

echo ""
echo "--- netrun-sim Rust: cargo fmt ---"
cargo fmt --manifest-path netrun-sim/Cargo.toml --all

echo ""
echo "--- netrun-sim Rust: cargo clippy --fix ---"
cargo clippy --manifest-path netrun-sim/Cargo.toml --workspace --all-features --fix --allow-dirty --allow-staged

echo ""
echo "--- netrun: ruff format ---"
uv run --directory netrun ruff format src

echo ""
echo "--- netrun: ruff check --fix ---"
uv run --directory netrun ruff check src --fix || true

echo ""
echo "=== Done ==="

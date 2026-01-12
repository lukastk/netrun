#!/bin/bash
# Run the basic setup example

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Run with the project's virtual environment
"$PROJECT_DIR/.venv/bin/python" "$SCRIPT_DIR/example.py"

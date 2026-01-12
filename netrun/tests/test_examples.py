"""Tests that verify all examples run successfully."""

import subprocess
import sys
from pathlib import Path

import pytest


EXAMPLES_DIR = Path(__file__).parent.parent / "src" / "examples"


def get_example_files():
    """Get all example Python files in src/examples/."""
    examples = []
    if EXAMPLES_DIR.exists():
        for f in sorted(EXAMPLES_DIR.glob("*.py")):
            # Skip __init__.py and other internal files
            if not f.name.startswith("_"):
                examples.append(f)
    return examples


@pytest.mark.parametrize("example_file", get_example_files(), ids=lambda f: f.stem)
def test_example_runs(example_file):
    """Test that each example runs without errors."""
    result = subprocess.run(
        [sys.executable, str(example_file)],
        capture_output=True,
        text=True,
        timeout=60,
        cwd=example_file.parent.parent.parent,  # Run from netrun/ directory
    )

    # Print output for debugging if test fails
    if result.returncode != 0:
        print(f"STDOUT:\n{result.stdout}")
        print(f"STDERR:\n{result.stderr}")

    assert result.returncode == 0, f"Example {example_file.name} failed with return code {result.returncode}"

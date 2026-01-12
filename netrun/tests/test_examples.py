"""Tests that verify all examples run successfully."""

import subprocess
import sys
from pathlib import Path

import pytest


EXAMPLES_DIR = Path(__file__).parent.parent / "examples"


def get_example_dirs():
    """Get all example directories that have a run_example.sh script."""
    examples = []
    if EXAMPLES_DIR.exists():
        for d in sorted(EXAMPLES_DIR.iterdir()):
            if d.is_dir() and (d / "run_example.sh").exists():
                examples.append(d)
    return examples


@pytest.mark.parametrize("example_dir", get_example_dirs(), ids=lambda d: d.name)
def test_example_runs(example_dir):
    """Test that each example runs without errors."""
    run_script = example_dir / "run_example.sh"

    result = subprocess.run(
        ["bash", str(run_script)],
        capture_output=True,
        text=True,
        timeout=60,
    )

    # Print output for debugging if test fails
    if result.returncode != 0:
        print(f"STDOUT:\n{result.stdout}")
        print(f"STDERR:\n{result.stderr}")

    assert result.returncode == 0, f"Example {example_dir.name} failed with return code {result.returncode}"

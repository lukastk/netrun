#!/usr/bin/env python3
"""
Launch netrun-ui with automatic Python environment detection.

This script finds the right Python environment and launches netrun-ui.
It's designed to be run from any directory.

Python resolution order:
1. --python argument if provided
2. .venv/bin/python in current directory
3. venv/bin/python in current directory
4. The Python running this script

Usage:
    python launch.py                    # Auto-detect Python, open in browser
    python launch.py --app              # Auto-detect Python, open native window
    python launch.py --python /path/to/python  # Use specific Python
    python launch.py --working-dir /path/to/project  # Set working directory
"""

import argparse
import os
import subprocess
import sys
from pathlib import Path


def find_python(working_dir: Path) -> str:
    """
    Find the best Python executable to use.

    Resolution order:
    1. .venv in working directory
    2. venv in working directory
    3. Current Python interpreter
    """
    # Check for .venv
    venv_python = working_dir / ".venv" / ("Scripts" if sys.platform == "win32" else "bin") / "python"
    if venv_python.exists():
        return str(venv_python)

    # Check for venv (without dot)
    venv_python = working_dir / "venv" / ("Scripts" if sys.platform == "win32" else "bin") / "python"
    if venv_python.exists():
        return str(venv_python)

    # Fall back to current Python
    return sys.executable


def get_netrun_ui_dir() -> Path:
    """Get the netrun-ui directory (where this script lives)."""
    return Path(__file__).parent.resolve()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Launch netrun-ui with automatic Python environment detection"
    )
    parser.add_argument(
        "--app",
        action="store_true",
        help="Open in native window instead of browser",
    )
    parser.add_argument(
        "--python",
        help="Path to Python executable to use",
    )
    parser.add_argument(
        "--working-dir", "-C",
        help="Working directory (for .venv detection and file explorer)",
    )
    parser.add_argument(
        "--no-frontend",
        action="store_true",
        help="Don't start the frontend dev server (assume it's running)",
    )

    args = parser.parse_args()

    # Determine working directory
    working_dir = Path(args.working_dir).resolve() if args.working_dir else Path.cwd()

    # Find Python to use
    if args.python:
        python_path = args.python
        if not Path(python_path).exists():
            print(f"Error: Python not found at {python_path}", file=sys.stderr)
            sys.exit(1)
    else:
        python_path = find_python(working_dir)

    print(f"Using Python: {python_path}")
    print(f"Working directory: {working_dir}")

    netrun_ui_dir = get_netrun_ui_dir()
    backend_dir = netrun_ui_dir / "backend"

    # Build the start.sh arguments
    start_script = netrun_ui_dir / "start.sh"

    if not start_script.exists():
        print(f"Error: start.sh not found at {start_script}", file=sys.stderr)
        sys.exit(1)

    # Set environment variables
    env = os.environ.copy()
    env["VITE_INITIAL_PATH"] = str(working_dir)

    # Build command
    cmd = [str(start_script)]
    if args.app:
        cmd.append("--app")

    print(f"Running: {' '.join(cmd)}")
    print()

    # Run start.sh
    try:
        subprocess.run(cmd, env=env, cwd=str(netrun_ui_dir), check=True)
    except KeyboardInterrupt:
        print("\nStopped.")
    except subprocess.CalledProcessError as e:
        sys.exit(e.returncode)


if __name__ == "__main__":
    main()

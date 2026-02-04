#!/usr/bin/env python3
"""
Build script for netrun-ui.

This script:
1. Builds the Svelte frontend with npm
2. Copies the built files to netrun_ui_backend/static/
3. The package can then be built with `pip install .` or `python -m build`

Usage:
    python build.py           # Build frontend and copy to package
    python build.py --clean   # Clean build artifacts
"""

import argparse
import shutil
import subprocess
import sys
from pathlib import Path


def get_project_root() -> Path:
    """Get the project root directory."""
    return Path(__file__).parent.resolve()


def clean(project_root: Path) -> None:
    """Clean build artifacts."""
    dirs_to_remove = [
        project_root / "build",
        project_root / "netrun_ui_backend" / "static",
        project_root / "dist",
        project_root / ".svelte-kit",
    ]

    for d in dirs_to_remove:
        if d.exists():
            print(f"Removing {d}")
            shutil.rmtree(d)

    print("Clean complete.")


def build_frontend(project_root: Path) -> bool:
    """Build the frontend with npm."""
    print("Building frontend...")

    # Check if node_modules exists
    if not (project_root / "node_modules").exists():
        print("Installing npm dependencies...")
        result = subprocess.run(
            ["npm", "install"],
            cwd=str(project_root),
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            print(f"Error installing dependencies: {result.stderr}", file=sys.stderr)
            return False

    # Build the frontend
    result = subprocess.run(
        ["npm", "run", "build"],
        cwd=str(project_root),
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        print(f"Error building frontend: {result.stderr}", file=sys.stderr)
        print(result.stdout)
        return False

    print("Frontend built successfully.")
    return True


def copy_to_package(project_root: Path) -> bool:
    """Copy built frontend to netrun_ui_backend/static/."""
    build_dir = project_root / "build"
    static_dir = project_root / "netrun_ui_backend" / "static"

    if not build_dir.exists():
        print(f"Error: Build directory not found: {build_dir}", file=sys.stderr)
        return False

    # Remove existing static dir
    if static_dir.exists():
        shutil.rmtree(static_dir)

    # Copy build to static
    print(f"Copying {build_dir} to {static_dir}")
    shutil.copytree(build_dir, static_dir)

    print("Frontend copied to package.")
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="Build netrun-ui for distribution")
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Clean build artifacts instead of building",
    )

    args = parser.parse_args()

    project_root = get_project_root()

    if args.clean:
        clean(project_root)
        return

    # Build frontend
    if not build_frontend(project_root):
        sys.exit(1)

    # Copy to package
    if not copy_to_package(project_root):
        sys.exit(1)

    print()
    print("Build complete!")
    print()
    print("To install locally:")
    print("  pip install -e .")
    print()
    print("To build a distributable package:")
    print("  python -m build")


if __name__ == "__main__":
    main()

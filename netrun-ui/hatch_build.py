"""Custom build hook for hatchling to build the frontend before packaging."""

import shutil
import subprocess
import sys
from pathlib import Path

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class FrontendBuildHook(BuildHookInterface):
    """Build the Svelte frontend before creating the Python package."""

    PLUGIN_NAME = "frontend-build"

    def initialize(self, version: str, build_data: dict) -> None:
        """Build the frontend before packaging."""
        root = Path(self.root)
        static_dir = root / "netrun_ui_backend" / "static"

        # Check if we have frontend source
        if not (root / "package.json").exists():
            # No source - this will fail at runtime
            # but we can't build without source
            return

        print("Building frontend...")

        # Install npm dependencies if needed
        if not (root / "node_modules").exists():
            print("Installing npm dependencies...")
            result = subprocess.run(
                ["npm", "install"],
                cwd=str(root),
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                print(f"Warning: npm install failed: {result.stderr}", file=sys.stderr)
                return

        # Build the frontend
        result = subprocess.run(
            ["npm", "run", "build"],
            cwd=str(root),
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            print(f"Warning: Frontend build failed: {result.stderr}", file=sys.stderr)
            print(result.stdout)
            return

        # Copy build to static directory
        build_dir = root / "build"
        if build_dir.exists():
            if static_dir.exists():
                shutil.rmtree(static_dir)
            shutil.copytree(build_dir, static_dir)
            print("Frontend built successfully.")
        else:
            print("Warning: Build directory not found after npm build", file=sys.stderr)

        # Bundle vis app assets for export-html
        self._bundle_vis_assets(root)

    def _bundle_vis_assets(self, root: Path) -> None:
        """Copy netrun-ui-vis app assets into the package for export-html."""
        vis_assets_dir = root / "netrun_ui_backend" / "vis_assets"

        # Try node_modules symlink first (normal dev layout)
        src = root / "node_modules" / "netrun-ui-vis" / "dist" / "app" / "assets"
        if not src.is_dir():
            # Try sibling directory
            src = root.parent / "netrun-ui-vis" / "dist" / "app" / "assets"
        if not src.is_dir():
            print("Warning: vis app assets not found, export-html will not work in installed packages", file=sys.stderr)
            return

        if vis_assets_dir.exists():
            shutil.rmtree(vis_assets_dir)
        shutil.copytree(src, vis_assets_dir)
        print("Vis app assets bundled for export-html.")

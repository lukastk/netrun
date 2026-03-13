#!/usr/bin/env python3
"""
netrun-ui CLI

Usage:
    netrun-ui                        # Open native window (runs in background)
    netrun-ui --fg                   # Open native window (foreground, blocks)
    netrun-ui --server               # Start server mode (production)
    netrun-ui --dev                  # Development mode with Vite
    netrun-ui --port 8080            # Custom backend port
    netrun-ui -C /path/to/project    # Set working directory
    netrun-ui export-html in.netrun.json -o out.html  # Export static HTML
"""

import argparse
import os
import signal
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Optional

import uvicorn
from importlib.metadata import version as pkg_version

APP_TITLE = f"netrun-ui v{pkg_version('netrun-ui')}"


def find_free_port(start: int = 8000, end: int = 8099) -> int:
    """Find a free port in the given range."""
    for port in range(start, end + 1):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(("127.0.0.1", port))
                return port
            except OSError:
                continue
    raise RuntimeError(f"No free port found in range {start}-{end}")


def get_package_dir() -> Path:
    """Get the directory containing this package."""
    return Path(__file__).parent.resolve()


def get_frontend_dir() -> Path:
    """Get the frontend directory (for development mode)."""
    # The frontend is one level up from netrun_ui_backend/
    return get_package_dir().parent


def has_static_files() -> bool:
    """Check if built static files exist."""
    static_dir = get_package_dir() / "static"
    return static_dir.exists() and (static_dir / "index.html").exists()


def _is_source_tree() -> bool:
    """Check if we're running from a source tree (has package.json)."""
    return (get_frontend_dir() / "package.json").exists()


def _build_frontend() -> bool:
    """Build the frontend from source. Returns True on success."""
    frontend_dir = get_frontend_dir()
    print("Building frontend from source...", file=sys.stderr)

    # Install npm dependencies for netrun-ui-vis (local file dep) if needed
    vis_dir = frontend_dir.parent / "netrun-ui-vis"
    if vis_dir.exists() and not (vis_dir / "node_modules").exists():
        print("Installing netrun-ui-vis dependencies...", file=sys.stderr)
        result = subprocess.run(
            ["npm", "install"], cwd=str(vis_dir),
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            print(f"netrun-ui-vis npm install failed: {result.stderr}", file=sys.stderr)
            return False

    # Install npm dependencies if needed
    if not (frontend_dir / "node_modules").exists():
        print("Installing npm dependencies...", file=sys.stderr)
        result = subprocess.run(
            ["npm", "install"], cwd=str(frontend_dir),
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            print(f"npm install failed: {result.stderr}", file=sys.stderr)
            return False

    # Build
    result = subprocess.run(
        ["npm", "run", "build"], cwd=str(frontend_dir),
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        print(f"Frontend build failed: {result.stderr}", file=sys.stderr)
        return False

    # Copy build output to static/
    import shutil
    build_dir = frontend_dir / "build"
    static_dir = get_package_dir() / "static"
    if build_dir.exists():
        if static_dir.exists():
            shutil.rmtree(static_dir)
        shutil.copytree(build_dir, static_dir)
        print("Frontend built successfully.", file=sys.stderr)
        return True

    print("Build directory not found after npm build.", file=sys.stderr)
    return False


def ensure_static_files() -> None:
    """Ensure static files exist, auto-building from source if needed."""
    if has_static_files():
        return
    if _is_source_tree():
        if not _build_frontend():
            sys.exit(1)
    else:
        print("Error: No built frontend found.", file=sys.stderr)
        print("Either:", file=sys.stderr)
        print("  1. Run 'netrun-ui --dev' for development mode", file=sys.stderr)
        print("  2. Build the frontend first with the build script", file=sys.stderr)
        sys.exit(1)


def has_frontend_source() -> bool:
    """Check if frontend source exists (for development)."""
    frontend_dir = get_frontend_dir()
    return (frontend_dir / "package.json").exists()


def start_backend_server(host: str = "127.0.0.1", port: int = 8000, log_level: str = "info") -> None:
    """Start the FastAPI backend server."""
    uvicorn.run(
        "netrun_ui_backend.main:app",
        host=host,
        port=port,
        log_level=log_level,
    )


def wait_for_server(url: str, timeout: float = 30.0) -> bool:
    """Wait for a server to become available."""
    import urllib.request
    import urllib.error

    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            urllib.request.urlopen(url, timeout=1)
            return True
        except (urllib.error.URLError, ConnectionRefusedError):
            time.sleep(0.2)
    return False


def start_frontend_dev_server(
    frontend_dir: Path,
    port: int = 5173,
    initial_path: Optional[str] = None,
) -> subprocess.Popen:
    """Start the Vite frontend dev server (development mode only)."""
    env = os.environ.copy()
    if initial_path:
        env["VITE_INITIAL_PATH"] = initial_path

    # Pass port to Vite via -- separator
    cmd = ["npm", "run", "dev", "--", "--port", str(port)]

    try:
        process = subprocess.Popen(
            cmd,
            cwd=str(frontend_dir),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        return process
    except FileNotFoundError:
        print("Error: npm not found. Please install Node.js.", file=sys.stderr)
        sys.exit(1)


def run_production_server(
    port: int = 8000,
    initial_path: Optional[str] = None,
) -> None:
    """Run in production server mode: backend serves static files."""
    ensure_static_files()

    print("Starting netrun-ui server...")
    print()
    print(f"  URL: http://127.0.0.1:{port}")
    print(f"  Working dir: {initial_path or os.getcwd()}")
    print()
    print("Press Ctrl+C to stop.")

    # Set working directory for file explorer
    if initial_path:
        os.environ["NETRUN_UI_WORKING_DIR"] = initial_path

    # Start backend (blocks)
    start_backend_server(port=port, log_level="info")


def run_production_app(
    port: int = 8000,
    initial_path: Optional[str] = None,
    width: int = 1400,
    height: int = 900,
) -> None:
    """Run in production app mode: native window with built frontend."""
    import webview

    ensure_static_files()

    print("Starting netrun-ui...")

    # Set working directory for file explorer
    if initial_path:
        os.environ["NETRUN_UI_WORKING_DIR"] = initial_path

    # Start backend in background thread
    backend_thread = threading.Thread(
        target=start_backend_server,
        kwargs={"port": port, "log_level": "warning"},
        daemon=True,
    )
    backend_thread.start()

    # Wait for backend
    backend_url = f"http://127.0.0.1:{port}"
    if not wait_for_server(f"{backend_url}/health", timeout=10):
        print("Error: Backend failed to start", file=sys.stderr)
        sys.exit(1)

    # Open native window pointing to backend (which serves static files)
    # Close guard is handled by the frontend via beforeunload event
    print("Opening window...")
    window = webview.create_window(
        APP_TITLE,
        backend_url,
        width=width,
        height=height,
        min_size=(800, 600),
    )

    webview.start()
    print("Shutting down...")


def run_dev_server(
    backend_port: int = 8000,
    frontend_port: int = 5173,
    initial_path: Optional[str] = None,
) -> None:
    """Run in development server mode: Vite + backend."""
    if not has_frontend_source():
        print("Error: Frontend source not found.", file=sys.stderr)
        print("Development mode requires the frontend source code.", file=sys.stderr)
        sys.exit(1)

    frontend_dir = get_frontend_dir()

    # Set working directory for file explorer
    if initial_path:
        os.environ["NETRUN_UI_WORKING_DIR"] = initial_path

    print("Starting netrun-ui in development mode...")
    print()

    # Start backend in background thread
    print(f"Starting backend on http://127.0.0.1:{backend_port}...")
    backend_thread = threading.Thread(
        target=start_backend_server,
        kwargs={"port": backend_port, "log_level": "info"},
        daemon=True,
    )
    backend_thread.start()

    # Wait for backend
    if not wait_for_server(f"http://127.0.0.1:{backend_port}/health", timeout=10):
        print("Warning: Backend may not be ready", file=sys.stderr)

    # Start frontend
    print(f"Starting frontend on http://localhost:{frontend_port}...")
    frontend_process = start_frontend_dev_server(
        frontend_dir,
        port=frontend_port,
        initial_path=initial_path or os.getcwd(),
    )

    print()
    print("Development servers started!")
    print(f"  Backend:  http://127.0.0.1:{backend_port}")
    print(f"  Frontend: http://localhost:{frontend_port}")
    print()
    print("Press Ctrl+C to stop.")

    # Handle shutdown
    def shutdown(signum, frame):
        print("\nShutting down...")
        frontend_process.terminate()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # Wait for frontend process
    try:
        frontend_process.wait()
    except KeyboardInterrupt:
        frontend_process.terminate()


def run_dev_app(
    backend_port: int = 8000,
    frontend_port: int = 5173,
    initial_path: Optional[str] = None,
    width: int = 1400,
    height: int = 900,
) -> None:
    """Run in development app mode: native window with Vite."""
    import webview

    if not has_frontend_source():
        print("Error: Frontend source not found.", file=sys.stderr)
        print("Development mode requires the frontend source code.", file=sys.stderr)
        sys.exit(1)

    frontend_dir = get_frontend_dir()

    # Set working directory for file explorer
    if initial_path:
        os.environ["NETRUN_UI_WORKING_DIR"] = initial_path

    print("Starting netrun-ui in development mode...")

    # Start frontend dev server in background
    print("Starting frontend server...")
    frontend_process = start_frontend_dev_server(
        frontend_dir,
        port=frontend_port,
        initial_path=initial_path or os.getcwd(),
    )

    # Wait for frontend to be ready
    frontend_url = f"http://localhost:{frontend_port}"
    if not wait_for_server(frontend_url, timeout=30):
        print("Error: Frontend server failed to start", file=sys.stderr)
        frontend_process.terminate()
        sys.exit(1)

    # Start backend in background thread
    print("Starting backend server...")
    backend_thread = threading.Thread(
        target=start_backend_server,
        kwargs={"port": backend_port, "log_level": "warning"},
        daemon=True,
    )
    backend_thread.start()

    # Wait for backend
    if not wait_for_server(f"http://127.0.0.1:{backend_port}/health", timeout=10):
        print("Warning: Backend may not be ready", file=sys.stderr)

    # Open native window
    # Close guard is handled by the frontend via beforeunload event
    print("Opening window...")
    window = webview.create_window(
        f"{APP_TITLE} (dev)",
        frontend_url,
        width=width,
        height=height,
        min_size=(800, 600),
    )

    # This blocks until window is closed
    webview.start()

    # Cleanup
    print("Shutting down...")
    frontend_process.terminate()


def run_export_html(argv: list[str]) -> None:
    """Handle the ``export-html`` subcommand."""
    parser = argparse.ArgumentParser(
        prog="netrun-ui export-html",
        description="Export a .netrun.json config as a standalone HTML visualization",
    )
    parser.add_argument("input", help="Input .netrun.json file")
    parser.add_argument(
        "-o", "--output",
        default=None,
        help="Output HTML file (default: <input>.html)",
    )
    parser.add_argument(
        "--minimap",
        action="store_true",
        help="Show the minimap overlay in the exported HTML",
    )
    parser.add_argument(
        "--expand-descriptions",
        action="store_true",
        help="Keep node descriptions expanded (default: collapsed)",
    )
    parser.add_argument(
        "--vis-assets-dir",
        default=None,
        help="Override path to built vis app assets directory",
    )
    args = parser.parse_args(argv)

    from .export_html import export_html_from_file, find_vis_assets_dir

    input_path = Path(args.input).resolve()
    if not input_path.exists():
        print(f"Error: File not found: {input_path}", file=sys.stderr)
        sys.exit(1)

    if args.output:
        output_path = Path(args.output).resolve()
    else:
        # foo.netrun.json -> foo.netrun.html
        output_path = input_path.with_suffix(".html")

    vis_assets_dir = Path(args.vis_assets_dir).resolve() if args.vis_assets_dir else None

    try:
        export_html_from_file(input_path, output_path, vis_assets_dir, minimap=args.minimap, expand_descriptions=args.expand_descriptions)
        print(f"Wrote {output_path}")
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def main() -> None:
    # Intercept subcommands before argparse
    if len(sys.argv) > 1 and sys.argv[1] == "export-html":
        run_export_html(sys.argv[2:])
        return

    parser = argparse.ArgumentParser(
        description="netrun-ui - Visual editor for netrun flow configurations",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  netrun-ui                        Open native window (runs in background)
  netrun-ui myfile.netrun.json     Open a specific file
  netrun-ui --fg                   Open native window (blocks until closed)
  netrun-ui --server               Start server for browser access
  netrun-ui --dev                  Development mode (requires Node.js)
  netrun-ui --dev --server         Development server mode
  netrun-ui -C /path/to/project    Set working directory
  netrun-ui --port 8080            Use custom port

Subcommands:
  netrun-ui export-html <file> [-o output.html]  Export static HTML visualization
        """,
    )

    parser.add_argument(
        "--server", "-s",
        action="store_true",
        help="Run in server mode (browser) instead of native window",
    )
    parser.add_argument(
        "--fg", "--foreground",
        action="store_true",
        dest="foreground",
        help="Run in foreground (blocks until window closes)",
    )
    parser.add_argument(
        "--dev", "-d",
        action="store_true",
        help="Development mode: use Vite dev server (requires Node.js)",
    )
    parser.add_argument(
        "--port", "-p",
        type=int,
        default=None,
        help="Backend server port (default: auto-select from 8000-8099)",
    )
    parser.add_argument(
        "--frontend-port",
        type=int,
        default=5173,
        help="Frontend dev server port, only used with --dev (default: 5173)",
    )
    parser.add_argument(
        "--working-dir", "-C",
        help="Working directory for file explorer (default: current directory)",
    )
    parser.add_argument(
        "--width",
        type=int,
        default=1400,
        help="Window width in app mode (default: 1400)",
    )
    parser.add_argument(
        "--height",
        type=int,
        default=900,
        help="Window height in app mode (default: 900)",
    )

    parser.add_argument(
        "file",
        nargs="?",
        default=None,
        help="Netrun file to open (.netrun.json or .netrun.toml)",
    )

    args = parser.parse_args()

    # Resolve port: auto-find a free one if not explicitly set
    if args.port is None:
        try:
            args.port = find_free_port()
        except RuntimeError as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)

    # Resolve file argument
    initial_file = None
    if args.file:
        file_path = Path(args.file).resolve()
        if not file_path.exists():
            print(f"Error: File not found: {file_path}", file=sys.stderr)
            sys.exit(1)
        initial_file = str(file_path)
        # If no explicit working dir, use file's parent
        if not args.working_dir:
            args.working_dir = str(file_path.parent)

    initial_path = args.working_dir or os.getcwd()

    # Set initial file env var before any run functions
    if initial_file:
        os.environ["NETRUN_UI_INITIAL_FILE"] = initial_file

    # For app modes (not --server), run in background by default
    if not args.server and not args.foreground:
        # Re-exec with --fg in background
        cmd = [sys.executable, "-m", "netrun_ui_backend.cli", "--fg"]
        if args.dev:
            cmd.append("--dev")
        cmd.extend(["--port", str(args.port)])
        cmd.extend(["--frontend-port", str(args.frontend_port)])
        cmd.extend(["-C", initial_path])
        cmd.extend(["--width", str(args.width)])
        cmd.extend(["--height", str(args.height)])
        if initial_file:
            cmd.append(initial_file)

        # Start detached process
        if sys.platform == "win32":
            # Windows: use CREATE_NEW_PROCESS_GROUP
            subprocess.Popen(
                cmd,
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        else:
            # Unix: fork and exec
            subprocess.Popen(
                cmd,
                start_new_session=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        print("netrun-ui started in background.")
        return

    if args.dev:
        # Development mode
        if args.server:
            run_dev_server(
                backend_port=args.port,
                frontend_port=args.frontend_port,
                initial_path=initial_path,
            )
        else:
            run_dev_app(
                backend_port=args.port,
                frontend_port=args.frontend_port,
                initial_path=initial_path,
                width=args.width,
                height=args.height,
            )
    else:
        # Production mode
        if args.server:
            run_production_server(
                port=args.port,
                initial_path=initial_path,
            )
        else:
            run_production_app(
                port=args.port,
                initial_path=initial_path,
                width=args.width,
                height=args.height,
            )


if __name__ == "__main__":
    main()

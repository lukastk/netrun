#!/usr/bin/env python3
"""
netrun-ui CLI

Usage:
    netrun-ui                        # Open native window (default)
    netrun-ui --server               # Start server mode (browser)
    netrun-ui --port 8080            # Custom backend port
    netrun-ui --frontend-port 3000   # Custom frontend port
    netrun-ui -C /path/to/project    # Set working directory
"""

import argparse
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Optional

import uvicorn


def get_package_dir() -> Path:
    """Get the directory containing this package."""
    return Path(__file__).parent.parent.resolve()


def get_frontend_dir() -> Path:
    """Get the frontend directory."""
    # The frontend is one level up from backend
    return get_package_dir().parent


def find_venv_python(working_dir: Path) -> Optional[str]:
    """Find Python in a virtual environment in the working directory."""
    for venv_name in [".venv", "venv"]:
        if sys.platform == "win32":
            python_path = working_dir / venv_name / "Scripts" / "python.exe"
        else:
            python_path = working_dir / venv_name / "bin" / "python"

        if python_path.exists():
            return str(python_path)
    return None


def start_backend_server(host: str = "127.0.0.1", port: int = 8000, log_level: str = "info") -> None:
    """Start the FastAPI backend server."""
    uvicorn.run(
        "app.main:app",
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
    """Start the Vite frontend dev server."""
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


def run_server_mode(
    backend_port: int = 8000,
    frontend_port: int = 5173,
    initial_path: Optional[str] = None,
) -> None:
    """Run in server mode: backend + frontend for browser access."""
    frontend_dir = get_frontend_dir()
    backend_dir = get_package_dir()

    print("Starting netrun-ui in server mode...")
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
    print("Services started!")
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


def run_app_mode(
    backend_port: int = 8000,
    frontend_port: int = 5173,
    initial_path: Optional[str] = None,
    width: int = 1400,
    height: int = 900,
) -> None:
    """Run in app mode: native window with pywebview."""
    import webview

    frontend_dir = get_frontend_dir()

    print("Starting netrun-ui...")

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
    print("Opening window...")
    window = webview.create_window(
        "netrun-ui",
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


def main() -> None:
    parser = argparse.ArgumentParser(
        description="netrun-ui - Visual editor for netrun flow configurations",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  netrun-ui                        Open native window (cwd as working dir)
  netrun-ui --server               Start server for browser access
  netrun-ui -C /path/to/project    Set working directory
  netrun-ui --port 8080            Use custom backend port
  netrun-ui --frontend-port 3000   Use custom frontend port
        """,
    )

    parser.add_argument(
        "--server", "-s",
        action="store_true",
        help="Run in server mode (browser) instead of native window",
    )
    parser.add_argument(
        "--port", "-p",
        type=int,
        default=8000,
        help="Backend server port (default: 8000)",
    )
    parser.add_argument(
        "--frontend-port",
        type=int,
        default=5173,
        help="Frontend dev server port (default: 5173)",
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

    args = parser.parse_args()

    initial_path = args.working_dir or os.getcwd()

    if args.server:
        run_server_mode(
            backend_port=args.port,
            frontend_port=args.frontend_port,
            initial_path=initial_path,
        )
    else:
        run_app_mode(
            backend_port=args.port,
            frontend_port=args.frontend_port,
            initial_path=initial_path,
            width=args.width,
            height=args.height,
        )


if __name__ == "__main__":
    main()

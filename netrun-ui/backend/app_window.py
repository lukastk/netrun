#!/usr/bin/env python3
"""
Launch netrun-ui as a native desktop window using pywebview.

This script:
1. Starts the FastAPI backend server in a background thread
2. Opens a native window pointing to the frontend

Usage:
    python app_window.py [--frontend-url URL] [--port PORT]

If --frontend-url is not specified, it defaults to http://localhost:5173
(assuming the Vite dev server is running separately).
"""

import argparse
import sys
import threading
import time
from typing import Optional

import uvicorn


def start_backend_server(host: str = "127.0.0.1", port: int = 8000) -> None:
    """Start the FastAPI backend server."""
    uvicorn.run(
        "app.main:app",
        host=host,
        port=port,
        log_level="warning",  # Reduce noise in app mode
    )


def wait_for_server(url: str, timeout: float = 10.0) -> bool:
    """Wait for a server to become available."""
    import urllib.request
    import urllib.error

    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            urllib.request.urlopen(url, timeout=1)
            return True
        except (urllib.error.URLError, ConnectionRefusedError):
            time.sleep(0.1)
    return False


def open_window(
    frontend_url: str,
    title: str = "netrun-ui",
    width: int = 1400,
    height: int = 900,
) -> None:
    """Open a native window with pywebview."""
    try:
        import webview
    except ImportError:
        print("Error: pywebview is not installed.", file=sys.stderr)
        print("Install it with: pip install pywebview", file=sys.stderr)
        print("Or install with app extras: pip install -e '.[app]'", file=sys.stderr)
        sys.exit(1)

    # Create and start the window
    window = webview.create_window(
        title,
        frontend_url,
        width=width,
        height=height,
        min_size=(800, 600),
    )

    # Start the webview (this blocks until window is closed)
    webview.start()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Launch netrun-ui as a native desktop window"
    )
    parser.add_argument(
        "--frontend-url",
        default="http://localhost:5173",
        help="URL of the frontend (default: http://localhost:5173)",
    )
    parser.add_argument(
        "--backend-port",
        type=int,
        default=8000,
        help="Port for the backend server (default: 8000)",
    )
    parser.add_argument(
        "--no-backend",
        action="store_true",
        help="Don't start the backend (assume it's already running)",
    )
    parser.add_argument(
        "--width",
        type=int,
        default=1400,
        help="Window width (default: 1400)",
    )
    parser.add_argument(
        "--height",
        type=int,
        default=900,
        help="Window height (default: 900)",
    )

    args = parser.parse_args()

    # Start backend server in background thread (unless --no-backend)
    if not args.no_backend:
        print(f"Starting backend server on port {args.backend_port}...")
        backend_thread = threading.Thread(
            target=start_backend_server,
            kwargs={"port": args.backend_port},
            daemon=True,  # Thread will be killed when main process exits
        )
        backend_thread.start()

        # Wait for backend to be ready
        backend_url = f"http://127.0.0.1:{args.backend_port}/health"
        if not wait_for_server(backend_url, timeout=10.0):
            print("Warning: Backend server may not be ready", file=sys.stderr)

    print(f"Opening window to {args.frontend_url}...")
    open_window(
        args.frontend_url,
        width=args.width,
        height=args.height,
    )


if __name__ == "__main__":
    main()

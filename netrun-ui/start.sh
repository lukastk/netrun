#!/bin/bash
# Start netrun-ui backend and frontend
#
# Usage:
#   ./start.sh           # Normal mode: backend + frontend in browser
#   ./start.sh --app     # App mode: backend + frontend in native window

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TESTING_DIR="$SCRIPT_DIR/testing"
APP_MODE=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --app)
            APP_MODE=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--app]"
            exit 1
            ;;
    esac
done

echo "Starting netrun-ui..."
echo ""

# Setup backend
echo "Setting up backend..."
cd "$SCRIPT_DIR/backend"
uv sync --all-extras
uv pip install ../../netrun --reinstall --quiet

if [ "$APP_MODE" = true ]; then
    # App mode: start frontend, then open native window
    echo "Starting in app mode..."

    # Start frontend in background
    echo "Starting frontend (Vite) on http://localhost:5173..."
    cd "$SCRIPT_DIR"
    VITE_INITIAL_PATH="$TESTING_DIR" npm run dev &
    FRONTEND_PID=$!

    # Wait for frontend to be ready
    echo "Waiting for frontend to start..."
    sleep 3

    # Start backend + native window (this blocks until window closes)
    echo "Opening native window..."
    cd "$SCRIPT_DIR/backend"
    uv run python app_window.py --frontend-url "http://localhost:5173"

    # When window closes, kill frontend
    kill $FRONTEND_PID 2>/dev/null

else
    # Normal mode: backend + frontend in browser

    # Start backend in background
    echo "Starting backend (FastAPI) on http://127.0.0.1:8000..."
    uv run python run.py &
    BACKEND_PID=$!

    # Wait a moment for backend to start
    sleep 2

    # Start frontend with testing directory as initial path
    echo "Starting frontend (Vite) on http://localhost:5173..."
    echo "File explorer will open to: $TESTING_DIR"
    cd "$SCRIPT_DIR"
    VITE_INITIAL_PATH="$TESTING_DIR" npm run dev &
    FRONTEND_PID=$!

    echo ""
    echo "Both services started!"
    echo "  Backend:  http://127.0.0.1:8000"
    echo "  Frontend: http://localhost:5173"
    echo "  Testing:  $TESTING_DIR"
    echo ""
    echo "Press Ctrl+C to stop both services."

    # Handle Ctrl+C to kill both processes
    cleanup() {
        echo ""
        echo "Stopping services..."
        kill $BACKEND_PID 2>/dev/null
        kill $FRONTEND_PID 2>/dev/null
        exit 0
    }

    trap cleanup SIGINT SIGTERM

    # Wait for either process to exit
    wait
fi

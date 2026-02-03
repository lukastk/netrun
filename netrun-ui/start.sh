#!/bin/bash
# Start netrun-ui backend and frontend

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TESTING_DIR="$SCRIPT_DIR/testing"

echo "Starting netrun-ui..."
echo ""

# Start backend in background
echo "Starting backend (FastAPI) on http://127.0.0.1:8000..."
cd "$SCRIPT_DIR/backend"
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

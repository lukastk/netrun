#!/usr/bin/env bash
# Starts the netrun-dashboard and a sample net with ObserveServer.
# The net stays alive so you can observe it in the dashboard UI.
#
# Usage: ./run_dashboard.sh
#   Then open http://localhost:18400

set -e

DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"

cleanup() {
    echo ""
    echo "Shutting down..."
    kill $DASHBOARD_PID 2>/dev/null || true
    kill $NET_PID 2>/dev/null || true
    wait $DASHBOARD_PID 2>/dev/null || true
    wait $NET_PID 2>/dev/null || true
    echo "Done."
}
trap cleanup EXIT

# 1. Start the dashboard server
echo "Starting dashboard on http://localhost:18400 ..."
uv run netrun-dashboard --port 18400 &
DASHBOARD_PID=$!
sleep 1

# 2. Start the sample net with ObserveServer
echo "Starting sample net with ObserveServer on http://localhost:8000 ..."
uv run python -c "
import asyncio
from pathlib import Path
from netrun.core import Net, NetConfig
from netrun_utils.observe import ObserveServer

async def main():
    config = NetConfig.from_file(Path('main.netrun.json'))
    async with Net(config, run_source_nodes=False) as net:
        async with ObserveServer(net, port=8000, name='demo-pipeline') as server:
            print(f'ObserveServer running at {server.url}')
            print()
            print('Open http://localhost:18400 in your browser.')
            print('Press Ctrl+C to stop.')
            await asyncio.Event().wait()

asyncio.run(main())
" &
NET_PID=$!

wait

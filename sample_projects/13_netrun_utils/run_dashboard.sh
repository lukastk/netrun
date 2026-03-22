#!/usr/bin/env bash
# Starts the netrun-dashboard and a sample net with ObserveServer.
# Parallel pipeline: fetch_text fans out to transform, reverse, analyze.
# transform feeds into summarize. Nodes sleep 1-3s to simulate work.
#
# Usage: ./run_dashboard.sh
#   Then open http://localhost:<port>

set -e

DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"

# Find a free port starting from a preferred port
find_free_port() {
    local port=$1
    while lsof -i :"$port" >/dev/null 2>&1; do
        port=$((port + 1))
    done
    echo "$port"
}

DASHBOARD_PORT=$(find_free_port 18400)
OBSERVE_PORT=$(find_free_port 8000)

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
echo "Starting dashboard on http://localhost:$DASHBOARD_PORT ..."
uv run netrun-dashboard --port "$DASHBOARD_PORT" &
DASHBOARD_PID=$!
sleep 1

# 2. Start the sample net with background execution
echo "Starting sample net with ObserveServer on http://localhost:$OBSERVE_PORT ..."
uv run python -c "
import asyncio
from pathlib import Path
from netrun.core import Net, NetConfig
from netrun_utils.observe import ObserveServer

DASHBOARD_PORT = $DASHBOARD_PORT
OBSERVE_PORT = $OBSERVE_PORT

async def main():
    config = NetConfig.from_file(Path('main.netrun.json'))
    async with Net(config, run_source_nodes=False) as net:
        async with ObserveServer(
            net,
            port=OBSERVE_PORT,
            name='demo-pipeline',
            registry_url=f'http://localhost:{DASHBOARD_PORT}',
        ) as server:
            print(f'ObserveServer running at {server.url}')
            print()
            print(f'Open http://localhost:{DASHBOARD_PORT} in your browser.')

            # Start background execution loop — continuously processes
            # any startable epochs (from inject, controls, etc.)
            await net.start_background()

            # Kick off the pipeline periodically
            print('Starting pipeline in 3 seconds...')
            await asyncio.sleep(3)
            run = 1
            while True:
                print(f'--- Run {run} ---')
                await net.execute_node('fetch_text')
                print(f'Run {run} started.')
                run += 1
                # Wait for pipeline to finish + gap between runs
                while not net.is_blocked():
                    await asyncio.sleep(0.1)
                await asyncio.sleep(1)

asyncio.run(main())
" &
NET_PID=$!

wait

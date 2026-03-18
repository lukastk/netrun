#!/usr/bin/env bash
# Starts the netrun-dashboard and a sample net with ObserveServer.
# Each node sleeps for 3s to simulate long-running jobs.
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

# 2. Start the sample net and run the pipeline
echo "Starting sample net with ObserveServer on http://localhost:8000 ..."
uv run python -c "
import asyncio
from pathlib import Path
from netrun.core import Net, NetConfig
from netrun_utils.observe import ObserveServer

async def run_pipeline(net):
    while True:
        await net.run_until_blocked()
        startable = net.get_startable_epochs()
        if not startable:
            break
        for epoch_id in startable:
            await net.execute_epoch(epoch_id)

async def main():
    config = NetConfig.from_file(Path('main.netrun.json'))
    async with Net(config, run_source_nodes=False) as net:
        await asyncio.sleep(1)
        async with ObserveServer(net, port=8000, name='demo-pipeline') as server:
            print(f'ObserveServer running at {server.url}')
            print()
            print('Open http://localhost:18400 in your browser.')
            print('Starting pipeline in 5 seconds...')
            await asyncio.sleep(5)
            run = 1
            while True:
                print(f'--- Run {run} ---')
                await net.execute_node('fetch_text')
                await run_pipeline(net)
                print(f'Run {run} complete. Next run in 1 second...')
                run += 1
                await asyncio.sleep(1)

asyncio.run(main())
" &
NET_PID=$!

wait

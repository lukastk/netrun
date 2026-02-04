"""Test that multiprocess pools work with factory-generated functions.

This script tests the lazy resolution feature that enables function factories
to work with multiprocess pools.
"""

import asyncio
import json
import sys
import time
from copy import deepcopy
from pathlib import Path

# Add parent directory for imports
sys.path.insert(0, str(Path(__file__).parent))

from netrun.core import Net, NetConfig


async def run_with_pool_type(base_config: dict, pool_type: str, num_workers: int) -> tuple[float, dict]:
    """Run the network with a specific pool type and return elapsed time and results."""
    config = deepcopy(base_config)

    # Configure pool with specified type and workers
    config["pools"] = {
        "compute_pool": {
            "spec": {"type": pool_type, "num_workers": num_workers}
        },
        "main": {"spec": {"type": "main"}},
    }

    # Update all nodes to use compute_pool (except main pool for coordination)
    for node in config["graph"]["nodes"]:
        node["execution_config"] = {"pools": ["compute_pool"]}

    net_config = NetConfig.model_validate(config)

    start_time = time.perf_counter()

    async with Net(net_config) as net:
        # Inject data for each hash node
        for i, name in enumerate(["alpha", "beta", "gamma", "delta"], 1):
            net.inject_data(f"hash_{i}", "data", [name])
            net.inject_data(f"hash_{i}", "iterations", [50_000])  # Reduced for faster testing

        # Run until complete
        while True:
            await net.run_until_blocked()
            startable = net.get_startable_epochs()
            if not startable:
                break
            for epoch_id in startable:
                await net.execute_epoch(epoch_id)

        results = net.get_all_outputs("results")

    elapsed = time.perf_counter() - start_time
    return elapsed, results


async def main():
    # Load the base configuration
    config_path = Path(__file__).parent / "main.netrun.json"
    config_data = json.loads(config_path.read_text())

    print("=" * 60)
    print("Testing Multiprocess Pool with Factory Functions")
    print("=" * 60)
    print()

    # Test thread pool (baseline - should work)
    print("Testing thread pool (baseline)...")
    try:
        time_thread, results_thread = await run_with_pool_type(config_data, "thread", 2)
        print(f"  Thread pool: {time_thread:.2f}s - SUCCESS")
        print(f"  Results: {results_thread}")
    except Exception as e:
        print(f"  Thread pool: FAILED - {e}")
        return 1

    print()

    # Test multiprocess pool (this was failing before the fix)
    print("Testing multiprocess pool...")
    try:
        time_mp, results_mp = await run_with_pool_type(config_data, "multiprocess", 2)
        print(f"  Multiprocess pool: {time_mp:.2f}s - SUCCESS")
        print(f"  Results: {results_mp}")
    except Exception as e:
        print(f"  Multiprocess pool: FAILED - {e}")
        import traceback
        traceback.print_exc()
        return 1

    print()
    print("=" * 60)
    print("All tests passed!")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)

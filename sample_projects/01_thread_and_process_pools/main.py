"""Demonstrate thread and process pools in netrun.

This example shows how to configure both thread and process pools for
parallel execution of CPU-bound work.

Key concepts:
- Thread pools: Multiple worker threads for concurrent execution
- Process pools: Multiple worker processes for true CPU parallelism
- Pool configuration in NetConfig
- Assigning nodes to specific pools via execution_config

Thread pools are limited by Python's GIL for CPU-bound work, while
process pools bypass the GIL by running in separate processes.
Factory-based nodes work with process pools via lazy resolution -
the factory function is resolved on each worker process rather than
being pickled across process boundaries.
"""

import asyncio
import json
import time
from copy import deepcopy
from pathlib import Path

from netrun.core import Net, NetConfig


async def run_with_pool(config_data: dict, pool_type: str, num_workers: int) -> float:
    """Run the network with a specific pool type and return elapsed time."""
    config = deepcopy(config_data)

    # Configure pool with specified type and workers
    config["pools"] = {
        "compute_pool": {
            "spec": {"type": pool_type, "num_workers": num_workers}
        },
        "main": {"spec": {"type": "main"}},
    }

    # Update all hash nodes to use compute_pool
    for node in config["graph"]["nodes"]:
        if node["name"].startswith("hash_"):
            node["execution_config"] = {"pools": ["compute_pool"]}

    net_config = NetConfig.model_validate(config)

    start_time = time.perf_counter()

    async with Net(net_config) as net:
        # Inject data - each hash node gets different input
        net.inject_data("hash_1", "data", ["alpha"])
        net.inject_data("hash_1", "iterations", [200_000])

        net.inject_data("hash_2", "data", ["beta"])
        net.inject_data("hash_2", "iterations", [200_000])

        net.inject_data("hash_3", "data", ["gamma"])
        net.inject_data("hash_3", "iterations", [200_000])

        net.inject_data("hash_4", "data", ["delta"])
        net.inject_data("hash_4", "iterations", [200_000])

        # Run until all processing is complete
        while True:
            await net.run_until_blocked()
            startable = net.get_startable_epochs()
            if not startable:
                break
            for epoch_id in startable:
                await net.execute_epoch(epoch_id)

        results = net.get_all_outputs("results")

    elapsed = time.perf_counter() - start_time
    return elapsed


async def main():
    # Load the network configuration
    config_path = Path(__file__).parent / "main.netrun.json"
    config_data = json.loads(config_path.read_text())

    print("=" * 60)
    print("Thread Pool vs Process Pool Demonstration")
    print("=" * 60)
    print()
    print("This example runs 4 parallel CPU-bound hash computations")
    print("using both thread and process pools.")
    print()
    print("Each hash node computes 200,000 SHA-256 iterations.")
    print()

    # --- Thread Pool ---
    print("-" * 60)
    print("Thread Pool")
    print("-" * 60)

    print("Running with 1 thread (sequential)...")
    time_t1 = await run_with_pool(config_data, "thread", 1)
    print(f"  1 thread: {time_t1:.2f}s")

    print("Running with 4 threads (parallel)...")
    time_t4 = await run_with_pool(config_data, "thread", 4)
    print(f"  4 threads: {time_t4:.2f}s")
    print()

    # --- Process Pool ---
    print("-" * 60)
    print("Process Pool")
    print("-" * 60)

    print("Running with 1 process (sequential)...")
    time_p1 = await run_with_pool(config_data, "multiprocess", 1)
    print(f"  1 process: {time_p1:.2f}s")

    print("Running with 4 processes (parallel)...")
    time_p4 = await run_with_pool(config_data, "multiprocess", 4)
    print(f"  4 processes: {time_p4:.2f}s")
    print()

    # --- Summary ---
    print("=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"Thread pool  - 1 worker:  {time_t1:.2f}s")
    print(f"Thread pool  - 4 workers: {time_t4:.2f}s  ({time_t1 / time_t4:.2f}x)")
    print(f"Process pool - 1 worker:  {time_p1:.2f}s")
    print(f"Process pool - 4 workers: {time_p4:.2f}s  ({time_p1 / time_p4:.2f}x)")
    print()
    print("Thread pools are limited by Python's GIL for CPU-bound work.")
    print("Process pools bypass the GIL and achieve true parallelism.")


if __name__ == "__main__":
    asyncio.run(main())

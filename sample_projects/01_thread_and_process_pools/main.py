"""Demonstrate thread pools in netrun.

This example shows how to configure thread pools for parallel execution
of CPU-bound work across multiple worker threads.

Key concepts:
- Thread pools: Multiple worker threads for concurrent execution
- Pool configuration in NetConfig
- Assigning nodes to specific pools via execution_config

Note on Multiprocess Pools:
Multiprocess pools require functions to be picklable (serializable).
The function factory creates closures that can't be pickled across
process boundaries. For multiprocess execution, you would need to:
1. Define node functions at module level
2. Use explicit NodeConfig with import paths instead of closures
"""

import asyncio
import json
import time
from copy import deepcopy
from pathlib import Path

from netrun.core import Net, NetConfig


async def run_with_workers(config_data: dict, num_workers: int) -> float:
    """Run the network with a specific number of workers and return elapsed time."""
    config = deepcopy(config_data)

    # Configure thread pool with specified workers
    config["pools"] = {
        "compute_pool": {
            "spec": {"type": "thread", "num_workers": num_workers}
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
    print("Thread Pool Demonstration")
    print("=" * 60)
    print()
    print("This example runs 4 parallel CPU-bound hash computations")
    print("using thread pools with different numbers of workers.")
    print()
    print("Each hash node computes 200,000 SHA-256 iterations.")
    print()

    # Run with 1 worker (sequential)
    print("Running with 1 worker (sequential)...")
    time_1 = await run_with_workers(config_data, 1)
    print(f"  1 worker: {time_1:.2f}s")
    print()

    # Run with 4 workers (parallel)
    print("Running with 4 workers (parallel)...")
    time_4 = await run_with_workers(config_data, 4)
    print(f"  4 workers: {time_4:.2f}s")
    print()

    # Summary
    print("=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"Sequential (1 worker): {time_1:.2f}s")
    print(f"Parallel (4 workers):  {time_4:.2f}s")

    if time_4 < time_1:
        speedup = time_1 / time_4
        print(f"Speedup: {speedup:.2f}x faster with 4 workers")
    else:
        print("Note: For very short tasks, thread overhead may exceed benefits.")

    print()
    print("Why threads may show limited speedup for CPU-bound work:")
    print("-" * 60)
    print("Python's GIL (Global Interpreter Lock) prevents true parallel")
    print("execution of CPU-bound Python code. However, threads still")
    print("provide benefits for:")
    print("  - I/O-bound operations (network, file I/O)")
    print("  - Operations that release the GIL (numpy, etc.)")
    print("  - Overlapping computation with I/O")


if __name__ == "__main__":
    asyncio.run(main())

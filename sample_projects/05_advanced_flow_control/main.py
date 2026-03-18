"""Run the advanced flow control examples.

Demonstrates:
1. Custom salvo conditions (batch_processor fires only when 3 packets accumulate)
2. Finite port slots (batch_processor.data has capacity=5)
3. Rate limiting (rate_limited_worker at 2 epochs/sec)
4. Multiple output ports (multi_output sends to main_out, debug_out, extra_out)
"""

import asyncio
import time
from pathlib import Path

from netrun.core import Net, NetConfig


async def main():
    config_path = Path(__file__).parent / "main.netrun.json"
    config = NetConfig.from_file(config_path)

    async with Net(config) as net:
        # --- 1. Batch processor: custom salvo condition ---
        # Inject 5 items. The salvo condition fires when exactly 3 packets
        # are on the 'data' port. After consuming 3, the remaining 2 won't
        # trigger another epoch (need exactly 3).
        for item in ["alpha", "beta", "gamma", "delta", "epsilon"]:
            net.inject_data("batch_processor", "data", [item])

        await net.run_until_blocked()

        # --- 2. Rate-limited worker ---
        # Inject 4 items. rate_limit_per_second=2 means at most 2 per second.
        for item in ["one", "two", "three", "four"]:
            net.inject_data("rate_limited_worker", "data", [item])

        t0 = time.time()
        await net.run_until_blocked()
        elapsed = time.time() - t0

        # --- 3. Multi-output with secondary queue ---
        net.inject_data("multi_output", "value", [42])
        net.inject_data("multi_output", "value", [99])

        await net.run_until_blocked()

        # --- Report results ---
        print("=" * 60)

        # Batch processor
        batch_results = net.flush_output_queue("batch_results")
        print(f"1. Batch processor results: {batch_results}")
        print(f"   (injected 5 items, fired once with 3; 2 remain unbatched)")

        # Rate-limited worker
        rl_results = net.flush_output_queue("rate_limited_results")
        print(f"\n2. Rate-limited worker results: {rl_results}")
        print(f"   (4 items at rate_limit=2/sec took {elapsed:.2f}s)")

        # Multi-output: main_out goes to named queue
        main_results = net.flush_output_queue("main_results")
        print(f"\n3. Multi-output main results: {main_results}")

        # Secondary queue captures debug_out and extra_out
        secondary = net.flush_output_queue("secondary_results")
        print(f"   Secondary results (debug_out + extra_out): {secondary}")

        # Logs
        print("\nNode Logs:")
        net.print_all_logs()


if __name__ == "__main__":
    asyncio.run(main())

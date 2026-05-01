"""Run the resources sample.

Three GPU jobs with a 1-slot `gpu` resource. They all sit on a 3-worker
thread pool, so they could parallelize — but the resource forces them to
run serially. Total wall-clock time is ~3 × 0.2s, not ~0.2s.
"""

import asyncio
import time
from pathlib import Path

from netrun.core import Net, NetConfig


async def main():
    config = NetConfig.from_file(Path(__file__).parent / "main.netrun.json")
    async with Net(config) as net:
        net.inject_data("job_1", "trigger", ["go"])
        net.inject_data("job_2", "trigger", ["go"])
        net.inject_data("job_3", "trigger", ["go"])

        start = time.time()
        await net.run_until_blocked()
        elapsed = time.time() - start

        print(f"\nTotal wall-clock: {elapsed:.2f}s (serial — expected ~0.6s, not ~0.2s)")
        print("Results:", net.flush_output_queue("results"))

        print("\nNode logs:")
        for node in ("job_1", "job_2", "job_3"):
            for ts, msg in net.logs.for_node(node):
                print(f"  {ts.strftime('%H:%M:%S.%f')[:-3]} | {msg}", end="")


if __name__ == "__main__":
    asyncio.run(main())

"""Run the depends_on sample.

Three nodes A, B, C with no data edges between them. The depends_on field
forces A → B → C ordering. Watch the output queue: results should appear
in order regardless of injection order.
"""

import asyncio
from pathlib import Path

from netrun.core import Net, NetConfig


async def main():
    config = NetConfig.from_file(Path(__file__).parent / "main.netrun.json")
    async with Net(config) as net:
        # Inject all triggers up front. depends_on enforces the order.
        net.inject_data("step_a", "trigger", ["go"])
        net.inject_data("step_b", "trigger", ["go"])
        net.inject_data("step_c", "trigger", ["go"])

        await net.run_until_blocked()

        print("\nCompletion order:", net.flush_output_queue("all_done"))


if __name__ == "__main__":
    asyncio.run(main())

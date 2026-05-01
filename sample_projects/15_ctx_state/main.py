"""Run the ctx.state sample.

Demonstrates that data cached in `ctx.state` survives a failed attempt and
is reused on retry, so expensive precomputation isn't redone.
"""

import asyncio
from pathlib import Path

from netrun.core import Net, NetConfig


async def main():
    config = NetConfig.from_file(Path(__file__).parent / "main.netrun.json")
    async with Net(config) as net:
        net.inject_data("pipeline", "x", [42])
        await net.run_until_blocked()

        results = net.flush_output_queue("results")
        print("\nResults:", results)

        print("\nNode logs:")
        for ts, msg in net.logs.for_node("pipeline"):
            print(f"  {ts.strftime('%H:%M:%S.%f')[:-3]} | {msg}", end="")


if __name__ == "__main__":
    asyncio.run(main())

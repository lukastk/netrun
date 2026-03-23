"""Run the actions and recipes example.

Demonstrates:
1. TOML config format
2. File-path imports (./nodes.py::func syntax)
3. Node variables (global + per-node override)
4. Linear pipeline: greeter -> transformer -> format_output
5. Actions and recipes (designed for netrun-ui interaction)

Note: Actions and recipes are best experienced through netrun-ui or
the CLI (`netrun actions list`, `netrun recipes list`). This script
demonstrates the runtime node execution.
"""

import asyncio
from pathlib import Path

from netrun.core import Net, NetConfig


async def main():
    config_path = Path(__file__).parent / "main.netrun.toml"
    config = NetConfig.from_file(config_path)

    async with Net(config) as net:
        # Inject names into the greeter
        net.inject_data("greeter", "name", ["Alice", "Bob"])
        await net.run_until_blocked()

        # Collect results
        results = net.flush_output_queue("results")

        print("=" * 60)
        print("Pipeline: greeter -> transformer -> format_output")
        print(f"\nResults:")
        for r in results:
            print(f"  {r}")

        # Show node variables used
        print("\nNode Variables:")
        print("  greeter: greeting_template = 'Welcome aboard, {name}!' (per-node override)")
        print("  transformer: mode = 'uppercase' (global default)")

        # Show logs
        print("\nNode Logs:")
        net.logs.print_all()


if __name__ == "__main__":
    asyncio.run(main())

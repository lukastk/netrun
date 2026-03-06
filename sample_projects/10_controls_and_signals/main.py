"""Run the controls and signals example.

Demonstrates:
1. Signals — epoch_finished signal from 'process' triggers 'monitor'
2. Controls — start_epoch to fire 'generate', enable/disable to toggle 'process'

Network:
  [generate] --out--> [process] --out--> (output queue: results)
                          |
                          └── __signal_epoch_finished__ --> [monitor] --out--> (output queue: signals_log)
"""

import asyncio
from pathlib import Path

from netrun.core import Net, NetConfig


async def run_net(net):
    """Run the net until no more progress can be made."""
    while True:
        await net.run_until_blocked()
        startable = net.get_startable_epochs()
        if not startable:
            break
        for epoch_id in startable:
            await net.execute_epoch(epoch_id)


async def main():
    config_path = Path(__file__).parent / "main.netrun.json"
    config = NetConfig.from_file(config_path)

    async with Net(config) as net:
        # === Part 1: Signals ===
        # Trigger 'generate' via start_epoch control. The generated item
        # flows to 'process', which emits an epoch_finished signal that
        # triggers 'monitor'.
        print("=== Part 1: Signals ===")

        net.send_control("generate", "start_epoch")
        await run_net(net)

        results = net.flush_output_queue("results")
        signals = net.flush_output_queue("signals_log")
        print(f"Results: {results}")
        print(f"Signal log: {signals}")

        # Trigger again to see the signal a second time
        net.send_control("generate", "start_epoch")
        await run_net(net)

        results = net.flush_output_queue("results")
        signals = net.flush_output_queue("signals_log")
        print(f"Results: {results}")
        print(f"Signal log: {signals}")

        # === Part 2: Enable / Disable controls ===
        print("\n=== Part 2: Enable / Disable ===")

        # Disable the process node
        net.send_control("process", "disable")
        await run_net(net)
        print(f"Process enabled: {net.is_node_enabled('process')}")

        # Re-enable the process node
        net.send_control("process", "enable")
        await run_net(net)
        print(f"Process enabled: {net.is_node_enabled('process')}")

        # Trigger generator again — full pipeline works after re-enable
        net.send_control("generate", "start_epoch")
        await run_net(net)

        results = net.flush_output_queue("results")
        signals = net.flush_output_queue("signals_log")
        print(f"Results after re-enable: {results}")
        print(f"Signal log: {signals}")

        # === Print captured node logs ===
        print("\n=== Node Logs ===")
        net.print_all_logs()


if __name__ == "__main__":
    asyncio.run(main())

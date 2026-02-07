"""Run the basic netrun example.

This demonstrates:
1. Loading a network configuration from JSON
2. Creating and starting a Net
3. Injecting data into the network
4. Running the network until all processing is complete
5. Retrieving results from output queues
6. Multiple output ports (analyze node)
7. _node_config attribute overrides (format_result node)
"""

import asyncio
import json
from pathlib import Path

from netrun.core import Net, NetConfig


async def main():
    # Load the network configuration from JSON
    config_path = Path(__file__).parent / "main.netrun.json"
    config_data = json.loads(config_path.read_text())
    config = NetConfig.model_validate(config_data)

    # Create and start the network
    async with Net(config) as net:
        # Inject input data:
        # - 'double' node receives x=5
        # - 'add' node receives b=10
        # - 'analyze' node receives value=42 (standalone, demonstrates multi-output)
        net.inject_data("double", "x", [5])
        net.inject_data("add", "b", [10])
        net.inject_data("analyze", "value", [42])

        # Run until all processing is complete
        while True:
            await net.run_until_blocked()

            startable = net.get_startable_epochs()
            if not startable:
                break

            for epoch_id in startable:
                await net.execute_epoch(epoch_id)

        # Retrieve results from output queues
        results = net.flush_output_queue("results")
        summaries = net.flush_output_queue("summaries")
        breakdowns = net.flush_output_queue("breakdowns")

        print("=" * 50)
        print("Results (from format_result):")
        for value in results:
            print(f"  {value}")

        print("\nSummaries (from analyze.summary):")
        for value in summaries:
            print(f"  {value}")

        print("\nBreakdowns (from analyze.breakdown):")
        for value in breakdowns:
            print(f"  {value}")

        # Show captured print logs from all nodes
        print()
        print("Node Logs:")
        for node_name in ["double", "add", "format_result", "analyze"]:
            logs = net.get_node_logs(node_name)
            if logs:
                print(f"\n  [{node_name}]")
                for timestamp, message in logs:
                    print(f"    {timestamp.strftime('%H:%M:%S.%f')[:-3]} | {message}", end="")


if __name__ == "__main__":
    asyncio.run(main())

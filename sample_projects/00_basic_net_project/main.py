"""Run the basic netrun example.

This demonstrates:
1. Loading a network configuration from JSON
2. Creating and starting a Net
3. Injecting data into the network
4. Running the network until all processing is complete
5. Retrieving results from output queues
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
        net.inject_data("double", "x", [5])
        net.inject_data("add", "b", [10])

        # Run until all processing is complete
        while True:
            # Move packets through edges
            await net.run_until_blocked()

            # Execute any startable epochs
            startable = net.get_startable_epochs()
            if not startable:
                break

            for epoch_id in startable:
                await net.execute_epoch(epoch_id)

        # Retrieve results from the output queue
        results = net.get_all_outputs("results")

        print("=" * 50)
        print("Results:")
        for packet in results:
            print(f"  {packet.value}")

        # Show captured print logs
        print()
        print("Logs from format_result node:")
        for timestamp, message in net.get_node_log("format_result"):
            print(f"  [{timestamp.strftime('%H:%M:%S')}] {message}", end="")


if __name__ == "__main__":
    asyncio.run(main())

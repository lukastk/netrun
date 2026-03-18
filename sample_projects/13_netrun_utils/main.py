"""Demonstrates netrun-utils: observe module.

This script showcases the observe module:
1. NetObserver — query live net status (nodes, edges, epochs, logs)
2. ObserveServer + ObserveClient — REST + WebSocket API for monitoring/control

The dev module (set_node_func_args, show_node_vars) is demonstrated in the
node notebooks under nbs/nodes/. Open them in Jupyter to see the interactive
node development workflow.

Pipeline:  fetch_text -> transform -> summarize -> [output queues]
"""

import asyncio
from pathlib import Path

from netrun.core import Net, NetConfig

from netrun_utils.observe import NetObserver, ObserveServer, ObserveClient


CONFIG_PATH = Path(__file__).parent / "main.netrun.json"


async def run_pipeline(net: Net):
    """Run the pipeline to completion."""
    while True:
        await net.run_until_blocked()
        startable = net.get_startable_epochs()
        if not startable:
            break
        for epoch_id in startable:
            await net.execute_epoch(epoch_id)


async def demo_observer():
    """Demo 1: NetObserver — query live net status."""
    print("=" * 60)
    print("1. NetObserver — Live Status Queries")
    print("=" * 60)
    print()

    config = NetConfig.from_file(CONFIG_PATH)
    async with Net(config) as net:
        obs = NetObserver(net)

        # Query status
        status = obs.get_status()
        print(f"Net started: {status.started}")
        print(f"Nodes: {status.node_names}")
        print(f"Edges: {status.edge_count}")
        print()

        # Query individual nodes
        print("Node details:")
        for node_status in obs.get_nodes():
            print(f"  {node_status.name}: enabled={node_status.enabled}, "
                  f"in_ports={node_status.in_port_names}, "
                  f"out_ports={node_status.out_port_names}")

        # Query edges
        print()
        print("Edge details:")
        for edge in obs.get_edges():
            print(f"  {edge.source_node}.{edge.source_port} -> "
                  f"{edge.target_node}.{edge.target_port} "
                  f"(packets: {edge.packet_count})")

        # Run the pipeline (fetch_text has run_on_startup=true)
        print()
        print("Running pipeline...")
        await run_pipeline(net)

        # Epoch logs
        epoch_logs = obs.get_epoch_logs()
        print(f"\nEpoch logs ({len(epoch_logs)} total):")
        for ep in epoch_logs:
            duration = f"{ep.duration_ms:.0f}ms" if ep.duration_ms else "n/a"
            print(f"  [{ep.node_name}] state={ep.state}, outcome={ep.outcome}, "
                  f"duration={duration}")

        # Print logs
        all_logs = obs.get_all_logs()
        print(f"\nPrint logs ({len(all_logs)} entries):")
        for log in all_logs[:6]:
            print(f"  [{log.node_name}] {log.message.strip()}")
        if len(all_logs) > 6:
            print(f"  ... and {len(all_logs) - 6} more")

        # Control: disable and re-enable a node
        print()
        resp = obs.disable_node("transform")
        print(f"Disable transform: {resp.message}")
        print(f"  transform.enabled = {obs.get_node('transform').enabled}")
        resp = obs.enable_node("transform")
        print(f"Re-enable transform: {resp.message}")

        # Show results
        summaries = net.flush_output_queue("summaries")
        print(f"\nOutput: {summaries}")


async def demo_server_and_client():
    """Demo 2: ObserveServer + ObserveClient — REST API."""
    print()
    print("=" * 60)
    print("2. ObserveServer + ObserveClient — REST API")
    print("=" * 60)
    print()

    config = NetConfig.from_file(CONFIG_PATH)
    async with Net(config, run_source_nodes=False) as net:
        async with ObserveServer(net, port=18400) as server:
            print(f"Server running at {server.url}")
            print()

            async with ObserveClient(server.url) as client:
                # Query via client
                status = await client.get_status()
                print(f"[via client] Nodes: {status.node_names}")
                print(f"[via client] Edges: {status.edge_count}")

                nodes = await client.get_nodes()
                for n in nodes:
                    print(f"[via client] Node '{n.name}': busy={n.is_busy}, "
                          f"epochs={n.epoch_count}")

                # Control via client
                resp = await client.disable_node("summarize")
                print(f"\n[via client] {resp.message}")
                node = await client.get_node("summarize")
                print(f"[via client] summarize.enabled = {node.enabled}")

                resp = await client.enable_node("summarize")
                print(f"[via client] {resp.message}")

                # Inject data via client and run
                resp = await client.inject_data("transform", "text", ["Hello from client!"])
                print(f"\n[via client] {resp.message}")

                await run_pipeline(net)
                epochs = await client.get_epoch_logs()
                print(f"\n[via client] Epochs after run: {len(epochs)}")
                for ep in epochs:
                    print(f"  [{ep.node_name}] {ep.outcome}")


async def main():
    await demo_observer()
    await demo_server_and_client()

    print()
    print("=" * 60)
    print("Done!")
    print()
    print("The dev module (set_node_func_args, show_node_vars) is demonstrated")
    print("in the node notebooks under nbs/nodes/. Open them in Jupyter to see")
    print("the interactive node development workflow.")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())

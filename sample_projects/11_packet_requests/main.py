"""Packet Requests & Dependency Edges

Demonstrates pull-based execution in netrun:
1. On-startup pull: DataSource is automatically triggered to feed Consumer
2. Manual request: net.request() cascades backward through a 3-node chain
3. Hybrid push-pull: Merger combines pulled config with pushed live data

Graph (dependency edges marked with ===):

  Scenario 1:  DataSource ===> Consumer
  Scenario 2:  Generator ===> Processor ===> Reporter
  Scenario 3:  ConfigSource ===> Merger <--- inject_data("live")
"""

import asyncio
from pathlib import Path

from netrun.core import Net, NetConfig


async def main():
    config_path = Path(__file__).parent / "main.netrun.json"
    config = NetConfig.from_file(config_path)

    async with Net(config) as net:

        # =============================================================
        # Scenario 1: On-Startup Pull
        # =============================================================
        # Consumer has dependency_request with on_startup trigger.
        # run_until_blocked() fires the startup request, which cascades
        # backward to DataSource (a source node), executes it, and
        # delivers the result forward to Consumer.
        #
        # Note: Merger also has on_startup, so ConfigSource gets
        # triggered too.
        print("=" * 60)
        print("SCENARIO 1: On-Startup Pull")
        print("=" * 60)
        print()

        await net.run_until_blocked()

        results1 = net.flush_output_queue("consumer_output")
        print(f"Consumer got: {results1}")
        print()

        # =============================================================
        # Scenario 2: Manual Request
        # =============================================================
        # Reporter has no dependency_request config — we trigger it
        # manually. The cascade goes:
        #   Reporter -> Processor -> Generator (source)
        # Generator gets an epoch, then forward flow delivers data
        # through Processor to Reporter.
        print("=" * 60)
        print("SCENARIO 2: Manual Request")
        print("=" * 60)
        print()

        net.request("Reporter", "report_v1")
        await net.run_until_blocked()

        results2 = net.flush_output_queue("reporter_output")
        print(f"Reporter produced: {results2}")
        print()

        # =============================================================
        # Scenario 3: Hybrid Push-Pull
        # =============================================================
        # Merger already received its config data from ConfigSource
        # (pulled via on_startup in scenario 1). Now we push live data
        # directly into Merger's "live" input port. Once both ports
        # have data, Merger's salvo fires.
        print("=" * 60)
        print("SCENARIO 3: Hybrid Push-Pull")
        print("=" * 60)
        print()

        net.inject_data("Merger", "live", ["user_click_event"])
        await net.run_until_blocked()

        results3 = net.flush_output_queue("merger_output")
        print(f"Merger produced: {results3}")
        print()

        # =============================================================
        # All Node Logs
        # =============================================================
        print("=" * 60)
        print("NODE LOGS")
        print("=" * 60)
        net.print_all_logs(include_timestamps=False)


if __name__ == "__main__":
    asyncio.run(main())

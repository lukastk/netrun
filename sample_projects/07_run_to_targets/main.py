"""Demonstrates run_to_targets for testing individual nodes in a pipeline.

run_to_targets() executes only the upstream nodes needed to deliver data
to your target node, then returns the input salvos that would fire at the
target — without executing it. This lets you inspect exactly what a node
would receive, then test the node function directly.

Pipeline: fetch_data -> validate -> transform -> enrich -> format_output
"""

import asyncio
from pathlib import Path

from netrun.core import Net, NetConfig


async def main():
    config_path = Path(__file__).parent / "main.netrun.json"
    config = NetConfig.from_file(config_path)

    # ----------------------------------------------------------------
    # 1. Normal run: execute the full pipeline
    # ----------------------------------------------------------------
    print("=" * 60)
    print("1. FULL PIPELINE RUN")
    print("=" * 60)

    async with Net(config) as net:
        net.inject_data("fetch_data", "url", ["https://example.com/data"])
        await net.run_until_blocked()

        results = net.flush_output_queue("results")
        print(results[0])
        print()
        net.print_all_logs(include_timestamps=False)

    # ----------------------------------------------------------------
    # 2. run_to_targets: inspect what 'transform' would receive
    # ----------------------------------------------------------------
    print()
    print("=" * 60)
    print("2. RUN_TO_TARGETS: inspect input to 'transform'")
    print("=" * 60)
    print()
    print("Only fetch_data and validate run. transform does NOT execute.")
    print("We get back the exact input salvo that would fire at transform.")
    print()

    async with Net(config) as net:
        net.inject_data("fetch_data", "url", ["https://example.com/data"])
        salvos = await net.run_to_targets("transform")

        for salvo in salvos:
            print(f"Target: {salvo.node_name} (salvo condition: {salvo.salvo_condition})")
            for port_name, values in salvo.packets.items():
                for value in values:
                    print(f"  Port '{port_name}':")
                    print(f"    source: {value['source']}")
                    print(f"    records ({len(value['records'])}):")
                    for r in value["records"]:
                        print(f"      {r}")
        print()
        print("Upstream logs (fetch_data, validate only):")
        net.print_all_logs(include_timestamps=False)

    # ----------------------------------------------------------------
    # 3. run_to_targets: test the node function with captured inputs
    # ----------------------------------------------------------------
    print()
    print("=" * 60)
    print("3. TEST NODE FUNCTION: call transform() directly with captured input")
    print("=" * 60)
    print()

    from nodes import transform as transform_func

    async with Net(config) as net:
        net.inject_data("fetch_data", "url", ["https://example.com/data"])
        salvos = await net.run_to_targets("transform")

        # Extract the input value from the salvo
        input_data = salvos[0].packets["data"][0]

        # Call the node function directly to test it
        result = transform_func(input_data, print=print)
        print(f"\ntransform() returned {len(result['records'])} records:")
        for r in result["records"]:
            print(f"  {r['name']}: score={r['score']}, normalized={r['score_normalized']}, grade={r['grade']}")

    # ----------------------------------------------------------------
    # 4. run_to_targets: inspect what 'format_output' (final node) would receive
    # ----------------------------------------------------------------
    print()
    print("=" * 60)
    print("4. RUN_TO_TARGETS: inspect input to 'format_output' (final node)")
    print("=" * 60)
    print()
    print("All upstream nodes run (fetch_data, validate, transform, enrich).")
    print("format_output does NOT execute — we just see what it would get.")
    print()

    async with Net(config) as net:
        net.inject_data("fetch_data", "url", ["https://example.com/data"])
        salvos = await net.run_to_targets("format_output")

        for salvo in salvos:
            print(f"Target: {salvo.node_name}")
            for port_name, values in salvo.packets.items():
                for value in values:
                    print(f"  Port '{port_name}':")
                    print(f"    source: {value['source']}")
                    print(f"    records ({len(value['records'])}):")
                    for r in value["records"]:
                        print(f"      {r}")

    # ----------------------------------------------------------------
    # 5. run_to_targets: target a source node (no upstream at all)
    # ----------------------------------------------------------------
    print()
    print("=" * 60)
    print("5. RUN_TO_TARGETS: target source node 'fetch_data'")
    print("=" * 60)
    print()
    print("fetch_data has no upstream nodes. Injecting data into its input")
    print("port and targeting it returns the salvo immediately — nothing executes.")
    print()

    async with Net(config) as net:
        net.inject_data("fetch_data", "url", ["https://example.com/data"])
        salvos = await net.run_to_targets("fetch_data")

        for salvo in salvos:
            print(f"Target: {salvo.node_name}")
            for port_name, values in salvo.packets.items():
                print(f"  Port '{port_name}': {values}")
        print(f"\nUpstream epochs executed: {len(net.epochs)}")


if __name__ == "__main__":
    asyncio.run(main())

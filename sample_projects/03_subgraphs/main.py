"""Run the subgraphs example.

This demonstrates:
1. Inline subgraphs (preprocess: normalize -> validate)
2. File-referenced subgraphs (shared: from shared_pipeline.netrun.json)
3. Subgraph factories (factory_pipeline: from pipeline_factory.py)
4. Port groups (extract_features: features.color, features.shape, features.size)
5. Edges crossing subgraph boundaries
"""

import asyncio
import json
from pathlib import Path

from netrun.core import Net, NetConfig


async def main():
    config_path = Path(__file__).parent / "main.netrun.json"
    config = NetConfig.from_file(config_path)

    async with Net(config) as net:
        # Source generates data and feeds into the preprocess subgraph
        net.inject_data("source", "seed", [42])

        # Extract features from sample data (demonstrates port groups)
        net.inject_data("extract_features", "data", [
            {"color": "blue", "shape": "triangle", "size": 3.5}
        ])

        # Feed data into the factory-generated pipeline
        # After resolution, factory_pipeline.stage_0 is the first stage
        net.inject_data("factory_pipeline.stage_0", "data", [{"value": 100, "label": "factory_test"}])

        # Run until all processing is complete
        await net.run_until_blocked()

        # Results
        print("=" * 60)
        results = net.flush_output_queue("results")
        print("Results (from format_output):")
        for value in results:
            print(f"  {value}")

        features = net.flush_output_queue("features")
        print("\nFeatures (from extract_features port group):")
        for value in features:
            print(f"  {value}")

        # Node logs — shows subgraph-prefixed node names
        print("\nNode Logs:")
        net.print_all_logs()


if __name__ == "__main__":
    asyncio.run(main())

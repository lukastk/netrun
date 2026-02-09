"""10. Epoch Record Tracking (was_cache_hit)

Each epoch records whether it was replayed from file storage.
"""

import asyncio
import tempfile

from netrun.storage.config import (
    LocalBackendConfig,
    NodeFileStorageConfig,
    OnHashChange,
    StorageConfig,
)

from _helpers import make_net, run_pipeline
from nodes import reset_call_counts


async def main():
    print("=" * 60)
    print("10. EPOCH RECORD TRACKING (was_cache_hit)")
    print("=" * 60)
    print()
    print("Each epoch records whether it was replayed from file storage.")
    print()

    reset_call_counts()

    with tempfile.TemporaryDirectory() as tmpdir:
        fs_config = NodeFileStorageConfig(
            backend=LocalBackendConfig(base_path=tmpdir + "/data"),
            serialization="pickle",
            on_hash_change=OnHashChange.overwrite,
        )
        storage = StorageConfig(file_storage_metadata_path=tmpdir + "/meta")

        # First run: execute all nodes
        async with make_net(fs_config, storage) as net:
            await run_pipeline(net, seed=42)

            print("Run 1 — epoch records:")
            for epoch_id, epoch in net.epochs.items():
                status = "REPLAYED" if epoch.was_cache_hit else "executed"
                print(f"  {epoch.node_name}: {status}")

        # Second run: all replayed
        async with make_net(fs_config, storage) as net2:
            await run_pipeline(net2, seed=42)

            print("\nRun 2 — epoch records:")
            for epoch_id, epoch in net2.epochs.items():
                status = "REPLAYED" if epoch.was_cache_hit else "executed"
                print(f"  {epoch.node_name}: {status}")


if __name__ == "__main__":
    asyncio.run(main())

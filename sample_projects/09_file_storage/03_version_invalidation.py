"""3. Version Invalidation

Bumping the version forces re-execution even with same inputs.
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
from nodes import get_call_count, reset_call_counts


async def main():
    print("=" * 60)
    print("3. VERSION INVALIDATION")
    print("=" * 60)
    print()
    print("Bumping the version forces re-execution even with same inputs.")
    print()

    reset_call_counts()

    with tempfile.TemporaryDirectory() as tmpdir:
        storage = StorageConfig(file_storage_metadata_path=tmpdir + "/meta")

        # Version 0: first run
        fs_v0 = NodeFileStorageConfig(
            backend=LocalBackendConfig(base_path=tmpdir + "/data"),
            serialization="pickle",
            on_hash_change=OnHashChange.overwrite,
            version=0,
        )

        async with make_net(fs_v0, storage) as net:
            await run_pipeline(net, seed=42)
        print(f"Version 0, run 1: generate_data called {get_call_count('generate_data')}x")

        # Version 0: second run (replay)
        reset_call_counts()
        async with make_net(fs_v0, storage) as net2:
            await run_pipeline(net2, seed=42)
        print(f"Version 0, run 2: generate_data called {get_call_count('generate_data')}x  (replayed)")

        # Version 1: forces re-execution
        reset_call_counts()
        fs_v1 = NodeFileStorageConfig(
            backend=LocalBackendConfig(base_path=tmpdir + "/data"),
            serialization="pickle",
            on_hash_change=OnHashChange.overwrite,
            version=1,
        )

        async with make_net(fs_v1, storage) as net3:
            await run_pipeline(net3, seed=42)
        print(f"Version 1, run 1: generate_data called {get_call_count('generate_data')}x  (re-executed!)")

        # Version 1: second run (replay)
        reset_call_counts()
        async with make_net(fs_v1, storage) as net4:
            await run_pipeline(net4, seed=42)
        print(f"Version 1, run 2: generate_data called {get_call_count('generate_data')}x  (replayed)")


if __name__ == "__main__":
    asyncio.run(main())

"""4. Hash Mismatch (Error Mode)

Safety mode: error when inputs change after storing.
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
    print("4. HASH MISMATCH (Error Mode)")
    print("=" * 60)
    print()
    print("Safety mode: error when inputs change after storing.")
    print()

    reset_call_counts()

    with tempfile.TemporaryDirectory() as tmpdir:
        fs_config = NodeFileStorageConfig(
            backend=LocalBackendConfig(base_path=tmpdir + "/data"),
            serialization="pickle",
            on_hash_change=OnHashChange.error,
        )
        storage = StorageConfig(file_storage_metadata_path=tmpdir + "/meta")

        # First run: store with seed=42
        async with make_net(fs_config, storage) as net:
            await run_pipeline(net, seed=42)
        print(f"Stored with seed=42: generate_data called {get_call_count('generate_data')}x")

        # Second run: different input (seed=99) -> should error
        try:
            async with make_net(fs_config, storage) as net2:
                await run_pipeline(net2, seed=99)
            print("  ERROR: Expected RuntimeError but none was raised!")
        except RuntimeError as e:
            print(f"Caught expected error: {e}")


if __name__ == "__main__":
    asyncio.run(main())

"""8. Store Inputs

Store input packets alongside outputs for auditability.
"""

import asyncio
import tempfile

from netrun.storage.config import (
    LocalBackendConfig,
    NodeFileStorageConfig,
    OnHashChange,
    StorageConfig,
)

from _helpers import list_stored_files, make_net, run_pipeline
from nodes import get_call_count, reset_call_counts


async def main():
    print("=" * 60)
    print("8. STORE INPUTS")
    print("=" * 60)
    print()
    print("Store input packets alongside outputs for auditability.")
    print()

    reset_call_counts()

    with tempfile.TemporaryDirectory() as tmpdir:
        fs_config = NodeFileStorageConfig(
            backend=LocalBackendConfig(base_path=tmpdir + "/data"),
            serialization="json",
            store_inputs=True,
            on_hash_change=OnHashChange.overwrite,
        )
        storage = StorageConfig(file_storage_metadata_path=tmpdir + "/meta")

        async with make_net(fs_config, storage) as net:
            await run_pipeline(net, seed=42)

        print(f"generate_data called: {get_call_count('generate_data')}x")
        print()
        print("Stored files (notice _inputs/ directories):")
        list_stored_files(tmpdir + "/data")


if __name__ == "__main__":
    asyncio.run(main())

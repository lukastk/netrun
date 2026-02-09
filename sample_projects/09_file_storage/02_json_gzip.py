"""2. JSON Serialization + Gzip Compression

Store outputs as compressed .json.gz files.
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
    print("2. JSON SERIALIZATION + GZIP COMPRESSION")
    print("=" * 60)
    print()
    print("Store outputs as compressed .json.gz files.")
    print()

    reset_call_counts()

    with tempfile.TemporaryDirectory() as tmpdir:
        fs_config = NodeFileStorageConfig(
            backend=LocalBackendConfig(base_path=tmpdir + "/data"),
            serialization="json",
            compression="gzip",
            on_hash_change=OnHashChange.overwrite,
        )
        storage = StorageConfig(file_storage_metadata_path=tmpdir + "/meta")

        async with make_net(fs_config, storage) as net:
            results, elapsed = await run_pipeline(net, seed=42)

        print(f"Run 1: {elapsed:.3f}s")
        print(f"  generate_data called: {get_call_count('generate_data')}x")
        print()
        print("Stored files:")
        list_stored_files(tmpdir + "/data")

        # Replay
        reset_call_counts()
        async with make_net(fs_config, storage) as net2:
            results2, _ = await run_pipeline(net2, seed=42)

        print(f"\nReplay confirmed: {results == results2}")


if __name__ == "__main__":
    asyncio.run(main())

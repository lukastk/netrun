"""1. Basic File Storage (Pickle, Local)

Store node outputs as pickle files on first run.
Second run replays from stored files without re-executing.
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
    print("1. BASIC FILE STORAGE (Pickle, Local)")
    print("=" * 60)
    print()
    print("Store node outputs as pickle files on first run.")
    print("Second run replays from stored files without re-executing.")
    print()

    reset_call_counts()

    with tempfile.TemporaryDirectory() as tmpdir:
        fs_config = NodeFileStorageConfig(
            backend=LocalBackendConfig(base_path=tmpdir + "/data"),
            serialization="pickle",
            on_hash_change=OnHashChange.overwrite,
        )
        storage = StorageConfig(file_storage_metadata_path=tmpdir + "/meta")

        # First run: execute and store
        async with make_net(fs_config, storage) as net:
            results1, elapsed1 = await run_pipeline(net, seed=42)

        print(f"Run 1: {elapsed1:.3f}s")
        print(f"  generate_data called: {get_call_count('generate_data')}x")
        print(f"  transform called:     {get_call_count('transform')}x")
        print(f"  summarize called:     {get_call_count('summarize')}x")
        print(f"  Result: count={results1[0]['count']}, mean={results1[0]['mean']:.3f}")

        # Second run: replay from file storage
        reset_call_counts()
        async with make_net(fs_config, storage) as net2:
            results2, elapsed2 = await run_pipeline(net2, seed=42)

        print(f"\nRun 2 (replayed): {elapsed2:.3f}s")
        print(f"  generate_data called: {get_call_count('generate_data')}x  (0 = replayed!)")
        print(f"  transform called:     {get_call_count('transform')}x  (0 = replayed!)")
        print(f"  summarize called:     {get_call_count('summarize')}x  (0 = replayed!)")
        print(f"  Same output: {results1 == results2}")


if __name__ == "__main__":
    asyncio.run(main())

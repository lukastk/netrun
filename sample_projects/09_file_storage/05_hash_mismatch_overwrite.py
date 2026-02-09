"""5. Hash Mismatch (Overwrite Mode)

Overwrite mode: re-executes and stores new results on input change.
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
    print("5. HASH MISMATCH (Overwrite Mode)")
    print("=" * 60)
    print()
    print("Overwrite mode: re-executes and stores new results on input change.")
    print()

    reset_call_counts()

    with tempfile.TemporaryDirectory() as tmpdir:
        fs_config = NodeFileStorageConfig(
            backend=LocalBackendConfig(base_path=tmpdir + "/data"),
            serialization="pickle",
            on_hash_change=OnHashChange.overwrite,
        )
        storage = StorageConfig(file_storage_metadata_path=tmpdir + "/meta")

        # Run 1: seed=42
        async with make_net(fs_config, storage) as net:
            results1, _ = await run_pipeline(net, seed=42)
        print(f"Run 1 (seed=42): generate_data called {get_call_count('generate_data')}x, mean={results1[0]['mean']:.3f}")

        # Run 2: seed=99 (different input -> overwrite)
        reset_call_counts()
        async with make_net(fs_config, storage) as net2:
            results2, _ = await run_pipeline(net2, seed=99)
        print(f"Run 2 (seed=99): generate_data called {get_call_count('generate_data')}x  (re-executed + overwrote)")
        print(f"  mean={results2[0]['mean']:.3f}")

        # Run 3: seed=99 again (replay)
        reset_call_counts()
        async with make_net(fs_config, storage) as net3:
            results3, _ = await run_pipeline(net3, seed=99)
        print(f"Run 3 (seed=99): generate_data called {get_call_count('generate_data')}x  (replayed)")
        print(f"  Same output: {results2 == results3}")


if __name__ == "__main__":
    asyncio.run(main())

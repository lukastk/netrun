"""9. Custom Output Names

Override default output file names with custom names.
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
    print("9. CUSTOM OUTPUT NAMES")
    print("=" * 60)
    print()
    print("Override default output file names with custom names.")
    print()

    reset_call_counts()

    with tempfile.TemporaryDirectory() as tmpdir:
        fs_config = NodeFileStorageConfig(
            backend=LocalBackendConfig(base_path=tmpdir + "/data"),
            serialization="json",
            output_names={"send": {"out": "result_data"}},
            on_hash_change=OnHashChange.overwrite,
        )
        storage = StorageConfig(file_storage_metadata_path=tmpdir + "/meta")

        async with make_net(fs_config, storage) as net:
            await run_pipeline(net, seed=42)

        print(f"generate_data called: {get_call_count('generate_data')}x")
        print()
        print("Stored files (notice custom 'result_data' name):")
        list_stored_files(tmpdir + "/data")


if __name__ == "__main__":
    asyncio.run(main())

"""Demonstrates netrun's native file storage features.

Runs all 10 demo sections in order. Each can also be run individually:

    uv run python 01_basic_file_storage.py
    uv run python 02_json_gzip.py
    ...

Pipeline:  generate_data(seed) -> transform(data) -> summarize(stats) -> [output queue: "results"]
"""

import asyncio
import importlib


DEMOS = [
    "01_basic_file_storage",
    "02_json_gzip",
    "03_version_invalidation",
    "04_hash_mismatch_error",
    "05_hash_mismatch_overwrite",
    "06_bundle_tar_gz",
    "07_bundle_zip",
    "08_store_inputs",
    "09_custom_output_names",
    "10_epoch_record_tracking",
]


async def main():
    for i, name in enumerate(DEMOS):
        if i > 0:
            print()
        mod = importlib.import_module(name)
        await mod.main()

    print()
    print("=" * 60)
    print("Done! All file storage features demonstrated.")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())

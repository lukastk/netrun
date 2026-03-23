"""Demonstrates netrun's node caching and memoization features.

This example showcases:
1. Basic memoization — cache hit skips node execution on repeated inputs
2. Cache miss — different inputs trigger re-execution
3. Cache retrieval API — inspect cached inputs and outputs
4. Cache statistics
5. Cache clearing — selectively invalidate cached entries
6. Cache modes — OUTPUT_ONLY and INPUT_ONLY
7. Per-node cache overrides
8. Version-based cache invalidation
9. Per-node cache access
10. EpochRecord.was_cache_hit tracking

Pipeline:  fetch_data -> process -> format_report -> [output queue]
"""

import asyncio
import tempfile
import time
from pathlib import Path

from netrun.core import Net, NetConfig
from netrun.storage.config import CacheConfig, CacheWhat, NodeCacheConfig, StorageConfig


# Helper to run the pipeline and return elapsed time
async def run_pipeline(net: Net, url: str) -> float:
    t0 = time.monotonic()
    net.inject_data("fetch_data", "url", [url])
    await net.run_until_blocked()
    return time.monotonic() - t0


async def main():
    config_path = Path(__file__).parent / "main.netrun.json"
    base_config = NetConfig.from_file(config_path)

    # Use a persistent temp directory so we can demonstrate cross-session caching
    cache_dir = tempfile.mkdtemp(prefix="netrun_cache_demo_")

    # ==================================================================
    # 1. BASIC MEMOIZATION
    # ==================================================================
    print("=" * 60)
    print("1. BASIC MEMOIZATION")
    print("=" * 60)
    print()
    print("Running the same pipeline twice with identical input.")
    print("Second run should be instant (served from cache).")
    print()

    from nodes import get_call_count, reset_call_counts

    reset_call_counts()

    config = NetConfig.from_file(config_path)
    config.storage = StorageConfig(cache=CacheConfig(
        enabled=True,
        include_all_nodes=True,
        storage_path=cache_dir,
    ))

    async with Net(config) as net:
        # First run — all nodes execute
        elapsed1 = await run_pipeline(net, "https://example.com/data")
        report1 = net.flush_output_queue("reports")
        print(f"Run 1: {elapsed1:.3f}s")
        print(f"  fetch_data called: {get_call_count('fetch_data')}x")
        print(f"  process called:    {get_call_count('process')}x")
        print(f"  format_report called: {get_call_count('format_report')}x")

        # Second run — same input, should hit cache for all nodes
        elapsed2 = await run_pipeline(net, "https://example.com/data")
        report2 = net.flush_output_queue("reports")
        print(f"\nRun 2 (cached): {elapsed2:.3f}s")
        print(f"  fetch_data called: {get_call_count('fetch_data')}x  (still 1 — cache hit!)")
        print(f"  process called:    {get_call_count('process')}x  (still 1 — cache hit!)")
        print(f"  format_report called: {get_call_count('format_report')}x  (still 1 — cache hit!)")
        print(f"  Same output: {report1 == report2}")

    # ==================================================================
    # 2. CACHE MISS ON DIFFERENT INPUT
    # ==================================================================
    print()
    print("=" * 60)
    print("2. CACHE MISS ON DIFFERENT INPUT")
    print("=" * 60)
    print()
    print("Running with a new URL. Nodes re-execute because the input changed.")
    print()

    reset_call_counts()

    config = NetConfig.from_file(config_path)
    config.storage = StorageConfig(cache=CacheConfig(
        enabled=True,
        include_all_nodes=True,
        storage_path=cache_dir,
    ))

    async with Net(config) as net:
        elapsed = await run_pipeline(net, "https://example.com/other")
        print(f"Run with new URL: {elapsed:.3f}s")
        print(f"  fetch_data called: {get_call_count('fetch_data')}x  (cache miss — new input)")
        print(f"  process called:    {get_call_count('process')}x")

    # ==================================================================
    # 3. CACHE RETRIEVAL API
    # ==================================================================
    print()
    print("=" * 60)
    print("3. CACHE RETRIEVAL API")
    print("=" * 60)
    print()
    print("Inspecting what's stored in the cache for each node.")
    print()

    config = NetConfig.from_file(config_path)
    config.storage = StorageConfig(cache=CacheConfig(
        enabled=True,
        include_all_nodes=True,
        storage_path=cache_dir,
    ))

    async with Net(config) as net:
        # Run twice with different inputs to populate cache
        await run_pipeline(net, "https://example.com/data")
        await run_pipeline(net, "https://example.com/other")

        # Inspect cached data for fetch_data
        print("Cached input salvos for 'fetch_data':")
        for i, salvo in enumerate(net.cache.input_salvos("fetch_data")):
            print(f"  Entry {i}: {salvo}")

        print()
        print("Cached output salvos for 'fetch_data':")
        for i, salvo in enumerate(net.cache.output_salvos("fetch_data")):
            source = salvo.get("out", [{}])[0].get("source", "?") if salvo.get("out") else "?"
            print(f"  Entry {i}: {{records: {len(salvo.get('out', [[]])[0].get('records', []))} items, source: {source}}}")

        # Lookup specific cached output
        print()
        print("Looking up cached output for specific input:")
        result = net.cache.output_for_input(
            "fetch_data",
            {"url": ["https://example.com/data"]},
        )
        if result:
            print(f"  Found! Cached at: {result.cached_at.strftime('%H:%M:%S')}")
            print(f"  Output ports: {list(result.output_port_values.keys())}")

    # ==================================================================
    # 4. CACHE STATISTICS
    # ==================================================================
    print()
    print("=" * 60)
    print("4. CACHE STATISTICS")
    print("=" * 60)
    print()

    config = NetConfig.from_file(config_path)
    config.storage = StorageConfig(cache=CacheConfig(
        enabled=True,
        include_all_nodes=True,
        storage_path=cache_dir,
    ))

    async with Net(config) as net:
        await run_pipeline(net, "https://example.com/data")
        await run_pipeline(net, "https://example.com/other")
        await run_pipeline(net, "https://example.com/data")  # cache hit

        stats = net.cache.stats()
        for node_name, node_stats in sorted(stats.items()):
            print(f"  {node_name}:")
            print(f"    Cached entries: {node_stats['entry_count']}")
            print(f"    Epochs seen:    {node_stats['epoch_count']}")

    # ==================================================================
    # 5. CACHE CLEARING
    # ==================================================================
    print()
    print("=" * 60)
    print("5. CACHE CLEARING")
    print("=" * 60)
    print()
    print("Selectively clearing cached entries.")
    print()

    reset_call_counts()

    config = NetConfig.from_file(config_path)
    config.storage = StorageConfig(cache=CacheConfig(
        enabled=True,
        include_all_nodes=True,
        storage_path=cache_dir,
    ))

    async with Net(config) as net:
        # These should both be cache hits from earlier
        await run_pipeline(net, "https://example.com/data")
        await run_pipeline(net, "https://example.com/other")
        print(f"After 2 cached runs: fetch_data called {get_call_count('fetch_data')}x")

        # Clear only the first URL's cache
        net.cache.clear_for_input(
            "fetch_data",
            {"url": ["https://example.com/data"]},
        )
        print("Cleared cache for fetch_data with url='https://example.com/data'")

        # Re-run first URL — fetch_data re-executes, but other URL still cached
        await run_pipeline(net, "https://example.com/data")
        print(f"After re-run: fetch_data called {get_call_count('fetch_data')}x  (re-executed)")

        # Re-run second URL — still cached
        await run_pipeline(net, "https://example.com/other")
        print(f"After cached run: fetch_data called {get_call_count('fetch_data')}x  (still cached)")

    # ==================================================================
    # 6. CACHE MODES: OUTPUT_ONLY
    # ==================================================================
    print()
    print("=" * 60)
    print("6. CACHE MODE: OUTPUT_ONLY")
    print("=" * 60)
    print()
    print("OUTPUT_ONLY always executes the node, but records outputs")
    print("for later inspection. Useful for logging/debugging.")
    print()

    reset_call_counts()

    output_cache_dir = tempfile.mkdtemp(prefix="netrun_output_cache_")
    config = NetConfig.from_file(config_path)
    config.storage = StorageConfig(cache=CacheConfig(
        enabled=True,
        include_all_nodes=True,
        cache_what=CacheWhat.OUTPUT_ONLY,
        storage_path=output_cache_dir,
    ))

    async with Net(config) as net:
        await run_pipeline(net, "https://example.com/data")
        await run_pipeline(net, "https://example.com/data")  # executes again (OUTPUT_ONLY)
        print(f"fetch_data called: {get_call_count('fetch_data')}x  (both runs executed)")

        outputs = net.cache.output_salvos("process")
        print(f"Recorded output salvos for 'process': {len(outputs)}")
        for i, out in enumerate(outputs):
            mean = out.get("out", [{}])[0].get("mean", "?") if out.get("out") else "?"
            print(f"  Run {i+1}: mean={mean}")

    # ==================================================================
    # 7. CACHE MODE: INPUT_ONLY
    # ==================================================================
    print()
    print("=" * 60)
    print("7. CACHE MODE: INPUT_ONLY")
    print("=" * 60)
    print()
    print("INPUT_ONLY always executes but records inputs for replay/testing.")
    print()

    reset_call_counts()

    input_cache_dir = tempfile.mkdtemp(prefix="netrun_input_cache_")
    config = NetConfig.from_file(config_path)
    config.storage = StorageConfig(cache=CacheConfig(
        enabled=True,
        include_all_nodes=True,
        cache_what=CacheWhat.INPUT_ONLY,
        storage_path=input_cache_dir,
    ))

    async with Net(config) as net:
        await run_pipeline(net, "https://example.com/data")
        await run_pipeline(net, "https://example.com/other")

        inputs = net.cache.input_salvos("process")
        print(f"Recorded input salvos for 'process': {len(inputs)}")
        for i, inp in enumerate(inputs):
            source = inp.get("data", [{}])[0].get("source", "?") if inp.get("data") else "?"
            print(f"  Run {i+1}: data.source={source}")

    # ==================================================================
    # 8. VERSION-BASED INVALIDATION
    # ==================================================================
    print()
    print("=" * 60)
    print("8. VERSION-BASED INVALIDATION")
    print("=" * 60)
    print()
    print("Changing the cache version invalidates all existing entries.")
    print()

    reset_call_counts()

    version_cache_dir = tempfile.mkdtemp(prefix="netrun_version_cache_")

    # Version 0
    config = NetConfig.from_file(config_path)
    config.storage = StorageConfig(cache=CacheConfig(
        enabled=True,
        include_all_nodes=True,
        version=0,
        storage_path=version_cache_dir,
    ))

    async with Net(config) as net:
        await run_pipeline(net, "https://example.com/data")
        print(f"Version 0, run 1: fetch_data called {get_call_count('fetch_data')}x")

        await run_pipeline(net, "https://example.com/data")
        print(f"Version 0, run 2: fetch_data called {get_call_count('fetch_data')}x  (cached)")

    # Bump version — same storage path, but entries won't match
    reset_call_counts()

    config = NetConfig.from_file(config_path)
    config.storage = StorageConfig(cache=CacheConfig(
        enabled=True,
        include_all_nodes=True,
        version=1,  # bumped!
        storage_path=version_cache_dir,
    ))

    async with Net(config) as net:
        await run_pipeline(net, "https://example.com/data")
        print(f"Version 1, run 1: fetch_data called {get_call_count('fetch_data')}x  (cache miss!)")

        await run_pipeline(net, "https://example.com/data")
        print(f"Version 1, run 2: fetch_data called {get_call_count('fetch_data')}x  (cached under v1)")

    # ==================================================================
    # 9. PER-NODE CACHE ACCESS
    # ==================================================================
    print()
    print("=" * 60)
    print("9. PER-NODE CACHE ACCESS")
    print("=" * 60)
    print()
    print("Access cache data through the net.cache API.")
    print()

    config = NetConfig.from_file(config_path)
    config.storage = StorageConfig(cache=CacheConfig(
        enabled=True,
        include_all_nodes=True,
        storage_path=cache_dir,
    ))

    async with Net(config) as net:
        await run_pipeline(net, "https://example.com/data")

        print(f"fetch_data.is_cache_enabled: {net.cache.is_enabled('fetch_data')}")
        print(f"fetch_data.cache_stats: {net.cache.stats('fetch_data')}")
        print(f"fetch_data.cached_entries: {len(net.cache.entries('fetch_data'))} entries")

        print(f"\nprocess.cached_input_salvos: {len(net.cache.input_salvos('process'))} entries")
        print(f"process.cached_output_salvos: {len(net.cache.output_salvos('process'))} entries")

        # Look up cached output for specific input
        result = net.cache.output_for_input("fetch_data", {"url": ["https://example.com/data"]})
        if result:
            print(f"\nCache lookup succeeded: cached at {result.cached_at.strftime('%H:%M:%S')}")

    # ==================================================================
    # 10. EPOCH RECORD TRACKING
    # ==================================================================
    print()
    print("=" * 60)
    print("10. EPOCH RECORD TRACKING (was_cache_hit)")
    print("=" * 60)
    print()
    print("Each epoch records whether it was served from cache.")
    print()

    epoch_cache_dir = tempfile.mkdtemp(prefix="netrun_epoch_cache_")
    config = NetConfig.from_file(config_path)
    config.storage = StorageConfig(cache=CacheConfig(
        enabled=True,
        include_all_nodes=True,
        storage_path=epoch_cache_dir,
    ))

    async with Net(config) as net:
        await run_pipeline(net, "https://example.com/data")
        await run_pipeline(net, "https://example.com/data")  # cache hit

        print("Epoch records:")
        for epoch_id, epoch in net.epochs.items():
            hit = "CACHE HIT" if epoch.was_cache_hit else "executed"
            print(f"  {epoch.node_name}: {hit}")

    print()
    print("=" * 60)
    print("Done! All caching features demonstrated.")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())

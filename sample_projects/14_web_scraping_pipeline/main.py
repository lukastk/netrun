"""
Web Scraping Pipeline — netrun feature showcase.

Demonstrates all major netrun features in a single pipeline:

  [load_config] → [validate_config] → [broadcast_config]
                                        /      |       \\
                              [scrape_news] [scrape_weather] [scrape_stocks]
                                        \\      |       /
                                       [join_results]
                                            |
                                       [analyze]
                                            |
  [setup_output] ─────────────────→ [export_report] → output_queue: results

Features used:
  - Async node functions on thread pool (shared event loop)
  - ctx.state: caches expensive computation across retries
  - depends_on: setup_output waits for load_config; export_report waits for setup_output
  - resources: http_conn=2 limits concurrent scrapes to 2 of 3
  - Broadcast factory: fans config to 3 parallel scrapers
  - Join factory: collects results from all 3 scrapers
  - Retries: scrape_stocks simulates intermittent failure
  - Structured logging: ctx.log() with key=value fields
  - Node variables: site URLs configurable via ctx.vars
  - Output queues: final report collected via flush_output_queue
  - Signals: epoch_finished/epoch_failed on all nodes
"""

import asyncio
import time
from pathlib import Path

from netrun.core import Net, NetConfig


async def main():
    config_path = Path(__file__).parent / "main.netrun.json"
    config = NetConfig.from_file(config_path)

    print("=" * 60)
    print("Web Scraping Pipeline")
    print("=" * 60)
    print()

    start = time.monotonic()

    async with Net(config) as net:
        # setup_output has no incoming data edge — inject a trigger packet.
        # It's gated by depends_on: ["load_config"], so it won't start
        # until load_config completes (which runs on init).
        net.inject_data("setup_output", "trigger", ["go"])

        # Run the full pipeline
        await net.run_until_blocked()

        elapsed = time.monotonic() - start

        # Collect results
        results = net.flush_output_queue("results")

        print()
        print("=" * 60)
        print("Pipeline Results")
        print("=" * 60)

        if results:
            report = results[0]
            print(f"  Total items scraped: {report['total_items']}")
            print(f"  Sources: {report['sources']}")
            print(f"  Breakdown: {report['summary']}")
        else:
            print("  No results (check dead letter queue)")
            if net.dead_letter_queue:
                for dl in net.dead_letter_queue:
                    print(f"  FAILED: {dl['node_name']} — {dl['error']}")

        print()
        print(f"  Elapsed: {elapsed:.2f}s")

        # Show all node print logs (includes multiprocess workers whose
        # ctx.print output doesn't echo to stdout directly)
        # Print all captured node output (includes multiprocess workers
        # whose ctx.print doesn't echo to terminal directly)
        print()
        print("All Node Output (via ctx.print):")
        net.logs.print_all()

        # Show structured logs
        print()
        print("Structured Logs:")
        for epoch_id, record in net._epochs.items():
            for log_entry in record.structured_logs:
                print(f"  [{record.node_name}] {log_entry.message} | {log_entry.fields}")

        # Show resource usage worked (at most 2 concurrent HTTP connections)
        print()
        print(f"  Pool types: main (SingleWorkerPool), scrape (ThreadPool x3), compute (MultiprocessPool x1)")
        print(f"  Resource capacities: {net._resource_capacities}")
        print(f"  Resource usage (should be 0 after completion): {net._resource_usage}")

        print()
        print("Done!")


if __name__ == "__main__":
    asyncio.run(main())

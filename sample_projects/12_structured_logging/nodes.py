"""Node functions for the structured logging example.

Demonstrates using the `log` special parameter for structured wide events.
Each ctx.log() call emits key=value fields that are merged into the epoch's
canonical log line (EpochLog) at completion.
"""

import time
import random


def fetch_data(url: str, log) -> dict:
    """Simulate fetching data from a URL."""
    log("Starting fetch", url=url)
    # Simulate network latency
    latency = random.uniform(0.05, 0.15)
    time.sleep(latency)
    records = [{"id": i, "value": random.randint(1, 100)} for i in range(5)]
    log("Fetch complete", record_count=len(records), latency_ms=round(latency * 1000, 1))
    return {"source": url, "records": records}


def process(data: dict, log) -> dict:
    """Process fetched data — compute summary statistics."""
    values = [r["value"] for r in data["records"]]
    total = sum(values)
    mean = total / len(values)
    maximum = max(values)
    minimum = min(values)
    log("Computed stats", source=data["source"], mean=round(mean, 1), min=minimum, max=maximum)
    return {"mean": mean, "total": total, "count": len(values), "source": data["source"]}


def format_report(stats: dict, log) -> str:
    """Format statistics into a human-readable report."""
    report = (
        f"Report for {stats['source']}:\n"
        f"  Records: {stats['count']}\n"
        f"  Mean:    {stats['mean']:.1f}\n"
        f"  Total:   {stats['total']}"
    )
    log("Report generated", source=stats["source"], report_length=len(report))
    return report

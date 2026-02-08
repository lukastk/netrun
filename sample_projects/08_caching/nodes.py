"""Node functions for the caching example.

These functions simulate expensive computations. The caching system will
memoize their results so repeated calls with the same input are served
from cache without re-executing the function.
"""

import time

# Track call counts so we can demonstrate cache hits
_call_counts: dict[str, int] = {}


def get_call_count(name: str) -> int:
    return _call_counts.get(name, 0)


def reset_call_counts() -> None:
    _call_counts.clear()


def fetch_data(url: str, print) -> dict:
    """Simulate fetching data from a remote source (expensive I/O)."""
    _call_counts["fetch_data"] = _call_counts.get("fetch_data", 0) + 1
    print(f"Fetching data from {url}...")
    time.sleep(0.1)  # simulate network latency
    data = {
        "source": url,
        "records": [
            {"name": "Alice", "score": 85},
            {"name": "Bob", "score": 92},
            {"name": "Charlie", "score": 78},
        ],
    }
    print(f"Fetched {len(data['records'])} records")
    return data


def process(data: dict, print) -> dict:
    """Process fetched data by computing statistics (expensive CPU work)."""
    _call_counts["process"] = _call_counts.get("process", 0) + 1
    print(f"Processing {len(data['records'])} records...")
    time.sleep(0.1)  # simulate heavy computation
    scores = [r["score"] for r in data["records"]]
    result = {
        "source": data["source"],
        "count": len(scores),
        "mean": sum(scores) / len(scores),
        "min": min(scores),
        "max": max(scores),
        "records": data["records"],
    }
    print(f"Computed stats: mean={result['mean']:.1f}, min={result['min']}, max={result['max']}")
    return result


def format_report(stats: dict, print) -> str:
    """Format processed stats into a human-readable report."""
    _call_counts["format_report"] = _call_counts.get("format_report", 0) + 1
    print("Formatting report...")
    lines = [
        f"=== Report for {stats['source']} ===",
        f"Records: {stats['count']}",
        f"Score range: {stats['min']} - {stats['max']}",
        f"Average score: {stats['mean']:.1f}",
        "",
        "Details:",
    ]
    for r in stats["records"]:
        lines.append(f"  {r['name']}: {r['score']}")
    report = "\n".join(lines)
    print(f"Report generated ({len(lines)} lines)")
    return report

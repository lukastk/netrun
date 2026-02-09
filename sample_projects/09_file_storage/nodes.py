"""Node functions for the file storage example.

These functions form a simple data pipeline:
  generate_data -> transform -> summarize

Each function tracks its call count so we can verify when file storage
replays outputs without re-executing the function.
"""

import random

# Track call counts to demonstrate file storage replays
_call_counts: dict[str, int] = {
    "generate_data": 0,
    "transform": 0,
    "summarize": 0,
}


def get_call_count(name: str) -> int:
    return _call_counts[name]


def reset_call_counts() -> None:
    for key in _call_counts:
        _call_counts[key] = 0


def generate_data(seed: int, print) -> dict:
    """Generate synthetic data from a seed."""
    _call_counts["generate_data"] += 1
    print(f"Generating data from seed={seed}")
    rng = random.Random(seed)
    records = [{"id": i, "value": rng.random()} for i in range(5)]
    return {"source": f"seed_{seed}", "records": records}


def transform(data: dict, print) -> dict:
    """Transform data: double values, add metadata."""
    _call_counts["transform"] += 1
    print(f"Transforming {len(data['records'])} records")
    transformed = [
        {"id": r["id"], "value": r["value"] * 2, "label": f"item_{r['id']}"}
        for r in data["records"]
    ]
    return {"source": data["source"], "records": transformed, "transform": "doubled"}


def summarize(stats: dict, print) -> dict:
    """Summarize: compute count, mean, min, max."""
    _call_counts["summarize"] += 1
    values = [r["value"] for r in stats["records"]]
    summary = {
        "source": stats["source"],
        "count": len(values),
        "mean": sum(values) / len(values),
        "min": min(values),
        "max": max(values),
    }
    print(f"Summary: count={summary['count']}, mean={summary['mean']:.3f}")
    return summary

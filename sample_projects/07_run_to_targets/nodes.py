"""Node functions for the run_to_targets example.

A data processing pipeline:
  fetch_data -> validate -> transform -> enrich -> format_output

Each node does a distinct processing step, making it easy to demonstrate
how run_to_targets lets you test any node in isolation by running only
its upstream dependencies.
"""


def fetch_data(url: str, print) -> dict:
    """Simulate fetching data from a URL."""
    print(f"Fetching data from: {url}")
    # Simulate fetched data
    data = {
        "source": url,
        "records": [
            {"name": "Alice", "age": 30, "score": 85},
            {"name": "Bob", "age": -5, "score": 92},
            {"name": "Charlie", "age": 25, "score": 110},
        ],
    }
    print(f"Fetched {len(data['records'])} records")
    return data


def validate(data: dict, print) -> dict:
    """Validate records, filtering out invalid ones."""
    print(f"Validating {len(data['records'])} records")
    valid = []
    rejected = 0
    for record in data["records"]:
        if record["age"] < 0 or record["score"] > 100:
            print(f"  Rejected: {record['name']} (age={record['age']}, score={record['score']})")
            rejected += 1
        else:
            valid.append(record)
    print(f"Validation complete: {len(valid)} valid, {rejected} rejected")
    return {"source": data["source"], "records": valid}


def transform(data: dict, print) -> dict:
    """Transform records by normalizing scores to 0-1 range."""
    print(f"Transforming {len(data['records'])} records")
    transformed = []
    for record in data["records"]:
        transformed.append({
            **record,
            "score_normalized": round(record["score"] / 100, 2),
            "grade": "A" if record["score"] >= 90 else "B" if record["score"] >= 80 else "C",
        })
    print(f"Transformation complete")
    return {"source": data["source"], "records": transformed}


def enrich(data: dict, print) -> dict:
    """Enrich records with additional computed fields."""
    print(f"Enriching {len(data['records'])} records")
    enriched = []
    for record in data["records"]:
        enriched.append({
            **record,
            "label": f"{record['name']} (Grade {record['grade']})",
        })
    print(f"Enrichment complete")
    return {"source": data["source"], "records": enriched}


def format_output(data: dict, print) -> str:
    """Format the final output as a summary string."""
    print(f"Formatting output for {len(data['records'])} records")
    lines = [f"Results from {data['source']}:"]
    for record in data["records"]:
        lines.append(f"  {record['label']}: score={record['score']} ({record['score_normalized']})")
    result = "\n".join(lines)
    print("Formatting complete")
    return result

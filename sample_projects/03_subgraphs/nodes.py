"""Node functions for the subgraph example.

Demonstrates nodes used within subgraphs — same function factory pattern,
but these functions will be used inside inline, file-referenced, and
factory-generated subgraphs.
"""


def generate_data(seed: int, print) -> dict:
    """Source node that generates sample data from a seed value."""
    data = {"value": seed, "label": "sample"}
    print(f"Generated: {data}")
    return data


def validate(data: dict, print) -> dict:
    """Validate input data."""
    print(f"Validating: {data}")
    assert "value" in data, "Missing 'value' key"
    return data


def normalize(data: dict, print) -> dict:
    """Normalize values in the data."""
    print(f"Normalizing: {data}")
    result = {**data, "value": data["value"] / 100.0}
    print(f"Normalized: {result}")
    return result


def transform(data: dict, print) -> dict:
    """Apply a transformation."""
    print(f"Transforming: {data}")
    result = {**data, "value": data["value"] * 2}
    print(f"Transformed: {result}")
    return result


def enrich(data: dict, print) -> dict:
    """Enrich data with additional fields."""
    print(f"Enriching: {data}")
    result = {**data, "enriched": True}
    return result


def format_output(data: dict, print) -> str:
    """Format data for final output."""
    print(f"Formatting: {data}")
    return f"{data.get('label', '?')}: {data['value']} (enriched={data.get('enriched', False)})"


def extract_features(print, data: dict) -> {"features.color": str, "features.shape": str, "features.size": float}:
    """Extract features from data into separate output ports.

    Demonstrates port groups: the dot-separated port names (features.color,
    features.shape, features.size) are rendered as a collapsible 'features'
    group in netrun-ui.
    """
    print(f"Extracting features from: {data}")
    return {
        "features.color": data.get("color", "red"),
        "features.shape": data.get("shape", "circle"),
        "features.size": float(data.get("size", 1.0)),
    }


def classify(print, **kwargs) -> str:
    """Classify based on feature inputs.

    Demonstrates nested port groups: the dot-separated input port names
    (batch.features.color, batch.features.shape, batch.labels.expected)
    form a nested group hierarchy in netrun-ui.
    """
    color = kwargs.get("batch.features.color", "unknown")
    shape = kwargs.get("batch.features.shape", "unknown")
    expected = kwargs.get("batch.labels.expected", "unknown")
    result = f"{color}-{shape} (expected: {expected})"
    print(f"Classified: {result}")
    return result

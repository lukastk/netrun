"""Node functions for the sample project."""


def fetch_data(url: str) -> dict:
    """Fetches data from a URL and returns it as a dictionary."""
    return {"url": url, "status": 200, "body": f"Response from {url}"}


def transform(data: dict, print) -> {"result": str, "metadata": str}:
    """Transforms input data into a result and metadata."""
    print(f"Transforming: {data}")
    body = data.get("body", "")
    return {
        "result": body.upper(),
        "metadata": f"source={data.get('url', 'unknown')}, status={data.get('status', 0)}",
    }


def save_result(result: str, print) -> str:
    """Saves the result and returns a confirmation."""
    print(f"Saving: {result}")
    return f"saved:{result[:50]}"

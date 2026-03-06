"""Node functions for the packet requests example.

Each function becomes a node via the function factory. Parameters become input
ports (except 'print' which is the captured print function). Return type
determines the output port.
"""


# --- Scenario 1: On-Startup Pull ---

def data_source(print) -> str:
    """Produces initial dataset when pulled via dependency request."""
    print("Fetching dataset...")
    return "dataset_v1"


def consumer(data: str, print) -> str:
    """Consumes pulled data."""
    print(f"Received: {data}")
    return f"consumed({data})"


# --- Scenario 2: Manual Request ---

def generator(print) -> int:
    """Generates a seed value when requested."""
    print("Generating seed...")
    return 42


def processor(seed: int, print) -> str:
    """Processes a seed into a result."""
    print(f"Processing seed {seed}")
    return f"result_{seed * 2}"


def reporter(result: str, print) -> str:
    """Creates a report from processed result."""
    print(f"Reporting: {result}")
    return f"[REPORT] {result}"


# --- Scenario 3: Hybrid Push-Pull ---

def config_source(print) -> str:
    """Provides configuration when pulled."""
    print("Loading config...")
    return "theme=dark,lang=en"


def merger(config: str, live: str, print) -> str:
    """Merges pulled config with pushed live data."""
    print(f"Merging config='{config}' with live='{live}'")
    return f"{live} (config: {config})"

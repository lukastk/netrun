"""Node functions demonstrating advanced flow control features."""


def batch_processor(data: list[str], print) -> str:
    """Processes a batch of items.

    Has a custom input salvo condition: fires only when the 'data' port
    has exactly 3 packets accumulated (uses equals(3) predicate).
    The port config also uses finite slots to limit buffering.
    """
    print(f"Processing batch of {len(data)} items: {data}")
    return f"batch({', '.join(data)})"


def rate_limited_worker(data: str, print) -> str:
    """A rate-limited node.

    Configured with rate_limit_per_second=2 to throttle execution.
    """
    if not hasattr(rate_limited_worker, "_count"):
        rate_limited_worker._count = 0
    rate_limited_worker._count += 1
    count = rate_limited_worker._count
    print(f"Processing item {count}: {data}")
    return f"[{count}] {data}"


def multi_output(value: int, print) -> {"main_out": str, "debug_out": str, "extra_out": str}:
    """A node with multiple output ports.

    Only 'main_out' is configured in a named output queue.
    'debug_out' and 'extra_out' go to the catch-all queue.
    """
    print(f"Processing value: {value}")
    return {
        "main_out": f"result={value * 2}",
        "debug_out": f"debug: input was {value}",
        "extra_out": f"extra metadata for {value}",
    }

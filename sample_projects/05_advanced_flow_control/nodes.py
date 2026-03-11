"""Node functions demonstrating advanced flow control features."""

from netrun.node_factories.from_function import Batch


def batch_processor(data: Batch(str, count=3), print) -> str:
    """Processes a batch of items.

    Has a custom input salvo condition: fires when the 'data' port
    has 3 or more packets accumulated (uses equals_or_greater_than(3) predicate).
    The Batch annotation tells the factory this port collects multiple packets.
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

    'main_out' goes to the 'results' output queue.
    'debug_out' and 'extra_out' go to the 'other_outputs' queue.
    """
    print(f"Processing value: {value}")
    return {
        "main_out": f"result={value * 2}",
        "debug_out": f"debug: input was {value}",
        "extra_out": f"extra metadata for {value}",
    }

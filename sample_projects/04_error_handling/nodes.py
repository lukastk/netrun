"""Node functions demonstrating error handling features."""

import time


def flaky_node(data: str, print) -> str:
    """A node that fails on the first two attempts, then succeeds.

    Demonstrates retries with retry_wait.
    """
    # Use a module-level counter to track attempts across retries
    if not hasattr(flaky_node, "_attempts"):
        flaky_node._attempts = 0
    flaky_node._attempts += 1

    if flaky_node._attempts <= 2:
        print(f"Attempt {flaky_node._attempts}: failing...")
        raise ValueError(f"Transient error (attempt {flaky_node._attempts})")

    print(f"Attempt {flaky_node._attempts}: success!")
    result = data.upper()
    flaky_node._attempts = 0  # Reset for next run
    return result


def on_failure(ctx):
    """Callback invoked when a node fails.

    Demonstrates on_node_failure — receives a NodeFailureContext with
    epoch_id, node_name, exception, retry_count, etc.
    """
    print(f"[on_failure] Node '{ctx.node_name}' failed on attempt {ctx.retry_count + 1}: {ctx.exception}")


def run_once(data: str, print) -> str:
    """A node that should only execute once (max_epochs=1)."""
    print(f"Processing (single run): {data}")
    return f"processed: {data}"


def cancelling_node(data: str, print, ctx) -> str:
    """A node that cancels itself via ctx.cancel_epoch()."""
    print(f"Received: {data}")
    if data == "cancel":
        print("Cancelling epoch!")
        ctx.cancel_epoch()  # Raises EpochCancelled, epoch is discarded
    return data.upper()


def typed_output(value: int, print) -> int:
    """A node with typed output port.

    When type_checking_enabled=True, returning a wrong type
    causes PacketTypeMismatch.
    """
    print(f"Input value: {value}")
    # Intentionally return wrong type to trigger PacketTypeMismatch
    return str(value)  # type: ignore — deliberate mismatch for demo


def slow_node(data: str, print) -> str:
    """A node that takes too long, demonstrating timeout enforcement.

    Must run on a thread pool (not main) so the timeout can interrupt
    the blocking sleep.
    """
    print(f"Starting slow processing of: {data}")
    time.sleep(10)  # Will be interrupted by timeout
    return data.upper()


def quiet_fail(data: str, print) -> str:
    """A node with propagate_exceptions=False that always fails.

    Exceptions are queued instead of propagated, so the net keeps running.
    """
    print(f"About to fail on: {data}")
    raise RuntimeError(f"Quiet failure on '{data}'")

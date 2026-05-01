"""Node demonstrating ctx.state for retry-persistent caching.

The expensive_pipeline node simulates a workflow that:
1. Loads heavy data (slow on the first attempt)
2. Then runs flaky logic that fails on the first try and succeeds on retry

Without ctx.state, retry #2 would reload the heavy data — wasted work.
With ctx.state, the loaded data is cached on the same dict instance reused
across retries, so the second attempt skips straight to the flaky step.
"""

import time


def _load_heavy_data() -> list[int]:
    """Simulates a slow / expensive data load."""
    time.sleep(0.5)
    return list(range(1000))


def expensive_pipeline(ctx, x: int, print) -> int:
    # First-attempt-only work cached in ctx.state.
    # Same dict instance survives across retries; cleared on success/permanent fail.
    if "heavy" not in ctx.state:
        print("Loading heavy data (slow)...")
        ctx.state["heavy"] = _load_heavy_data()
    else:
        print("Reusing heavy data from previous attempt — no reload.")

    heavy = ctx.state["heavy"]

    # Flaky logic: fail on attempt 1, succeed on attempt 2.
    attempt = ctx.state.get("attempts", 0) + 1
    ctx.state["attempts"] = attempt
    print(f"Attempt #{attempt}")

    if attempt == 1:
        raise RuntimeError("Transient failure on first attempt")

    return sum(heavy) + x

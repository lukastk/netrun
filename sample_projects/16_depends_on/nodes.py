"""Three independent nodes ordered with depends_on.

There are NO data edges between A, B, C — the only thing forcing them to
run in order A → B → C is the `depends_on` field on each node's
execution_config.

Each node simulates a side-effecting setup step (e.g. provisioning,
schema migration, warmup) where ordering matters but data doesn't flow.
"""

import time


def step_a(trigger, print) -> str:
    print("step_a: starting (no dependencies)")
    time.sleep(0.05)
    print("step_a: done")
    return "A complete"


def step_b(trigger, print) -> str:
    print("step_b: starting (depends on A)")
    time.sleep(0.05)
    print("step_b: done")
    return "B complete"


def step_c(trigger, print) -> str:
    print("step_c: starting (depends on B)")
    time.sleep(0.05)
    print("step_c: done")
    return "C complete"

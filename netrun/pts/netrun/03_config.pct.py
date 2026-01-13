# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Node Configuration
#
# Configuration dataclasses for nodes and their execution settings.

# %%
#|default_exp config

# %%
#|hide
from nblite import nbl_export, show_doc; nbl_export();

# %%
#|export
from dataclasses import dataclass
from typing import Any, Callable, List, Optional, Union


@dataclass
class NodeConfig:
    """
    Configuration for a node's execution behavior.

    Attributes:
        pool: Name of thread/process pool to use for execution. Can be a single
            name or a list of pool names (least busy pool is selected).
        max_parallel_epochs: Maximum number of epochs that can run simultaneously
            for this node. None means unlimited.
        rate_limit_per_second: Maximum epoch starts per second. None means no limit.
        defer_net_actions: If True, buffer all packet operations until successful
            completion. Required when retries > 0 to enable rollback on failure.
        retries: Number of retry attempts after failure (0 = no retries).
            Requires defer_net_actions=True.
        retry_wait: Seconds to wait between retry attempts.
        timeout: Maximum execution time in seconds. None means no timeout.
        dead_letter_queue: If True, failed epochs are added to the dead letter queue.
        capture_stdout: If True, capture print() output during execution.
        echo_stdout: If True, also print captured output to real stdout.
        pool_init_mode: When to call start_func - "per_worker" calls it for each
            pool worker, "global" calls it once when the net starts.
    """
    pool: Optional[Union[str, List[str]]] = None
    max_parallel_epochs: Optional[int] = None
    rate_limit_per_second: Optional[float] = None
    defer_net_actions: bool = False
    retries: int = 0
    retry_wait: float = 0.0
    timeout: Optional[float] = None
    dead_letter_queue: bool = True
    capture_stdout: bool = True
    echo_stdout: bool = False
    pool_init_mode: str = "per_worker"  # "per_worker" or "global"

    def __post_init__(self):
        # Enforce constraint: retries > 0 requires defer_net_actions
        if self.retries > 0 and not self.defer_net_actions:
            raise ValueError(
                "defer_net_actions must be True when retries > 0"
            )


@dataclass
class NodeExecFuncs:
    """
    Execution functions for a node.

    Attributes:
        exec_func: Main execution function called for each epoch.
            Signature: (ctx: NodeExecutionContext, packets: dict[str, list[Packet]]) -> None
        start_func: Called when the net starts (before any epochs run).
            Signature: () -> None
        stop_func: Called when the net stops (after all epochs complete).
            Signature: () -> None
        failed_func: Called after each failed execution attempt (including retries).
            Signature: (ctx: NodeFailureContext) -> None
    """
    exec_func: Optional[Callable] = None
    start_func: Optional[Callable] = None
    stop_func: Optional[Callable] = None
    failed_func: Optional[Callable] = None

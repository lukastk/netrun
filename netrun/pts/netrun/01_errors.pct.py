# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Error Types
#
# This module defines all error types for `netrun`. It re-exports errors from `netrun_sim`
# and defines `netrun`-specific error types for higher-level execution concerns.

# %%
#|default_exp errors

# %%
#|hide
from nblite import nbl_export, show_doc; nbl_export();

# %% [markdown]
# ## Re-exports from netrun_sim
#
# We re-export all error types from `netrun_sim` so users can import everything from `netrun`.

# %%
#|export
from netrun_sim import (
    # Base error
    NetrunError,
    # Graph validation errors
    GraphValidationError,
    # Action errors (InvalidAction in Rust)
    NodeNotFoundError,
    EdgeNotFoundError,
    InputPortNotFoundError,
    OutputPortNotFoundError,
    SalvoConditionNotFoundError,
    PacketNotFoundError,
    EpochNotFoundError,
    EpochNotStartableError,
    EpochNotRunningError,
    CannotFinishNonEmptyEpochError,
    CannotMovePacketFromRunningEpochError,
    CannotMovePacketIntoRunningEpochError,
    PacketNotInNodeError,
    PacketNotAtInputPortError,
    InputPortFullError,
    OutputPortFullError,
    SalvoConditionNotMetError,
    UnconnectedOutputPortError,
    MaxSalvosExceededError,
    UnsentOutputSalvoError,
)

# %% [markdown]
# ## netrun-specific Error Types
#
# These errors are specific to `netrun` (not `netrun-sim`) and handle higher-level
# execution concerns.

# %%
#|export
class NetrunRuntimeError(Exception):
    """Base class for netrun runtime errors (distinct from netrun_sim errors)."""
    pass


class PacketTypeMismatch(NetrunRuntimeError):
    """Raised when a packet value doesn't match the expected port type."""
    def __init__(self, packet_id, expected_type, actual_type, port_name=None):
        self.packet_id = packet_id
        self.expected_type = expected_type
        self.actual_type = actual_type
        self.port_name = port_name
        port_info = f" on port '{port_name}'" if port_name else ""
        super().__init__(
            f"Packet {packet_id}{port_info}: expected type {expected_type}, got {actual_type}"
        )


class ValueFunctionFailed(NetrunRuntimeError):
    """Raised when a packet's value function raises an exception."""
    def __init__(self, packet_id, original_exception):
        self.packet_id = packet_id
        self.original_exception = original_exception
        super().__init__(
            f"Value function for packet {packet_id} failed: {original_exception}"
        )


class NodeExecutionFailed(NetrunRuntimeError):
    """Raised when a node's exec function raises an exception."""
    def __init__(self, node_name, epoch_id, original_exception):
        self.node_name = node_name
        self.epoch_id = epoch_id
        self.original_exception = original_exception
        super().__init__(
            f"Node '{node_name}' (epoch {epoch_id}) execution failed: {original_exception}"
        )


class EpochTimeout(NetrunRuntimeError):
    """Raised when an epoch exceeds its configured timeout."""
    def __init__(self, node_name, epoch_id, timeout_seconds):
        self.node_name = node_name
        self.epoch_id = epoch_id
        self.timeout_seconds = timeout_seconds
        super().__init__(
            f"Node '{node_name}' (epoch {epoch_id}) timed out after {timeout_seconds}s"
        )


class EpochCancelled(NetrunRuntimeError):
    """Raised when an epoch is cancelled via ctx.cancel_epoch()."""
    def __init__(self, node_name, epoch_id):
        self.node_name = node_name
        self.epoch_id = epoch_id
        super().__init__(f"Epoch {epoch_id} for node '{node_name}' was cancelled")


class NetNotPausedError(NetrunRuntimeError):
    """Raised when an operation requires the net to be paused but it isn't."""
    def __init__(self, operation):
        self.operation = operation
        super().__init__(f"Operation '{operation}' requires the net to be paused")


class DeferredPacketIdAccessError(NetrunRuntimeError):
    """Raised when trying to access the ID of a deferred packet before commit."""
    def __init__(self):
        super().__init__(
            "Cannot access packet ID before deferred actions are committed. "
            "The packet ID is assigned when the epoch completes successfully."
        )

# ---
# ---

# %% [markdown]
# # netrun
#
# A flow-based development (FBD) runtime system.
#
# `netrun` is a pure Python package built on top of `netrun-sim`. It provides:
# - Actual node execution logic via configurable execution functions
# - Packet value storage and retrieval
# - Thread and process pool management for parallel execution
# - Error handling with retries and dead letter queues

# %%
#|default_exp __init__

# %%
#|export

# Re-export errors
from netrun.errors import (
    # Base error
    NetrunRuntimeError,
    # Python-side errors
    PacketTypeMismatch,
    ValueFunctionFailed,
    NodeExecutionFailed,
    EpochTimeout,
    EpochCancelled,
    NetNotPausedError,
    DeferredPacketIdAccessError,
    # netrun_sim errors (re-exported)
    NetrunError,
    GraphValidationError,
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

# Re-export storage
from netrun.storage import PacketValueStore, StoredValue

# Re-export config
from netrun.config import NodeConfig, NodeExecFuncs

# Re-export DLQ
from netrun.dlq import DeadLetterQueue, DeadLetterEntry

# Re-export deferred
from netrun.deferred import (
    DeferredPacket,
    DeferredAction,
    DeferredActionType,
    DeferredActionQueue,
)

# Re-export context
from netrun.context import NodeExecutionContext, NodeFailureContext

# Re-export pools
from netrun.pools import (
    PoolType,
    PoolConfig,
    PoolInitMode,
    WorkerState,
    ManagedPool,
    PoolManager,
    BackgroundNetRunner,
)

# Re-export history and logging
from netrun.history import (
    HistoryEntry,
    EventHistory,
    NodeLogEntry,
    NodeLog,
    NodeLogManager,
    StdoutCapture,
    capture_stdout,
)

# Re-export port types
from netrun.port_types import (
    PortTypeSpec,
    PortTypeRegistry,
    check_value_type,
)

# Re-export Net and netrun_sim types
from netrun.net import (
    # Main class
    Net,
    NetState,
    # netrun_sim re-exports
    Graph,
    Node,
    Edge,
    Port,
    PortType,
    PortRef,
    PortSlotSpec,
    PortState,
    PacketCount,
    SalvoCondition,
    SalvoConditionTerm,
    MaxSalvos,
    Packet,
    PacketLocation,
    Epoch,
    EpochState,
    Salvo,
    NetAction,
    NetActionResponseData,
    NetEvent,
    NetSim,
)

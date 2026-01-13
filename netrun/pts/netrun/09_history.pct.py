# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # History and Logging
#
# Event history recording and node-level logging for netrun.

# %%
#|default_exp history

# %%
#|hide
from nblite import nbl_export, show_doc; nbl_export();

# %%
#|export
import io
import json
import sys
import threading
import uuid
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union


@dataclass
class HistoryEntry:
    """A single entry in the event history."""
    id: str
    timestamp: datetime
    entry_type: str  # "action" or "event"
    action_type: Optional[str] = None  # For action entries
    event_type: Optional[str] = None   # For event entries
    action_id: Optional[str] = None    # For events, links to originating action
    data: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        result = {
            "id": self.id,
            "timestamp": self.timestamp.isoformat(),
            "type": self.entry_type,
        }
        if self.action_type:
            result["action"] = self.action_type
        if self.event_type:
            result["event"] = self.event_type
        if self.action_id:
            result["action_id"] = self.action_id
        if self.data:
            result["data"] = self.data
        return result

    @classmethod
    def from_dict(cls, d: dict) -> "HistoryEntry":
        """Create from dictionary."""
        return cls(
            id=d["id"],
            timestamp=datetime.fromisoformat(d["timestamp"]),
            entry_type=d["type"],
            action_type=d.get("action"),
            event_type=d.get("event"),
            action_id=d.get("action_id"),
            data=d.get("data", {}),
        )


class EventHistory:
    """
    Records and persists NetAction and NetEvent history.

    Features:
    - In-memory circular buffer with configurable max size
    - Optional JSONL file persistence
    - Chunked writes for efficiency
    - Flush on demand or when paused
    """

    def __init__(
        self,
        max_size: Optional[int] = None,
        file_path: Optional[Union[str, Path]] = None,
        chunk_size: int = 100,
        flush_on_pause: bool = True,
    ):
        """
        Initialize event history.

        Args:
            max_size: Maximum entries in memory (None = unlimited)
            file_path: Path to JSONL file for persistence
            chunk_size: Number of entries to buffer before writing
            flush_on_pause: Whether to flush when net is paused
        """
        self._max_size = max_size
        self._file_path = Path(file_path) if file_path else None
        self._chunk_size = chunk_size
        self._flush_on_pause = flush_on_pause

        # In-memory storage
        if max_size is not None:
            self._entries: deque = deque(maxlen=max_size)
        else:
            self._entries: deque = deque()

        # Pending entries for file write
        self._pending_entries: List[HistoryEntry] = []

        # Thread safety
        self._lock = threading.Lock()

        # Current action context for linking events
        self._current_action_id: Optional[str] = None

    def record_action(
        self,
        action_type: str,
        data: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Record a NetAction.

        Args:
            action_type: The action type name
            data: Additional action data

        Returns:
            The action ID
        """
        action_id = str(uuid.uuid4())
        entry = HistoryEntry(
            id=action_id,
            timestamp=datetime.now(),
            entry_type="action",
            action_type=action_type,
            data=data or {},
        )

        with self._lock:
            self._entries.append(entry)
            self._pending_entries.append(entry)
            self._current_action_id = action_id

            if len(self._pending_entries) >= self._chunk_size:
                self._flush_to_file_locked()

        return action_id

    def record_event(
        self,
        event_type: str,
        data: Optional[Dict[str, Any]] = None,
        action_id: Optional[str] = None,
    ) -> str:
        """
        Record a NetEvent.

        Args:
            event_type: The event type name
            data: Additional event data
            action_id: The action that triggered this event (defaults to current)

        Returns:
            The event ID
        """
        event_id = str(uuid.uuid4())
        entry = HistoryEntry(
            id=event_id,
            timestamp=datetime.now(),
            entry_type="event",
            event_type=event_type,
            action_id=action_id or self._current_action_id,
            data=data or {},
        )

        with self._lock:
            self._entries.append(entry)
            self._pending_entries.append(entry)

            if len(self._pending_entries) >= self._chunk_size:
                self._flush_to_file_locked()

        return event_id

    def get_entries(
        self,
        entry_type: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[HistoryEntry]:
        """
        Get history entries.

        Args:
            entry_type: Filter by "action" or "event"
            limit: Maximum entries to return (most recent)

        Returns:
            List of history entries
        """
        with self._lock:
            entries = list(self._entries)

        if entry_type:
            entries = [e for e in entries if e.entry_type == entry_type]

        if limit:
            entries = entries[-limit:]

        return entries

    def get_actions(self, limit: Optional[int] = None) -> List[HistoryEntry]:
        """Get action entries."""
        return self.get_entries(entry_type="action", limit=limit)

    def get_events(self, limit: Optional[int] = None) -> List[HistoryEntry]:
        """Get event entries."""
        return self.get_entries(entry_type="event", limit=limit)

    def get_events_for_action(self, action_id: str) -> List[HistoryEntry]:
        """Get all events triggered by a specific action."""
        with self._lock:
            return [e for e in self._entries if e.action_id == action_id]

    def flush(self) -> None:
        """Flush pending entries to file."""
        with self._lock:
            self._flush_to_file_locked()

    def _flush_to_file_locked(self) -> None:
        """Flush pending entries to file (must hold lock)."""
        if not self._file_path or not self._pending_entries:
            return

        with open(self._file_path, "a") as f:
            for entry in self._pending_entries:
                f.write(json.dumps(entry.to_dict()) + "\n")

        self._pending_entries.clear()

    def clear(self) -> None:
        """Clear all history (memory and file)."""
        with self._lock:
            self._entries.clear()
            self._pending_entries.clear()

        if self._file_path and self._file_path.exists():
            self._file_path.unlink()

    @property
    def file_path(self) -> Optional[Path]:
        """The history file path."""
        return self._file_path

    @property
    def flush_on_pause(self) -> bool:
        """Whether to flush when net is paused."""
        return self._flush_on_pause

    def __len__(self) -> int:
        """Number of entries in memory."""
        with self._lock:
            return len(self._entries)

# %%
#|export
class NodeLogEntry:
    """A single log entry for a node."""

    def __init__(
        self,
        message: str,
        timestamp: Optional[datetime] = None,
        epoch_id: Optional[str] = None,
        level: str = "info",
    ):
        self.message = message
        self.timestamp = timestamp or datetime.now()
        self.epoch_id = epoch_id
        self.level = level

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "message": self.message,
            "timestamp": self.timestamp.isoformat(),
            "epoch_id": self.epoch_id,
            "level": self.level,
        }


class NodeLog:
    """
    Log storage for a single node.

    Captures print statements during node execution.
    """

    def __init__(self, node_name: str, max_entries: Optional[int] = None):
        self.node_name = node_name
        self._max_entries = max_entries

        if max_entries is not None:
            self._entries: deque = deque(maxlen=max_entries)
        else:
            self._entries: deque = deque()

        self._lock = threading.Lock()

    def add(
        self,
        message: str,
        epoch_id: Optional[str] = None,
        level: str = "info",
    ) -> None:
        """Add a log entry."""
        entry = NodeLogEntry(
            message=message,
            epoch_id=epoch_id,
            level=level,
        )
        with self._lock:
            self._entries.append(entry)

    def get_entries(
        self,
        epoch_id: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[NodeLogEntry]:
        """
        Get log entries.

        Args:
            epoch_id: Filter by epoch ID
            limit: Maximum entries to return
        """
        with self._lock:
            entries = list(self._entries)

        if epoch_id:
            entries = [e for e in entries if e.epoch_id == epoch_id]

        if limit:
            entries = entries[-limit:]

        return entries

    def clear(self) -> None:
        """Clear all entries."""
        with self._lock:
            self._entries.clear()

    def __len__(self) -> int:
        with self._lock:
            return len(self._entries)


class NodeLogManager:
    """
    Manages logs for all nodes in a net.
    """

    def __init__(self, max_entries_per_node: Optional[int] = None):
        self._max_entries_per_node = max_entries_per_node
        self._logs: Dict[str, NodeLog] = {}
        self._lock = threading.Lock()

    def get_log(self, node_name: str) -> NodeLog:
        """Get or create log for a node."""
        with self._lock:
            if node_name not in self._logs:
                self._logs[node_name] = NodeLog(
                    node_name,
                    max_entries=self._max_entries_per_node,
                )
            return self._logs[node_name]

    def get_node_log(
        self,
        node_name: str,
        limit: Optional[int] = None,
    ) -> List[NodeLogEntry]:
        """Get log entries for a node."""
        log = self.get_log(node_name)
        return log.get_entries(limit=limit)

    def get_epoch_log(
        self,
        node_name: str,
        epoch_id: str,
        limit: Optional[int] = None,
    ) -> List[NodeLogEntry]:
        """Get log entries for a specific epoch."""
        log = self.get_log(node_name)
        return log.get_entries(epoch_id=epoch_id, limit=limit)

    def clear(self) -> None:
        """Clear all logs."""
        with self._lock:
            for log in self._logs.values():
                log.clear()
            self._logs.clear()

# %%
#|export
class StdoutCapture:
    """
    Context manager for capturing stdout during node execution.

    Redirects print statements to a NodeLog while optionally echoing
    to the original stdout.
    """

    def __init__(
        self,
        node_log: NodeLog,
        epoch_id: str,
        echo: bool = False,
    ):
        self._node_log = node_log
        self._epoch_id = epoch_id
        self._echo = echo
        self._original_stdout = None
        self._capture_buffer = None

    def __enter__(self):
        self._original_stdout = sys.stdout
        self._capture_buffer = io.StringIO()

        # Create a custom stdout that captures and optionally echoes
        self._custom_stdout = _CaptureStdout(
            buffer=self._capture_buffer,
            echo_to=self._original_stdout if self._echo else None,
        )
        sys.stdout = self._custom_stdout
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout = self._original_stdout

        # Process captured output
        captured = self._capture_buffer.getvalue()
        if captured:
            # Add each line as a separate log entry
            for line in captured.splitlines():
                if line.strip():
                    self._node_log.add(
                        message=line,
                        epoch_id=self._epoch_id,
                        level="info",
                    )

        return False


class _CaptureStdout:
    """Custom stdout that captures to buffer and optionally echoes."""

    def __init__(
        self,
        buffer: io.StringIO,
        echo_to: Optional[io.TextIOBase] = None,
    ):
        self._buffer = buffer
        self._echo_to = echo_to

    def write(self, s: str) -> int:
        result = self._buffer.write(s)
        if self._echo_to:
            self._echo_to.write(s)
        return result

    def flush(self) -> None:
        self._buffer.flush()
        if self._echo_to:
            self._echo_to.flush()


@contextmanager
def capture_stdout(
    node_log: NodeLog,
    epoch_id: str,
    echo: bool = False,
):
    """
    Context manager for capturing stdout.

    Usage:
        with capture_stdout(node_log, epoch_id):
            print("This will be captured")
    """
    capture = StdoutCapture(node_log, epoch_id, echo)
    with capture:
        yield capture

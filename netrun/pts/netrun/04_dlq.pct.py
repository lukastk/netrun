# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Dead Letter Queue
#
# The Dead Letter Queue (DLQ) stores information about epochs that failed after
# exhausting all retries. This enables debugging and potential manual recovery.

# %%
#|default_exp dlq

# %%
#|hide
from nblite import nbl_export, show_doc; nbl_export();

# %%
#|export
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable, List, Optional


@dataclass
class DeadLetterEntry:
    """An entry in the dead letter queue."""
    epoch_id: str
    node_name: str
    exception: Exception
    retry_count: int
    retry_timestamps: List[datetime]
    retry_exceptions: List[Exception]
    input_packets: dict  # port_name -> list of packet IDs
    packet_values: dict  # packet_id -> value (for consumed packets)
    timestamp: datetime


class DeadLetterQueue:
    """
    Queue for failed epochs that exhausted all retries.

    Supports three modes:
    - "memory": Store entries in memory only
    - "file": Persist entries to a JSON/pickle file
    - callback: Call a user-provided function for each entry

    Entries contain full context about the failure for debugging
    and potential manual recovery.
    """

    def __init__(
        self,
        mode: str = "memory",
        file_path: Optional[Path] = None,
        callback: Optional[Callable[[DeadLetterEntry], None]] = None,
    ):
        """
        Initialize the dead letter queue.

        Args:
            mode: Storage mode - "memory", "file", or "callback"
            file_path: Path to file for "file" mode
            callback: Function to call for each entry in "callback" mode
        """
        if mode not in ("memory", "file", "callback"):
            raise ValueError(f"Invalid DLQ mode: {mode}")

        if mode == "file" and file_path is None:
            raise ValueError("file_path required for file mode")

        if mode == "callback" and callback is None:
            raise ValueError("callback required for callback mode")

        self._mode = mode
        self._file_path = file_path
        self._callback = callback
        self._entries: List[DeadLetterEntry] = []

    @property
    def mode(self) -> str:
        """The storage mode."""
        return self._mode

    def add(self, entry: DeadLetterEntry) -> None:
        """Add an entry to the dead letter queue."""
        if self._mode == "callback":
            # Call the callback immediately
            if self._callback is not None:
                self._callback(entry)
        else:
            # Store in memory
            self._entries.append(entry)

            # Persist to file if in file mode
            if self._mode == "file" and self._file_path is not None:
                self._persist_to_file()

    def _persist_to_file(self) -> None:
        """Persist entries to file."""
        import json

        # Convert entries to JSON-serializable format
        data = []
        for entry in self._entries:
            data.append({
                "epoch_id": entry.epoch_id,
                "node_name": entry.node_name,
                "exception_type": type(entry.exception).__name__,
                "exception_message": str(entry.exception),
                "retry_count": entry.retry_count,
                "retry_timestamps": [ts.isoformat() for ts in entry.retry_timestamps],
                "retry_exception_messages": [str(e) for e in entry.retry_exceptions],
                "input_packets": entry.input_packets,
                "timestamp": entry.timestamp.isoformat(),
                # Note: packet_values may not be JSON-serializable
            })

        with open(self._file_path, 'w') as f:
            json.dump(data, f, indent=2)

    def get_all(self) -> List[DeadLetterEntry]:
        """Get all entries in the queue."""
        return list(self._entries)

    def get_by_node(self, node_name: str) -> List[DeadLetterEntry]:
        """Get all entries for a specific node."""
        return [e for e in self._entries if e.node_name == node_name]

    def clear(self) -> None:
        """Clear all entries from the queue."""
        self._entries.clear()
        if self._mode == "file" and self._file_path is not None:
            # Clear the file too
            if self._file_path.exists():
                self._file_path.unlink()

    def __len__(self) -> int:
        """Return the number of entries in the queue."""
        return len(self._entries)

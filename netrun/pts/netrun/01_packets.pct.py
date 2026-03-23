# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %%
#|default_exp packets

# %%
#|hide
from nblite import nbl_export, show_doc; nbl_export();
import netrun.packets as this_module

# %%
#|export
from dataclasses import dataclass
from typing import Any
import threading
import importlib
from ulid import ULID

# %%
#|hide
show_doc(this_module.LazyPacketValueEvaluationError, show_class_methods=False)

# %%
#|export
class LazyPacketValueEvaluationError(Exception):
    """Exception raised when a LazyPacketValueSpec raises during evaluation."""

    def __init__(self, packet_id: ULID, original_exception: Exception):
        self.packet_id = packet_id
        self.original_exception = original_exception
        super().__init__(
            f"LazyPacketValueSpec for packet '{packet_id}' raised an exception: {original_exception}"
        )

# %%
#|hide
show_doc(this_module.LazyPacketValueSpec, show_class_methods=False)

# %%
#|export
@dataclass
class LazyPacketValueSpec:
    """Lazy value for a packet."""

    func_import_path: str
    args: tuple
    kwargs: dict

# %%
#|hide
show_doc(this_module.PacketStore, show_class_methods=False)

# %%
#|export
class PacketStore:
    """Stores packet values and lazy values. Thread-safe.

    Values are stored until consumed or destroyed. Lazy values are evaluated
    only at consumption time.

    Example:
    ```python
    >>> store = PacketStore()
    >>> store.register("pkt-1", 42)
    >>> store.consume("pkt-1")
    42
    >>> store.exists("pkt-1")
    False
    ```
    """

    def __init__(self):
        """Initialize the packet store."""
        self._store: dict[ULID, Any | LazyPacketValueSpec] = {}
        self._lock = threading.RLock()

    def register(self, packet_id: ULID, value_or_lazy: Any | LazyPacketValueSpec) -> None:
        """Register a value or lazy value for a packet.

        Args:
            packet_id: The packet ID to register.
            value_or_lazy: The value or LazyPacketValueSpec to store.

        Raises:
            ValueError: If the packet ID is already registered.
        """
        with self._lock:
            if packet_id in self._store:
                raise ValueError(f"Packet '{packet_id}' is already registered")
            self._store[packet_id] = value_or_lazy

    def _evaluate_lazy_value(self, lazy_value: LazyPacketValueSpec, packet_id: ULID) -> Any:
        module_path, func_name = lazy_value.func_import_path.rsplit(".", 1)
        mod = importlib.import_module(module_path)
        if hasattr(mod, func_name):
            func = getattr(mod, func_name)
        else:
            raise ValueError(
                f"Function '{func_name}' not found in module '{module_path}'"
            )
        try:
            return func(*lazy_value.args, **lazy_value.kwargs)
        except Exception as e:
            raise LazyPacketValueEvaluationError(packet_id, e) from e

    def destroy(self, packet_id: ULID) -> None:
        """Remove packet without returning value (for cancelled epochs).

        Raises:
            KeyError: If the packet ID is not found.
        """
        with self._lock:
            if packet_id not in self._store:
                raise KeyError(f"Packet '{packet_id}' not found")
            del self._store[packet_id]

    def consume(self, packet_id: ULID) -> Any:
        """Remove packet and return its value. Evaluates LazyPacketValueSpec if needed.

        Returns:
            The packet's value.

        Raises:
            KeyError: If the packet ID is not found.
            LazyPacketValueEvaluationError: If a LazyPacketValueSpec raises during evaluation.
        """
        with self._lock:
            if packet_id not in self._store:
                raise KeyError(f"Packet '{packet_id}' not found")
            value_or_lazy = self._store.pop(packet_id)

        if isinstance(value_or_lazy, LazyPacketValueSpec):
            try:
                return self._evaluate_lazy_value(value_or_lazy, packet_id)
            except:
                with self._lock:
                    self._store[packet_id] = value_or_lazy
                raise

        return value_or_lazy

    def peek(self, packet_id: ULID) -> Any | LazyPacketValueSpec:
        """Get the raw value or LazyPacketValueSpec without evaluating or removing.

        Args:
            packet_id: The packet ID to peek at.

        Returns:
            The raw value or LazyPacketValueSpec.

        Raises:
            KeyError: If the packet ID is not found.
        """
        with self._lock:
            if not self.exists(packet_id):
                raise KeyError(f"Packet '{packet_id}' not found")
            return self._store[packet_id]

    def exists(self, packet_id: ULID) -> bool:
        """Check if a packet ID exists in the store."""
        with self._lock:
            return packet_id in self._store

# %%
packet_store = PacketStore()

# Test registering and consumption
packet_id = ULID()
packet_store.register(packet_id, "my_value")
assert packet_store.peek(packet_id) == "my_value"
assert packet_store.consume(packet_id) == "my_value"
assert not packet_store.exists(packet_id)

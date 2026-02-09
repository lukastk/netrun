# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %%
#|default_exp storage._cache

# %%
#|hide
from nblite import nbl_export, show_doc; nbl_export();
import netrun.storage._cache as this_module

# %%
#|export
import fnmatch
import random
import tempfile
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import diskcache

from netrun._iutils import get_timestamp_utc
from netrun._iutils.hashing import HashMethod, hash as hash_data
from netrun._iutils.pickling import PicklingMethod, pickle_data, unpickle_data
from netrun.storage.config import CacheConfig, CacheWhat, NodeCacheConfig
from netrun.storage._file_storage import compute_input_hash

# %%
#|hide
show_doc(this_module.CachedEpochData)

# %%
#|export
@dataclass
class CachedEpochData:
    """Data cached for a single epoch execution."""
    input_salvo_hash: int
    net_version: int
    node_version: int
    input_values_by_port: dict[str, list[Any]]
    output_port_values: dict[str, list[Any]]
    output_salvo_names: list[str]
    consumed_input_ports: set[str]
    node_name: str
    original_epoch_id: str
    cached_at: datetime = field(default_factory=get_timestamp_utc)

# %%
#|hide
show_doc(this_module.NodeCacheStore)

# %%
#|export
class NodeCacheStore:
    """Per-node cache backed by diskcache.Cache."""

    def __init__(
        self,
        cache_dir: str,
        pickling_method: PicklingMethod = PicklingMethod.pickle,
        pickling_args: dict | None = None,
        sample_size: int | None = None,
    ):
        self._disk_cache = diskcache.Cache(cache_dir)
        self._pickling_method = pickling_method
        self._pickling_args = pickling_args or {}
        self._sample_size = sample_size
        self._epoch_count = 0

    def _serialize(self, data: CachedEpochData) -> bytes:
        return pickle_data(data, self._pickling_method, **self._pickling_args)

    def _deserialize(self, data: bytes) -> CachedEpochData:
        return unpickle_data(data, self._pickling_method)

    def lookup(self, input_hash: int, net_version: int, node_version: int) -> CachedEpochData | None:
        """Look up cached data by input hash, checking version match."""
        raw = self._disk_cache.get(input_hash)
        if raw is None:
            return None
        cached = self._deserialize(raw)
        if cached.net_version != net_version or cached.node_version != node_version:
            return None
        return cached

    def store(self, data: CachedEpochData) -> None:
        """Store cached epoch data with optional reservoir sampling."""
        self._epoch_count += 1

        if self._sample_size is None:
            self._disk_cache.set(data.input_salvo_hash, self._serialize(data))
            return

        if self._epoch_count <= self._sample_size:
            self._disk_cache.set(data.input_salvo_hash, self._serialize(data))
        else:
            j = random.randint(1, self._epoch_count)
            if j <= self._sample_size:
                existing_keys = list(self._disk_cache)
                if existing_keys:
                    evict_key = random.choice(existing_keys)
                    del self._disk_cache[evict_key]
                self._disk_cache.set(data.input_salvo_hash, self._serialize(data))

    def get_all_entries(self) -> list[CachedEpochData]:
        """Get all cached entries."""
        entries = []
        for key in self._disk_cache:
            raw = self._disk_cache.get(key)
            if raw is not None:
                entries.append(self._deserialize(raw))
        return entries

    def clear(self) -> None:
        """Clear all entries."""
        self._disk_cache.clear()

    def clear_version(self, net_version: int, node_version: int) -> None:
        """Clear entries matching specific version."""
        for key in list(self._disk_cache):
            raw = self._disk_cache.get(key)
            if raw is not None:
                cached = self._deserialize(raw)
                if cached.net_version == net_version and cached.node_version == node_version:
                    del self._disk_cache[key]

    def clear_by_input_hash(self, input_hash: int) -> None:
        """Clear specific entry by input hash."""
        if input_hash in self._disk_cache:
            del self._disk_cache[input_hash]

    def clear_input_entries(self) -> None:
        """Clear entries with empty output (INPUT_ONLY entries)."""
        for key in list(self._disk_cache):
            raw = self._disk_cache.get(key)
            if raw is not None:
                cached = self._deserialize(raw)
                if not cached.output_port_values and not cached.output_salvo_names:
                    del self._disk_cache[key]

    @property
    def entry_count(self) -> int:
        """Number of entries in the cache."""
        return len(self._disk_cache)

    def close(self) -> None:
        """Close the diskcache."""
        self._disk_cache.close()

# %%
#|hide
show_doc(this_module.NetCacheStore)

# %%
#|export
class NetCacheStore:
    """Manages per-node cache stores, resolves effective config, handles lifecycle."""

    def __init__(self, net_cache_config: CacheConfig | None):
        self._net_config = net_cache_config
        self._node_configs: dict[str, NodeCacheConfig | None] = {}
        self._node_stores: dict[str, NodeCacheStore] = {}

        if net_cache_config is not None and net_cache_config.storage_path is not None:
            self._storage_path = net_cache_config.storage_path
        else:
            self._storage_path = tempfile.mkdtemp(prefix="netrun_cache_")

    @property
    def storage_path(self) -> str:
        """The resolved storage path."""
        return self._storage_path

    def register_node(self, node_name: str, node_cache_config: NodeCacheConfig | None) -> None:
        """Register a node for caching."""
        self._node_configs[node_name] = node_cache_config

    def _matches_glob_list(self, node_name: str, patterns: list[str]) -> bool:
        """Check if node_name matches any glob pattern in list."""
        return any(fnmatch.fnmatch(node_name, p) for p in patterns)

    def is_cache_enabled(self, node_name: str) -> bool:
        """Check if caching is enabled for a node."""
        if self._net_config is None:
            return False

        node_config = self._node_configs.get(node_name)

        # Per-node explicit override
        if node_config is not None and node_config.enabled is not None:
            return node_config.enabled

        # Net-level disabled
        if not self._net_config.enabled:
            return False

        # include_all_nodes
        if self._net_config.include_all_nodes:
            # Still check exclude_nodes
            if self._net_config.exclude_nodes and self._matches_glob_list(node_name, self._net_config.exclude_nodes):
                return False
            return True

        # Check exclude_nodes (takes precedence)
        if self._net_config.exclude_nodes and self._matches_glob_list(node_name, self._net_config.exclude_nodes):
            return False

        # Check include_nodes
        if self._net_config.include_nodes:
            return self._matches_glob_list(node_name, self._net_config.include_nodes)

        # Net-level enabled but no include patterns → no nodes included
        return False

    def get_effective_config(self, node_name: str) -> dict[str, Any]:
        """Get merged config for a node (node overrides net)."""
        if self._net_config is None:
            return {}

        result = {
            "cache_what": self._net_config.cache_what,
            "hash_method": self._net_config.hash_method,
            "pickling_method": self._net_config.pickling_method,
            "pickling_args": dict(self._net_config.pickling_args),
            "evaluate_lazy_value_for_cache": self._net_config.evaluate_lazy_value_for_cache,
            "sample_size": self._net_config.sample_size,
            "net_version": self._net_config.version,
            "node_version": self._net_config.version,  # default to net version
        }

        node_config = self._node_configs.get(node_name)
        if node_config is not None:
            if node_config.cache_what is not None:
                result["cache_what"] = node_config.cache_what
            if node_config.hash_method is not None:
                result["hash_method"] = node_config.hash_method
            if node_config.pickling_method is not None:
                result["pickling_method"] = node_config.pickling_method
            if node_config.pickling_args is not None:
                result["pickling_args"] = dict(node_config.pickling_args)
            if node_config.evaluate_lazy_value_for_cache is not None:
                result["evaluate_lazy_value_for_cache"] = node_config.evaluate_lazy_value_for_cache
            if node_config.sample_size is not None:
                result["sample_size"] = node_config.sample_size
            if node_config.version is not None:
                result["node_version"] = node_config.version

        return result

    def _get_or_create_store(self, node_name: str) -> NodeCacheStore:
        """Get or create the NodeCacheStore for a node."""
        if node_name not in self._node_stores:
            import os
            effective = self.get_effective_config(node_name)
            cache_dir = os.path.join(self._storage_path, node_name)
            self._node_stores[node_name] = NodeCacheStore(
                cache_dir=cache_dir,
                pickling_method=effective["pickling_method"],
                pickling_args=effective["pickling_args"],
                sample_size=effective["sample_size"],
            )
        return self._node_stores[node_name]

    def hash_input_salvo(
        self,
        node_name: str,
        packets: dict[str, list[str]],
        packet_store: Any,
        evaluate_lazy: bool | None = None,
    ) -> int:
        """Hash all input packet values for a node's epoch.

        Args:
            node_name: The node name.
            packets: Dict of port_name -> list of packet_ids.
            packet_store: The PacketStore containing packet values.
            evaluate_lazy: Override for evaluate_lazy_value_for_cache.

        Returns:
            Combined hash of all input values.
        """
        effective = self.get_effective_config(node_name)
        if evaluate_lazy is None:
            evaluate_lazy = effective["evaluate_lazy_value_for_cache"]

        return compute_input_hash(
            packets=packets,
            packet_store=packet_store,
            hash_method=effective["hash_method"],
            pickling_method=effective["pickling_method"],
            pickling_args=effective["pickling_args"],
            evaluate_lazy=evaluate_lazy,
        )

    def lookup(self, node_name: str, input_hash: int) -> CachedEpochData | None:
        """Look up cached data for a node."""
        if not self.is_cache_enabled(node_name):
            return None
        effective = self.get_effective_config(node_name)
        store = self._get_or_create_store(node_name)
        return store.lookup(input_hash, effective["net_version"], effective["node_version"])

    def store(self, data: CachedEpochData) -> None:
        """Store cached epoch data."""
        store = self._get_or_create_store(data.node_name)
        store.store(data)

    def get_entries(self, node_name: str) -> list[CachedEpochData]:
        """Get all cached entries for a node."""
        store = self._get_or_create_store(node_name)
        return store.get_all_entries()

    def clear_all(self) -> None:
        """Clear all node caches."""
        for store in self._node_stores.values():
            store.clear()

    def clear_node(self, node_name: str) -> None:
        """Clear cache for specific node."""
        if node_name in self._node_stores:
            self._node_stores[node_name].clear()

    def clear_version(self, net_version: int | None = None, node_name: str | None = None, node_version: int | None = None) -> None:
        """Clear entries matching version criteria."""
        stores = {node_name: self._node_stores[node_name]} if node_name and node_name in self._node_stores else self._node_stores

        for name, store in stores.items():
            effective = self.get_effective_config(name)
            nv = net_version if net_version is not None else effective["net_version"]
            nodv = node_version if node_version is not None else effective["node_version"]
            store.clear_version(nv, nodv)

    def clear_output_for_input(self, node_name: str, input_hash: int) -> None:
        """Clear cached output for specific input."""
        if node_name in self._node_stores:
            self._node_stores[node_name].clear_by_input_hash(input_hash)

    def clear_inputs(self, node_name: str | None = None) -> None:
        """Clear input-only cache entries."""
        stores = {node_name: self._node_stores[node_name]} if node_name and node_name in self._node_stores else self._node_stores
        for store in stores.values():
            store.clear_input_entries()

    def cache_stats(self) -> dict[str, dict[str, Any]]:
        """Get per-node cache statistics."""
        stats = {}
        for node_name, store in self._node_stores.items():
            stats[node_name] = {
                "entry_count": store.entry_count,
                "epoch_count": store._epoch_count,
                "sample_size": store._sample_size,
            }
        return stats

    def close(self) -> None:
        """Close all diskcache instances."""
        for store in self._node_stores.values():
            store.close()
        self._node_stores.clear()

# %% [markdown]
# ## Quick validation

# %%
import tempfile, os

config = CacheConfig(enabled=True, include_all_nodes=True)
store = NetCacheStore(config)

store.register_node("NodeA", None)
store.register_node("NodeB", NodeCacheConfig(enabled=False))

assert store.is_cache_enabled("NodeA") is True
assert store.is_cache_enabled("NodeB") is False

store.close()

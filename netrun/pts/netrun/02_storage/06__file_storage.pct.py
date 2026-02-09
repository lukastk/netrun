# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %%
#|default_exp storage._file_storage

# %%
#|hide
from nblite import nbl_export; nbl_export();

# %% [markdown]
# # File Storage Store
#
# `NetFileStorageStore` manages file-storage-enabled nodes: metadata tracking
# (via diskcache), backend resolution, input hashing, and manifest CRUD.
#
# The actual store/replay logic lives in `Net._store_epoch_in_file_storage()`
# and `Net._replay_file_storage_epoch()` (future integration).

# %%
#|export
import json
import tempfile
import warnings
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import diskcache

from netrun._iutils import get_timestamp_utc
from netrun._iutils.hashing import HashMethod, hash as hash_data
from netrun._iutils.pickling import PicklingMethod
from netrun.packets import LazyPacketValueSpec
from netrun.storage.config import (
    BackendConfig,
    GCSBackendConfig,
    LocalBackendConfig,
    NodeFileStorageConfig,
    RcloneBackendConfig,
    S3BackendConfig,
    SSHBackendConfig,
    StorageConfig,
)

# %%
#|export
@dataclass
class FileStorageManifest:
    """Metadata stored in diskcache for a file-storage-enabled node.

    Tracks the input hash, version, file keys, and replay info needed
    to reconstruct output packets from stored files.
    """
    input_salvo_hash: int
    version: int
    node_name: str
    stored_at: datetime = field(default_factory=get_timestamp_utc)

    # Per-file mode: {salvo_name: [{port_name: file_key, ...}, ...]}
    file_keys: dict[str, list[dict[str, str]]] | None = None
    # Bundle mode: single archive key
    bundle_key: str | None = None

    # Replay info
    output_salvo_names: list[str] = field(default_factory=list)
    consumed_input_ports: set[str] = field(default_factory=set)

    # Input file keys (if store_inputs=True)
    input_file_keys: dict[str, list[str]] | None = None  # {port_name: [file_key, ...]}

# %%
#|export
def compute_input_hash(
    packets: dict[str, list[str]],
    packet_store: Any,
    hash_method: HashMethod,
    pickling_method: PicklingMethod,
    pickling_args: dict,
    evaluate_lazy: bool = False,
) -> int:
    """Compute a deterministic hash of input packet values.

    Shared by both NetCacheStore and NetFileStorageStore.

    Args:
        packets: Dict of port_name -> list of packet_ids.
        packet_store: The PacketStore containing packet values.
        hash_method: Hash algorithm to use.
        pickling_method: Pickling method for serialization.
        pickling_args: Arguments passed to the pickler.
        evaluate_lazy: Whether to evaluate lazy values before hashing.

    Returns:
        Combined hash of all input values.
    """
    hash_input = []
    for port_name in sorted(packets.keys()):
        port_values = []
        for packet_id in packets[port_name]:
            value = packet_store.peek(packet_id)
            if evaluate_lazy and isinstance(value, LazyPacketValueSpec):
                value = packet_store._evaluate_lazy_value(value, packet_id)
            port_values.append(value)
        hash_input.append((port_name, port_values))

    return hash_data(
        hash_input,
        method=hash_method,
        pickling_method=pickling_method,
        pickling_args=pickling_args,
    )

# %%
#|export
def create_backend_instance(backend_config: BackendConfig, project_root: Path | None = None):
    """Create a StorageBackend instance from a BackendConfig.

    This is the single point of backend construction used by both
    ``NetFileStorageStore.resolve_backend()`` and ``_retrieval._create_backend_from_json()``.

    Args:
        backend_config: A concrete BackendConfig (Local, S3, GCS, SSH, Rclone).
        project_root: For resolving relative paths in Local/Rclone configs.

    Returns:
        A StorageBackend instance ready for read/write operations.
    """
    from netrun.storage._backends import (
        LocalBackend,
        S3Backend,
        GCSBackend,
        SSHBackend,
        RcloneBackend,
    )

    match backend_config:
        case LocalBackendConfig():
            base_path = backend_config.base_path
            if project_root and not Path(base_path).is_absolute():
                base_path = str(project_root / base_path)
            return LocalBackend(base_path=base_path)
        case S3BackendConfig():
            return S3Backend(
                bucket=backend_config.bucket,
                prefix=backend_config.prefix,
                region=backend_config.region,
                endpoint_url=backend_config.endpoint_url,
                access_key=backend_config.access_key,
                secret_key=backend_config.secret_key,
            )
        case GCSBackendConfig():
            return GCSBackend(
                bucket=backend_config.bucket,
                prefix=backend_config.prefix,
                credentials_path=backend_config.credentials_path,
            )
        case SSHBackendConfig():
            return SSHBackend(
                host=backend_config.host,
                base_path=backend_config.base_path,
                port=backend_config.port,
                username=backend_config.username,
                key_path=backend_config.key_path,
                password=backend_config.password,
            )
        case RcloneBackendConfig():
            config_path = backend_config.config_path
            if config_path and project_root and not Path(config_path).is_absolute():
                config_path = str(project_root / config_path)
            return RcloneBackend(remote=backend_config.remote, config_path=config_path)
        case _:
            raise ValueError(f"Unknown backend type: {backend_config.type}")


class NetFileStorageStore:
    """Manages file storage metadata and backend resolution for all nodes.

    Parallels NetCacheStore but for file-based persistence. Stores
    FileStorageManifest entries in a diskcache keyed by node name.
    """

    def __init__(
        self,
        storage_config: StorageConfig | None,
        node_configs: dict[str, NodeFileStorageConfig],
    ):
        self._storage_config = storage_config
        self._node_configs = node_configs

        if storage_config and storage_config.file_storage_metadata_path:
            self._metadata_path = storage_config.file_storage_metadata_path
        else:
            self._metadata_path = tempfile.mkdtemp(prefix="netrun_file_storage_meta_")
            if node_configs:  # Only warn if there are actually file-storage nodes
                warnings.warn(
                    f"No file_storage_metadata_path configured. Using temporary directory "
                    f"'{self._metadata_path}' — file storage manifests will be lost on restart. "
                    f"Set storage.file_storage_metadata_path in your net config for persistence.",
                    UserWarning,
                    stacklevel=2,
                )

        self._metadata_cache: diskcache.Cache | None = None
        self._backends_registry = storage_config.backends if storage_config else {}
        self._resolved_backends: dict[str, Any] = {}

    def _get_metadata_cache(self) -> diskcache.Cache:
        """Lazily open the metadata diskcache."""
        if self._metadata_cache is None:
            self._metadata_cache = diskcache.Cache(self._metadata_path)
        return self._metadata_cache

    def is_enabled(self, node_name: str) -> bool:
        """Check if file storage is enabled for a node."""
        config = self._node_configs.get(node_name)
        if config is None:
            return False
        return config.enabled

    def get_config(self, node_name: str) -> NodeFileStorageConfig | None:
        """Get the file storage config for a node."""
        return self._node_configs.get(node_name)

    def lookup(self, node_name: str) -> FileStorageManifest | None:
        """Look up the stored manifest for a node.

        Returns the manifest if one exists, regardless of hash match.
        The caller is responsible for checking hash/version.
        """
        cache = self._get_metadata_cache()
        raw = cache.get(node_name)
        if raw is None:
            return None
        return raw  # Stored as Python object via diskcache's default serialization

    def store_manifest(self, node_name: str, manifest: FileStorageManifest) -> None:
        """Store a manifest for a node, replacing any existing one."""
        cache = self._get_metadata_cache()
        cache.set(node_name, manifest)

    def resolve_backend_config(self, node_name: str) -> BackendConfig:
        """Resolve the backend config for a node.

        If the node's config specifies a string backend name, looks it up
        in the registry. If it specifies an inline BackendConfig, returns it.

        Raises:
            KeyError: If a named backend is not found in the registry.
            ValueError: If no config exists for the node.
        """
        config = self._node_configs.get(node_name)
        if config is None:
            raise ValueError(f"No file storage config for node '{node_name}'")

        if isinstance(config.backend, str):
            # Named backend from registry
            if config.backend not in self._backends_registry:
                raise KeyError(
                    f"Backend '{config.backend}' not found in storage.backends registry. "
                    f"Available: {list(self._backends_registry.keys())}"
                )
            return self._backends_registry[config.backend]
        else:
            # Inline BackendConfig
            return config.backend

    def resolve_backend(
        self,
        node_name: str,
        project_root: Path | None = None,
    ):
        """Resolve and create a StorageBackend instance for a node.

        Caches backend instances so repeated calls for the same node reuse
        the same instance (avoiding connection leaks for remote backends).

        Args:
            node_name: The node name.
            project_root: For resolving relative paths in LocalBackendConfig.

        Returns:
            A StorageBackend instance ready for read/write operations.
        """
        if node_name in self._resolved_backends:
            return self._resolved_backends[node_name]

        backend_config = self.resolve_backend_config(node_name)
        backend = create_backend_instance(backend_config, project_root=project_root)
        self._resolved_backends[node_name] = backend
        return backend

    def compute_input_hash(
        self,
        node_name: str,
        packets: dict[str, list[str]],
        packet_store: Any,
    ) -> int:
        """Compute the input hash for a file-storage-enabled node.

        Uses the node's configured hash method and pickling settings.
        """
        config = self._node_configs[node_name]
        return compute_input_hash(
            packets=packets,
            packet_store=packet_store,
            hash_method=config.hash_method,
            pickling_method=config.hash_pickling_method,
            pickling_args=config.hash_pickling_args if isinstance(config.hash_pickling_args, dict) else {},
            evaluate_lazy=config.evaluate_lazy_value_for_hash,
        )

    def close(self) -> None:
        """Close all backends and the metadata diskcache."""
        for backend in self._resolved_backends.values():
            if hasattr(backend, 'close'):
                backend.close()
        self._resolved_backends.clear()
        if self._metadata_cache is not None:
            self._metadata_cache.close()
            self._metadata_cache = None

# %% [markdown]
# ## Quick validation

# %%
import tempfile

# Test NetFileStorageStore with local backend
with tempfile.TemporaryDirectory() as tmpdir:
    storage_config = StorageConfig(
        file_storage_metadata_path=tmpdir + "/meta",
        backends={
            "local_test": LocalBackendConfig(base_path=tmpdir + "/data"),
        },
    )

    node_configs = {
        "NodeA": NodeFileStorageConfig(backend="local_test"),
        "NodeB": NodeFileStorageConfig(backend=LocalBackendConfig(base_path=tmpdir + "/inline")),
    }

    store = NetFileStorageStore(storage_config, node_configs)

    assert store.is_enabled("NodeA")
    assert store.is_enabled("NodeB")
    assert not store.is_enabled("NonExistent")

    # Resolve named backend
    bc = store.resolve_backend_config("NodeA")
    assert bc.type == "local"

    # Resolve inline backend
    bc2 = store.resolve_backend_config("NodeB")
    assert bc2.type == "local"

    # Store and lookup manifest
    manifest = FileStorageManifest(
        input_salvo_hash=12345,
        version=0,
        node_name="NodeA",
        output_salvo_names=["send"],
        consumed_input_ports={"x"},
        file_keys={"send": [{"out": "NodeA/out.pkl"}]},
    )
    store.store_manifest("NodeA", manifest)

    looked_up = store.lookup("NodeA")
    assert looked_up is not None
    assert looked_up.input_salvo_hash == 12345
    assert looked_up.node_name == "NodeA"

    assert store.lookup("NodeB") is None

    store.close()

print("NetFileStorageStore: OK")

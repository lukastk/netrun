# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %%
#|default_exp storage.config

# %%
#|hide
from nblite import nbl_export, show_doc; nbl_export();
import netrun.storage.config as this_module

# %%
#|export
from enum import Enum
from typing import Annotated, Literal
from pydantic import Field, model_validator

from netrun._iutils.var_ref import VarRef, EnvVar, VarResolvableModel, ProjectRootPath
from netrun._iutils.hashing import HashMethod
from netrun._iutils.pickling import PicklingMethod
from netrun.storage._serialization import SerializationMethod
from netrun.storage._compression import CompressionMethod

# %% [markdown]
# # Cache Configuration
#
# Existing cache (memoization) config models, preserved unchanged.

# %%
#|hide
show_doc(this_module.CacheWhat)

# %%
#|export
class CacheWhat(Enum):
    """What to cache for each epoch."""
    BOTH = "both"
    OUTPUT_ONLY = "output"
    INPUT_ONLY = "input"

# %%
#|hide
show_doc(this_module.CacheConfig)

# %%
#|export
class CacheConfig(VarResolvableModel):
    """Net-level cache configuration."""
    enabled: bool | VarRef = False
    version: int | VarRef = Field(default=0, description="Cache version. Changing this invalidates all cached entries.")
    storage_path: Annotated[str | VarRef | None, ProjectRootPath()] = Field(default=None, description="Directory for cache storage. None = auto-generated temp directory.")
    include_nodes: list[str] | VarRef | None = Field(default=None, description="Glob patterns for node names to cache.")
    exclude_nodes: list[str] | VarRef | None = Field(default=None, description="Glob patterns for node names to exclude from caching.")
    include_all_nodes: bool | VarRef = Field(default=False, description="Cache all nodes (overrides include_nodes).")
    cache_what: CacheWhat | VarRef = Field(default=CacheWhat.BOTH, description="What to cache: both (memoization), output only, or input only.")
    hash_method: HashMethod | VarRef = Field(default=HashMethod.xxh64, description="Hash algorithm for input salvo hashing.")
    pickling_method: PicklingMethod | VarRef = Field(default=PicklingMethod.pickle, description="Pickling method for serialization.")
    pickling_args: dict | VarRef = Field(default_factory=dict, description="Arguments passed to the pickler.")
    evaluate_lazy_value_for_cache: bool | VarRef = Field(default=False, description="Evaluate lazy values before hashing/caching.")
    sample_size: int | VarRef | None = Field(default=None, description="Max cached entries per node (reservoir sampling). None = unlimited.")

# %%
#|hide
show_doc(this_module.NodeCacheConfig)

# %%
#|export
class NodeCacheConfig(VarResolvableModel):
    """Per-node cache overrides. None values inherit from CacheConfig."""
    enabled: bool | VarRef | None = None
    version: int | VarRef | None = None
    cache_what: CacheWhat | VarRef | None = None
    hash_method: HashMethod | VarRef | None = None
    pickling_method: PicklingMethod | VarRef | None = None
    pickling_args: dict | VarRef | None = None
    evaluate_lazy_value_for_cache: bool | VarRef | None = None
    sample_size: int | VarRef | None = None

# %% [markdown]
# # File Storage Enums

# %%
#|export
class OnHashChange(str, Enum):
    """Policy when file storage detects an input hash change."""
    overwrite = "overwrite"
    error = "error"


class BundleFormat(str, Enum):
    """Archive format for bundle mode."""
    tar_gz = "tar.gz"
    zip = "zip"

# %% [markdown]
# # Backend Configuration
#
# Pydantic discriminated union for storage backend configs.

# %%
#|export
class LocalBackendConfig(VarResolvableModel):
    """Local filesystem backend configuration."""
    type: Literal["local"] = "local"
    base_path: Annotated[str | VarRef, ProjectRootPath()] = Field(description="Base directory for file storage. Resolved relative to project_root if relative.")


class S3BackendConfig(VarResolvableModel):
    """AWS S3 backend configuration."""
    type: Literal["s3"] = "s3"
    bucket: str | VarRef
    prefix: str | VarRef = ""
    region: str | VarRef | None = None
    endpoint_url: str | VarRef | None = None
    access_key: str | VarRef | None = None
    secret_key: str | VarRef | None = None


class GCSBackendConfig(VarResolvableModel):
    """Google Cloud Storage backend configuration."""
    type: Literal["gcs"] = "gcs"
    bucket: str | VarRef
    prefix: str | VarRef = ""
    credentials_path: Annotated[str | VarRef | None, ProjectRootPath()] = None


class SSHBackendConfig(VarResolvableModel):
    """SSH/SFTP backend configuration."""
    type: Literal["ssh"] = "ssh"
    host: str | VarRef
    base_path: str | VarRef
    port: int | VarRef = 22
    username: str | VarRef | None = None
    key_path: Annotated[str | VarRef | None, ProjectRootPath()] = None
    password: str | VarRef | None = None


class RcloneBackendConfig(VarResolvableModel):
    """Rclone backend configuration."""
    type: Literal["rclone"] = "rclone"
    remote: str | VarRef = Field(description="Rclone remote spec, e.g. 'myremote:bucket/path'.")
    config_path: Annotated[str | VarRef | None, ProjectRootPath()] = Field(default=None, description="Path to rclone config file. None uses rclone default (~/.config/rclone/rclone.conf). Relative paths resolved against project_root.")


BackendConfig = Annotated[
    LocalBackendConfig | S3BackendConfig | GCSBackendConfig | SSHBackendConfig | RcloneBackendConfig,
    Field(discriminator="type")
]

# %% [markdown]
# # File Storage Configuration

# %%
#|hide
show_doc(this_module.NodeFileStorageConfig)

# %%
#|export
class NodeFileStorageConfig(VarResolvableModel):
    """Per-node file storage configuration."""
    enabled: bool | VarRef = True
    backend: str | BackendConfig = Field(description="Named backend from registry, or inline BackendConfig.")

    # Serialization
    serialization: SerializationMethod | VarRef = Field(default=SerializationMethod.pickle, description="Serialization format for stored values.")
    pickling_method: PicklingMethod | VarRef = PicklingMethod.pickle
    pickling_args: dict | VarRef = Field(default_factory=dict)

    # Compression
    compression: CompressionMethod | VarRef | None = Field(default=None, description="Compression algorithm, or None for no compression.")

    # Hash change policy
    on_hash_change: OnHashChange | VarRef = OnHashChange.error

    # Input storage
    store_inputs: bool | VarRef = False

    # Hashing
    hash_method: HashMethod | VarRef = HashMethod.xxh64
    hash_pickling_method: PicklingMethod | VarRef = PicklingMethod.pickle
    hash_pickling_args: dict | VarRef = Field(default_factory=dict)
    evaluate_lazy_value_for_hash: bool | VarRef = False

    # Bundle mode
    bundle: bool | VarRef = False
    bundle_format: BundleFormat | VarRef = BundleFormat.tar_gz

    # Output naming overrides (per-file mode only)
    output_names: dict[str, dict[str, str]] | VarRef | None = Field(
        default=None,
        description="Override output file names: {salvo_name: {port_name: custom_base_name}}.",
    )

    # Version for invalidation
    version: int | VarRef = 0

# %% [markdown]
# # Umbrella Storage Configs
#
# `NodeStorageConfig` wraps per-node cache and file storage (mutually exclusive).
# `StorageConfig` wraps net-level cache config, backend registry, and metadata path.

# %%
#|hide
show_doc(this_module.NodeStorageConfig)

# %%
#|export
class NodeStorageConfig(VarResolvableModel):
    """Per-node storage configuration. Cache and file_storage are mutually exclusive."""
    cache: NodeCacheConfig | None = None
    file_storage: NodeFileStorageConfig | None = None

    @model_validator(mode='after')
    def validate_mutual_exclusion(self):
        if self.cache is not None and self.file_storage is not None:
            cache_enabled = self.cache.enabled if self.cache.enabled is not None else True
            fs_enabled = self.file_storage.enabled
            if cache_enabled and fs_enabled:
                raise ValueError("cache and file_storage cannot both be enabled on the same node")
        return self

# %%
#|hide
show_doc(this_module.StorageConfig)

# %%
#|export
class StorageConfig(VarResolvableModel):
    """Net-level storage configuration."""
    backends: dict[str, BackendConfig] = Field(
        default_factory=dict,
        description="Named backend registry. Nodes reference these by name.",
    )
    cache: CacheConfig | None = None
    file_storage_metadata_path: Annotated[str | VarRef | None, ProjectRootPath()] = Field(
        default=None,
        description="Diskcache path for file storage hash/manifest tracking. Required if any node uses file storage.",
    )

# %% [markdown]
# ## Quick validation

# %%
config = CacheConfig()
assert config.enabled is False
assert config.cache_what == CacheWhat.BOTH
assert config.hash_method == HashMethod.xxh64

node_config = NodeCacheConfig()
assert node_config.enabled is None
assert node_config.version is None

# Backend discriminated union
local = LocalBackendConfig(base_path="/tmp/test")
assert local.type == "local"

s3 = S3BackendConfig(bucket="my-bucket", prefix="data/")
assert s3.type == "s3"

# Umbrella configs
storage = StorageConfig(cache=CacheConfig(enabled=True, include_all_nodes=True))
assert storage.cache.enabled is True
assert storage.file_storage_metadata_path is None

node_storage = NodeStorageConfig(cache=NodeCacheConfig(enabled=True))
assert node_storage.cache is not None
assert node_storage.file_storage is None

# Mutual exclusion validation
import pytest as _pytest
with _pytest.raises(ValueError, match="cache and file_storage cannot both be enabled"):
    NodeStorageConfig(
        cache=NodeCacheConfig(enabled=True),
        file_storage=NodeFileStorageConfig(backend=LocalBackendConfig(base_path="/tmp")),
    )

# But both disabled is fine
NodeStorageConfig(
    cache=NodeCacheConfig(enabled=False),
    file_storage=NodeFileStorageConfig(backend=LocalBackendConfig(base_path="/tmp")),
)

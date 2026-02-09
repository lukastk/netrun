# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Tests for File Storage Config Validation

# %%
#|default_exp storage.test_file_config

# %%
#|export
import pytest

from netrun.storage.config import (
    CacheConfig,
    NodeCacheConfig,
    StorageConfig,
    NodeStorageConfig,
    NodeFileStorageConfig,
    LocalBackendConfig,
    S3BackendConfig,
    GCSBackendConfig,
    SSHBackendConfig,
    RcloneBackendConfig,
    BackendConfig,
    OnHashChange,
    BundleFormat,
)

# %% [markdown]
# ## NodeStorageConfig: mutual exclusion

# %%
#|export
def test_node_storage_both_enabled_raises():
    """Cache and file_storage cannot both be enabled."""
    with pytest.raises(ValueError, match="cache and file_storage cannot both be enabled"):
        NodeStorageConfig(
            cache=NodeCacheConfig(enabled=True),
            file_storage=NodeFileStorageConfig(backend=LocalBackendConfig(base_path="/tmp")),
        )


def test_node_storage_both_with_cache_disabled_ok():
    """If cache is explicitly disabled, file_storage can be enabled."""
    config = NodeStorageConfig(
        cache=NodeCacheConfig(enabled=False),
        file_storage=NodeFileStorageConfig(backend=LocalBackendConfig(base_path="/tmp")),
    )
    assert config.file_storage is not None
    assert config.cache is not None


def test_node_storage_cache_only():
    """Cache-only config works fine."""
    config = NodeStorageConfig(cache=NodeCacheConfig(enabled=True))
    assert config.cache is not None
    assert config.file_storage is None


def test_node_storage_file_storage_only():
    """File-storage-only config works fine."""
    config = NodeStorageConfig(
        file_storage=NodeFileStorageConfig(backend=LocalBackendConfig(base_path="/tmp")),
    )
    assert config.cache is None
    assert config.file_storage is not None


def test_node_storage_empty():
    """Both None is valid."""
    config = NodeStorageConfig()
    assert config.cache is None
    assert config.file_storage is None

# %% [markdown]
# ## StorageConfig

# %%
#|export
def test_storage_config_defaults():
    config = StorageConfig()
    assert config.backends == {}
    assert config.cache is None
    assert config.file_storage_metadata_path is None


def test_storage_config_with_cache():
    config = StorageConfig(cache=CacheConfig(enabled=True, include_all_nodes=True))
    assert config.cache.enabled is True


def test_storage_config_with_backends():
    config = StorageConfig(
        backends={
            "local": LocalBackendConfig(base_path="/data"),
            "s3": S3BackendConfig(bucket="my-bucket"),
        }
    )
    assert len(config.backends) == 2
    assert config.backends["local"].type == "local"
    assert config.backends["s3"].type == "s3"

# %% [markdown]
# ## Backend config discriminated union

# %%
#|export
def test_local_backend_config():
    config = LocalBackendConfig(base_path="/data")
    assert config.type == "local"
    assert config.base_path == "/data"


def test_s3_backend_config():
    config = S3BackendConfig(bucket="test", prefix="data/", region="us-east-1")
    assert config.type == "s3"
    assert config.bucket == "test"
    assert config.prefix == "data/"


def test_gcs_backend_config():
    config = GCSBackendConfig(bucket="test")
    assert config.type == "gcs"
    assert config.prefix == ""


def test_ssh_backend_config():
    config = SSHBackendConfig(host="example.com", base_path="/data", port=2222)
    assert config.type == "ssh"
    assert config.port == 2222


def test_rclone_backend_config():
    config = RcloneBackendConfig(remote="myremote:bucket/path")
    assert config.type == "rclone"

# %% [markdown]
# ## NodeFileStorageConfig defaults

# %%
#|export
def test_node_file_storage_defaults():
    config = NodeFileStorageConfig(backend=LocalBackendConfig(base_path="/tmp"))
    assert config.enabled is True
    assert config.serialization == "pickle"
    assert config.compression is None
    assert config.on_hash_change == OnHashChange.error
    assert config.store_inputs is False
    assert config.bundle is False
    assert config.bundle_format == BundleFormat.tar_gz
    assert config.version == 0


def test_node_file_storage_custom():
    config = NodeFileStorageConfig(
        backend="my_s3",
        serialization="json",
        compression="gzip",
        on_hash_change=OnHashChange.overwrite,
        store_inputs=True,
        bundle=True,
        bundle_format=BundleFormat.zip,
        version=3,
    )
    assert config.backend == "my_s3"
    assert config.serialization == "json"
    assert config.compression == "gzip"
    assert config.on_hash_change == OnHashChange.overwrite
    assert config.store_inputs is True
    assert config.bundle is True
    assert config.bundle_format == BundleFormat.zip
    assert config.version == 3

# %% [markdown]
# ## JSON serialization round-trip

# %%
#|export
def test_storage_config_json_round_trip():
    """StorageConfig should survive JSON serialization."""
    config = StorageConfig(
        backends={
            "local": LocalBackendConfig(base_path="/data"),
        },
        cache=CacheConfig(enabled=True, include_all_nodes=True),
        file_storage_metadata_path="/tmp/meta",
    )

    json_str = config.model_dump_json()
    loaded = StorageConfig.model_validate_json(json_str)

    assert loaded.cache.enabled is True
    assert loaded.file_storage_metadata_path == "/tmp/meta"
    assert loaded.backends["local"].type == "local"
    assert loaded.backends["local"].base_path == "/data"


def test_node_file_storage_json_round_trip():
    """NodeFileStorageConfig should survive JSON serialization."""
    config = NodeFileStorageConfig(
        backend=LocalBackendConfig(base_path="/tmp"),
        serialization="torch",
        compression="gzip",
        on_hash_change=OnHashChange.overwrite,
    )

    json_str = config.model_dump_json()
    loaded = NodeFileStorageConfig.model_validate_json(json_str)

    assert loaded.backend.type == "local"
    assert loaded.serialization == "torch"
    assert loaded.compression == "gzip"
    assert loaded.on_hash_change == OnHashChange.overwrite

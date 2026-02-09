# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Tests for Retrieval Functions

# %%
#|default_exp storage.test_retrieval

# %%
#|export
import json
import tempfile
import pytest

from netrun.storage._backends import LocalBackend
from netrun.storage._serialization import SerializationMethod, serialize
from netrun.storage._compression import CompressionMethod, compress
from netrun.storage._retrieval import retrieve_value, retrieve_from_bundle, _create_backend_from_json
from netrun.storage.config import LocalBackendConfig, S3BackendConfig

# %% [markdown]
# ## retrieve_value

# %%
#|export
def test_retrieve_value_pickle():
    """retrieve_value with pickle serialization."""
    with tempfile.TemporaryDirectory() as tmpdir:
        backend = LocalBackend(tmpdir)
        value = {"key": [1, 2, 3], "nested": {"a": True}}
        data = serialize(value, SerializationMethod.pickle)
        backend.write("data.pkl", data)

        config_json = json.dumps({"type": "local", "base_path": tmpdir})
        result = retrieve_value(config_json, "data.pkl", "pickle")
        assert result == value


def test_retrieve_value_json():
    """retrieve_value with json serialization."""
    with tempfile.TemporaryDirectory() as tmpdir:
        backend = LocalBackend(tmpdir)
        value = {"test": [1, 2, 3]}
        data = serialize(value, SerializationMethod.json)
        backend.write("data.json", data)

        config_json = json.dumps({"type": "local", "base_path": tmpdir})
        result = retrieve_value(config_json, "data.json", "json")
        assert result == value


def test_retrieve_value_with_gzip_compression():
    """retrieve_value with gzip compression."""
    with tempfile.TemporaryDirectory() as tmpdir:
        backend = LocalBackend(tmpdir)
        value = {"compressed": True}
        data = serialize(value, SerializationMethod.json)
        compressed = compress(data, CompressionMethod.gzip)
        backend.write("data.json.gz", compressed)

        config_json = json.dumps({"type": "local", "base_path": tmpdir})
        result = retrieve_value(config_json, "data.json.gz", "json", compression="gzip")
        assert result == value


def test_retrieve_value_with_zlib_compression():
    """retrieve_value with zlib compression."""
    with tempfile.TemporaryDirectory() as tmpdir:
        backend = LocalBackend(tmpdir)
        value = [1, 2, 3, 4, 5]
        data = serialize(value, SerializationMethod.pickle)
        compressed = compress(data, CompressionMethod.zlib)
        backend.write("data.pkl.zlib", compressed)

        config_json = json.dumps({"type": "local", "base_path": tmpdir})
        result = retrieve_value(config_json, "data.pkl.zlib", "pickle", compression="zlib")
        assert result == value

# %% [markdown]
# ## retrieve_from_bundle

# %%
#|export
def test_retrieve_from_bundle_tar_gz():
    """retrieve_from_bundle with tar.gz format."""
    import io
    import tarfile

    with tempfile.TemporaryDirectory() as tmpdir:
        backend = LocalBackend(tmpdir)
        value = {"bundled": 42}
        data = serialize(value, SerializationMethod.json)

        # Create tar.gz archive
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w:gz") as tf:
            info = tarfile.TarInfo(name="send/out.json")
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))
        backend.write("bundle.tar.gz", buf.getvalue())

        config_json = json.dumps({"type": "local", "base_path": tmpdir})
        result = retrieve_from_bundle(
            config_json, "bundle.tar.gz", "send/out.json",
            "json", bundle_format="tar.gz",
        )
        assert result == value


def test_retrieve_from_bundle_zip():
    """retrieve_from_bundle with zip format."""
    import io
    import zipfile

    with tempfile.TemporaryDirectory() as tmpdir:
        backend = LocalBackend(tmpdir)
        value = {"zipped": 99}
        data = serialize(value, SerializationMethod.pickle)

        # Create zip archive
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("send/out.pkl", data)
        backend.write("bundle.zip", buf.getvalue())

        config_json = json.dumps({"type": "local", "base_path": tmpdir})
        result = retrieve_from_bundle(
            config_json, "bundle.zip", "send/out.pkl",
            "pickle", bundle_format="zip",
        )
        assert result == value

# %% [markdown]
# ## _create_backend_from_json

# %%
#|export
def test_create_backend_from_json_local():
    """_create_backend_from_json round-trip for local backend."""
    config = LocalBackendConfig(base_path="/tmp/test")
    config_json = config.model_dump_json()
    backend = _create_backend_from_json(config_json)
    assert isinstance(backend, LocalBackend)

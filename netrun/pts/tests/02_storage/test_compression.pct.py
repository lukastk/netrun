# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Tests for Compression Module

# %%
#|default_exp storage.test_compression

# %%
#|export
import pytest

from netrun.storage._compression import (
    CompressionMethod,
    compress,
    decompress,
    get_compression_extension,
    validate_compression_imports,
)

# %% [markdown]
# ## Round-trip tests for stdlib methods

# %%
#|export
@pytest.mark.parametrize("method", [
    CompressionMethod.gzip,
    CompressionMethod.bz2,
    CompressionMethod.lzma,
    CompressionMethod.zlib,
])
def test_stdlib_round_trip(method):
    """All stdlib compression methods should round-trip correctly."""
    data = b"hello world " * 100  # Repetitive data compresses well
    compressed = compress(data, method)
    assert isinstance(compressed, bytes)
    assert compressed != data  # Should actually compress
    decompressed = decompress(compressed, method)
    assert decompressed == data

# %% [markdown]
# ## Compression actually reduces size

# %%
#|export
@pytest.mark.parametrize("method", [
    CompressionMethod.gzip,
    CompressionMethod.bz2,
    CompressionMethod.lzma,
    CompressionMethod.zlib,
])
def test_compression_reduces_size(method):
    """Compression of repetitive data should be smaller than original."""
    data = b"AAAA" * 10000
    compressed = compress(data, method)
    assert len(compressed) < len(data)

# %% [markdown]
# ## Extension mapping

# %%
#|export
def test_gzip_extension():
    assert get_compression_extension(CompressionMethod.gzip) == ".gz"

def test_bz2_extension():
    assert get_compression_extension(CompressionMethod.bz2) == ".bz2"

def test_lzma_extension():
    assert get_compression_extension(CompressionMethod.lzma) == ".xz"

def test_zlib_extension():
    assert get_compression_extension(CompressionMethod.zlib) == ".zlib"

# %%
#|export
def test_all_methods_have_extensions():
    """Every CompressionMethod should have a file extension."""
    for method in CompressionMethod:
        ext = get_compression_extension(method)
        assert ext.startswith("."), f"{method} extension should start with '.', got {ext!r}"

# %% [markdown]
# ## Validate imports for stdlib methods

# %%
#|export
@pytest.mark.parametrize("method", [
    CompressionMethod.gzip,
    CompressionMethod.bz2,
    CompressionMethod.lzma,
    CompressionMethod.zlib,
])
def test_validate_stdlib_imports(method):
    """Stdlib compression methods should always pass validation."""
    validate_compression_imports(method)

# %% [markdown]
# ## Empty data

# %%
#|export
@pytest.mark.parametrize("method", [
    CompressionMethod.gzip,
    CompressionMethod.bz2,
    CompressionMethod.lzma,
    CompressionMethod.zlib,
])
def test_empty_data_round_trip(method):
    """Compression should handle empty bytes."""
    data = b""
    compressed = compress(data, method)
    decompressed = decompress(compressed, method)
    assert decompressed == data

# %% [markdown]
# ## Binary data with all byte values

# %%
#|export
@pytest.mark.parametrize("method", [
    CompressionMethod.gzip,
    CompressionMethod.bz2,
    CompressionMethod.lzma,
    CompressionMethod.zlib,
])
def test_binary_data_round_trip(method):
    """Compression should handle arbitrary binary data."""
    data = bytes(range(256)) * 10
    compressed = compress(data, method)
    decompressed = decompress(compressed, method)
    assert decompressed == data

# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %%
#|default_exp storage._compression

# %%
#|hide
from nblite import nbl_export; nbl_export();

# %%
#|export
import bz2
import gzip
import lzma
import zlib
from enum import Enum

# %%
#|export
class CompressionMethod(str, Enum):
    """Compression algorithm for file storage."""
    gzip = "gzip"
    bz2 = "bz2"
    lzma = "lzma"
    zlib = "zlib"
    zstd = "zstd"
    lz4 = "lz4"

# %%
#|export
_COMPRESSION_EXTENSION_MAP: dict[CompressionMethod, str] = {
    CompressionMethod.gzip: ".gz",
    CompressionMethod.bz2: ".bz2",
    CompressionMethod.lzma: ".xz",
    CompressionMethod.zlib: ".zlib",
    CompressionMethod.zstd: ".zst",
    CompressionMethod.lz4: ".lz4",
}

# %%
#|export
def get_compression_extension(method: CompressionMethod) -> str:
    """Get the file extension for a compression method.

    Args:
        method: The compression method.

    Returns:
        The file extension including the leading dot (e.g., ".gz").
    """
    return _COMPRESSION_EXTENSION_MAP[method]

# %%
#|export
def compress(data: bytes, method: CompressionMethod, **kwargs) -> bytes:
    """Compress bytes using the specified method.

    Args:
        data: The bytes to compress.
        method: The compression method.
        **kwargs: Additional arguments passed to the compressor (e.g., level).

    Returns:
        The compressed bytes.

    Raises:
        ImportError: If required library is not installed.
    """
    match method:
        case CompressionMethod.gzip:
            return gzip.compress(data, **kwargs)
        case CompressionMethod.bz2:
            return bz2.compress(data, **kwargs)
        case CompressionMethod.lzma:
            return lzma.compress(data, **kwargs)
        case CompressionMethod.zlib:
            return zlib.compress(data, **kwargs)
        case CompressionMethod.zstd:
            try:
                import zstandard
            except ImportError:
                raise ImportError(
                    "zstandard is required for CompressionMethod.zstd. "
                    "Install it with: pip install zstandard"
                )
            cctx = zstandard.ZstdCompressor(**kwargs)
            return cctx.compress(data)
        case CompressionMethod.lz4:
            try:
                import lz4.frame
            except ImportError:
                raise ImportError(
                    "lz4 is required for CompressionMethod.lz4. "
                    "Install it with: pip install lz4"
                )
            return lz4.frame.compress(data, **kwargs)
        case _:
            raise ValueError(f"Unknown compression method: {method}")

# %%
#|export
def decompress(data: bytes, method: CompressionMethod, **kwargs) -> bytes:
    """Decompress bytes using the specified method.

    Args:
        data: The bytes to decompress.
        method: The compression method that was used.
        **kwargs: Additional arguments passed to the decompressor.

    Returns:
        The decompressed bytes.

    Raises:
        ImportError: If required library is not installed.
    """
    match method:
        case CompressionMethod.gzip:
            return gzip.decompress(data)
        case CompressionMethod.bz2:
            return bz2.decompress(data)
        case CompressionMethod.lzma:
            return lzma.decompress(data)
        case CompressionMethod.zlib:
            return zlib.decompress(data)
        case CompressionMethod.zstd:
            try:
                import zstandard
            except ImportError:
                raise ImportError(
                    "zstandard is required for CompressionMethod.zstd. "
                    "Install it with: pip install zstandard"
                )
            dctx = zstandard.ZstdDecompressor()
            return dctx.decompress(data)
        case CompressionMethod.lz4:
            try:
                import lz4.frame
            except ImportError:
                raise ImportError(
                    "lz4 is required for CompressionMethod.lz4. "
                    "Install it with: pip install lz4"
                )
            return lz4.frame.decompress(data)
        case _:
            raise ValueError(f"Unknown compression method: {method}")

# %%
#|export
def validate_compression_imports(method: CompressionMethod) -> None:
    """Eagerly validate that required libraries are installed for a compression method.

    Args:
        method: The compression method to validate.

    Raises:
        ImportError: If required library is not installed, with install instructions.
    """
    match method:
        case CompressionMethod.gzip | CompressionMethod.bz2 | CompressionMethod.lzma | CompressionMethod.zlib:
            pass  # stdlib, always available
        case CompressionMethod.zstd:
            import zstandard  # noqa: F401
        case CompressionMethod.lz4:
            import lz4.frame  # noqa: F401

# %% [markdown]
# ## Quick validation

# %%
# Test round-trip for all stdlib methods
test_data = b"Hello, world! " * 100

for method in [CompressionMethod.gzip, CompressionMethod.bz2, CompressionMethod.lzma, CompressionMethod.zlib]:
    compressed = compress(test_data, method)
    decompressed = decompress(compressed, method)
    assert decompressed == test_data
    ratio = len(compressed) / len(test_data)
    print(f"{method.value}: {len(test_data)} -> {len(compressed)} bytes ({ratio:.1%})")

# Test extensions
assert get_compression_extension(CompressionMethod.gzip) == ".gz"
assert get_compression_extension(CompressionMethod.zstd) == ".zst"

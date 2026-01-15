# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %%
#|default_exp _iutils.hashing

# %%
#|hide
from nblite import nbl_export, show_doc

nbl_export()
import netrun._iutils.hashing as this_module

# %%
#|export
from typing import Any

import pickle
import pickletools
import zlib
import binascii
import hashlib
import struct
import xxhash
from enum import Enum

# %%
class MyObj:
    pass

obj = MyObj()
obj.foo = "bar"

# %%
#|hide
show_doc(this_module._to_bytes)

# %%
#|exporti
def _to_bytes(data: Any, pickle_protocol: int):
    """
    Converts data to bytes for hashing.
    """
    type_data = type(data)

    if type_data is bytes:
        return data
    elif type_data is str:
        return data.encode("utf-8")
    elif type_data is int:
        return data.to_bytes((data.bit_length() + 8) // 8, byteorder="big", signed=True)
    elif type_data is float:
        return struct.pack("!d", data)
    else:
        _data = pickle.dumps(data, protocol=pickle_protocol)
        return pickletools.optimize(_data)

# %%
#|hide
show_doc(this_module.adler32)

# %%
#|export
def adler32(data: Any, pickle_protocol: int) -> int:
    """
    Compute portable hash for given data.
    """
    mask = 0xFFFFFFFF
    _bdata = _to_bytes(data, pickle_protocol=pickle_protocol)
    return zlib.adler32(_bdata) & mask

# %%
#|hide
show_doc(this_module.crc32)

# %%
#|export
def crc32(data: Any, pickle_protocol: int) -> int:
    """
    Compute portable hash using CRC32.
    """
    mask = 0xFFFFFFFF
    _bdata = _to_bytes(data, pickle_protocol=pickle_protocol)
    return binascii.crc32(_bdata) & mask

# %%
#|hide
show_doc(this_module.sha256)

# %%
#|export
def sha256(data: Any, pickle_protocol: int) -> int:
    """
    Compute hash using SHA-256.
    """
    _bdata = _to_bytes(data, pickle_protocol=pickle_protocol)
    return int.from_bytes(hashlib.sha256(_bdata).digest(), byteorder="big")

# %%
#|hide
show_doc(this_module.blake2b)

# %%
#|export
def blake2b(data: Any, pickle_protocol: int) -> int:
    """
    Compute hash using BLAKE2b.
    """
    _bdata = _to_bytes(data, pickle_protocol=pickle_protocol)
    return int.from_bytes(hashlib.blake2b(_bdata).digest(), byteorder="big")

# %%
#|hide
show_doc(this_module.xxh64)

# %%
#|export
def xxh64(data: Any, pickle_protocol: int) -> int:
    """
    Compute hash using xxHash (64-bit).
    """
    _bdata = _to_bytes(data, pickle_protocol=pickle_protocol)
    return xxhash.xxh64(_bdata).intdigest()

# %%
#|hide
show_doc(this_module.hash)

# %%
#|export
class HashMethod(Enum):
    adler32 = "adler32"
    crc32 = "crc32"
    sha256 = "sha256"
    blake2b = "blake2b"
    xxh64 = "xxh64"

def hash(data: Any, method: HashMethod, pickle_protocol: int) -> int:
    if method == HashMethod.adler32:
        return adler32(data, pickle_protocol=pickle_protocol)
    elif method == HashMethod.crc32:
        return crc32(data, pickle_protocol=pickle_protocol)
    elif method == HashMethod.sha256:
        return sha256(data, pickle_protocol=pickle_protocol)
    elif method == HashMethod.blake2b:
        return blake2b(data, pickle_protocol=pickle_protocol)
    elif method == HashMethod.xxh64:
        return xxh64(data, pickle_protocol=pickle_protocol)
    else:
        raise ValueError(f"Invalid hash method: {method}")

# %%
pickle_protocol = 4

for method in HashMethod:
    print(f"{method.value}: {hash(obj, method, pickle_protocol=pickle_protocol)}")

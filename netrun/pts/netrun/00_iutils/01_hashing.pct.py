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
import json
from enum import Enum

# %%
#|hide
show_doc(this_module._preprocess_data)

# %%
#|exporti
def _preprocess_data(data: Any, pickle_protocol: int, try_json_dump: bool):
    """
    Preprocesses and converts the data to bytes for hashing.
    """
    if try_json_dump:
        try:
            data = json.dumps(data).encode("utf-8")
        except TypeError:
            pass

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
def adler32(bdata: bytes) -> int:
    """
    Compute portable hash for given data.
    """
    mask = 0xFFFFFFFF
    return zlib.adler32(bdata) & mask

# %%
#|hide
show_doc(this_module.crc32)

# %%
#|export
def crc32(bdata: bytes) -> int:
    """
    Compute portable hash using CRC32.
    """
    mask = 0xFFFFFFFF
    return binascii.crc32(bdata) & mask

# %%
#|hide
show_doc(this_module.sha256)

# %%
#|export
def sha256(bdata: bytes) -> int:
    """
    Compute hash using SHA-256.
    """
    return int.from_bytes(hashlib.sha256(bdata).digest(), byteorder="big")

# %%
#|hide
show_doc(this_module.blake2b)

# %%
#|export
def blake2b(bdata: bytes) -> int:
    """
    Compute hash using BLAKE2b.
    """
    return int.from_bytes(hashlib.blake2b(bdata).digest(), byteorder="big")

# %%
#|hide
show_doc(this_module.xxh64)

# %%
#|export
def xxh64(bdata: bytes) -> int:
    """
    Compute hash using xxHash (64-bit).
    """
    return xxhash.xxh64(bdata).intdigest()

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

def hash(data: Any, method: HashMethod, pickle_protocol: int, try_json_dump: bool) -> int:
    bdata = _preprocess_data(data, pickle_protocol=pickle_protocol, try_json_dump=try_json_dump)
    if method == HashMethod.adler32:
        return adler32(bdata)
    elif method == HashMethod.crc32:
        return crc32(bdata)
    elif method == HashMethod.sha256:
        return sha256(bdata)
    elif method == HashMethod.blake2b:
        return blake2b(bdata)
    elif method == HashMethod.xxh64:
        return xxh64(bdata)
    else:
        raise ValueError(f"Invalid hash method: {method}")

# %% [markdown]
# Try out the hashes

# %%
def hash_test(data, pickle_protocol, try_json_dump):
    return {
        method.value: hash(data, method, pickle_protocol=pickle_protocol, try_json_dump=try_json_dump)
        for method in HashMethod
    }

# %% [markdown]
# Hash a non-serializable Python object

# %%
pickle_protocol = 4
try_json_dump = False

class MyObj:
    pass
data = MyObj()
data.foo = "bar"

no_try_json_hashes = hash_test(data, pickle_protocol, try_json_dump)
for method, hash_value in no_try_json_hashes.items():
    print(f"{method}: {hash_value}")

# %% [markdown]
# Hashing it using `try_json_dump == True` should not make a difference

# %%
try_json_dump = True

try_json_hashes = hash_test(data, pickle_protocol, try_json_dump)
for hash_value1, hash_value2 in zip(no_try_json_hashes.values(), try_json_hashes.values()):
    assert hash_value1 == hash_value2

# %% [markdown]
# Hash a JSON-serializable value

# %%
pickle_protocol = 4
try_json_dump = False

data = {
    "foo": "bar",
}

no_try_json_hashes = hash_test(data, pickle_protocol, try_json_dump)
for method, hash_value in no_try_json_hashes.items():
    print(f"{method}: {hash_value}")

# %% [markdown]
# Now `try_json_dump == True` should yield different hashes

# %%
try_json_dump = True

try_json_hashes = hash_test(data, pickle_protocol, try_json_dump)
for hash_value1, hash_value2 in zip(no_try_json_hashes.values(), try_json_hashes.values()):
    assert hash_value1 != hash_value2
